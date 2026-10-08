package org.kiwiproject.postgres.leader;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.kiwiproject.base.KiwiPreconditions.requireNotBlank;
import static org.kiwiproject.base.KiwiPreconditions.requireNotNull;
import static org.kiwiproject.base.KiwiStrings.f;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;
import org.kiwiproject.postgres.leader.LockGateway.Lease;

import java.sql.Connection;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * A {@link LeaderLatch} that uses a Postgres session-level advisory lock.
 * <p>
 * All participants with the same leadership key contend for one advisory lock. The leader holds the lock on
 * a dedicated database connection and periodically validates that the connection is alive, the server is a
 * primary, and the lock is still held. If validation fails repeatedly, this latch stops reporting leadership
 * and resumes trying to acquire the lock.
 * <p>
 * The connection is created from the {@link Supplier} given by the caller, but it is owned by this latch:
 * it is dedicated to the latch (never taken from or returned to a pool) and is closed when the latch is closed.
 * Connect to the writer (primary) endpoint directly, not through a connection pooler in transaction mode.
 * Configure the connection (SSL, connect and socket timeouts, TCP keepalive) when creating it.
 */
@Slf4j
@ToString(onlyExplicitlyIncluded = true)
public class PostgresLeaderLatch implements LeaderLatch {

    private enum State { NEW, STARTED, CLOSED }

    private static final long DEFAULT_CLOSE_TIMEOUT_MILLIS = 5_000;

    @Getter(onMethod_ = @Override)
    @ToString.Include
    private final String id;

    @Getter(onMethod_ = @Override)
    @ToString.Include
    private final String leadershipKey;

    @Getter(onMethod_ = @Override)
    @ToString.Include
    private final long lockKey;

    private final LeaderLatchConfiguration configuration;
    private final Supplier<LockGateway> gatewayFactory;
    private final List<LeaderLatchListener> listeners = new CopyOnWriteArrayList<>();
    private final Object stateLock = new Object();
    private final AtomicInteger acquisitionAttempts = new AtomicInteger();

    @ToString.Include
    private volatile State state = State.NEW;
    private final AtomicReference<@Nullable LockGateway> gateway = new AtomicReference<>();
    private final AtomicReference<@Nullable ScheduledExecutorService> executor = new AtomicReference<>();
    private final AtomicReference<@Nullable Lease> lease = new AtomicReference<>();
    private final AtomicReference<@Nullable Throwable> lastAcquisitionError = new AtomicReference<>();

    // only accessed on the latch executor thread
    private boolean acquisitionErrorLogged;

    private volatile long closeTimeoutMillis = DEFAULT_CLOSE_TIMEOUT_MILLIS;
    private final AtomicReference<@Nullable Thread> latchThread = new AtomicReference<>();

    /**
     * Create a latch.
     *
     * @param connectionSupplier creates a new connection to the primary; called when the latch starts and
     *                           whenever a replacement connection is needed. The latch closes every
     *                           connection it obtains.
     * @param configuration      the latch configuration
     * @param leadershipKey      the key shared by all participants of the same election, e.g. "order-service"
     * @param participantId      the unique ID of this participant, e.g. from {@link #leaderLatchId}; it is
     *                           carried in SQL comments, so it must not contain {@code /}{@code *}, {@code *}{@code /}, or
     *                           control characters
     * @throws IllegalArgumentException if any argument is null or blank, or the participant ID is not valid
     */
    public PostgresLeaderLatch(Supplier<Connection> connectionSupplier,
                               LeaderLatchConfiguration configuration,
                               String leadershipKey,
                               String participantId) {
        this(configuration, leadershipKey, participantId,
                gatewaySupplier(connectionSupplier, configuration, leadershipKey, participantId));
    }

    @VisibleForTesting
    PostgresLeaderLatch(LeaderLatchConfiguration configuration,
                        String leadershipKey,
                        String participantId,
                        Supplier<LockGateway> gatewayFactory) {
        this.configuration = requireNotNull(configuration, "configuration must not be null");
        this.leadershipKey = requireNotBlank(leadershipKey, "leadershipKey must not be blank");
        this.id = requireNotBlank(participantId, "participantId must not be blank");
        ParticipantIdentity.validate(participantId);
        this.lockKey = resolveLockKey(configuration, leadershipKey);
        this.gatewayFactory = requireNotNull(gatewayFactory, "gatewayFactory must not be null");
    }

    private static Supplier<LockGateway> gatewaySupplier(Supplier<Connection> connectionSupplier,
                                                         LeaderLatchConfiguration configuration,
                                                         String leadershipKey,
                                                         String participantId) {
        // configuration, leadershipKey and participantId are validated by the constructor
        requireNotNull(connectionSupplier, "connectionSupplier must not be null");
        requireNotNull(configuration, "configuration must not be null");
        requireNotBlank(leadershipKey, "leadershipKey must not be blank");
        var lockKey = resolveLockKey(configuration, leadershipKey);
        return () -> new PgLockGateway(connectionSupplier, configuration, leadershipKey, lockKey, participantId);
    }

    private static long resolveLockKey(LeaderLatchConfiguration configuration, String leadershipKey) {
        var override = configuration.lockKeyOverride();
        return nonNull(override) ? override : LockKeys.fromLeadershipKey(leadershipKey);
    }

    /**
     * Generate a standard participant ID, in the same format used by dropwizard-leader-latch.
     *
     * @param serviceName    the name of the service
     * @param serviceVersion the version of the service
     * @param hostname       the host name where the service instance is running
     * @param port           the port on which the service instance is running
     * @return a participant ID
     */
    public static String leaderLatchId(String serviceName,
                                       String serviceVersion,
                                       String hostname,
                                       int port) {
        return f("{}/{}/{}:{}", serviceName, serviceVersion, hostname, port);
    }

    @VisibleForTesting
    int acquisitionAttemptCount() {
        return acquisitionAttempts.get();
    }

    @VisibleForTesting
    void setCloseTimeoutMillis(long closeTimeoutMillis) {
        this.closeTimeoutMillis = closeTimeoutMillis;
    }

    @Override
    public StartResult start() {
        synchronized (stateLock) {
            if (state == State.CLOSED) {
                return new StartResult.Closed();
            }
            if (state == State.STARTED) {
                LOG.trace("start() already called for leader latch {} (key {}); ignoring", id, leadershipKey);
                return new StartResult.AlreadyStarted();
            }

            LockGateway newGateway = null;
            ScheduledExecutorService newExecutor = null;
            try {
                LOG.info("Starting leader latch {} for key {}", id, leadershipKey);
                newGateway = gatewayFactory.get();
                newExecutor = newExecutor();

                var retryMillis = configuration.acquisitionRetryInterval().toMillis();
                var validationMillis = configuration.validationInterval().toMillis();

                gateway.set(newGateway);
                executor.set(newExecutor);
                state = State.STARTED;

                newExecutor.scheduleWithFixedDelay(() -> runSafely("acquisition", this::acquisitionTick),
                        0, retryMillis, TimeUnit.MILLISECONDS);
                newExecutor.scheduleWithFixedDelay(() -> runSafely("lock validation", this::validationTick),
                        validationMillis, validationMillis, TimeUnit.MILLISECONDS);

                return new StartResult.Started();
            } catch (Exception e) {
                LOG.error("Unable to start leader latch {} for key {}", id, leadershipKey, e);
                state = State.NEW;
                gateway.set(null);
                executor.set(null);
                if (nonNull(newExecutor)) {
                    newExecutor.shutdownNow();
                }
                if (nonNull(newGateway)) {
                    newGateway.close();
                }
                return new StartResult.Failed(e);
            }
        }
    }

    private ScheduledExecutorService newExecutor() {
        return Executors.newSingleThreadScheduledExecutor(runnable -> {
            var thread = new Thread(runnable, "postgres-leader-latch-" + leadershipKey);
            thread.setDaemon(true);
            latchThread.set(thread);
            return thread;
        });
    }

    private void runSafely(String taskName, Runnable task) {
        try {
            task.run();
        } catch (Exception e) {
            // never let an exception cancel the periodic task
            LOG.error("Unexpected error in {} for leader latch {} (key {})", taskName, id, leadershipKey, e);
        }
    }

    private void acquisitionTick() {
        var currentGateway = gateway.get();
        if (state != State.STARTED || isNull(currentGateway) || nonNull(lease.get())) {
            return;
        }

        try {
            acquisitionAttempts.incrementAndGet();
            var acquired = currentGateway.tryAcquire();
            lastAcquisitionError.set(null);
            acquisitionErrorLogged = false;

            if (acquired.isPresent()) {
                becomeLeader(acquired.get());
            } else {
                LOG.trace("Leadership for key {} is held by another participant", leadershipKey);
            }
        } catch (Exception e) {
            lastAcquisitionError.set(e);
            if (acquisitionErrorLogged) {
                LOG.debug("Error trying to acquire leadership for key {}", leadershipKey, e);
            } else {
                acquisitionErrorLogged = true;
                LOG.warn("Error trying to acquire leadership for key {}. Will keep retrying.", leadershipKey, e);
            }
        }
    }

    private void validationTick() {
        var current = lease.get();
        if (isNull(current)) {
            return;
        }

        current.validate();
        if (!current.isHeld()) {
            becomeFollower("the lock can no longer be proven to be held");
        }
    }

    private void becomeLeader(Lease newLease) {
        boolean accepted;
        synchronized (stateLock) {
            accepted = state == State.STARTED && isNull(lease.get());
            if (accepted) {
                lease.set(newLease);
            }
        }

        if (!accepted) {
            // release outside the lock; it is a network call
            releaseQuietly(newLease);
            return;
        }

        LOG.info("Leadership acquired for key {} by {}", leadershipKey, id);
        notifyListeners(true);
    }

    private void becomeFollower(String reason) {
        Lease old;
        synchronized (stateLock) {
            old = lease.getAndSet(null);
        }

        if (isNull(old)) {
            return;
        }

        LOG.warn("Leadership lost for key {} by {}: {}", leadershipKey, id, reason);

        // Tell listeners first so leader-only work stops promptly; releasing is a network call that can be slow
        notifyListeners(false);

        // Always release, which also closes the connection so the server drops any lock still held
        releaseQuietly(old);
    }

    private void releaseQuietly(Lease toRelease) {
        try {
            toRelease.release();
        } catch (Exception e) {
            LOG.warn("Unable to release lock for key {}; the server releases it when the session ends",
                    leadershipKey, e);
        }
    }

    private void notifyListeners(boolean leader) {
        for (var listener : listeners) {
            try {
                if (leader) {
                    listener.isLeader();
                } else {
                    listener.notLeader();
                }
            } catch (Exception e) {
                LOG.error("Leader latch listener {} threw an exception for key {}", listener, leadershipKey, e);
            }
        }
    }

    @Override
    public boolean hasLeadership() {
        var current = lease.get();
        return state == State.STARTED && nonNull(current) && current.isHeld();
    }

    @Override
    public LeadershipStatus checkLeadershipStatus() {
        switch (state) {
            case NEW:
                return new LeadershipStatus.NotStarted();
            case CLOSED:
                return new LeadershipStatus.Closed();
            default:
                break;
        }

        var current = lease.get();
        if (nonNull(current)) {
            return current.isHeld() ? new LeadershipStatus.IsLeader() : new LeadershipStatus.NotLeader();
        }

        var error = lastAcquisitionError.get();
        return nonNull(error) ? new LeadershipStatus.Uncertain(error) : new LeadershipStatus.NotLeader();
    }

    @Override
    public LeaderInfo getLeader() {
        var currentGateway = gateway.get();
        if (state != State.STARTED || isNull(currentGateway)) {
            return new LeaderInfo.LookupFailed(new IllegalStateException("leader latch is not started"));
        }

        try {
            return currentGateway.currentOwner()
                    .<LeaderInfo>map(owner -> new LeaderInfo.Leader(owner, Instant.now()))
                    .orElseGet(LeaderInfo.NoLeader::new);
        } catch (Exception e) {
            return new LeaderInfo.LookupFailed(e);
        }
    }

    @Override
    public void addListener(LeaderLatchListener listener) {
        listeners.add(requireNotNull(listener, "listener must not be null"));
    }

    @Override
    public void close() {
        Lease held;
        ScheduledExecutorService currentExecutor;
        LockGateway currentGateway;

        synchronized (stateLock) {
            if (state == State.CLOSED) {
                return;
            }
            state = State.CLOSED;
            held = lease.getAndSet(null);
            currentExecutor = executor.get();
            currentGateway = gateway.get();
        }

        LOG.info("Stopping leader latch {} for key {}", id, leadershipKey);

        var deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(closeTimeoutMillis);

        if (nonNull(currentExecutor)) {
            stopExecutor(currentExecutor, nonNull(held), deadlineNanos);
        }
        if (nonNull(held)) {
            LOG.info("Leadership lost for key {} by {}: latch closed", leadershipKey, id);
        }

        var gatewayToClose = currentGateway;
        runBounded("release lock and close connection", () -> {
            if (nonNull(held)) {
                releaseQuietly(held);
            }
            if (nonNull(gatewayToClose)) {
                gatewayToClose.close();
            }
        }, deadlineNanos);
    }

    private void stopExecutor(ScheduledExecutorService toStop, boolean notifyNotLeader, long deadlineNanos) {
        try {
            if (notifyNotLeader) {
                // runs on the latch thread so it is ordered after any in-flight isLeader notification
                toStop.execute(() -> notifyListeners(false));
            }
        } catch (RejectedExecutionException e) {
            LOG.trace("Latch executor already shut down", e);
        }

        // Tasks that were already queued (such as the notification above) still run after shutdown()
        toStop.shutdown();

        if (Thread.currentThread() == latchThread.get()) {
            // close() was called from a listener on the latch thread; waiting here would only wait for ourselves
            return;
        }

        try {
            if (!toStop.awaitTermination(remainingMillis(deadlineNanos), TimeUnit.MILLISECONDS)) {
                LOG.warn("Latch executor for key {} did not stop within {} ms; forcing", leadershipKey, closeTimeoutMillis);
                toStop.shutdownNow();
            }
        } catch (InterruptedException e) {
            toStop.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private static long remainingMillis(long deadlineNanos) {
        return Math.max(1, TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime()));
    }

    private void runBounded(String description, Runnable task, long deadlineNanos) {
        var thread = new Thread(task, "postgres-leader-latch-close-" + leadershipKey);
        thread.setDaemon(true);
        thread.start();
        try {
            thread.join(remainingMillis(deadlineNanos));
            if (thread.isAlive()) {
                LOG.warn("Timed out after {} ms trying to {} for key {}", closeTimeoutMillis, description, leadershipKey);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
