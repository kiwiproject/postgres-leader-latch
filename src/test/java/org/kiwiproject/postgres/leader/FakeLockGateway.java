package org.kiwiproject.postgres.leader;

import static java.util.Objects.nonNull;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Controllable {@link LockGateway} for unit tests.
 */
class FakeLockGateway implements LockGateway {

    final AtomicBoolean lockAvailable = new AtomicBoolean(true);
    final AtomicBoolean failAcquisition = new AtomicBoolean();
    final AtomicBoolean failOwnerLookup = new AtomicBoolean();
    final AtomicBoolean failValidate = new AtomicBoolean();
    final AtomicBoolean loseOnValidate = new AtomicBoolean();
    final AtomicInteger validateCalls = new AtomicInteger();
    final AtomicBoolean leaseHeld = new AtomicBoolean(true);
    final AtomicInteger releaseCount = new AtomicInteger();
    final AtomicInteger acquireAttempts = new AtomicInteger();
    final AtomicBoolean closed = new AtomicBoolean();
    volatile CountDownLatch releaseGate;
    volatile String owner;

    @Override
    public Optional<Lease> tryAcquire() {
        acquireAttempts.incrementAndGet();

        if (failAcquisition.get()) {
            throw new IllegalStateException("simulated Postgres error");
        }

        if (!lockAvailable.get()) {
            return Optional.empty();
        }

        leaseHeld.set(true);
        return Optional.of(new Lease() {
            @Override
            public boolean isHeld() {
                return leaseHeld.get();
            }

            @Override
            public void validate() {
                validateCalls.incrementAndGet();
                if (failValidate.get()) {
                    throw new IllegalStateException("simulated validation error");
                }
                if (loseOnValidate.get()) {
                    leaseHeld.set(false);
                }
            }

            @Override
            public void release() {
                var gate = releaseGate;
                if (nonNull(gate)) {
                    awaitGate(gate);
                }
                releaseCount.incrementAndGet();
            }
        });
    }

    private static void awaitGate(CountDownLatch gate) {
        try {
            gate.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public Optional<String> currentOwner() {
        if (failOwnerLookup.get()) {
            throw new IllegalStateException("simulated lookup error");
        }
        return Optional.ofNullable(owner);
    }

    @Override
    public void close() {
        closed.set(true);
    }
}
