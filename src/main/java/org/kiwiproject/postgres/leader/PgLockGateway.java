package org.kiwiproject.postgres.leader;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.kiwiproject.base.KiwiPreconditions.requireNotBlank;
import static org.kiwiproject.base.KiwiPreconditions.requireNotNull;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * {@link LockGateway} backed by a Postgres session-level advisory lock on one dedicated connection.
 * <p>
 * The connection is created by the caller-supplied {@link Supplier} and is owned by this gateway: it is
 * opened lazily, replaced after a failure, and closed on release and on {@link #close()}. It is never
 * shared with an application connection pool.
 * <p>
 * Every statement run on the connection starts with a comment carrying the participant ID (see
 * {@link ParticipantIdentity}), so that {@link #currentOwner()} can read the holder's identity from
 * {@code pg_stat_activity}. All access to the single connection is serialized by {@code connectionLock}.
 */
@Slf4j
final class PgLockGateway implements LockGateway {

    // pg_is_in_recovery() is true on a standby, where an advisory lock would not exclude the primary's holder
    private static final String ACQUIRE_SQL = """
            SELECT pg_is_in_recovery() AS in_recovery,
                   CASE WHEN pg_is_in_recovery() THEN false ELSE pg_try_advisory_lock(?) END AS acquired
            """;

    private static final String VALIDATE_SQL = """
            SELECT pg_is_in_recovery() AS in_recovery,
                   EXISTS (SELECT 1
                             FROM pg_locks
                            WHERE locktype = 'advisory'
                              AND granted
                              AND pid = pg_backend_pid()
                              AND objsubid = 1
                              AND ((classid::bigint << 32) | objid::bigint) = ?
                              AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
                   ) AS holds_lock
            """;

    private static final String LOOKUP_SQL = """
            SELECT a.query
              FROM pg_locks l
              JOIN pg_stat_activity a ON a.pid = l.pid
             WHERE l.locktype = 'advisory'
               AND l.granted
               AND l.objsubid = 1
               AND ((l.classid::bigint << 32) | l.objid::bigint) = ?
               AND l.database = (SELECT oid FROM pg_database WHERE datname = current_database())
            """;

    private static final String UNLOCK_SQL = "SELECT pg_advisory_unlock(?)";

    private static final int MAX_QUERY_TEXT_IN_MESSAGE = 200;

    // Extra time beyond the statement timeout for the server's response, e.g. to a cancel, to arrive
    private static final int NETWORK_TIMEOUT_MARGIN_MILLIS = 2_000;

    private final Supplier<Connection> connectionSupplier;
    private final String leadershipKey;
    private final long lockKey;
    private final int maxConsecutiveValidationFailures;
    private final int statementTimeoutSeconds;
    private final int networkTimeoutMillis;
    private final String identityComment;
    private final String applicationName;
    private final Object connectionLock = new Object();

    // guarded by connectionLock
    private Connection connection;

    PgLockGateway(Supplier<Connection> connectionSupplier,
                  LeaderLatchConfiguration configuration,
                  String leadershipKey,
                  long lockKey,
                  String participantId) {

        this.connectionSupplier = requireNotNull(connectionSupplier, "connectionSupplier must not be null");
        requireNotNull(configuration, "configuration must not be null");
        // The only non-constant text in any SQL this class runs is the comment built from the participant ID, so
        // enforce here that it cannot end or nest a comment, even though the latch has already checked it.
        ParticipantIdentity.validate(participantId);
        this.leadershipKey = requireNotBlank(leadershipKey, "leadershipKey must not be blank");
        this.lockKey = lockKey;
        this.maxConsecutiveValidationFailures = configuration.maxConsecutiveValidationFailures();
        this.statementTimeoutSeconds = (int) Math.max(1, (configuration.validationTimeout().toMillis() + 999) / 1000);
        this.networkTimeoutMillis = statementTimeoutSeconds * 1000 + NETWORK_TIMEOUT_MARGIN_MILLIS;
        this.identityComment = ParticipantIdentity.comment(participantId);
        this.applicationName = ParticipantIdentity.applicationName(participantId);
    }

    @Override
    public Optional<Lease> tryAcquire() {
        synchronized (connectionLock) {
            try (var statement = prepare(ACQUIRE_SQL)) {
                statement.setLong(1, lockKey);
                try (var resultSet = statement.executeQuery()) {
                    resultSet.next();
                    if (resultSet.getBoolean("in_recovery")) {
                        throw new LockGatewayException(
                                "Connected to a standby server (pg_is_in_recovery() is true); connect to the primary");
                    }
                    return resultSet.getBoolean("acquired")
                            ? Optional.of(new PgLease())
                            : Optional.empty();
                }
            } catch (SQLException e) {
                discardConnection();
                throw new LockGatewayException("Unable to acquire advisory lock for key " + leadershipKey, e);
            } catch (LockGatewayException e) {
                discardConnection();
                throw e;
            }
        }
    }

    @Override
    public Optional<String> currentOwner() {
        synchronized (connectionLock) {
            try (var statement = prepare(LOOKUP_SQL)) {
                statement.setLong(1, lockKey);
                try (var resultSet = statement.executeQuery()) {
                    if (!resultSet.next()) {
                        return Optional.empty();
                    }

                    var queryText = resultSet.getString(1);
                    return Optional.of(ParticipantIdentity.parse(queryText).orElseThrow(() ->
                            new LockGatewayException("The lock for key " + leadershipKey
                                    + " is held, but the holder's identity could not be read from pg_stat_activity."
                                    + " Query text: " + abbreviate(queryText))));
                }
            } catch (SQLException e) {
                // Do not discard the connection here. If this latch is the leader, closing its connection would
                // release the lock. A broken connection is detected by validation (leader) or the next
                // acquisition attempt (follower).
                throw new LockGatewayException("Unable to look up the lock holder for key " + leadershipKey, e);
            }
        }
    }

    @Override
    public void close() {
        synchronized (connectionLock) {
            discardConnection();
        }
    }

    // must be called while holding connectionLock
    private PreparedStatement prepare(String sql) throws SQLException {
        var statement = ensureConnection().prepareStatement(identityComment + " " + sql);
        try {
            statement.setQueryTimeout(statementTimeoutSeconds);
            return statement;
        } catch (SQLException e) {
            statement.close();
            throw e;
        }
    }

    // must be called while holding connectionLock
    private Connection ensureConnection() throws SQLException {
        if (nonNull(connection) && !connection.isClosed()) {
            return connection;
        }

        var newConnection = connectionSupplier.get();
        if (isNull(newConnection)) {
            throw new LockGatewayException("The connection supplier returned null");
        }

        try {
            newConnection.setAutoCommit(true);
            newConnection.setClientInfo("ApplicationName", applicationName);
            setNetworkTimeout(newConnection);
        } catch (SQLException e) {
            closeQuietly(newConnection);
            throw e;
        }

        connection = newConnection;
        return newConnection;
    }

    /**
     * Bound how long any read on the connection can block. Without this, and without a {@code socketTimeout}
     * set by the caller, a connection whose network silently drops (no reset, no close) blocks the validation
     * forever, so the leader would never stop believing it leads.
     */
    private void setNetworkTimeout(Connection newConnection) throws SQLException {
        try {
            // the driver performs the timeout itself; the executor is not needed
            newConnection.setNetworkTimeout(Runnable::run, networkTimeoutMillis);
        } catch (SQLFeatureNotSupportedException e) {
            LOG.warn("The JDBC driver does not support setNetworkTimeout. Set a socket timeout when creating the"
                    + " connection, or a leader on a silently dropped connection will not step down.", e);
        }
    }

    // must be called while holding connectionLock
    private void discardConnection() {
        var old = connection;
        connection = null;
        if (nonNull(old)) {
            closeQuietly(old);
        }
    }

    private void closeQuietly(Connection toClose) {
        try {
            toClose.close();
        } catch (SQLException e) {
            LOG.debug("Error closing connection for key {}", leadershipKey, e);
        }
    }

    private static String abbreviate(String text) {
        if (isNull(text)) {
            return "<null>";
        }
        return text.length() <= MAX_QUERY_TEXT_IN_MESSAGE ? text : text.substring(0, MAX_QUERY_TEXT_IN_MESSAGE) + "...";
    }

    private final class PgLease implements Lease {

        // read by any thread; written under connectionLock
        private volatile boolean held = true;

        // guarded by connectionLock
        private int consecutiveFailures;

        @Override
        public boolean isHeld() {
            return held;
        }

        @Override
        public void validate() {
            if (!held) {
                return;
            }

            synchronized (connectionLock) {
                try {
                    validateLocked();
                } catch (SQLException | RuntimeException e) {
                    ++consecutiveFailures;
                    if (isConnectionClosed()) {
                        // A closed connection (for example after a network timeout) cannot recover by waiting
                        lose("validation failed and the connection is now closed", e);
                    } else if (consecutiveFailures >= maxConsecutiveValidationFailures) {
                        lose("validation failed " + consecutiveFailures + " times in a row", e);
                    } else {
                        LOG.warn("Validation failed for key {} ({} of {} allowed); still treating the lock as held",
                                leadershipKey, consecutiveFailures, maxConsecutiveValidationFailures, e);
                    }
                }
            }
        }

        // must be called while holding connectionLock
        private void validateLocked() throws SQLException {
            if (isNull(connection) || connection.isClosed()) {
                lose("the connection is closed", null);
                return;
            }

            try (var statement = prepare(VALIDATE_SQL)) {
                statement.setLong(1, lockKey);
                try (var resultSet = statement.executeQuery()) {
                    resultSet.next();
                    if (resultSet.getBoolean("in_recovery")) {
                        lose("the server is no longer a primary", null);
                    } else if (!resultSet.getBoolean("holds_lock")) {
                        lose("this session no longer holds the advisory lock", null);
                    } else {
                        consecutiveFailures = 0;
                    }
                }
            }
        }

        // must be called while holding connectionLock
        private boolean isConnectionClosed() {
            try {
                return isNull(connection) || connection.isClosed();
            } catch (SQLException e) {
                return true;
            }
        }

        // must be called while holding connectionLock
        private void lose(String reason, Throwable cause) {
            held = false;
            if (nonNull(cause)) {
                LOG.warn("Lost the advisory lock for key {}: {}", leadershipKey, reason, cause);
            } else {
                LOG.warn("Lost the advisory lock for key {}: {}", leadershipKey, reason);
            }
        }

        @Override
        public void release() {
            held = false;
            synchronized (connectionLock) {
                try {
                    if (nonNull(connection) && !connection.isClosed()) {
                        unlock();
                    }
                } catch (SQLException e) {
                    throw new LockGatewayException("Unable to release advisory lock for key " + leadershipKey, e);
                } finally {
                    // closing the session also releases the lock, even if the explicit unlock failed
                    discardConnection();
                }
            }
        }

        // must be called while holding connectionLock
        private void unlock() throws SQLException {
            try (var statement = prepare(UNLOCK_SQL)) {
                statement.setLong(1, lockKey);
                try (var resultSet = statement.executeQuery()) {
                    resultSet.next();
                }
            }
        }
    }
}
