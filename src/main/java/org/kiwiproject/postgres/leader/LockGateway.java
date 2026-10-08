package org.kiwiproject.postgres.leader;

import java.util.Optional;

/**
 * Small package-private seam over the Postgres connection so the latch's state machine can be
 * unit tested without a database.
 */
interface LockGateway extends AutoCloseable {

    /**
     * Try to acquire the lock without waiting for leadership.
     *
     * @return the lease if acquired, or empty if the lock is held by someone else
     * @throws RuntimeException     for any connection, SQL, or "not a primary" error
     */
    Optional<Lease> tryAcquire();

    /**
     * Read the participant ID of the session currently holding the lock.
     *
     * @return the holder, or empty if no session holds the lock
     * @throws RuntimeException for any connection or SQL error, or if the holder's identity cannot be read
     */
    Optional<String> currentOwner();

    @Override
    void close();

    /**
     * A held lock.
     */
    interface Lease {

        /**
         * Whether the lock is currently believed to be held. This is a cheap local check that never blocks
         * and never throws; it is only updated by {@link #validate()}.
         *
         * @return true only if the lock is believed held
         */
        boolean isHeld();

        /**
         * Prove the lock is still held, performing I/O. Updates the result of {@link #isHeld()}. Never throws.
         */
        void validate();

        /**
         * Release the lock and the connection.
         *
         * @throws RuntimeException if the release fails
         */
        void release();
    }
}
