package org.kiwiproject.postgres.leader;

import static org.kiwiproject.base.KiwiPreconditions.checkArgumentNotBlank;
import static org.kiwiproject.base.KiwiPreconditions.checkArgumentNotNull;

import java.time.Instant;

/**
 * The outcome of asking which participant is the leader, as recorded in Postgres.
 * <p>
 * This reflects the advisory lock currently held in the database and the identity of the session holding it.
 * Postgres releases the lock when the holder's session ends, but a leader that has lost its connection
 * without the server noticing can still be named for a short time. Combine it with each participant's own
 * {@link LeaderLatch#hasLeadership()} to detect disagreement or more than one self-reported leader.
 */
public sealed interface LeaderInfo {

    /**
     * Postgres shows the given participant as holding the leadership lock.
     *
     * @param participantId the ID of the participant holding the lock
     * @param observedAt    when the lock was read
     */
    record Leader(String participantId, Instant observedAt) implements LeaderInfo {
        /**
         * Validates the participant ID and observation time.
         *
         * @throws IllegalArgumentException if the participant ID is blank or the time is null
         */
        public Leader {
            checkArgumentNotBlank(participantId, "participantId must not be blank");
            checkArgumentNotNull(observedAt, "observedAt must not be null");
        }
    }

    /**
     * No session currently holds the lock, so no participant is the leader.
     */
    record NoLeader() implements LeaderInfo {}

    /**
     * The leader could not be determined.
     *
     * @param cause the error that occurred looking up the lock holder
     */
    record LookupFailed(Throwable cause) implements LeaderInfo {
        /**
         * Validates the cause.
         *
         * @throws IllegalArgumentException if the cause is null
         */
        public LookupFailed {
            checkArgumentNotNull(cause, "cause must not be null");
        }
    }
}
