package org.kiwiproject.postgres.leader;

import static com.google.common.base.Preconditions.checkArgument;
import static org.kiwiproject.base.KiwiPreconditions.checkArgumentNotNull;
import static org.kiwiproject.time.KiwiDurations.isPositive;

import org.jspecify.annotations.Nullable;

import java.time.Duration;

/**
 * Configuration for a {@link PostgresLeaderLatch}.
 * <p>
 * Postgres advisory locks have no lease: the lock is held until it is released or the database session ends.
 * So that a leader whose connection has silently died does not keep believing it leads, the leader
 * periodically validates its session. A validation proves the connection works, the server is a primary
 * (not a standby), and the lock is still held. If validation fails
 * {@code maxConsecutiveValidationFailures} times in a row the latch stops reporting leadership.
 * <p>
 * The worst case time a leader can wrongly believe it leads after losing its connection is about
 * {@code maxConsecutiveValidationFailures * (validationInterval + validationTimeout)}.
 * <p>
 * The defaults are initial values that have not been tuned.
 *
 * @param validationInterval               how often the leader validates its session
 * @param validationTimeout                the statement timeout for each validation (and for other statements);
 *                                         must be shorter than {@code validationInterval}
 * @param maxConsecutiveValidationFailures how many validations in a row may fail before the leader steps down
 * @param acquisitionRetryInterval         how often a follower tries to acquire the lock
 * @param lockKeyOverride                  an explicit 64-bit advisory lock key to use instead of the key derived
 *                                         from the leadership key, or null to derive it; this configuration is
 *                                         then only suitable for a single leadership key
 */
public record LeaderLatchConfiguration(Duration validationInterval,
                                       Duration validationTimeout,
                                       int maxConsecutiveValidationFailures,
                                       Duration acquisitionRetryInterval,
                                       @Nullable Long lockKeyOverride) {

    private static final Duration MIN_DURATION = Duration.ofMillis(1);

    /**
     * Default validation interval.
     */
    public static final Duration DEFAULT_VALIDATION_INTERVAL = Duration.ofSeconds(5);

    /**
     * Default validation timeout.
     */
    public static final Duration DEFAULT_VALIDATION_TIMEOUT = Duration.ofSeconds(3);

    /**
     * Default maximum number of consecutive validation failures.
     */
    public static final int DEFAULT_MAX_CONSECUTIVE_VALIDATION_FAILURES = 3;

    /**
     * Default acquisition retry interval.
     */
    public static final Duration DEFAULT_ACQUISITION_RETRY_INTERVAL = Duration.ofSeconds(5);

    /**
     * Validates the configuration.
     *
     * @throws IllegalArgumentException if any duration is missing or non-positive, the validation timeout is not
     *                                  shorter than the validation interval, or the maximum number of
     *                                  consecutive validation failures is less than one
     */
    public LeaderLatchConfiguration {
        checkPositive(validationInterval, "validationInterval");
        checkPositive(validationTimeout, "validationTimeout");
        checkPositive(acquisitionRetryInterval, "acquisitionRetryInterval");
        checkArgument(validationTimeout.compareTo(validationInterval) < 0,
                "validationTimeout (%s) must be shorter than validationInterval (%s)",
                validationTimeout, validationInterval);
        checkArgument(maxConsecutiveValidationFailures >= 1,
                "maxConsecutiveValidationFailures must be at least 1 (was %s)", maxConsecutiveValidationFailures);
    }

    private static void checkPositive(Duration duration, String name) {
        checkArgumentNotNull(duration, "{} must not be null", name);
        checkArgument(isPositive(duration), "%s must be positive", name);
        checkArgument(duration.compareTo(MIN_DURATION) >= 0, "%s must be at least 1 millisecond", name);
    }

    /**
     * Create a configuration with default timings, deriving the lock key from the leadership key.
     *
     * @return a new configuration
     */
    public static LeaderLatchConfiguration defaults() {
        return new LeaderLatchConfiguration(DEFAULT_VALIDATION_INTERVAL, DEFAULT_VALIDATION_TIMEOUT,
                DEFAULT_MAX_CONSECUTIVE_VALIDATION_FAILURES, DEFAULT_ACQUISITION_RETRY_INTERVAL, null);
    }

    /**
     * Create a copy with different validation settings.
     *
     * @param validationInterval               the new validation interval
     * @param validationTimeout                the new validation timeout
     * @param maxConsecutiveValidationFailures the new maximum number of consecutive failures
     * @return a new configuration
     */
    public LeaderLatchConfiguration withValidation(Duration validationInterval,
                                                   Duration validationTimeout,
                                                   int maxConsecutiveValidationFailures) {
        return new LeaderLatchConfiguration(validationInterval, validationTimeout,
                maxConsecutiveValidationFailures, acquisitionRetryInterval, lockKeyOverride);
    }

    /**
     * Create a copy with a different acquisition retry interval.
     *
     * @param acquisitionRetryInterval the new retry interval
     * @return a new configuration
     */
    public LeaderLatchConfiguration withAcquisitionRetryInterval(Duration acquisitionRetryInterval) {
        return new LeaderLatchConfiguration(validationInterval, validationTimeout,
                maxConsecutiveValidationFailures, acquisitionRetryInterval, lockKeyOverride);
    }

    /**
     * Create a copy that uses the given explicit advisory lock key instead of deriving one from the leadership key.
     *
     * @param lockKey the 64-bit advisory lock key
     * @return a new configuration
     */
    public LeaderLatchConfiguration withLockKeyOverride(long lockKey) {
        return new LeaderLatchConfiguration(validationInterval, validationTimeout,
                maxConsecutiveValidationFailures, acquisitionRetryInterval, lockKey);
    }
}
