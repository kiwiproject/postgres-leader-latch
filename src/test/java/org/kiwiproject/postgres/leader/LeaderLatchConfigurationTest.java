package org.kiwiproject.postgres.leader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.junit.jupiter.api.Assertions.assertAll;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

@DisplayName("LeaderLatchConfiguration")
class LeaderLatchConfigurationTest {

    @Test
    void shouldHaveDefaults() {
        var config = LeaderLatchConfiguration.defaults();

        assertAll(
                () -> assertThat(config.validationInterval()).isEqualTo(Duration.ofSeconds(5)),
                () -> assertThat(config.validationTimeout()).isEqualTo(Duration.ofSeconds(3)),
                () -> assertThat(config.maxConsecutiveValidationFailures()).isEqualTo(3),
                () -> assertThat(config.acquisitionRetryInterval()).isEqualTo(Duration.ofSeconds(5)),
                () -> assertThat(config.lockKeyOverride()).isNull()
        );
    }

    @Test
    void shouldRejectNonPositiveDurations() {
        var config = LeaderLatchConfiguration.defaults();

        assertAll(
                () -> assertThatIllegalArgumentException()
                        .isThrownBy(() -> config.withValidation(Duration.ZERO, Duration.ofMillis(10), 3))
                        .withMessage("validationInterval must be positive"),
                () -> assertThatIllegalArgumentException()
                        .isThrownBy(() -> config.withValidation(Duration.ofSeconds(5), Duration.ofSeconds(-1), 3))
                        .withMessage("validationTimeout must be positive"),
                () -> assertThatIllegalArgumentException()
                        .isThrownBy(() -> config.withAcquisitionRetryInterval(Duration.ZERO))
                        .withMessage("acquisitionRetryInterval must be positive")
        );
    }

    @Test
    void shouldRejectDurationsUnderOneMillisecond() {
        var config = LeaderLatchConfiguration.defaults();

        assertAll(
                () -> assertThatIllegalArgumentException()
                        .isThrownBy(() -> config.withAcquisitionRetryInterval(Duration.ofNanos(1)))
                        .withMessage("acquisitionRetryInterval must be at least 1 millisecond"),
                () -> assertThatIllegalArgumentException()
                        .isThrownBy(() -> config.withValidation(Duration.ofSeconds(5), Duration.ofNanos(500_000), 3))
                        .withMessage("validationTimeout must be at least 1 millisecond")
        );
    }

    @Test
    void shouldRejectNullDurations() {
        var config = LeaderLatchConfiguration.defaults();

        assertAll(
                () -> assertThatIllegalArgumentException()
                        .isThrownBy(() -> config.withValidation(null, Duration.ofSeconds(1), 3))
                        .withMessage("validationInterval must not be null"),
                () -> assertThatIllegalArgumentException()
                        .isThrownBy(() -> config.withValidation(Duration.ofSeconds(5), null, 3))
                        .withMessage("validationTimeout must not be null"),
                () -> assertThatIllegalArgumentException()
                        .isThrownBy(() -> config.withAcquisitionRetryInterval(null))
                        .withMessage("acquisitionRetryInterval must not be null")
        );
    }

    @Test
    void shouldRequireValidationTimeoutShorterThanInterval() {
        var config = LeaderLatchConfiguration.defaults();

        assertAll(
                () -> assertThatIllegalArgumentException()
                        .isThrownBy(() -> config.withValidation(Duration.ofSeconds(5), Duration.ofSeconds(5), 3))
                        .withMessage("validationTimeout (PT5S) must be shorter than validationInterval (PT5S)"),
                () -> assertThat(config.withValidation(Duration.ofSeconds(5), Duration.ofMillis(4999), 3)
                        .validationTimeout()).isEqualTo(Duration.ofMillis(4999))
        );
    }

    @Test
    void shouldRequireAtLeastOneAllowedFailure() {
        var config = LeaderLatchConfiguration.defaults();

        assertAll(
                () -> assertThatIllegalArgumentException()
                        .isThrownBy(() -> config.withValidation(Duration.ofSeconds(5), Duration.ofSeconds(3), 0))
                        .withMessage("maxConsecutiveValidationFailures must be at least 1 (was 0)"),
                () -> assertThat(config.withValidation(Duration.ofSeconds(5), Duration.ofSeconds(3), 1)
                        .maxConsecutiveValidationFailures()).isEqualTo(1)
        );
    }

    @Test
    void shouldKeepOtherSettingsWhenChangingOne() {
        var config = LeaderLatchConfiguration.defaults()
                .withLockKeyOverride(7L)
                .withAcquisitionRetryInterval(Duration.ofSeconds(9))
                .withValidation(Duration.ofSeconds(8), Duration.ofSeconds(2), 4);

        assertAll(
                () -> assertThat(config.lockKeyOverride()).isEqualTo(7L),
                () -> assertThat(config.acquisitionRetryInterval()).isEqualTo(Duration.ofSeconds(9)),
                () -> assertThat(config.validationInterval()).isEqualTo(Duration.ofSeconds(8)),
                () -> assertThat(config.validationTimeout()).isEqualTo(Duration.ofSeconds(2)),
                () -> assertThat(config.maxConsecutiveValidationFailures()).isEqualTo(4)
        );
    }
}
