package org.kiwiproject.postgres.leader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.junit.jupiter.api.Assertions.assertAll;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("ParticipantIdentity")
class ParticipantIdentityTest {

    private static final String LONG_ID =
            "analysis-controller-service_development/1.2.3-SNAPSHOT/ip-10-0-1-23.ec2.internal:8080";

    @Test
    void shouldRoundTripThroughAComment() {
        var query = ParticipantIdentity.comment(LONG_ID) + "\nSELECT 1";

        assertThat(ParticipantIdentity.parse(query)).contains(LONG_ID);
    }

    @Test
    void shouldReturnEmptyWhenNoComment() {
        assertAll(
                () -> assertThat(ParticipantIdentity.parse("SELECT 1")).isEmpty(),
                () -> assertThat(ParticipantIdentity.parse("<insufficient privilege>")).isEmpty(),
                () -> assertThat(ParticipantIdentity.parse(null)).isEmpty()
        );
    }

    @Test
    void shouldAcceptIdsWithColonsSlashesAndQuestionMarks() {
        assertThatCode(() -> ParticipantIdentity.validate("svc/1.0/host:8080?x")).doesNotThrowAnyException();
    }

    @Test
    void shouldRejectIdsThatWouldBreakTheComment() {
        assertAll(
                () -> assertThatIllegalArgumentException()
                        .isThrownBy(() -> ParticipantIdentity.validate("a*/b"))
                        .withMessage("participantId must not contain \"/*\" or \"*/\""),
                () -> assertThatIllegalArgumentException()
                        .isThrownBy(() -> ParticipantIdentity.validate("a/*b"))
                        .withMessage("participantId must not contain \"/*\" or \"*/\""),
                () -> assertThatIllegalArgumentException()
                        .isThrownBy(() -> ParticipantIdentity.validate("a\tb"))
                        .withMessage("participantId must not contain control characters"),
                () -> assertThatIllegalArgumentException()
                        .isThrownBy(() -> ParticipantIdentity.validate(" "))
                        .withMessage("participantId must not be blank")
        );
    }

    @Test
    void shouldTruncateApplicationNameToSixtyThreeBytes() {
        var applicationName = ParticipantIdentity.applicationName(LONG_ID);

        assertAll(
                () -> assertThat(applicationName).hasSize(63),
                () -> assertThat(LONG_ID).startsWith(applicationName)
        );
    }

    @Test
    void shouldReplaceNonAsciiInApplicationName() {
        assertThat(ParticipantIdentity.applicationName("café/1.0")).isEqualTo("caf?/1.0");
    }

    @Test
    void shouldLeaveShortAsciiApplicationNameUnchanged() {
        assertThat(ParticipantIdentity.applicationName("svc/1.0/host:8080")).isEqualTo("svc/1.0/host:8080");
    }
}
