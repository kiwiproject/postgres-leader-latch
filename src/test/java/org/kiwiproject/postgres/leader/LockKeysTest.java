package org.kiwiproject.postgres.leader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("LockKeys")
class LockKeysTest {

    @Test
    void shouldBeStableForTheSameKey() {
        assertThat(LockKeys.fromLeadershipKey("order-service"))
                .isEqualTo(LockKeys.fromLeadershipKey("order-service"));
    }

    @Test
    void shouldDifferForDifferentKeys() {
        assertThat(LockKeys.fromLeadershipKey("order-service"))
                .isNotEqualTo(LockKeys.fromLeadershipKey("order-service_development"));
    }

    @Test
    void shouldUseFirstEightBytesOfSha256AsSignedLong() {
        // SHA-256("abc") = ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad
        assertThat(LockKeys.fromLeadershipKey("abc")).isEqualTo(0xba7816bf8f01cfeaL);
    }

    @Test
    void shouldRejectBlankKey() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> LockKeys.fromLeadershipKey(" "))
                .withMessage("leadershipKey must not be blank");
    }
}
