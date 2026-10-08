package org.kiwiproject.postgres.leader;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.isNull;
import static org.kiwiproject.base.KiwiPreconditions.checkArgumentNotBlank;

import lombok.experimental.UtilityClass;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Carries a participant ID in the text of every SQL statement a latch runs, as a leading comment, so that
 * other sessions can read it from {@code pg_stat_activity.query}.
 * <p>
 * {@code application_name} is limited to 63 bytes, which real participant IDs can easily exceed.
 * The query text of an idle session is the last statement it ran, so as long as every statement
 * on the latch's connection starts with this comment, the holder of the lock can be identified.
 * {@code application_name} is also set, truncated, as a convenience for people inspecting sessions.
 */
@UtilityClass
class ParticipantIdentity {

    static final String COMMENT_PREFIX = "/* kiwi-leader-latch: ";
    static final String COMMENT_SUFFIX = " */";

    // Postgres truncates application_name at NAMEDATALEN - 1 bytes
    static final int MAX_APPLICATION_NAME_BYTES = 63;

    private static final Pattern COMMENT_PATTERN =
            Pattern.compile(Pattern.quote(COMMENT_PREFIX) + "(.+?)" + Pattern.quote(COMMENT_SUFFIX));

    /**
     * Check a participant ID can be carried in a SQL comment.
     *
     * @param participantId the participant ID
     * @throws IllegalArgumentException if blank, contains {@code *}{@code /} (which would end the comment),
     *                                  contains {@code /}{@code *} (Postgres block comments nest, so this
     *                                  would leave the comment unterminated), or contains control characters
     */
    static void validate(String participantId) {
        checkArgumentNotBlank(participantId, "participantId must not be blank");
        checkArgument(!participantId.contains("*/") && !participantId.contains("/*"),
                "participantId must not contain \"/*\" or \"*/\"");
        checkArgument(participantId.chars().noneMatch(Character::isISOControl),
                "participantId must not contain control characters");
    }

    static String comment(String participantId) {
        return COMMENT_PREFIX + participantId + COMMENT_SUFFIX;
    }

    /**
     * Extract the participant ID from the query text of a session, if present.
     *
     * @param queryText the {@code pg_stat_activity.query} text
     * @return the participant ID, or empty if the text does not carry one
     */
    static Optional<String> parse(String queryText) {
        if (isNull(queryText)) {
            return Optional.empty();
        }
        var matcher = COMMENT_PATTERN.matcher(queryText);
        return matcher.find() ? Optional.of(matcher.group(1)) : Optional.empty();
    }

    /**
     * Make a participant ID safe to use as {@code application_name}: printable ASCII only (Postgres
     * replaces anything else with question marks) and at most 63 bytes.
     *
     * @param participantId the participant ID
     * @return the application name
     */
    static String applicationName(String participantId) {
        var builder = new StringBuilder(participantId.length());
        for (var ch : participantId.toCharArray()) {
            builder.append(ch >= 0x20 && ch < 0x7f ? ch : '?');
        }
        var ascii = builder.toString();
        if (ascii.getBytes(StandardCharsets.US_ASCII).length <= MAX_APPLICATION_NAME_BYTES) {
            return ascii;
        }
        return ascii.substring(0, MAX_APPLICATION_NAME_BYTES);
    }
}
