package org.kiwiproject.postgres.leader;

import static org.kiwiproject.base.KiwiPreconditions.requireNotBlank;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Derives the 64-bit Postgres advisory lock key from a leadership key.
 */
final class LockKeys {

    private LockKeys() {
        // utility class
    }

    /**
     * Use the first eight bytes of the SHA-256 digest of the UTF-8 bytes of the leadership key,
     * read as a signed big-endian long. This is stable across JVMs and releases.
     *
     * @param leadershipKey the leadership key
     * @return the advisory lock key
     */
    static long fromLeadershipKey(String leadershipKey) {
        requireNotBlank(leadershipKey, "leadershipKey must not be blank");
        try {
            var digest = MessageDigest.getInstance("SHA-256")
                    .digest(leadershipKey.getBytes(StandardCharsets.UTF_8));
            return ByteBuffer.wrap(digest, 0, Long.BYTES).getLong();
        } catch (NoSuchAlgorithmException e) {
            // every Java platform is required to support SHA-256
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }
}
