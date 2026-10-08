package org.kiwiproject.postgres.leader;

/**
 * Unchecked exception used inside the package to report a failure talking to Postgres, so that callers
 * of {@link LockGateway} do not need to handle {@link java.sql.SQLException}.
 */
final class LockGatewayException extends RuntimeException {

    LockGatewayException(String message) {
        super(message);
    }

    LockGatewayException(String message, Throwable cause) {
        super(message, cause);
    }
}
