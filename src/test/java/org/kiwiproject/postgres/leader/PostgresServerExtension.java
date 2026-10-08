package org.kiwiproject.postgres.leader;

import static java.util.Objects.nonNull;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Starts a Postgres Testcontainers container once per test class and hands out new connections to it.
 * <p>
 * The image defaults to {@code postgres:18-alpine}, and can be overridden with the {@code kiwi.test.postgres.image}
 * system property or the {@code KIWI_TEST_POSTGRES_IMAGE} environment variable, as in kiwi-test.
 */
class PostgresServerExtension implements BeforeAllCallback, AfterAllCallback {

    static final String IMAGE_SYSTEM_PROPERTY = "kiwi.test.postgres.image";
    static final String IMAGE_ENV_VAR = "KIWI_TEST_POSTGRES_IMAGE";
    static final String DEFAULT_IMAGE = "postgres:18-alpine";

    private PostgreSQLContainer container;

    @Override
    public void beforeAll(ExtensionContext context) {
        container = new PostgreSQLContainer(DockerImageName.parse(resolveImage()).asCompatibleSubstituteFor("postgres"));
        container.start();
    }

    @Override
    public void afterAll(ExtensionContext context) {
        if (nonNull(container)) {
            container.stop();
        }
    }

    static String resolveImage() {
        var fromProperty = System.getProperty(IMAGE_SYSTEM_PROPERTY);
        if (nonNull(fromProperty) && !fromProperty.isBlank()) {
            return fromProperty;
        }

        var fromEnv = System.getenv(IMAGE_ENV_VAR);
        return nonNull(fromEnv) && !fromEnv.isBlank() ? fromEnv : DEFAULT_IMAGE;
    }

    /**
     * A new connection to the server. The caller closes it.
     */
    Connection newConnection() {
        try {
            return DriverManager.getConnection(
                    container.getJdbcUrl() + "&socketTimeout=10&connectTimeout=10&tcpKeepAlive=true",
                    container.getUsername(),
                    container.getPassword());
        } catch (SQLException e) {
            throw new IllegalStateException("Unable to connect to the test Postgres server", e);
        }
    }
}
