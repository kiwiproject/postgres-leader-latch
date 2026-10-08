package org.kiwiproject.postgres.leader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Duration;

/**
 * Unit tests for {@link PgLockGateway} using mocked JDBC objects. The behavior against a real server is covered
 * by {@link PostgresLeaderLatchIntegrationTest}.
 */
@DisplayName("PgLockGateway")
class PgLockGatewayTest {

    private Connection connection;
    private PreparedStatement statement;
    private ResultSet resultSet;
    private PgLockGateway gateway;

    @BeforeEach
    void setUp() throws SQLException {
        connection = mock(Connection.class);
        statement = mock(PreparedStatement.class);
        resultSet = mock(ResultSet.class);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);

        // 3 second validation timeout; allow 2 consecutive failures
        var config = LeaderLatchConfiguration.defaults()
                .withValidation(Duration.ofSeconds(5), Duration.ofSeconds(3), 2);
        gateway = new PgLockGateway(() -> connection, config, "key", 42L, "svc/1.0/host:8080");
    }

    private void acquirable() throws SQLException {
        when(resultSet.getBoolean("in_recovery")).thenReturn(false);
        when(resultSet.getBoolean("acquired")).thenReturn(true);
    }

    @Test
    void shouldBoundReadsOnTheConnectionSoASilentlyDroppedNetworkIsDetected() throws SQLException {
        acquirable();

        gateway.tryAcquire();

        // statement timeout (3s) plus a margin for the server's response
        verify(connection).setNetworkTimeout(any(), eq(5_000));
    }

    @Test
    void shouldStillWorkWhenTheDriverDoesNotSupportNetworkTimeout() throws SQLException {
        acquirable();
        doThrow(new SQLFeatureNotSupportedException("no")).when(connection).setNetworkTimeout(any(), anyInt());

        var lease = gateway.tryAcquire();

        assertThat(lease).isPresent();
    }

    @Test
    void shouldSetAutoCommitAndApplicationName() throws SQLException {
        acquirable();

        gateway.tryAcquire();

        assertAll(
                () -> verify(connection).setAutoCommit(true),
                () -> verify(connection).setClientInfo("ApplicationName", "svc/1.0/host:8080")
        );
    }

    @Test
    void shouldRefuseToLeadOnAStandbyAndDiscardTheConnection() throws SQLException {
        when(resultSet.getBoolean("in_recovery")).thenReturn(true);

        assertAll(
                () -> org.assertj.core.api.Assertions.assertThatThrownBy(gateway::tryAcquire)
                        .isInstanceOf(LockGatewayException.class)
                        .hasMessageContaining("standby"),
                () -> verify(connection).close()
        );
    }

    @Test
    void shouldLoseTheLockImmediatelyWhenValidationFailsAndTheConnectionIsClosed() throws SQLException {
        acquirable();
        var lease = gateway.tryAcquire().orElseThrow();
        when(statement.executeQuery()).thenThrow(new SQLException("socket timed out"));
        when(connection.isClosed()).thenReturn(true);

        lease.validate();

        assertThat(lease.isHeld()).isFalse();
    }

    @Test
    void shouldOnlyLoseTheLockAfterTheMaximumConsecutiveFailuresWhileTheConnectionIsOpen() throws SQLException {
        acquirable();
        var lease = gateway.tryAcquire().orElseThrow();
        when(statement.executeQuery()).thenThrow(new SQLException("temporary failure"));
        when(connection.isClosed()).thenReturn(false);

        lease.validate();
        var heldAfterFirstFailure = lease.isHeld();
        lease.validate();

        assertAll(
                () -> assertThat(heldAfterFirstFailure).isTrue(),
                () -> assertThat(lease.isHeld()).isFalse()
        );
    }

    @Test
    void shouldResetTheFailureCountAfterASuccessfulValidation() throws SQLException {
        acquirable();
        var lease = gateway.tryAcquire().orElseThrow();
        when(connection.isClosed()).thenReturn(false);

        // fail, succeed, fail: never two failures in a row, so the lock must stay held
        when(statement.executeQuery())
                .thenThrow(new SQLException("temporary failure"))
                .thenReturn(resultSet)
                .thenThrow(new SQLException("temporary failure"));
        when(resultSet.getBoolean("in_recovery")).thenReturn(false);
        when(resultSet.getBoolean("holds_lock")).thenReturn(true);

        lease.validate();
        lease.validate();
        lease.validate();

        assertThat(lease.isHeld()).isTrue();
    }

    @Test
    void shouldLoseTheLockWhenTheServerSaysItIsNoLongerHeldOrNoLongerPrimary() throws SQLException {
        acquirable();
        var lease = gateway.tryAcquire().orElseThrow();
        when(resultSet.getBoolean("in_recovery")).thenReturn(false);
        when(resultSet.getBoolean("holds_lock")).thenReturn(false);

        lease.validate();

        assertThat(lease.isHeld()).isFalse();
    }

    @Test
    void shouldNotUseTheConnectionAfterRelease() throws SQLException {
        acquirable();
        var lease = gateway.tryAcquire().orElseThrow();

        lease.release();

        assertAll(
                () -> assertThat(lease.isHeld()).isFalse(),
                () -> verify(connection).close()
        );
        clearInvocations(statement);
        lease.validate();
        verify(statement, never()).executeQuery(); // validation after release does nothing
    }
}
