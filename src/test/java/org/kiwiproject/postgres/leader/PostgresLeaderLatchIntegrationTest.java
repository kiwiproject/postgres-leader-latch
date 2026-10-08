package org.kiwiproject.postgres.leader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@DisplayName("PostgresLeaderLatch (integration)")
@Testcontainers(disabledWithoutDocker = true)
class PostgresLeaderLatchIntegrationTest {

    private static final Duration WAIT = Duration.ofSeconds(20);

    // longer than the 63-byte application_name limit, like real service names
    private static final String LONG_SERVICE = "analysis-controller-service_development";

    @RegisterExtension
    static final PostgresServerExtension POSTGRES = new PostgresServerExtension();

    private static final LeaderLatchConfiguration CONFIG = LeaderLatchConfiguration.defaults()
            .withValidation(Duration.ofMillis(500), Duration.ofMillis(400), 2)
            .withAcquisitionRetryInterval(Duration.ofMillis(100));

    private final List<PostgresLeaderLatch> latches = new ArrayList<>();
    private final List<Connection> connections = new ArrayList<>();

    @AfterEach
    void tearDown() throws SQLException {
        latches.forEach(PostgresLeaderLatch::close);
        for (var connection : connections) {
            connection.close();
        }
    }

    private PostgresLeaderLatch newLatch(String leadershipKey, String host) {
        var id = PostgresLeaderLatch.leaderLatchId(leadershipKey, "1.2.3-SNAPSHOT", host, 8080);
        var latch = new PostgresLeaderLatch(POSTGRES::newConnection, CONFIG, leadershipKey, id);
        latches.add(latch);
        return latch;
    }

    private Connection adminConnection() {
        var connection = POSTGRES.newConnection();
        connections.add(connection);
        return connection;
    }

    @Test
    void shouldElectOneLeaderAndReportItsFullIdentity() {
        var first = newLatch(LONG_SERVICE, "host-1.internal");
        var second = newLatch(LONG_SERVICE, "host-2.internal");

        first.start();
        second.start();

        await().atMost(WAIT).until(() -> first.hasLeadership() || second.hasLeadership());
        var leader = first.hasLeadership() ? first : second;
        var follower = leader == first ? second : first;

        assertAll(
                () -> assertThat(leader.getId().getBytes().length).isGreaterThan(63),
                () -> assertThat(follower.hasLeadership()).isFalse(),
                () -> assertThat(leader.getLeader()).isInstanceOfSatisfying(LeaderInfo.Leader.class,
                        info -> assertThat(info.participantId()).isEqualTo(leader.getId())),
                () -> assertThat(follower.getLeader()).isInstanceOfSatisfying(LeaderInfo.Leader.class,
                        info -> assertThat(info.participantId()).isEqualTo(leader.getId()))
        );
    }

    @Test
    void shouldHandOverLeadershipWhenTheLeaderCloses() {
        var first = newLatch("handover-service", "host-1");
        var second = newLatch("handover-service", "host-2");
        first.start();
        await().atMost(WAIT).until(first::hasLeadership);
        second.start();

        first.close();

        await().atMost(WAIT).until(second::hasLeadership);
        assertThat(second.getLeader()).isInstanceOfSatisfying(LeaderInfo.Leader.class,
                info -> assertThat(info.participantId()).isEqualTo(second.getId()));
    }

    @Test
    void shouldKeepSeparateElectionsForSeparateKeys() {
        var orders = newLatch("orders", "host-1");
        var billing = newLatch("billing", "host-1");

        orders.start();
        billing.start();

        await().atMost(WAIT).until(() -> orders.hasLeadership() && billing.hasLeadership());
        assertThat(orders.getLockKey()).isNotEqualTo(billing.getLockKey());
    }

    @Test
    void shouldStepDownWhenTheConnectionIsKilledAndLaterLeadAgain() throws SQLException {
        var events = new CopyOnWriteArrayList<String>();
        var latch = newLatch("killed-service", "host-1");
        latch.addListener(new LeaderLatchListener() {
            @Override
            public void isLeader() {
                events.add("isLeader");
            }

            @Override
            public void notLeader() {
                events.add("notLeader");
            }
        });
        latch.start();
        await().atMost(WAIT).until(latch::hasLeadership);

        terminateHolderOf(latch);

        await().atMost(WAIT).until(() -> events.size() >= 3);
        assertAll(
                () -> assertThat(events).containsExactly("isLeader", "notLeader", "isLeader"),
                () -> assertThat(latch.hasLeadership()).isTrue()
        );
    }

    @Test
    void shouldSetApplicationNameOnTheLeadersSessionAsTruncatedIdentity() throws SQLException {
        var latch = newLatch(LONG_SERVICE, "host-1.internal");
        latch.start();
        await().atMost(WAIT).until(latch::hasLeadership);

        var applicationName = holderApplicationName(latch);

        assertAll(
                () -> assertThat(applicationName).hasSize(63),
                () -> assertThat(latch.getId()).startsWith(applicationName)
        );
    }

    @Test
    void shouldReportLookupFailedWhenTheHolderDoesNotCarryAnIdentity() throws SQLException {
        var outsider = adminConnection();
        var follower = newLatch("outsider-service", "host-1");
        try (var statement = outsider.prepareStatement("SELECT pg_advisory_lock(?)")) {
            statement.setLong(1, follower.getLockKey());
            statement.execute();
        }

        follower.start();

        await().atMost(WAIT).until(() -> follower.getLeader() instanceof LeaderInfo.LookupFailed);
        assertAll(
                () -> assertThat(follower.hasLeadership()).isFalse(),
                () -> assertThat(follower.getLeader()).isInstanceOfSatisfying(LeaderInfo.LookupFailed.class,
                        failed -> assertThat(failed.cause()).hasMessageContaining("identity could not be read"))
        );
    }

    @Test
    void shouldWorkWithNegativeAndOverriddenLockKeys() {
        var negativeKey = -4_611_686_018_427_387_905L;
        var config = CONFIG.withLockKeyOverride(negativeKey);
        var id = PostgresLeaderLatch.leaderLatchId("negative-key-service", "1.0", "host-1", 8080);
        var latch = new PostgresLeaderLatch(POSTGRES::newConnection, config, "negative-key-service", id);
        latches.add(latch);

        latch.start();
        await().atMost(WAIT).until(latch::hasLeadership);

        assertAll(
                () -> assertThat(latch.getLockKey()).isEqualTo(negativeKey),
                () -> assertThat(latch.getLeader()).isInstanceOfSatisfying(LeaderInfo.Leader.class,
                        info -> assertThat(info.participantId()).isEqualTo(id))
        );
    }

    @Test
    void shouldHandleParticipantIdsWithSqlSpecialCharacters() {
        var id = "svc/1.0/it's \"quoted\" $1 ? ; -- \\ host:8080";
        var latch = new PostgresLeaderLatch(POSTGRES::newConnection, CONFIG, "special-chars-service", id);
        latches.add(latch);

        latch.start();
        await().atMost(WAIT).until(latch::hasLeadership);

        assertThat(latch.getLeader()).isInstanceOfSatisfying(LeaderInfo.Leader.class,
                info -> assertThat(info.participantId()).isEqualTo(id));
    }

    @Test
    void shouldReportNoOwnerWhenNoSessionHoldsTheLock() {
        var id = "nobody/1.0/host:8080";
        try (var gateway = new PgLockGateway(POSTGRES::newConnection, CONFIG, "unheld-key", 1234567L, id)) {
            assertThat(gateway.currentOwner()).isEmpty();
        }
    }

    @Test
    void shouldReleaseTheLockWhenClosed() {
        var leader = newLatch("release-service", "host-1");
        var follower = newLatch("release-service", "host-2");
        leader.start();
        await().atMost(WAIT).until(leader::hasLeadership);
        follower.start();

        leader.close();

        await().atMost(WAIT).until(follower::hasLeadership);
        assertThat(leader.checkLeadershipStatus()).isInstanceOf(LeadershipStatus.Closed.class);
    }

    private void terminateHolderOf(PostgresLeaderLatch latch) throws SQLException {
        var admin = adminConnection();
        try (var statement = admin.prepareStatement("""
                SELECT pg_terminate_backend(l.pid)
                  FROM pg_locks l
                 WHERE l.locktype = 'advisory'
                   AND ((l.classid::bigint << 32) | l.objid::bigint) = ?
                """)) {
            statement.setLong(1, latch.getLockKey());
            statement.execute();
        }
    }

    private String holderApplicationName(PostgresLeaderLatch latch) throws SQLException {
        var admin = adminConnection();
        try (var statement = admin.prepareStatement("""
                SELECT a.application_name
                  FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
                 WHERE l.locktype = 'advisory'
                   AND ((l.classid::bigint << 32) | l.objid::bigint) = ?
                """)) {
            statement.setLong(1, latch.getLockKey());
            try (var resultSet = statement.executeQuery()) {
                resultSet.next();
                return resultSet.getString(1);
            }
        }
    }
}
