### Postgres Leader Latch

[![Build](https://github.com/kiwiproject/postgres-leader-latch/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/kiwiproject/postgres-leader-latch/actions/workflows/build.yml?query=branch%3Amain)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=kiwiproject_postgres-leader-latch&metric=alert_status)](https://sonarcloud.io/dashboard?id=kiwiproject_postgres-leader-latch)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=kiwiproject_postgres-leader-latch&metric=coverage)](https://sonarcloud.io/dashboard?id=kiwiproject_postgres-leader-latch)
[![CodeQL](https://github.com/kiwiproject/postgres-leader-latch/actions/workflows/codeql.yml/badge.svg)](https://github.com/kiwiproject/postgres-leader-latch/actions/workflows/codeql.yml)
[![javadoc](https://javadoc.io/badge2/org.kiwiproject/postgres-leader-latch/javadoc.svg)](https://javadoc.io/doc/org.kiwiproject/postgres-leader-latch)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Maven Central](https://img.shields.io/maven-central/v/org.kiwiproject/postgres-leader-latch)](https://central.sonatype.com/artifact/org.kiwiproject/postgres-leader-latch/)

A framework-independent Java library for leader election (a leader latch) among multiple instances of the
same logical service, using Postgres advisory locks as the backend.

It is the Postgres counterpart to [dynamodb-leader-latch](https://github.com/kiwiproject/dynamodb-leader-latch)
and [dropwizard-leader-latch](https://github.com/kiwiproject/dropwizard-leader-latch), and has no Curator,
ZooKeeper, Dropwizard, or Helidon dependency.

## How it works

All instances of a service that use the same leadership key compete for one Postgres
[session-level advisory lock](https://www.postgresql.org/docs/current/explicit-locking.html#ADVISORY-LOCKS).
The instance that holds it is the leader. Each instance keeps **one dedicated database connection** for its
latch; it is never taken from or returned to your connection pool.

* **Failover:** Postgres releases the lock the moment the leader's session ends, so another instance takes over as
  soon as the server notices the connection is gone, or when the leader closes its latch.
* **Validation:** Advisory locks have no lease, so a leader whose connection has silently died could keep believing it
  leads. The leader therefore validates its session periodically: the connection works, the server is a primary
  (not a standby), and the lock is still held. After a configurable number of consecutive failures it stops reporting
  leadership.
* **Identity:** `getLeader()` reports the participant ID of the session holding the lock. Every statement the latch
  runs starts with a comment carrying the participant ID, which is read back from `pg_stat_activity.query`.
  (`application_name` is limited to 63 bytes, which real participant IDs easily exceed; it is also set, truncated.)
  No table or schema is needed.
* **No exceptions for expected failures:** like [dynamodb-leader-latch](https://github.com/kiwiproject/dynamodb-leader-latch),
  results are values (`StartResult`, `LeadershipStatus`, `LeaderInfo`, `WhenLeaderResult`), not exceptions.

## Usage

Add the dependency:

```xml
<dependency>
    <groupId>org.kiwiproject</groupId>
    <artifactId>postgres-leader-latch</artifactId>
    <version>[current-version]</version>
</dependency>
```

Then create and start a latch. You supply a `Supplier<Connection>`; the latch calls it when it starts and whenever it
needs a replacement connection, and it closes every connection it obtains:

```java
Supplier<Connection> connectionSupplier = () -> DriverManager.getConnection(
        "jdbc:postgresql://my-db.example.com:5432/order-service?connectTimeout=10&socketTimeout=10&tcpKeepAlive=true",
        user, password);

var latch = new PostgresLeaderLatch(
        connectionSupplier,
        LeaderLatchConfiguration.defaults(),
        "order-service",  // the leadership key shared by all instances
        PostgresLeaderLatch.leaderLatchId("order-service", "1.2.3", hostname, port));

latch.addListener(new LeaderLatchListener() {
    @Override public void isLeader() { /* start leader-only work */ }
    @Override public void notLeader() { /* stop leader-only work */ }
});

latch.start();   // returns immediately; never waits for leadership

if (latch.hasLeadership()) {
    // ...
}

var result = latch.whenLeader(() -> doLeaderOnlyWork());

latch.close();   // releases the lock and closes the connection
```

The participant ID is carried in a SQL comment, so it must not contain `/*`, `*/`, or control characters.

## Configuration

| Setting | Default | Meaning |
|---|---|---|
| `validationInterval` | 5s | How often the leader validates its session |
| `validationTimeout` | 3s | Statement timeout for each statement (rounded up to whole seconds by JDBC); must be shorter than `validationInterval` |
| `maxConsecutiveValidationFailures` | 3 | Failures in a row before the leader steps down |
| `acquisitionRetryInterval` | 5s | How often a follower tries to acquire the lock |
| `lockKeyOverride` | none | Explicit 64-bit advisory lock key; by default the key is derived from the leadership key (first 8 bytes of its SHA-256) |

A leader that loses its connection can wrongly believe it leads for about
`maxConsecutiveValidationFailures * (validationInterval + validationTimeout)`. The defaults are initial values and have
not been tuned. `latch.getLockKey()` returns the numeric key, which is how the lock appears in `pg_locks`.

## Deployment notes (RDS and other Postgres)

* **Connect to the primary (writer) endpoint directly.** Advisory locks are per server, so a latch connected to a read
  replica or standby refuses to lead (the validation checks `pg_is_in_recovery()`). Do not connect through PgBouncer in
  transaction-pooling mode or a proxy that does not preserve the session; session-level locks need a stable session.
  AWS documents that RDS Proxy pins a client connection to one database connection when it takes a session-level advisory
  lock, which keeps the lock valid but means the proxy gives no pooling benefit for this connection. Connecting directly
  is still recommended. Behavior through RDS Proxy has not been tested with this library.
* **Configure the connection you return from the supplier.** Set `connectTimeout` and `tcpKeepAlive` (or the
  equivalent for your driver). The latch itself bounds every read on its connection with
  `Connection.setNetworkTimeout` (the validation timeout plus two seconds), so a network that silently drops traffic
  is detected even if you set no `socketTimeout`. With the default settings a leader behind a blackholed network
  stopped reporting leadership after about 18 seconds in a local test. Postgres itself only notices a dead client after
  its own TCP keepalive settings expire, and until it does the lock is still held, so another instance cannot take over
  before then.
* **After a database failover** the old session's lock disappears and a follower reconnects to the new primary. Keep the
  JVM's DNS cache TTL low (`networkaddress.cache.ttl`) so reconnects follow the endpoint.
* **Connections:** one extra connection per instance, in addition to your application pool. Count them against
  `max_connections` and any per-role connection limit.
* **Identity visibility:** `pg_stat_activity.query` is only visible to the same role (or members of `pg_read_all_stats`).
  If every instance of a service connects as the same role, as is typical, `getLeader()` works. The server setting
  `track_activity_query_size` (default 1024 bytes) limits the length of the participant ID that can be read back.
* **Tested against** Postgres 16 and 18.

## Limitations

* There is no fencing token. During a network partition, a stale leader and a new leader can briefly overlap, for up to
  the validation window above. If correctness depends on never having two leaders, protect the leader-only work
  itself (for example with the database transaction it performs).
* Leadership is lost while the database is unavailable, and `getLeader()` reports a failed lookup.
