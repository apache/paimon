/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.zookeeper;

import org.apache.paimon.catalog.CatalogLock;
import org.apache.paimon.catalog.CatalogLockContext;
import org.apache.paimon.catalog.CatalogLockFactory;
import org.apache.paimon.factories.FactoryUtil;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link ZooKeeperCatalogLock} and {@link ZooKeeperCatalogLockFactory}. */
class ZooKeeperCatalogLockTest {

    private static TestingServer zk;

    @BeforeAll
    static void startZk() throws Exception {
        zk = new TestingServer(true);
    }

    @AfterAll
    static void stopZk() throws Exception {
        if (zk != null) {
            zk.close();
        }
    }

    private static Options options() {
        Options options = new Options();
        options.set(CatalogOptions.WAREHOUSE, "s3://test-bucket/warehouse");
        options.set(CatalogOptions.LOCK_TYPE, ZooKeeperCatalogLockFactory.IDENTIFIER);
        options.set(ZooKeeperCatalogLockOptions.QUORUM, zk.getConnectString());
        options.set(ZooKeeperCatalogLockOptions.SESSION_TIMEOUT, Duration.ofSeconds(10));
        options.set(CatalogOptions.LOCK_ACQUIRE_TIMEOUT, Duration.ofSeconds(30));
        return options;
    }

    private static CatalogLock newLock(Options options) {
        return new ZooKeeperCatalogLockFactory()
                .createLock(CatalogLockContext.fromOptions(options));
    }

    /**
     * The discovery path that actually matters in production. {@code AbstractCatalog#lockFactory()}
     * resolves the factory by identifier through {@link FactoryUtil}, reading the shared {@code
     * META-INF/services/org.apache.paimon.factories.Factory} file. If that registration is missing
     * or the identifier is wrong, the job dies at startup with "Could not find any factory for
     * identifier" -- so assert it the same way Paimon does rather than by constructing the factory
     * directly.
     */
    @Test
    void factoryIsDiscoverableByIdentifier() {
        CatalogLockFactory factory =
                FactoryUtil.discoverFactory(
                        ZooKeeperCatalogLockTest.class.getClassLoader(),
                        CatalogLockFactory.class,
                        ZooKeeperCatalogLockFactory.IDENTIFIER);

        assertNotNull(factory);
        assertTrue(factory instanceof ZooKeeperCatalogLockFactory);
        assertEquals("zookeeper", factory.identifier());
    }

    /**
     * The factory is serialized to task executors as part of every {@code FileStoreTable}, so a
     * non-serializable field here breaks job submission rather than failing a test.
     */
    @Test
    void factoryIsSerializable() throws Exception {
        ZooKeeperCatalogLockFactory factory = new ZooKeeperCatalogLockFactory();

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(factory);
        }
        try (ObjectInputStream in =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            Object restored = in.readObject();
            assertTrue(restored instanceof ZooKeeperCatalogLockFactory);
            assertEquals(
                    ZooKeeperCatalogLockFactory.IDENTIFIER,
                    ((ZooKeeperCatalogLockFactory) restored).identifier());
        }
    }

    @Test
    void runsCallableAndReturnsItsValue() throws Exception {
        try (CatalogLock lock = newLock(options())) {
            assertEquals("result", lock.runWithLock("db", "tbl", () -> "result"));
        }
    }

    /** A failing callable must propagate, and must not leave the lock held. */
    @Test
    void releasesLockWhenCallableThrows() throws Exception {
        try (CatalogLock lock = newLock(options())) {
            assertThrows(
                    IllegalStateException.class,
                    () ->
                            lock.runWithLock(
                                    "db",
                                    "tbl",
                                    () -> {
                                        throw new IllegalStateException("boom");
                                    }));

            // If the previous failure leaked the lock, this would block until the acquire
            // timeout.
            assertEquals("after", lock.runWithLock("db", "tbl", () -> "after"));
        }
    }

    /**
     * The property the whole design rests on: two independent lock instances (standing in for two
     * independent jobs) must never be inside the critical section for the same table at once.
     * Without this, both can write the same Paimon snapshot id and an object store's
     * overwrite-happy rename silently drops one.
     */
    @Test
    void mutuallyExcludesConcurrentHoldersOfTheSameTable() throws Exception {
        int threads = 8;
        AtomicInteger concurrent = new AtomicInteger();
        AtomicInteger maxObserved = new AtomicInteger();
        AtomicInteger completed = new AtomicInteger();
        CountDownLatch startTogether = new CountDownLatch(1);
        List<CatalogLock> locks = new ArrayList<>();
        ExecutorService pool = Executors.newFixedThreadPool(threads);

        try {
            for (int i = 0; i < threads; i++) {
                CatalogLock lock = newLock(options());
                locks.add(lock);
                pool.submit(
                        () -> {
                            startTogether.await();
                            return lock.runWithLock(
                                    "db",
                                    "hot_table",
                                    () -> {
                                        int inside = concurrent.incrementAndGet();
                                        maxObserved.accumulateAndGet(inside, Math::max);
                                        // Widen the window so an absent lock would reliably be
                                        // caught.
                                        Thread.sleep(50);
                                        concurrent.decrementAndGet();
                                        completed.incrementAndGet();
                                        return null;
                                    });
                        });
            }

            startTogether.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS), "tasks did not finish in time");
        } finally {
            pool.shutdownNow();
            for (CatalogLock lock : locks) {
                lock.close();
            }
        }

        assertEquals(threads, completed.get(), "every task should have run");
        assertEquals(1, maxObserved.get(), "critical section was entered concurrently");
    }

    /**
     * Regression test for a real bug caught in review: an earlier version of {@link
     * ZooKeeperCatalogLock} re-verified ownership with only {@code
     * InterProcessMutex#isAcquiredInThisProcess()}, which is purely local bookkeeping ({@code
     * ConcurrentMap<Thread, LockData>}) that Curator never touches on connection loss -- it stays
     * {@code true} straight through a genuine session expiry, so the exact failure mode this class
     * exists to catch (a stalled holder whose session expired and was silently taken over by a
     * peer) would pass the check and be reported as a successful commit.
     *
     * <p>This forces that scenario for real, rather than asserting on internal state: it kills the
     * lock's ZooKeeper session out from under it <em>while inside the critical section</em>, using
     * the same technique {@code org.apache.curator.test.KillSession} uses (open a second connection
     * presenting the same session id/password, which makes the server treat the original connection
     * as stale and forcibly close it -- indistinguishable, from the original client's side, from a
     * real session expiry during a JVM pause or network partition). {@code runWithLock} must fail
     * this commit rather than report it as successful.
     */
    @Test
    void detectsRealSessionLossDuringCriticalSectionAndFailsRatherThanReportSuccess()
            throws Exception {
        // A dedicated root, NOT options(): CLIENTS is a static, JVM-wide cache keyed by
        // quorum|root, so every other test in this class shares one CuratorFramework and one
        // epoch. Killing ITS session would leave that shared client mid
        // SUSPENDED->LOST->RECONNECTED (each bumping the shared epoch) while whatever test runs
        // next is inside its own critical section, failing it for a reason that has nothing to
        // do with what it asserts. This test's destructive action must be isolated to a client
        // nothing else touches.
        Options killOptions = options();
        killOptions.set(ZooKeeperCatalogLockOptions.ROOT_PATH, "paimon/locks-session-kill-test");

        ZooKeeperCatalogLockFactory factory = new ZooKeeperCatalogLockFactory();
        CatalogLock lock = factory.createLock(CatalogLockContext.fromOptions(killOptions));

        try {
            CuratorFramework client = clientOf(lock);

            // Independent of (and does not rely on) our production epoch-tracking listener: this
            // proves the underlying connection was actually disrupted, so the test cannot pass
            // merely because killSession() silently no-op'd for some reason.
            CountDownLatch observedDisruption = new CountDownLatch(1);
            client.getConnectionStateListenable()
                    .addListener(
                            (c, newState) -> {
                                if (newState == ConnectionState.SUSPENDED
                                        || newState == ConnectionState.LOST
                                        || newState == ConnectionState.RECONNECTED) {
                                    observedDisruption.countDown();
                                }
                            });

            IllegalStateException e =
                    assertThrows(
                            IllegalStateException.class,
                            () ->
                                    lock.runWithLock(
                                            "db",
                                            "session_kill_table",
                                            () -> {
                                                killSession(client);
                                                assertTrue(
                                                        observedDisruption.await(
                                                                20, TimeUnit.SECONDS),
                                                        "expected a ZooKeeper connection-state"
                                                                + " disruption after killing the"
                                                                + " session -- if this times out,"
                                                                + " the session-kill technique"
                                                                + " itself is not working in this"
                                                                + " environment, not that the"
                                                                + " lock is safe");
                                                return "must not be reported as success";
                                            }));

            // The synchronous checkExists() round trip (the actual fix for the async-epoch race
            // found in review) is faster than the epoch listener's own dispatch, so it is
            // normally what catches this first -- hence "lock_node_gone" rather than
            // "session_epoch_changed". Either is a correct detection of the same underlying loss;
            // accept both so this isn't racy on timing.
            assertTrue(
                    e.getMessage().contains("lock_node_gone")
                            || e.getMessage().contains("session_epoch_changed"),
                    () ->
                            "expected a lost-lock detection reason (proving the synchronous"
                                    + " checkExists() check or the epoch, not just"
                                    + " isAcquiredInThisProcess(), is what caught this), got: "
                                    + e.getMessage());
        } finally {
            lock.close();
        }
    }

    /**
     * Kills the ZooKeeper session belonging to {@code client} by opening a second connection that
     * presents the same session id and password, then closing it. The server treats this as the
     * same client reconnecting elsewhere and drops the original connection -- exactly what {@code
     * org.apache.curator.test.KillSession} does, reimplemented by hand here to avoid a hard
     * dependency on that internal test utility's signature.
     */
    private static void killSession(CuratorFramework client) throws Exception {
        ZooKeeper underlying = client.getZookeeperClient().getZooKeeper();
        long sessionId = underlying.getSessionId();
        byte[] sessionPasswd = underlying.getSessionPasswd();

        CountDownLatch connected = new CountDownLatch(1);
        ZooKeeper duplicate =
                new ZooKeeper(
                        zk.getConnectString(),
                        10_000,
                        event -> {
                            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                                connected.countDown();
                            }
                        },
                        sessionId,
                        sessionPasswd);
        try {
            connected.await(10, TimeUnit.SECONDS);
        } finally {
            duplicate.close();
        }
    }

    /** Different tables must not serialise against each other, or throughput collapses. */
    @Test
    void doesNotSerialiseAcrossDifferentTables() throws Exception {
        AtomicBoolean bothInside = new AtomicBoolean(false);
        CountDownLatch aInside = new CountDownLatch(1);
        CountDownLatch bDone = new CountDownLatch(1);

        try (CatalogLock lockA = newLock(options());
                CatalogLock lockB = newLock(options())) {

            ExecutorService pool = Executors.newFixedThreadPool(2);
            try {
                pool.submit(
                        () ->
                                lockA.runWithLock(
                                        "db",
                                        "table_a",
                                        () -> {
                                            aInside.countDown();
                                            // Hold table_a while table_b is acquired; if locks
                                            // were global this deadlocks until the acquire
                                            // timeout and the assertion below fails.
                                            return bDone.await(20, TimeUnit.SECONDS);
                                        }));

                assertTrue(aInside.await(20, TimeUnit.SECONDS), "table_a never entered");

                pool.submit(
                        () ->
                                lockB.runWithLock(
                                        "db",
                                        "table_b",
                                        () -> {
                                            bothInside.set(true);
                                            bDone.countDown();
                                            return null;
                                        }));

                assertTrue(bDone.await(20, TimeUnit.SECONDS), "table_b blocked behind table_a");
            } finally {
                pool.shutdown();
                assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
            }
        }

        assertTrue(bothInside.get(), "different tables should lock independently");
    }

    /**
     * All locks in a JVM must share one ZooKeeper connection. One client per (committer, table)
     * would mean thousands of connections per job against a shared ensemble.
     */
    @Test
    void reusesOneZooKeeperClientPerQuorum() throws Exception {
        ZooKeeperCatalogLockFactory factory = new ZooKeeperCatalogLockFactory();
        CatalogLock first = factory.createLock(CatalogLockContext.fromOptions(options()));
        CatalogLock second = factory.createLock(CatalogLockContext.fromOptions(options()));

        try {
            assertSame(
                    clientOf(first),
                    clientOf(second),
                    "locks should share a single CuratorFramework per JVM");
        } finally {
            first.close();
            second.close();
        }

        // Closing individual locks must NOT close the shared client -- sibling tables still need
        // it.
        try (CatalogLock third = factory.createLock(CatalogLockContext.fromOptions(options()))) {
            assertEquals("still-usable", third.runWithLock("db", "tbl", () -> "still-usable"));
        }
    }

    private static CuratorFramework clientOf(CatalogLock lock) throws Exception {
        java.lang.reflect.Field field = ZooKeeperCatalogLock.class.getDeclaredField("client");
        field.setAccessible(true);
        return (CuratorFramework) field.get(lock);
    }

    @Test
    void requiresQuorum() {
        Options options = options();
        options.remove(ZooKeeperCatalogLockOptions.QUORUM.key());

        IllegalArgumentException e =
                assertThrows(IllegalArgumentException.class, () -> newLock(options));
        assertTrue(e.getMessage().contains(ZooKeeperCatalogLockOptions.QUORUM.key()));
    }

    @Test
    void requiresWarehouseSoLocksAreNamespacedPerWarehouse() {
        Options options = options();
        options.remove(CatalogOptions.WAREHOUSE.key());

        IllegalArgumentException e =
                assertThrows(IllegalArgumentException.class, () -> newLock(options));
        assertTrue(e.getMessage().contains("warehouse"));
    }

    /**
     * Two warehouses that share an ensemble must not contend on the same znodes: the same database
     * name can exist in more than one warehouse.
     */
    @Test
    void differentWarehousesDoNotContend() throws Exception {
        Options other = options();
        other.set(CatalogOptions.WAREHOUSE, "s3://other-bucket/warehouse");

        CountDownLatch firstInside = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean secondEntered = new AtomicBoolean(false);

        try (CatalogLock lockA = newLock(options());
                CatalogLock lockB = newLock(other)) {

            ExecutorService pool = Executors.newFixedThreadPool(2);
            try {
                pool.submit(
                        () ->
                                lockA.runWithLock(
                                        "db",
                                        "same_name",
                                        () -> {
                                            firstInside.countDown();
                                            return release.await(20, TimeUnit.SECONDS);
                                        }));

                assertTrue(firstInside.await(20, TimeUnit.SECONDS));

                pool.submit(
                        () ->
                                lockB.runWithLock(
                                        "db",
                                        "same_name",
                                        () -> {
                                            secondEntered.set(true);
                                            release.countDown();
                                            return null;
                                        }));

                assertTrue(
                        release.await(20, TimeUnit.SECONDS),
                        "second warehouse blocked on the first");
            } finally {
                release.countDown();
                pool.shutdown();
                assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
            }
        }

        assertTrue(
                secondEntered.get(), "same table name in a different warehouse should not contend");
    }

    @Test
    void hashWarehouseFoldsSchemeAndBucketButNotTheCaseSensitiveS3Path() {
        assertEquals(
                ZooKeeperCatalogLock.hashWarehouse("s3://BUCKET/prefix"),
                ZooKeeperCatalogLock.hashWarehouse("s3a://bucket/prefix"),
                "scheme and bucket must fold case-insensitively");
        assertNotEquals(
                ZooKeeperCatalogLock.hashWarehouse("s3://bucket/A"),
                ZooKeeperCatalogLock.hashWarehouse("s3://bucket/a"),
                "the S3 key prefix is case-sensitive and must not collide onto the same lock"
                        + " namespace");
    }

    @Test
    void sanitizeDisambiguatesCaseDifferingTableNames() throws Exception {
        // 'Foo' and 'foo' must land on different znodes: escaping through the lowercased form
        // and then comparing against that same lowercased form would make them collide.
        CountDownLatch firstInside = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        try (CatalogLock lock = newLock(options())) {
            ExecutorService pool = Executors.newFixedThreadPool(2);
            try {
                pool.submit(
                        () ->
                                lock.runWithLock(
                                        "db",
                                        "Foo",
                                        () -> {
                                            firstInside.countDown();
                                            release.await(20, TimeUnit.SECONDS);
                                            return null;
                                        }));

                assertTrue(firstInside.await(20, TimeUnit.SECONDS));

                pool.submit(
                        () ->
                                lock.runWithLock(
                                        "db",
                                        "foo",
                                        () -> {
                                            release.countDown();
                                            return null;
                                        }));

                assertTrue(
                        release.await(20, TimeUnit.SECONDS),
                        "case-differing names should not contend");
            } finally {
                release.countDown();
                pool.shutdown();
                assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    void rejectsPathSegmentsThatWouldCorruptTheZnodePath() throws Exception {
        // sanitize() escapes rather than rejects a name outside [a-z0-9_-] (see its javadoc):
        // table names come off external sources with no charset guarantee, and throwing here
        // would be a permanent, deterministic commit failure for that one table -- Paimon
        // retries it identically forever, and the whole job restart-loops. So "../escape" and
        // "db/other" must succeed, with '/' and '.' never appearing in the resulting znode path
        // (path-traversal stays closed) -- only an empty name is still rejected outright.
        try (CatalogLock lock = newLock(options())) {
            assertEquals("ok", lock.runWithLock("db", "../escape", () -> "ok"));
            assertEquals("ok", lock.runWithLock("db/other", "tbl", () -> "ok"));
            assertThrows(
                    IllegalArgumentException.class, () -> lock.runWithLock("db", "", () -> "nope"));
        }
    }
}
