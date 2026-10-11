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

package org.apache.paimon.jdbc;

import org.apache.paimon.catalog.CatalogLock;
import org.apache.paimon.utils.ExecutorThreadFactory;
import org.apache.paimon.utils.TimeUtils;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import static org.apache.paimon.jdbc.JdbcCatalogOptions.LOCK_ACQUIRE_TIMEOUT;
import static org.apache.paimon.jdbc.JdbcCatalogOptions.LOCK_CHECK_MAX_SLEEP;

/** Jdbc catalog lock. */
public class JdbcCatalogLock implements CatalogLock {
    private static final ScheduledThreadPoolExecutor RENEWER = createRenewer();
    private final ScheduledExecutorService renewer;
    private final LongSupplier nanoTime;
    private final ThreadLocal<Lease> currentLease = new ThreadLocal<>();
    private final JdbcClientPool connections;
    private final long checkMaxSleep;
    private final long acquireTimeout;
    private final String catalogKey;

    public JdbcCatalogLock(
            JdbcClientPool connections,
            String catalogKey,
            long checkMaxSleep,
            long acquireTimeout) {
        this(connections, catalogKey, checkMaxSleep, acquireTimeout, RENEWER, System::nanoTime);
    }

    JdbcCatalogLock(
            JdbcClientPool connections,
            String catalogKey,
            long checkMaxSleep,
            long acquireTimeout,
            ScheduledExecutorService renewer,
            LongSupplier nanoTime) {
        this.renewer = renewer;
        this.nanoTime = nanoTime;
        this.connections = connections;
        this.checkMaxSleep = checkMaxSleep;
        this.acquireTimeout = acquireTimeout;
        this.catalogKey = catalogKey;
    }

    @Override
    public <T> T runWithLock(String database, String table, Callable<T> callable) throws Exception {
        String lockUniqueName = String.format("%s.%s.%s", catalogKey, database, table);
        String owner = UUID.randomUUID().toString();
        long startedAt = nanoTime.getAsLong();
        AbstractDistributedLockDialect dialect =
                (AbstractDistributedLockDialect)
                        DistributedLockDialectFactory.create(connections.getProtocol());
        long nextSleep = 50;
        while (true) {
            long requestedAt = nanoTime.getAsLong();
            if (dialect.acquireOwned(connections, lockUniqueName, owner, acquireTimeout)) {
                Lease lease = new Lease(dialect, lockUniqueName, owner, requestedAt);
                currentLease.set(lease);
                try {
                    lease.ensureValid();
                    return callable.call();
                } finally {
                    currentLease.remove();
                    lease.close();
                    // The former holder must never delete a lease acquired after its expiry.
                    dialect.releaseOwned(connections, lockUniqueName, owner);
                }
            }
            long elapsed = TimeUnit.NANOSECONDS.toMillis(nanoTime.getAsLong() - startedAt);
            if (elapsed >= acquireTimeout) {
                throw new IllegalStateException(
                        "Acquire lock failed with time: " + Duration.ofMillis(elapsed));
            }
            nextSleep = Math.min(nextSleep * 2, checkMaxSleep);
            Thread.sleep(Math.min(nextSleep, acquireTimeout - elapsed));
        }
    }

    @Override
    public void ensureValid() {
        Lease lease = currentLease.get();
        if (lease == null) {
            throw new IllegalStateException("No JDBC lock is held by this thread.");
        }
        lease.renew();
        lease.ensureValid();
    }

    private static ScheduledThreadPoolExecutor createRenewer() {
        ScheduledThreadPoolExecutor executor =
                new ScheduledThreadPoolExecutor(2, new ExecutorThreadFactory("jdbc-lock-renew"));
        executor.setRemoveOnCancelPolicy(true);
        return executor;
    }

    private class Lease implements AutoCloseable {
        private final AtomicBoolean closed = new AtomicBoolean();
        private final ScheduledFuture<?> renewal;
        private final AbstractDistributedLockDialect dialect;
        private final String lockId;
        private final String owner;
        private final AtomicLong renewedAt;
        private volatile Exception failure;

        private Lease(
                AbstractDistributedLockDialect dialect,
                String lockId,
                String owner,
                long requestedAt) {
            this.dialect = dialect;
            this.lockId = lockId;
            this.owner = owner;
            this.renewedAt = new AtomicLong(requestedAt);
            long interval = Math.max(1, acquireTimeout / 3);
            this.renewal =
                    renewer.scheduleWithFixedDelay(
                            this::renew, interval, interval, TimeUnit.MILLISECONDS);
        }

        private void renew() {
            if (closed.get() || failure != null) {
                return;
            }
            try {
                ensureValid();
                long requestedAt = nanoTime.getAsLong();
                if (dialect.renewOwned(connections, lockId, owner)) {
                    renewedAt.accumulateAndGet(requestedAt, Math::max);
                } else {
                    failure = new IllegalStateException("JDBC lock ownership was lost.");
                }
            } catch (Exception e) {
                failure = e;
            }
        }

        private void ensureValid() {
            if (TimeUnit.NANOSECONDS.toMillis(nanoTime.getAsLong() - renewedAt.get())
                            >= acquireTimeout
                    && failure == null) {
                failure = new IllegalStateException("JDBC lock expired locally.");
            }
            if (closed.get() || failure != null) {
                throw new IllegalStateException("The JDBC lock is no longer usable.", failure);
            }
        }

        @Override
        public void close() {
            closed.set(true);
            renewal.cancel(false);
        }
    }

    @Override
    public void close() throws IOException {
        // Do nothing
    }

    public static long checkMaxSleep(Map<String, String> conf) {
        return TimeUtils.parseDuration(
                        conf.getOrDefault(
                                LOCK_CHECK_MAX_SLEEP.key(),
                                TimeUtils.getStringInMillis(LOCK_CHECK_MAX_SLEEP.defaultValue())))
                .toMillis();
    }

    public static long acquireTimeout(Map<String, String> conf) {
        return TimeUtils.parseDuration(
                        conf.getOrDefault(
                                LOCK_ACQUIRE_TIMEOUT.key(),
                                TimeUtils.getStringInMillis(LOCK_ACQUIRE_TIMEOUT.defaultValue())))
                .toMillis();
    }
}
