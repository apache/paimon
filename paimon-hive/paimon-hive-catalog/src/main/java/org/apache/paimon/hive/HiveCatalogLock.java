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

package org.apache.paimon.hive;

import org.apache.paimon.catalog.CatalogLock;
import org.apache.paimon.client.ClientPool;
import org.apache.paimon.hive.pool.CachedClientPool;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.ExecutorThreadFactory;
import org.apache.paimon.utils.TimeUtils;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockLevel;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import static org.apache.paimon.options.CatalogOptions.LOCK_ACQUIRE_TIMEOUT;
import static org.apache.paimon.options.CatalogOptions.LOCK_CHECK_MAX_SLEEP;

/** Hive {@link CatalogLock}. */
public class HiveCatalogLock implements CatalogLock {

    private static final Logger LOG = LoggerFactory.getLogger(HiveCatalogLock.class);

    static final String LOCK_IDENTIFIER = "hive";

    private final ClientPool<IMetaStoreClient, TException> clients;
    private final long checkMaxSleep;
    private final long acquireTimeout;
    private static final ScheduledThreadPoolExecutor RENEWER = createRenewer();
    private final long leaseMillis;
    private final ScheduledExecutorService renewer;
    private final LongSupplier nanoTime;
    private final ThreadLocal<Lease> currentLease = new ThreadLocal<>();

    public HiveCatalogLock(
            ClientPool<IMetaStoreClient, TException> clients,
            long checkMaxSleep,
            long acquireTimeout) {
        this(clients, checkMaxSleep, acquireTimeout, leaseTimeout(new HiveConf()));
    }

    public HiveCatalogLock(
            ClientPool<IMetaStoreClient, TException> clients,
            long checkMaxSleep,
            long acquireTimeout,
            long leaseMillis) {
        this(clients, checkMaxSleep, acquireTimeout, leaseMillis, RENEWER, System::nanoTime);
    }

    HiveCatalogLock(
            ClientPool<IMetaStoreClient, TException> clients,
            long checkMaxSleep,
            long acquireTimeout,
            long leaseMillis,
            ScheduledExecutorService renewer,
            LongSupplier nanoTime) {
        this.leaseMillis = leaseMillis;
        this.renewer = renewer;
        this.nanoTime = nanoTime;
        this.clients = clients;
        this.checkMaxSleep = checkMaxSleep;
        this.acquireTimeout = acquireTimeout;
    }

    @Override
    public <T> T runWithLock(String database, String table, Callable<T> callable) throws Exception {
        long lockId = lock(database, table);
        Lease lease = null;
        try {
            long requestedAt = nanoTime.getAsLong();
            clients.execute(client -> client.heartbeat(0L, lockId));
            lease = new Lease(lockId, requestedAt);
            currentLease.set(lease);
            lease.ensureValid();
            return callable.call();
        } finally {
            currentLease.remove();
            if (lease != null) {
                lease.close();
            }
            unlock(lockId);
        }
    }

    @Override
    public void ensureValid() {
        Lease lease = currentLease.get();
        if (lease == null) {
            throw new IllegalStateException("No Hive lock is held by this thread.");
        }
        lease.heartbeat();
        lease.ensureValid();
    }

    private static ScheduledThreadPoolExecutor createRenewer() {
        ScheduledThreadPoolExecutor executor =
                new ScheduledThreadPoolExecutor(2, new ExecutorThreadFactory("hive-lock-renew"));
        executor.setRemoveOnCancelPolicy(true);
        return executor;
    }

    private class Lease implements AutoCloseable {
        private final ScheduledFuture<?> renewal;
        private final long lockId;
        private final AtomicBoolean closed = new AtomicBoolean();
        private final AtomicLong renewedAt;
        private volatile Exception failure;

        private Lease(long lockId, long requestedAt) {
            this.lockId = lockId;
            renewedAt = new AtomicLong(requestedAt);
            long interval = Math.max(1, leaseMillis / 3);
            renewal =
                    renewer.scheduleWithFixedDelay(
                            () -> {
                                if (closed.get()) {
                                    return;
                                }
                                heartbeat();
                            },
                            interval,
                            interval,
                            TimeUnit.MILLISECONDS);
        }

        private void heartbeat() {
            if (closed.get() || failure != null) {
                return;
            }
            try {
                ensureValid();
                long requestedAt = nanoTime.getAsLong();
                clients.execute(client -> client.heartbeat(0L, lockId));
                renewedAt.accumulateAndGet(requestedAt, Math::max);
            } catch (Exception e) {
                failure = e;
            }
        }

        private void ensureValid() {
            if (TimeUnit.NANOSECONDS.toMillis(nanoTime.getAsLong() - renewedAt.get()) >= leaseMillis
                    && failure == null) {
                failure = new IllegalStateException("Hive lock expired locally.");
            }
            if (closed.get() || failure != null) {
                throw new IllegalStateException("The Hive lock is no longer usable.", failure);
            }
        }

        @Override
        public void close() {
            closed.set(true);
            renewal.cancel(false);
        }
    }

    public static long leaseTimeout(HiveConf conf) {
        return HiveConf.getTimeVar(conf, HiveConf.ConfVars.HIVE_TXN_TIMEOUT, TimeUnit.MILLISECONDS);
    }

    private long lock(String database, String table)
            throws UnknownHostException, TException, InterruptedException {
        final LockComponent lockComponent =
                new LockComponent(LockType.EXCLUSIVE, LockLevel.TABLE, database);
        lockComponent.setTablename(table);
        lockComponent.unsetOperationType();

        long startMs = System.currentTimeMillis();
        final LockRequest lockRequest =
                new LockRequest(
                        Collections.singletonList(lockComponent),
                        System.getProperty("user.name"),
                        InetAddress.getLocalHost().getHostName());
        LockResponse lockResponse = clients.run(client -> client.lock(lockRequest));
        long lockId = lockResponse.getLockid();

        long nextSleep = 50;

        try {
            while (lockResponse.getState() == LockState.WAITING) {
                long elapsed = System.currentTimeMillis() - startMs;
                if (elapsed >= acquireTimeout) {
                    break;
                }

                nextSleep = Math.min(nextSleep * 2, checkMaxSleep);
                Thread.sleep(nextSleep);

                lockResponse = clients.run(client -> client.checkLock(lockId));
            }
        } finally {
            if (lockResponse.getState() != LockState.ACQUIRED) {
                // unlock if not acquired
                unlock(lockId);
            }
        }

        LockState lockState = lockResponse.getState();
        long duration = System.currentTimeMillis() - startMs;
        String msg =
                String.format(
                        "for table %s.%s (lockId=%d) after %dms. Final lock state: %s",
                        database, table, lockId, duration, lockState);
        LOG.info("Acquire lock {}", msg);
        if (lockState == LockState.ACQUIRED) {
            return lockId;
        }

        throw new RuntimeException("Acquire lock failed " + msg);
    }

    private void unlock(long lockId) {
        if (lockId <= 0) {
            return;
        }
        try {
            clients.execute(client -> client.unlock(lockId));
        } catch (Exception e) {
            LOG.warn("Unlock failed for lockId={}", lockId, e);
        }
    }

    @Override
    public void close() {
        // do nothing
    }

    public static ClientPool<IMetaStoreClient, TException> createClients(
            HiveConf conf, Options options, String clientClassName) {
        return new CachedClientPool(conf, options, clientClassName);
    }

    public static long checkMaxSleep(HiveConf conf) {
        return TimeUtils.parseDuration(
                        conf.get(
                                LOCK_CHECK_MAX_SLEEP.key(),
                                TimeUtils.getStringInMillis(LOCK_CHECK_MAX_SLEEP.defaultValue())))
                .toMillis();
    }

    public static long acquireTimeout(HiveConf conf) {
        return TimeUtils.parseDuration(
                        conf.get(
                                LOCK_ACQUIRE_TIMEOUT.key(),
                                TimeUtils.getStringInMillis(LOCK_ACQUIRE_TIMEOUT.defaultValue())))
                .toMillis();
    }
}
