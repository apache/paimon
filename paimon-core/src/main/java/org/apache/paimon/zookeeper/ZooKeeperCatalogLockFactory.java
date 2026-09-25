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
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/** ZooKeeper catalog lock factory. */
public class ZooKeeperCatalogLockFactory implements CatalogLockFactory {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperCatalogLockFactory.class);

    public static final String IDENTIFIER = "zookeeper";

    // One client per (quorum, root) per JVM, shared by every lock built from it. Static because
    // the factory is serialized and deserialized into independent instances.
    private static final ConcurrentHashMap<String, SessionTracker> CLIENTS =
            new ConcurrentHashMap<>();

    /** A client plus a session-loss counter derived from its connection-state events. */
    static final class SessionTracker {
        final CuratorFramework client;
        final AtomicLong epoch = new AtomicLong();

        SessionTracker(CuratorFramework client) {
            this.client = client;
        }
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public CatalogLock createLock(CatalogLockContext context) {
        Options options = context.options();

        String quorum = options.get(ZooKeeperCatalogLockOptions.QUORUM);
        if (quorum == null || quorum.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "Paimon catalog option '"
                            + ZooKeeperCatalogLockOptions.QUORUM.key()
                            + "' is required when lock.type=zookeeper");
        }

        String warehouse = options.get(CatalogOptions.WAREHOUSE);
        if (warehouse == null || warehouse.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "Paimon catalog option 'warehouse' is required when lock.type=zookeeper");
        }

        String root = options.get(ZooKeeperCatalogLockOptions.ROOT_PATH);
        long acquireTimeoutMillis = options.get(CatalogOptions.LOCK_ACQUIRE_TIMEOUT).toMillis();
        long sessionTimeoutMillis =
                options.get(ZooKeeperCatalogLockOptions.SESSION_TIMEOUT).toMillis();

        if (acquireTimeoutMillis <= sessionTimeoutMillis) {
            LOG.warn(
                    "{}={}ms is not greater than {}={}ms; prefer an acquire timeout of at least"
                            + " 2-3x the session timeout",
                    CatalogOptions.LOCK_ACQUIRE_TIMEOUT.key(),
                    acquireTimeoutMillis,
                    ZooKeeperCatalogLockOptions.SESSION_TIMEOUT.key(),
                    sessionTimeoutMillis);
        }

        SessionTracker tracker =
                CLIENTS.computeIfAbsent(
                        quorum + "|" + root, key -> newClient(quorum, root, options));

        return new ZooKeeperCatalogLock(
                tracker.client, tracker.epoch, warehouse, acquireTimeoutMillis);
    }

    private static SessionTracker newClient(String quorum, String root, Options options) {
        int sessionTimeoutMs =
                toBoundedIntMillis(
                        options.get(ZooKeeperCatalogLockOptions.SESSION_TIMEOUT).toMillis(),
                        ZooKeeperCatalogLockOptions.SESSION_TIMEOUT.key());
        int connectionTimeoutMs =
                toBoundedIntMillis(
                        options.get(ZooKeeperCatalogLockOptions.CONNECTION_TIMEOUT).toMillis(),
                        ZooKeeperCatalogLockOptions.CONNECTION_TIMEOUT.key());
        int baseSleepMs =
                toBoundedIntMillis(
                        options.get(ZooKeeperCatalogLockOptions.RETRY_BASE_SLEEP).toMillis(),
                        ZooKeeperCatalogLockOptions.RETRY_BASE_SLEEP.key());
        int maxRetries = options.get(ZooKeeperCatalogLockOptions.RETRY_MAX_ATTEMPTS);

        CuratorFramework client =
                CuratorFrameworkFactory.builder()
                        .connectString(quorum)
                        .sessionTimeoutMs(sessionTimeoutMs)
                        .connectionTimeoutMs(connectionTimeoutMs)
                        .retryPolicy(new ExponentialBackoffRetry(baseSleepMs, maxRetries))
                        .namespace(root.startsWith("/") ? root.substring(1) : root)
                        .build();

        SessionTracker tracker = new SessionTracker(client);

        // RECONNECTED is treated the same as LOST: Curator fires it both for a safe, self-healing
        // blip and for a new session negotiated after a real LOST, without saying which.
        client.getConnectionStateListenable()
                .addListener(
                        (c, newState) -> {
                            if (newState == ConnectionState.LOST
                                    || newState == ConnectionState.SUSPENDED
                                    || newState == ConnectionState.RECONNECTED) {
                                long newEpoch = tracker.epoch.incrementAndGet();
                                LOG.warn(
                                        "ZooKeeper connection state changed to {} for quorum '{}'"
                                                + " (epoch -> {})",
                                        newState,
                                        quorum,
                                        newEpoch);
                            } else {
                                LOG.info(
                                        "ZooKeeper connection state changed to {} for quorum '{}'",
                                        newState,
                                        quorum);
                            }
                        });

        client.start();

        LOG.info(
                "Started ZooKeeper client for Paimon catalog locks: quorum='{}', namespace='{}'",
                quorum,
                root);
        return tracker;
    }

    private static int toBoundedIntMillis(long millis, String key) {
        if (millis <= 0 || millis > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    key
                            + "="
                            + millis
                            + "ms must be positive and no greater than Integer.MAX_VALUE");
        }
        return Math.toIntExact(millis);
    }
}
