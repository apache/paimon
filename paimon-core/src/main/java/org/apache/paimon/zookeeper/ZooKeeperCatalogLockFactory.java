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

    // Static because the factory is serialized and deserialized into independent instances.
    private static final ConcurrentHashMap<ClientConfig, SessionTracker> CLIENTS =
            new ConcurrentHashMap<>();

    /** A client plus a session-loss counter derived from its connection-state events. */
    static final class SessionTracker {
        final CuratorFramework client;
        final AtomicLong epoch = new AtomicLong();
        int references;

        SessionTracker(CuratorFramework client) {
            this.client = client;
        }
    }

    private static final class ClientConfig {
        private final String quorum;
        private final String root;
        private final int sessionTimeoutMs;
        private final int connectionTimeoutMs;
        private final int retryBaseSleepMs;
        private final int retryMaxAttempts;

        private ClientConfig(
                String quorum,
                String root,
                int sessionTimeoutMs,
                int connectionTimeoutMs,
                int retryBaseSleepMs,
                int retryMaxAttempts) {
            this.quorum = quorum;
            this.root = root;
            this.sessionTimeoutMs = sessionTimeoutMs;
            this.connectionTimeoutMs = connectionTimeoutMs;
            this.retryBaseSleepMs = retryBaseSleepMs;
            this.retryMaxAttempts = retryMaxAttempts;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof ClientConfig)) {
                return false;
            }
            ClientConfig that = (ClientConfig) o;
            return sessionTimeoutMs == that.sessionTimeoutMs
                    && connectionTimeoutMs == that.connectionTimeoutMs
                    && retryBaseSleepMs == that.retryBaseSleepMs
                    && retryMaxAttempts == that.retryMaxAttempts
                    && quorum.equals(that.quorum)
                    && root.equals(that.root);
        }

        @Override
        public int hashCode() {
            int result = quorum.hashCode();
            result = 31 * result + root.hashCode();
            result = 31 * result + sessionTimeoutMs;
            result = 31 * result + connectionTimeoutMs;
            result = 31 * result + retryBaseSleepMs;
            return 31 * result + retryMaxAttempts;
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

        ClientConfig config = clientConfig(quorum, root, options);
        SessionTracker tracker =
                CLIENTS.compute(
                        config,
                        (key, existing) -> {
                            SessionTracker result = existing == null ? newClient(config) : existing;
                            result.references++;
                            return result;
                        });

        return new ZooKeeperCatalogLock(
                tracker.client,
                tracker.epoch,
                () -> releaseClient(config, tracker),
                warehouse,
                acquireTimeoutMillis);
    }

    private static ClientConfig clientConfig(String quorum, String root, Options options) {
        return new ClientConfig(
                quorum,
                root,
                toBoundedIntMillis(
                        options.get(ZooKeeperCatalogLockOptions.SESSION_TIMEOUT).toMillis(),
                        ZooKeeperCatalogLockOptions.SESSION_TIMEOUT.key()),
                toBoundedIntMillis(
                        options.get(ZooKeeperCatalogLockOptions.CONNECTION_TIMEOUT).toMillis(),
                        ZooKeeperCatalogLockOptions.CONNECTION_TIMEOUT.key()),
                toBoundedIntMillis(
                        options.get(ZooKeeperCatalogLockOptions.RETRY_BASE_SLEEP).toMillis(),
                        ZooKeeperCatalogLockOptions.RETRY_BASE_SLEEP.key()),
                options.get(ZooKeeperCatalogLockOptions.RETRY_MAX_ATTEMPTS));
    }

    private static SessionTracker newClient(ClientConfig config) {
        CuratorFramework client =
                CuratorFrameworkFactory.builder()
                        .connectString(config.quorum)
                        .sessionTimeoutMs(config.sessionTimeoutMs)
                        .connectionTimeoutMs(config.connectionTimeoutMs)
                        .retryPolicy(
                                new ExponentialBackoffRetry(
                                        config.retryBaseSleepMs, config.retryMaxAttempts))
                        .namespace(config.root.startsWith("/") ? config.root.substring(1) : config.root)
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
                                        config.quorum,
                                        newEpoch);
                            } else {
                                LOG.info(
                                        "ZooKeeper connection state changed to {} for quorum '{}'",
                                        newState,
                                        config.quorum);
                            }
                        });

        client.start();

        LOG.info(
                "Started ZooKeeper client for Paimon catalog locks: quorum='{}', namespace='{}'",
                config.quorum,
                config.root);
        return tracker;
    }

    private static void releaseClient(ClientConfig config, SessionTracker tracker) {
        CLIENTS.computeIfPresent(
                config,
                (key, current) -> {
                    if (current != tracker || --current.references > 0) {
                        return current;
                    }
                    current.client.close();
                    return null;
                });
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
