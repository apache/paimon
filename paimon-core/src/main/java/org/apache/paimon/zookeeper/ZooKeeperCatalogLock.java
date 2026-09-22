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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** ZooKeeper catalog lock, backed by a Curator {@link InterProcessMutex} per znode path. */
public class ZooKeeperCatalogLock implements CatalogLock {

    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperCatalogLock.class);

    private final CuratorFramework client;

    // Shared with every lock built from the same client; bumped on session loss. See
    // ZooKeeperCatalogLockFactory#newClient. isAcquiredInThisProcess() alone cannot detect a
    // session expiry, since Curator never updates it on connection loss, so runWithLock also
    // checks this epoch and does a synchronous checkExists() round trip.
    private final AtomicLong sessionEpoch;

    private final String warehouseHash;
    private final long acquireTimeoutMillis;

    private final ConcurrentHashMap<String, TrackedMutex> mutexes = new ConcurrentHashMap<>();

    /** Exposes the mutex's lock-node path so {@link #runWithLock} can re-verify it exists. */
    private static final class TrackedMutex extends InterProcessMutex {
        TrackedMutex(CuratorFramework client, String path) {
            super(client, path);
        }

        String heldNode() {
            return getLockPath();
        }
    }

    ZooKeeperCatalogLock(
            CuratorFramework client,
            AtomicLong sessionEpoch,
            String warehouse,
            long acquireTimeoutMillis) {
        this.client = client;
        this.sessionEpoch = sessionEpoch;
        this.warehouseHash = hashWarehouse(warehouse);
        this.acquireTimeoutMillis = acquireTimeoutMillis;
    }

    @Override
    public <T> T runWithLock(String database, String table, Callable<T> callable) throws Exception {
        String path = lockPath(database, table);
        TrackedMutex mutex = mutexes.computeIfAbsent(path, p -> new TrackedMutex(client, p));

        boolean acquired;
        try {
            acquired = mutex.acquire(acquireTimeoutMillis, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            LOG.error(
                    "paimon_lock_acquire_error acquiring lock '{}' for {}.{}",
                    path,
                    database,
                    table,
                    e);
            throw e;
        }

        if (!acquired) {
            LOG.error(
                    "paimon_lock_acquire_timeout Timed out after {}ms acquiring lock '{}' for {}.{}",
                    acquireTimeoutMillis,
                    path,
                    database,
                    table);
            throw new IllegalStateException(
                    String.format(
                            "Timed out after %dms acquiring ZooKeeper lock '%s' for %s.%s",
                            acquireTimeoutMillis, path, database, table));
        }

        long acquiredEpoch = sessionEpoch.get();
        String heldNode = mutex.heldNode();

        try {
            T result = callable.call();

            boolean epochChanged = sessionEpoch.get() != acquiredEpoch;
            boolean nodeGone;
            try {
                nodeGone = heldNode == null || client.checkExists().forPath(heldNode) == null;
            } catch (Exception e) {
                nodeGone = true;
                LOG.warn(
                        "paimon_lock_verify_failed could not re-verify lock node '{}'",
                        heldNode,
                        e);
            }

            if (!mutex.isOwnedByCurrentThread() || epochChanged || nodeGone) {
                String reason =
                        nodeGone
                                ? "lock_node_gone"
                                : epochChanged ? "session_epoch_changed" : "local_record_cleared";
                LOG.error(
                        "paimon_lock_lost Lost lock '{}' for {}.{} inside the commit critical"
                                + " section (reason={})",
                        path,
                        database,
                        table,
                        reason);
                throw new IllegalStateException(
                        String.format(
                                "Lost ZooKeeper lock '%s' for %s.%s while inside the commit"
                                        + " critical section (%s)",
                                path, database, table, reason));
            }
            return result;
        } finally {
            try {
                if (mutex.isOwnedByCurrentThread()) {
                    mutex.release();
                }
            } catch (Exception e) {
                LOG.warn(
                        "paimon_lock_release_failed releasing lock '{}' for {}.{}",
                        path,
                        database,
                        table,
                        e);
            }
        }
    }

    private String lockPath(String database, String table) {
        return "/" + warehouseHash + "/" + sanitize(database) + "/" + sanitize(table);
    }

    /** A warehouse URI cannot appear in a znode path literally, so it is hashed. */
    static String hashWarehouse(String warehouse) {
        String trimmed = warehouse.trim();
        String lowerScheme = trimmed.toLowerCase(Locale.ROOT);
        String normalized = trimmed;
        int schemeEnd = trimmed.indexOf("://");
        if (schemeEnd >= 0
                && (lowerScheme.startsWith("s3://")
                        || lowerScheme.startsWith("s3a://")
                        || lowerScheme.startsWith("s3n://"))) {
            String rest = trimmed.substring(schemeEnd + 3);
            int slash = rest.indexOf('/');
            String bucket = slash < 0 ? rest : rest.substring(0, slash);
            String path = slash < 0 ? "" : rest.substring(slash);
            normalized = "s3://" + bucket.toLowerCase(Locale.ROOT) + path;
        }
        while (normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return shortHash(normalized);
    }

    static String shortHash(String value) {
        try {
            byte[] digest =
                    MessageDigest.getInstance("SHA-256")
                            .digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(16);
            for (int i = 0; i < 8; i++) {
                sb.append(String.format("%02x", digest[i]));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    /**
     * Maps a database/table name onto a safe znode path segment; never throws for a non-empty name.
     */
    private static String sanitize(String segment) {
        if (segment == null || segment.isEmpty()) {
            throw new IllegalArgumentException("Lock path segment must not be null or empty");
        }
        String lower = segment.toLowerCase(Locale.ROOT);
        StringBuilder sb = new StringBuilder(lower.length());
        for (int i = 0; i < lower.length(); i++) {
            char c = lower.charAt(i);
            boolean ok = (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' || c == '-';
            sb.append(ok ? c : '_');
        }
        String safe = sb.toString();
        return safe.equals(segment) ? safe : safe + "-" + shortHash(segment);
    }

    /** Does not close the shared {@link CuratorFramework}; it outlives any single lock. */
    @Override
    public void close() {
        List<String> removed = new ArrayList<>();
        mutexes.forEach(
                (path, mutex) -> {
                    try {
                        if (mutex.isOwnedByCurrentThread()) {
                            mutex.release();
                            removed.add(path);
                        } else if (!mutex.isAcquiredInThisProcess()) {
                            removed.add(path);
                        }
                    } catch (Exception e) {
                        LOG.warn("paimon_lock_release_failed on close, lock '{}'", path, e);
                    }
                });
        removed.forEach(mutexes::remove);
    }
}
