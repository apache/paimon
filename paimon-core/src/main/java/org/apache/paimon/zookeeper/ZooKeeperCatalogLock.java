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

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.Path;
import org.apache.paimon.operation.Lock;

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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/** ZooKeeper catalog lock, backed by a Curator {@link InterProcessMutex} per znode path. */
public class ZooKeeperCatalogLock implements Lock {

    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperCatalogLock.class);

    private final CuratorFramework client;

    // Shared with every lock built from the same client; bumped on session loss. See
    // ZooKeeperCatalogLockFactory#newClient. isAcquiredInThisProcess() alone cannot detect a
    // session expiry, since Curator never updates it on connection loss, so ensureValid also
    // checks this epoch and does a synchronous checkExists() round trip.
    private final AtomicLong sessionEpoch;
    private final Runnable releaseClient;
    private final AtomicBoolean closed = new AtomicBoolean();

    private final String warehouseHash;
    private final long acquireTimeoutMillis;
    private final Identifier identifier;

    private final ConcurrentHashMap<String, TrackedMutex> mutexes = new ConcurrentHashMap<>();
    private final ThreadLocal<HeldLock> heldLock = new ThreadLocal<>();

    private static final class HeldLock {
        final TrackedMutex mutex;
        final String path;
        final String node;
        final long epoch;
        final HeldLock previous;

        HeldLock(TrackedMutex mutex, String path, String node, long epoch, HeldLock previous) {
            this.mutex = mutex;
            this.path = path;
            this.node = node;
            this.epoch = epoch;
            this.previous = previous;
        }
    }

    /** Exposes the mutex's lock-node path so {@link #ensureValid} can re-verify it exists. */
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
            Runnable releaseClient,
            String warehouse,
            long acquireTimeoutMillis,
            Identifier identifier) {
        this.client = client;
        this.sessionEpoch = sessionEpoch;
        this.releaseClient = releaseClient;
        this.warehouseHash = hashWarehouse(warehouse);
        this.acquireTimeoutMillis = acquireTimeoutMillis;
        this.identifier = identifier;
    }

    @Override
    public AutoCloseable acquire() throws Exception {
        if (identifier == null) {
            throw new IllegalStateException("A table identifier is required to acquire the lock.");
        }
        return acquire(identifier.getDatabaseName(), identifier.getTableName());
    }

    private AutoCloseable acquire(String database, String table) throws Exception {
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

        HeldLock acquiredLock =
                new HeldLock(mutex, path, mutex.heldNode(), sessionEpoch.get(), heldLock.get());
        heldLock.set(acquiredLock);
        return () -> {
            heldLock.set(acquiredLock.previous);
            try {
                if (mutex.heldNode() != null) {
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
        };
    }

    <T> T runWithLock(String database, String table, Callable<T> callable) throws Exception {
        AutoCloseable scope = acquire(database, table);
        Throwable failure = null;
        try {
            return callable.call();
        } catch (Exception | Error e) {
            failure = e;
            throw e;
        } finally {
            Lock.release(scope, failure);
        }
    }

    @Override
    public void ensureValid() {
        HeldLock current = heldLock.get();
        if (current == null) {
            throw new IllegalStateException("No ZooKeeper lock is held by this thread.");
        }

        boolean epochChanged = sessionEpoch.get() != current.epoch;
        boolean nodeGone;
        try {
            nodeGone = current.node == null || client.checkExists().forPath(current.node) == null;
        } catch (Exception e) {
            nodeGone = true;
            LOG.warn(
                    "paimon_lock_verify_failed could not re-verify lock node '{}'",
                    current.node,
                    e);
        }

        boolean localRecordChanged =
                current.node == null || !current.node.equals(current.mutex.heldNode());
        if (localRecordChanged || epochChanged || nodeGone) {
            String reason =
                    nodeGone
                            ? "lock_node_gone"
                            : epochChanged ? "session_epoch_changed" : "local_record_cleared";
            LOG.error("paimon_lock_lost Lost lock '{}' (reason={})", current.path, reason);
            throw new IllegalStateException(
                    String.format("Lost ZooKeeper lock '%s' (%s)", current.path, reason));
        }
    }

    private String lockPath(String database, String table) {
        return "/" + warehouseHash + "/" + sanitize(database) + "/" + sanitize(table);
    }

    /** A warehouse URI cannot appear in a znode path literally, so it is hashed. */
    static String hashWarehouse(String warehouse) {
        String normalized = normalizeS3Path(new Path(warehouse.trim()).toString());
        while (normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return shortHash(normalized);
    }

    /**
     * Folds the s3/s3a/s3n scheme aliases and bucket case onto one canonical {@code s3://} form, so
     * e.g. {@code s3a://Bucket/p} and {@code s3://bucket/p} hash to the same lock namespace. Only
     * S3 has this multi-scheme aliasing; any other scheme (e.g. {@code gs://}) is returned
     * unchanged.
     */
    private static String normalizeS3Path(String warehouse) {
        String lowerScheme = warehouse.toLowerCase(Locale.ROOT);
        int schemeEnd = warehouse.indexOf("://");
        boolean isS3 =
                schemeEnd >= 0
                        && (lowerScheme.startsWith("s3://")
                                || lowerScheme.startsWith("s3a://")
                                || lowerScheme.startsWith("s3n://"));
        if (!isS3) {
            return warehouse;
        }
        String rest = warehouse.substring(schemeEnd + 3);
        int slash = rest.indexOf('/');
        String bucket = slash < 0 ? rest : rest.substring(0, slash);
        String path = slash < 0 ? "" : rest.substring(slash);
        return "s3://" + bucket.toLowerCase(Locale.ROOT) + path;
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

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        List<String> removed = new ArrayList<>();
        mutexes.forEach(
                (path, mutex) -> {
                    try {
                        if (mutex.heldNode() != null) {
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
        releaseClient.run();
    }
}
