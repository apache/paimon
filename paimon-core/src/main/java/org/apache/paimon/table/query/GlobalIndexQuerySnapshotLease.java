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

package org.apache.paimon.table.query;

import org.apache.paimon.consumer.Consumer;
import org.apache.paimon.consumer.ConsumerManager;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Per-attempt consumer lease which protects building and published query snapshots from expiry.
 *
 * <p>The consumer ID contains a random attempt token and a daemon heartbeat updates its file while
 * the attempt is alive. {@link #close()} stops the heartbeat but deliberately leaves the lease for
 * {@code consumer.expiration-time}: a Flink source close callback cannot distinguish a terminal
 * cancellation from a failover, and deleting immediately would leave an expiry window before the
 * replacement attempt pins the snapshot. Table maintenance eventually removes every abandoned
 * attempt lease.
 *
 * <p>{@link #pinBuilding(long)} only moves the lease backwards, to the minimum of the currently
 * active and building snapshots. The caller may move it forward with {@link #promote(long)} only
 * after the new generation is globally published and its configured handover grace period has
 * elapsed.
 */
public class GlobalIndexQuerySnapshotLease implements AutoCloseable {

    private static final long MIN_HEARTBEAT_MILLIS = 1_000L;
    private static final long MAX_HEARTBEAT_MILLIS = 60_000L;
    public static final Duration MIN_EXPIRATION_TIME = Duration.ofSeconds(10);
    private static final int MAX_CONSUMER_ID_PREFIX_LENGTH = 128;
    private static final Pattern CONSUMER_ID_PREFIX = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]*");

    private final ConsumerManager consumerManager;
    private final String consumerId;
    private final ScheduledExecutorService heartbeatExecutor;

    @Nullable private Long pinnedSnapshotId;
    @Nullable private RuntimeException heartbeatFailure;
    private boolean closed;

    public GlobalIndexQuerySnapshotLease(
            ConsumerManager consumerManager, String consumerIdPrefix, Duration expirationTime) {
        validateConsumerIdPrefix(consumerIdPrefix);
        checkArgument(
                expirationTime != null && expirationTime.compareTo(MIN_EXPIRATION_TIME) >= 0,
                "consumer.expiration-time must be at least %s.",
                MIN_EXPIRATION_TIME);
        this.consumerManager = consumerManager;
        this.consumerId = consumerIdPrefix + '-' + UUID.randomUUID();
        this.heartbeatExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        runnable -> {
                            Thread thread =
                                    new Thread(
                                            runnable, "paimon-global-index-query-lease-heartbeat");
                            thread.setDaemon(true);
                            return thread;
                        });
        long heartbeatMillis =
                Math.max(
                        MIN_HEARTBEAT_MILLIS,
                        Math.min(MAX_HEARTBEAT_MILLIS, expirationTime.toMillis() / 3));
        heartbeatExecutor.scheduleWithFixedDelay(
                this::heartbeatSafely, heartbeatMillis, heartbeatMillis, TimeUnit.MILLISECONDS);
    }

    /** Validate a single safe consumer-path segment before a Flink job is submitted. */
    public static void validateConsumerIdPrefix(String consumerIdPrefix) {
        checkArgument(
                consumerIdPrefix != null
                        && consumerIdPrefix.length() <= MAX_CONSUMER_ID_PREFIX_LENGTH
                        && CONSUMER_ID_PREFIX.matcher(consumerIdPrefix).matches()
                        && !consumerIdPrefix.contains(".."),
                "--consumer-id prefix must be a single alphanumeric path segment of at most %s characters and may contain only '.', '_', or '-'.",
                MAX_CONSUMER_ID_PREFIX_LENGTH);
    }

    /** Pin the minimum snapshot needed by the active and currently building generations. */
    public synchronized void pinBuilding(long snapshotId) {
        checkOpenAndHealthy();
        checkArgument(snapshotId >= 0, "Cannot lease negative snapshot ID %s.", snapshotId);
        if (pinnedSnapshotId == null || snapshotId < pinnedSnapshotId) {
            pinnedSnapshotId = snapshotId;
        }
        writeLease();
    }

    /** Advance the lease after a new generation has been published and its grace period elapsed. */
    public synchronized void promote(long snapshotId) {
        checkOpenAndHealthy();
        checkArgument(snapshotId >= 0, "Cannot lease negative snapshot ID %s.", snapshotId);
        pinnedSnapshotId = snapshotId;
        writeLease();
    }

    public synchronized void checkHealthy() {
        checkOpenAndHealthy();
    }

    public String consumerId() {
        return consumerId;
    }

    @Nullable
    public synchronized Long pinnedSnapshotId() {
        return pinnedSnapshotId;
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        RuntimeException failure = null;
        try {
            // Refresh the modification time before abandoning the attempt. This preserves the
            // complete configured expiration window for a replacement task instead of shortening
            // it by up to one heartbeat interval.
            if (pinnedSnapshotId != null) {
                writeLease();
            }
        } catch (RuntimeException e) {
            failure = e;
        } finally {
            closed = true;
            heartbeatExecutor.shutdownNow();
        }
        if (failure != null) {
            throw failure;
        }
    }

    private synchronized void heartbeatSafely() {
        if (closed || pinnedSnapshotId == null) {
            return;
        }
        try {
            writeLease();
            heartbeatFailure = null;
        } catch (RuntimeException e) {
            heartbeatFailure = e;
        }
    }

    private void writeLease() {
        consumerManager.resetConsumer(consumerId, new Consumer(pinnedSnapshotId));
    }

    private void checkOpenAndHealthy() {
        checkArgument(!closed, "Snapshot lease is already closed.");
        if (heartbeatFailure != null) {
            throw new IllegalStateException(
                    "Global-index query snapshot lease heartbeat failed.", heartbeatFailure);
        }
    }
}
