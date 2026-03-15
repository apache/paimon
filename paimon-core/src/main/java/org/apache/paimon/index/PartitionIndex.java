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

package org.apache.paimon.index;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.utils.Int2ShortHashMap;
import org.apache.paimon.utils.IntIterator;
import org.apache.paimon.utils.ListUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;

import static org.apache.paimon.index.HashIndexFile.HASH_INDEX;

/** Bucket Index Per Partition. */
public class PartitionIndex {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionIndex.class);

    // Configuration for bucket refresh executor
    // Higher thread count because refresh operations are I/O bound (waiting on disk/network)
    private static final int REFRESH_CORE_THREADS = 4;
    private static final int REFRESH_MAX_THREADS = 12;

    // Shared executor for all PartitionIndex instances to control global concurrency
    // Uses unbounded queue because:
    // 1. Refresh tasks are infrequent (hours/days between refreshes)
    // 2. Low memory footprint (~400 bytes per task, max ~200KB for 500 partitions)
    // 3. Tasks process quickly (30-60s each) so queue drains fast
    // 4. Ensures all partitions eventually refresh (no task rejection)
    private static final ExecutorService REFRESH_EXECUTOR;

    static {
        ThreadFactory threadFactory =
                new ThreadFactory() {
                    private final AtomicInteger counter = new AtomicInteger(0);

                    @Override
                    public Thread newThread(Runnable r) {
                        Thread thread =
                                new Thread(r, "paimon-bucket-refresh-" + counter.getAndIncrement());
                        thread.setDaemon(true);
                        return thread;
                    }
                };

        // Unbounded queue - never rejects tasks
        // With 24h refresh interval, queue size is manageable even with many partitions
        REFRESH_EXECUTOR =
                new ThreadPoolExecutor(
                        REFRESH_CORE_THREADS,
                        REFRESH_MAX_THREADS,
                        60L,
                        TimeUnit.SECONDS,
                        new LinkedBlockingQueue<>(),
                        threadFactory);
    }

    public final Int2ShortHashMap hash2Bucket;

    public final ConcurrentHashMap<Integer, Long> nonFullBucketInformation;

    public final Set<Integer> totalBucketSet;
    public final List<Integer> totalBucketArray;

    private final long targetBucketRowNumber;

    public boolean accessed;

    public long lastAccessedCommitIdentifier;

    private final IndexFileHandler indexFileHandler;

    private final BinaryRow partition;

    private volatile CompletableFuture<Void> refreshFuture;

    private Instant lastRefreshTime;

    public PartitionIndex(
            Int2ShortHashMap hash2Bucket,
            ConcurrentHashMap<Integer, Long> bucketInformation,
            long targetBucketRowNumber,
            IndexFileHandler indexFileHandler,
            BinaryRow partition) {
        this.hash2Bucket = hash2Bucket;
        this.nonFullBucketInformation = bucketInformation;
        this.totalBucketSet = new LinkedHashSet<>(bucketInformation.keySet());
        this.totalBucketArray = new ArrayList<>(totalBucketSet);
        this.targetBucketRowNumber = targetBucketRowNumber;
        this.lastAccessedCommitIdentifier = Long.MIN_VALUE;
        this.accessed = true;
        this.indexFileHandler = indexFileHandler;
        this.partition = partition;
        this.lastRefreshTime = Instant.now();
    }

    public int assign(
            int hash,
            IntPredicate bucketFilter,
            int maxBucketsNum,
            int maxBucketId,
            int minEmptyBucketsBeforeAsyncCheck,
            Duration minRefreshInterval) {
        accessed = true;

        // 1. is it a key that has appeared before
        if (hash2Bucket.containsKey(hash)) {
            return hash2Bucket.get(hash);
        }

        // 2. find bucket from existing buckets
        boolean shouldRefresh = false;
        Iterator<Map.Entry<Integer, Long>> iterator =
                nonFullBucketInformation.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Integer, Long> entry = iterator.next();
            Integer bucket = entry.getKey();
            Long number = entry.getValue();
            if (number < targetBucketRowNumber) {
                // Check if this bucket is approaching capacity
                if (!shouldRefresh
                        && shouldRefreshWhenBucketNearFull(
                                number, targetBucketRowNumber, minEmptyBucketsBeforeAsyncCheck)) {
                    shouldRefresh = true;
                }
                entry.setValue(number + 1);
                hash2Bucket.put(hash, (short) bucket.intValue());
                return bucket;
            } else {
                iterator.remove();
            }
        }

        // Check if we should refresh bucket information from disk before creating new bucket
        if (shouldRefresh && isReachedTheMinRefreshInterval(minRefreshInterval)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Refresh conditions met for partition {}. "
                                + "Bucket approaching capacity, threshold: {}",
                        partition,
                        minEmptyBucketsBeforeAsyncCheck);
            }
            refreshBucketsFromDisk();
        }

        int globalMaxBucketId = (maxBucketsNum == -1 ? Short.MAX_VALUE : maxBucketsNum) - 1;
        if (totalBucketSet.isEmpty() || maxBucketId < globalMaxBucketId) {
            // 3. create a new bucket
            for (int i = 0; i <= globalMaxBucketId; i++) {
                if (bucketFilter.test(i) && !totalBucketSet.contains(i)) {
                    nonFullBucketInformation.put(i, 1L);
                    totalBucketSet.add(i);
                    totalBucketArray.add(i);
                    hash2Bucket.put(hash, (short) i);
                    return i;
                }
            }
            if (-1 == maxBucketsNum) {
                throw new RuntimeException(
                        String.format(
                                "Too more bucket %s, you should increase target bucket row number %s.",
                                maxBucketId, targetBucketRowNumber));
            }
        }

        // 4. exceed buckets upper bound
        int bucket = ListUtils.pickRandomly(totalBucketArray);
        hash2Bucket.put(hash, (short) bucket);
        return bucket;
    }

    public static PartitionIndex loadIndex(
            IndexFileHandler indexFileHandler,
            BinaryRow partition,
            long targetBucketRowNumber,
            IntPredicate loadFilter,
            IntPredicate bucketFilter) {
        List<IndexManifestEntry> files = indexFileHandler.scanEntries(HASH_INDEX, partition);
        Int2ShortHashMap.Builder mapBuilder = Int2ShortHashMap.builder();
        ConcurrentHashMap<Integer, Long> buckets = new ConcurrentHashMap<>();
        for (IndexManifestEntry file : files) {
            try (IntIterator iterator =
                    indexFileHandler
                            .hashIndex(file.partition(), file.bucket())
                            .read(file.indexFile())) {
                while (true) {
                    try {
                        int hash = iterator.next();
                        if (loadFilter.test(hash)) {
                            mapBuilder.put(hash, (short) file.bucket());
                        }
                        if (bucketFilter.test(file.bucket())) {
                            buckets.compute(
                                    file.bucket(),
                                    (bucket, number) -> number == null ? 1 : number + 1);
                        }
                    } catch (EOFException ignored) {
                        break;
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return new PartitionIndex(
                mapBuilder.build(), buckets, targetBucketRowNumber, indexFileHandler, partition);
    }

    private boolean shouldRefreshWhenBucketNearFull(
            long currentBucketRowCount,
            long targetBucketRowNumber,
            int minEmptyBucketsBeforeAsyncCheck) {
        // Only refresh if:
        // 1. Feature is enabled (minEmptyBucketsBeforeAsyncCheck != -1)
        // 2. Current bucket is approaching its target capacity
        // When bucket reaches (targetBucketRowNumber - minEmptyBucketsBeforeAsyncCheck),
        // trigger refresh to find buckets freed by compaction
        if (minEmptyBucketsBeforeAsyncCheck == -1) {
            return false;
        }

        long threshold = targetBucketRowNumber - minEmptyBucketsBeforeAsyncCheck;
        return currentBucketRowCount >= threshold;
    }

    private boolean isReachedTheMinRefreshInterval(final Duration duration) {
        return Instant.now().isAfter(lastRefreshTime.plus(duration));
    }

    /**
     * Cancel any ongoing refresh operation. Should be called when this PartitionIndex is no longer
     * needed (e.g., during cleanup in prepareCommit).
     */
    public void cancelOngoingRefresh() {
        if (refreshFuture != null && !refreshFuture.isDone()) {
            refreshFuture.cancel(false);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Cancelled ongoing refresh for partition {}", partition);
            }
        }
    }

    private void refreshBucketsFromDisk() {
        // Only start refresh if not already in progress
        if (refreshFuture == null || refreshFuture.isDone()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Triggering async refresh of bucket information for partition {}. "
                                + "Current non-full buckets: {}",
                        partition,
                        nonFullBucketInformation.size());
            }

            // With unbounded queue, tasks are never rejected
            // Note: Using exceptionally for error handling (Java 8 compatible)
            // Timeout is handled by the ThreadPoolExecutor's keep-alive time
            refreshFuture =
                    CompletableFuture.runAsync(
                                    () -> {
                                        try {
                                            List<IndexManifestEntry> files =
                                                    indexFileHandler.scanEntries(
                                                            HASH_INDEX, partition);

                                            // Use parallel stream to scan multiple files
                                            // concurrently
                                            // This reduces latency when dealing with many buckets
                                            Map<Integer, Long> tempBucketInfo =
                                                    files.parallelStream()
                                                            .filter(
                                                                    file ->
                                                                            file.indexFile()
                                                                                            .rowCount()
                                                                                    < targetBucketRowNumber)
                                                            .collect(
                                                                    Collectors.toMap(
                                                                            IndexManifestEntry
                                                                                    ::bucket,
                                                                            file ->
                                                                                    file.indexFile()
                                                                                            .rowCount(),
                                                                            (existing,
                                                                                    replacement) ->
                                                                                    existing));

                                            // Use putIfAbsent to avoid race conditions
                                            int newBucketsFound = 0;
                                            for (Map.Entry<Integer, Long> entry :
                                                    tempBucketInfo.entrySet()) {
                                                Long previous =
                                                        nonFullBucketInformation.putIfAbsent(
                                                                entry.getKey(), entry.getValue());
                                                if (previous == null) {
                                                    newBucketsFound++;
                                                }
                                            }

                                            lastRefreshTime = Instant.now();

                                            if (LOG.isDebugEnabled()) {
                                                LOG.debug(
                                                        "Async refresh completed for partition {}. "
                                                                + "Scanned {} files, found {} non-full buckets ({} new). "
                                                                + "Current non-full buckets: {}",
                                                        partition,
                                                        files.size(),
                                                        tempBucketInfo.size(),
                                                        newBucketsFound,
                                                        nonFullBucketInformation.size());
                                            }
                                        } catch (Exception e) {
                                            LOG.warn(
                                                    "Error refreshing buckets from disk for partition {}: {}",
                                                    partition,
                                                    e.getMessage(),
                                                    e);
                                        }
                                    },
                                    REFRESH_EXECUTOR)
                            .exceptionally(
                                    throwable -> {
                                        LOG.warn(
                                                "Error during bucket refresh for partition {}: {}",
                                                partition,
                                                throwable.getMessage());
                                        return null;
                                    });
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Skipping refresh for partition {} - refresh already in progress",
                        partition);
            }
        }
    }
}
