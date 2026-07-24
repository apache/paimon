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
import org.apache.paimon.utils.FileOperationThreadPool;
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
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;

import static org.apache.paimon.index.HashIndexFile.HASH_INDEX;

/** Bucket Index Per Partition. */
public class PartitionIndex {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionIndex.class);

    public final Int2ShortHashMap hash2Bucket;

    public final ConcurrentHashMap<Integer, Long> nonFullBucketInformation;

    // Disk row count per bucket at the last reconciliation; sessionDelta = inMemory - this.
    private final ConcurrentHashMap<Integer, Long> diskRowCountAtLastRefresh;

    public final Set<Integer> totalBucketSet;
    public final List<Integer> totalBucketArray;

    private final long targetBucketRowNumber;

    public boolean accessed;

    public long lastAccessedCommitIdentifier;

    private final IndexFileHandler indexFileHandler;

    private final BinaryRow partition;

    private volatile CompletableFuture<Void> refreshFuture;

    // null = never refreshed; the first refresh is not throttled by min-refresh-interval.
    // volatile: written by the async refresh thread, read by the assign thread.
    private volatile Instant lastRefreshTime;

    public PartitionIndex(
            Int2ShortHashMap hash2Bucket,
            ConcurrentHashMap<Integer, Long> bucketInformation,
            long targetBucketRowNumber,
            IndexFileHandler indexFileHandler,
            BinaryRow partition) {
        this.hash2Bucket = hash2Bucket;
        this.nonFullBucketInformation = bucketInformation;
        this.diskRowCountAtLastRefresh = new ConcurrentHashMap<>(bucketInformation);
        this.totalBucketSet = new LinkedHashSet<>(bucketInformation.keySet());
        this.totalBucketArray = new ArrayList<>(totalBucketSet);
        this.targetBucketRowNumber = targetBucketRowNumber;
        this.lastAccessedCommitIdentifier = Long.MIN_VALUE;
        this.accessed = true;
        this.indexFileHandler = indexFileHandler;
        this.partition = partition;
        this.lastRefreshTime = null;
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
        for (Integer bucket : nonFullBucketInformation.keySet()) {
            Long number = nonFullBucketInformation.get(bucket);
            if (number == null) {
                continue; // concurrently removed by a background refresh
            }
            // Non-blocking: lets later assignments see buckets freed by compaction.
            if (shouldRefreshWhenBucketNearFull(
                            number, targetBucketRowNumber, minEmptyBucketsBeforeAsyncCheck)
                    && isReachedTheMinRefreshInterval(minRefreshInterval)) {
                refreshBucketsFromDisk(bucketFilter);
            }
            // Atomic update so we don't clobber a value a concurrent refresh may have reconciled.
            Long assigned =
                    nonFullBucketInformation.computeIfPresent(
                            bucket,
                            (b, currentNumber) -> {
                                if (currentNumber < targetBucketRowNumber) {
                                    return currentNumber + 1;
                                }
                                diskRowCountAtLastRefresh.remove(b);
                                return null;
                            });
            if (assigned != null) {
                hash2Bucket.put(hash, (short) bucket.intValue());
                return bucket;
            }
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
        if (minEmptyBucketsBeforeAsyncCheck == -1) {
            return false;
        }

        long threshold = targetBucketRowNumber - minEmptyBucketsBeforeAsyncCheck;
        return currentBucketRowCount >= threshold;
    }

    private boolean isReachedTheMinRefreshInterval(final Duration duration) {
        return lastRefreshTime == null || Instant.now().isAfter(lastRefreshTime.plus(duration));
    }

    /** Called when this PartitionIndex is dropped (e.g. cleanup in prepareCommit). */
    public void cancelOngoingRefresh() {
        if (refreshFuture != null && !refreshFuture.isDone()) {
            refreshFuture.cancel(false);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Cancelled ongoing refresh for partition {}", partition);
            }
        }
    }

    // Merges disk row count into nonFullBucketInformation, keeping uncommitted increments.
    private void reconcileBucketWithDisk(int bucket, long diskNow, Set<Integer> knownBuckets) {
        if (!knownBuckets.contains(bucket)) {
            return;
        }
        nonFullBucketInformation.compute(
                bucket,
                (b, inMemory) ->
                        inMemory == null
                                ? diskNow
                                : diskNow
                                        + (inMemory
                                                - diskRowCountAtLastRefresh.getOrDefault(
                                                        b, inMemory)));
        diskRowCountAtLastRefresh.put(bucket, diskNow);
    }

    private void refreshBucketsFromDisk(IntPredicate bucketFilter) {
        if (refreshFuture == null || refreshFuture.isDone()) {
            // Snapshot the known buckets on the assign thread; the refresh reads this copy instead
            // of the live, non-thread-safe totalBucketSet that assign may mutate concurrently.
            Set<Integer> knownBuckets = new HashSet<>(totalBucketSet);
            // Reuse the shared FileOperationThreadPool (I/O-bound scan) rather than a dedicated
            // executor, to avoid extra thread-pool resources and fan-out.
            refreshFuture =
                    CompletableFuture.runAsync(
                                    () -> scanAndReconcile(bucketFilter, knownBuckets),
                                    FileOperationThreadPool.getExecutorService(
                                            Runtime.getRuntime().availableProcessors()))
                            .exceptionally(
                                    t -> {
                                        LOG.warn(
                                                "Bucket refresh failed for partition {}",
                                                partition,
                                                t);
                                        return null;
                                    });
        }
    }

    private void scanAndReconcile(IntPredicate bucketFilter, Set<Integer> knownBuckets) {
        try {
            List<IndexManifestEntry> files = indexFileHandler.scanEntries(HASH_INDEX, partition);
            // bucketFilter keeps only buckets owned by this assigner, so assigners never race.
            Map<Integer, Long> tempBucketInfo =
                    files.parallelStream()
                            .filter(f -> bucketFilter.test(f.bucket()))
                            .filter(f -> f.indexFile().rowCount() < targetBucketRowNumber)
                            .collect(
                                    Collectors.toMap(
                                            IndexManifestEntry::bucket,
                                            f -> f.indexFile().rowCount(),
                                            (existing, replacement) -> existing));
            tempBucketInfo.forEach(
                    (bucket, diskNow) -> reconcileBucketWithDisk(bucket, diskNow, knownBuckets));
            lastRefreshTime = Instant.now();
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Refreshed partition {}: scanned {} files, {} non-full buckets.",
                        partition,
                        files.size(),
                        tempBucketInfo.size());
            }
        } catch (Exception e) {
            LOG.warn("Error refreshing buckets from disk for partition {}", partition, e);
        }
    }
}
