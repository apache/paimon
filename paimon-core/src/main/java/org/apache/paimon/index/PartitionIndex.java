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
import java.util.Iterator;
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

    public final Set<Integer> totalBucketSet;
    public final List<Integer> totalBucketArray;

    private final long targetBucketRowNumber;

    public boolean accessed;

    public long lastAccessedCommitIdentifier;

    private final IndexFileHandler indexFileHandler;

    private final BinaryRow partition;

    private volatile CompletableFuture<Void> refreshFuture;

    // null = never refreshed; the first refresh is not throttled by min-refresh-interval.
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
        Iterator<Map.Entry<Integer, Long>> iterator =
                nonFullBucketInformation.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Integer, Long> entry = iterator.next();
            Integer bucket = entry.getKey();
            Long number = entry.getValue();
            // Trigger an async refresh as soon as we see a bucket at/over the near-full
            // threshold, regardless of whether we end up assigning to it or removing it.
            // The refresh is non-blocking and respects the minimum refresh interval, so
            // subsequent assignments can see buckets freed by compaction.
            if (shouldRefreshWhenBucketNearFull(
                            number, targetBucketRowNumber, minEmptyBucketsBeforeAsyncCheck)
                    && isReachedTheMinRefreshInterval(minRefreshInterval)) {
                refreshBucketsFromDisk(bucketFilter);
            }
            if (number < targetBucketRowNumber) {
                entry.setValue(number + 1);
                hash2Bucket.put(hash, (short) bucket.intValue());
                return bucket;
            } else {
                iterator.remove();
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
        return lastRefreshTime == null || Instant.now().isAfter(lastRefreshTime.plus(duration));
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

    private void refreshBucketsFromDisk(IntPredicate bucketFilter) {
        // Only start refresh if not already in progress
        if (refreshFuture == null || refreshFuture.isDone()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Triggering async refresh of bucket information for partition {}. "
                                + "Current non-full buckets: {}",
                        partition,
                        nonFullBucketInformation.size());
            }

            // Reuse the shared FileOperationThreadPool instead of creating a dedicated
            // executor: refresh is an I/O-bound file operation (scanning index manifest
            // entries) and the shared pool already exists for this purpose. This avoids
            // duplicating thread-pool resources and aligns with how other file-scan paths
            // in Paimon (FileDeletionBase, TableCommitImpl, ListUnexistingFiles) work.
            refreshFuture =
                    CompletableFuture.runAsync(
                                    () -> {
                                        try {
                                            List<IndexManifestEntry> files =
                                                    indexFileHandler.scanEntries(
                                                            HASH_INDEX, partition);

                                            // Use parallel stream to scan multiple files
                                            // concurrently
                                            // This reduces latency when dealing with many buckets.
                                            // Apply bucketFilter so we only surface buckets owned
                                            // by this assigner; otherwise multiple assigners would
                                            // race for buckets they do not own.
                                            Map<Integer, Long> tempBucketInfo =
                                                    files.parallelStream()
                                                            .filter(
                                                                    file ->
                                                                            bucketFilter.test(
                                                                                    file.bucket()))
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
                                    FileOperationThreadPool.getExecutorService(
                                            Runtime.getRuntime().availableProcessors()))
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
