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

import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.utils.Int2ShortHashMap;
import org.apache.paimon.utils.IntIterator;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.paimon.index.HashIndexFile.HASH_INDEX;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Assign bucket for key hashcode. */
public class HashBucketAssigner {

    private static final Logger LOG = LoggerFactory.getLogger(HashBucketAssigner.class);

    private final SnapshotManager snapshotManager;
    private final String commitUser;
    private final IndexFileHandler indexFileHandler;
    private final int numAssigners;
    private final int assignId;
    private final long targetBucketRowNumber;

    private final Map<BinaryRow, PartitionIndex> partitionIndex;

    public HashBucketAssigner(
            SnapshotManager snapshotManager,
            String commitUser,
            IndexFileHandler indexFileHandler,
            int numAssigners,
            int assignId,
            long targetBucketRowNumber) {
        this.snapshotManager = snapshotManager;
        this.commitUser = commitUser;
        this.indexFileHandler = indexFileHandler;
        this.numAssigners = numAssigners;
        this.assignId = assignId;
        this.targetBucketRowNumber = targetBucketRowNumber;
        this.partitionIndex = new HashMap<>();
    }

    /** Assign a bucket for key hash of a record. */
    public int assign(BinaryRow partition, int hash) {
        int recordAssignId = computeAssignId(hash);
        checkArgument(
                recordAssignId == assignId,
                "This is a bug, record assign id %s should equal to assign id %s.",
                recordAssignId,
                assignId);

        // 1. is it a key that has appeared before
        PartitionIndex index = partitionIndex.computeIfAbsent(partition, this::loadIndex);
        index.accessed = true;
        Int2ShortHashMap hash2Bucket = index.hash2Bucket;
        if (hash2Bucket.containsKey(hash)) {
            return hash2Bucket.get(hash);
        }

        // 2. find bucket from existing buckets
        Map<Integer, Long> buckets = index.bucketInformation;
        for (Integer bucket : buckets.keySet()) {
            if (computeAssignId(bucket) == assignId) {
                // it is my bucket
                Long number = buckets.get(bucket);
                if (number < targetBucketRowNumber) {
                    buckets.put(bucket, number + 1);
                    hash2Bucket.put(hash, bucket.shortValue());
                    return bucket;
                }
            }
        }

        // 3. create a new bucket
        for (int i = 0; i < Short.MAX_VALUE; i++) {
            if (computeAssignId(i) == assignId && !buckets.containsKey(i)) {
                hash2Bucket.put(hash, (short) i);
                buckets.put(i, 1L);
                return i;
            }
        }

        @SuppressWarnings("OptionalGetWithoutIsPresent")
        int maxBucket = buckets.keySet().stream().mapToInt(Integer::intValue).max().getAsInt();
        throw new RuntimeException(
                String.format(
                        "To more bucket %s, you should increase target bucket row number %s.",
                        maxBucket, targetBucketRowNumber));
    }

    /** Prepare commit to clear outdated partition index. */
    public void prepareCommit(long commitIdentifier) {
        long latestCommittedIdentifier;
        if (partitionIndex.values().stream()
                        .mapToLong(i -> i.lastAccessedCommitIdentifier)
                        .max()
                        .orElse(Long.MIN_VALUE)
                == Long.MIN_VALUE) {
            // Optimization for the first commit.
            //
            // If this is the first commit, no index has previous modified commit, so the value of
            // `latestCommittedIdentifier` does not matter.
            //
            // Without this optimization, we may need to scan through all snapshots only to find
            // that there is no previous snapshot by this user, which is very inefficient.
            latestCommittedIdentifier = Long.MIN_VALUE;
        } else {
            latestCommittedIdentifier =
                    snapshotManager
                            .latestSnapshotOfUser(commitUser)
                            .map(Snapshot::commitIdentifier)
                            .orElse(Long.MIN_VALUE);
        }

        Iterator<Map.Entry<BinaryRow, PartitionIndex>> iterator =
                partitionIndex.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<BinaryRow, PartitionIndex> entry = iterator.next();
            BinaryRow partition = entry.getKey();
            PartitionIndex index = entry.getValue();
            if (index.accessed) {
                index.lastAccessedCommitIdentifier = commitIdentifier;
            } else {
                if (index.lastAccessedCommitIdentifier <= latestCommittedIdentifier) {
                    // Clear writer if no update, and if its latest modification has committed.
                    //
                    // We need a mechanism to clear index, otherwise there will be more and
                    // more such as yesterday's partition that no longer needs to be accessed.
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                                "Removing index for partition {}. "
                                        + "Index's last accessed identifier is {}, "
                                        + "while latest committed identifier is {}",
                                partition,
                                index.lastAccessedCommitIdentifier,
                                latestCommittedIdentifier);
                    }
                    iterator.remove();
                }
            }
            index.accessed = false;
        }
    }

    @VisibleForTesting
    Set<BinaryRow> currentPartitions() {
        return partitionIndex.keySet();
    }

    private int computeAssignId(int hash) {
        return Math.abs(hash % numAssigners);
    }

    private PartitionIndex loadIndex(BinaryRow partition) {
        Int2ShortHashMap map = new Int2ShortHashMap();
        List<IndexManifestEntry> files = indexFileHandler.scan(HASH_INDEX, partition);
        Map<Integer, Long> buckets = new HashMap<>();
        for (IndexManifestEntry file : files) {
            try (IntIterator iterator = indexFileHandler.readHashIndex(file.indexFile())) {
                while (true) {
                    try {
                        int hash = iterator.next();
                        if (computeAssignId(hash) == assignId) {
                            map.put(hash, (short) file.bucket());
                        }
                        buckets.compute(
                                file.bucket(), (bucket, number) -> number == null ? 1 : number + 1);
                    } catch (EOFException ignored) {
                        break;
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return new PartitionIndex(map, buckets);
    }

    private static class PartitionIndex {

        private final Int2ShortHashMap hash2Bucket;

        private final Map<Integer, Long> bucketInformation;

        private boolean accessed;

        private long lastAccessedCommitIdentifier;

        private PartitionIndex(Int2ShortHashMap hash2Bucket, Map<Integer, Long> bucketInformation) {
            this.hash2Bucket = hash2Bucket;
            this.bucketInformation = bucketInformation;
            this.lastAccessedCommitIdentifier = Long.MIN_VALUE;
            this.accessed = true;
        }
    }
}
