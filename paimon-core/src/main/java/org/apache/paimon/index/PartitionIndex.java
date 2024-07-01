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

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.IntPredicate;

import static org.apache.paimon.index.HashIndexFile.HASH_INDEX;

/** Bucket Index Per Partition. */
public class PartitionIndex {

    public final Int2ShortHashMap hash2Bucket;

    public final Map<Integer, Long> nonFullBucketInformation;

    public final Set<Integer> totalBucket;

    private final long targetBucketRowNumber;

    public boolean accessed;

    public long lastAccessedCommitIdentifier;

    public PartitionIndex(
            Int2ShortHashMap hash2Bucket,
            Map<Integer, Long> bucketInformation,
            long targetBucketRowNumber) {
        this.hash2Bucket = hash2Bucket;
        this.nonFullBucketInformation = bucketInformation;
        this.totalBucket = new HashSet<>(bucketInformation.keySet());
        this.targetBucketRowNumber = targetBucketRowNumber;
        this.lastAccessedCommitIdentifier = Long.MIN_VALUE;
        this.accessed = true;
    }

    public int assign(int hash, IntPredicate bucketFilter) {
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
            if (number < targetBucketRowNumber) {
                entry.setValue(number + 1);
                hash2Bucket.put(hash, bucket.shortValue());
                return bucket;
            } else {
                iterator.remove();
            }
        }

        // 3. create a new bucket
        for (int i = 0; i < Short.MAX_VALUE; i++) {
            if (bucketFilter.test(i) && !totalBucket.contains(i)) {
                hash2Bucket.put(hash, (short) i);
                nonFullBucketInformation.put(i, 1L);
                totalBucket.add(i);
                return i;
            }
        }

        @SuppressWarnings("OptionalGetWithoutIsPresent")
        int maxBucket = totalBucket.stream().mapToInt(Integer::intValue).max().getAsInt();
        throw new RuntimeException(
                String.format(
                        "Too more bucket %s, you should increase target bucket row number %s.",
                        maxBucket, targetBucketRowNumber));
    }

    public static PartitionIndex loadIndex(
            IndexFileHandler indexFileHandler,
            BinaryRow partition,
            long targetBucketRowNumber,
            IntPredicate loadFilter,
            IntPredicate bucketFilter) {
        List<IndexManifestEntry> files = indexFileHandler.scanEntries(HASH_INDEX, partition);
        Int2ShortHashMap.Builder mapBuilder = Int2ShortHashMap.builder();
        Map<Integer, Long> buckets = new HashMap<>();
        for (IndexManifestEntry file : files) {
            try (IntIterator iterator = indexFileHandler.readHashIndex(file.indexFile())) {
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
        return new PartitionIndex(mapBuilder.build(), buckets, targetBucketRowNumber);
    }
}
