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

package org.apache.paimon.table.sink;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.table.FileStoreTable;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A mapping that resolves the number of buckets for each partition in a table.
 *
 * <p>Different partitions may have different bucket counts (e.g., after a rescale operation). This
 * class maintains a per-partition bucket count mapping and falls back to a default bucket count for
 * partitions that are not explicitly mapped.
 *
 * <p>This is used by components such as {@link FixedBucketRowKeyExtractor} and {@link
 * FixedBucketWriteSelector} to correctly determine the bucket assignment for rows in tables where
 * partitions may have been rescaled independently.
 *
 * @see #lazyLoadFromTable(FileStoreTable)
 * @see #resolveNumBuckets(BinaryRow)
 */
public class PartitionBucketMapping implements Serializable {

    private static final long serialVersionUID = 1L;

    /** The default number of buckets, used when a partition has no explicit mapping. */
    private final int defaultBucketCount;

    /** A map from partition to its specific bucket count. May be empty but never {@code null}. */
    private final Map<BinaryRow, Integer> partitionBucketMap;

    /**
     * Creates a mapping with only a default bucket count and no per-partition overrides.
     *
     * @param defaultBucketCount the default number of buckets for all partitions
     */
    public PartitionBucketMapping(int defaultBucketCount) {
        this(defaultBucketCount, Collections.emptyMap());
    }

    /**
     * Creates a mapping with a default bucket count and an explicit per-partition bucket map.
     *
     * @param defaultBucketCount the default number of buckets, used as a fallback
     * @param partitionBucketMap a map from partition (as {@link BinaryRow}) to its bucket count
     */
    public PartitionBucketMapping(
            int defaultBucketCount, Map<BinaryRow, Integer> partitionBucketMap) {
        this.defaultBucketCount = defaultBucketCount;
        this.partitionBucketMap = partitionBucketMap;
    }

    /**
     * Creates a {@link PartitionBucketMapping} that lazily loads per-partition bucket counts on
     * demand.
     *
     * <p>This method defers IO to the point where a specific partition's bucket count is requested.
     * Each partition's manifest entries are fetched only once and cached.
     *
     * <p>For non-partitioned tables, this returns a mapping with only the schema-defined default
     * bucket count and an empty partition map (no lazy loading needed).
     *
     * @param table the {@link FileStoreTable} to lazily load the mapping from
     * @return a {@link PartitionBucketMapping} that fetches partition bucket counts on demand
     */
    public static PartitionBucketMapping lazyLoadFromTable(FileStoreTable table) {
        int defaultBuckets = table.schema().numBuckets();
        if (table.partitionKeys().isEmpty()) {
            return new PartitionBucketMapping(defaultBuckets, Collections.emptyMap());
        }

        return new PartitionBucketMapping(defaultBuckets, new LazyPartitionBucketMap(table));
    }

    /**
     * Resolves the number of buckets for the given partition.
     *
     * <p>If the partition has an explicit entry in the partition-to-bucket map, that value is
     * returned. Otherwise, the default bucket count is returned.
     *
     * @param partition the partition key as a {@link BinaryRow}
     * @return the number of buckets for the given partition
     */
    public int resolveNumBuckets(BinaryRow partition) {
        if (partitionBucketMap != null) {
            Integer partitionBucketCount = partitionBucketMap.get(partition);
            if (partitionBucketCount != null) {
                return partitionBucketCount;
            }
        }
        return defaultBucketCount;
    }

    /**
     * A lazily-populated map from partition ({@link BinaryRow}) to bucket count.
     *
     * <p>When {@link #get(Object)} is called for a partition not yet in the cache, a
     * partition-filtered scan is performed to read only that partition's manifest entries and
     * extract the {@code totalBuckets} value. Results are cached in a {@link ConcurrentHashMap} so
     * each partition is scanned at most once.
     *
     * <p>This avoids the upfront cost of reading all manifest files when only a subset of
     * partitions will actually be queried.
     */
    static class LazyPartitionBucketMap extends AbstractMap<BinaryRow, Integer>
            implements Serializable {

        private static final long serialVersionUID = 1L;

        private final transient FileStoreTable table;
        private final ConcurrentHashMap<BinaryRow, Integer> cache;

        LazyPartitionBucketMap(FileStoreTable table) {
            this.table = table;
            this.cache = new ConcurrentHashMap<>();
        }

        @Override
        public Integer get(Object key) {
            if (!(key instanceof BinaryRow) || table == null) {
                return null;
            }
            BinaryRow partition = (BinaryRow) key;
            Integer cached = cache.get(partition);
            if (cached != null) {
                return cached;
            }
            return loadPartition(partition);
        }

        private Integer loadPartition(BinaryRow partition) {
            try {
                List<SimpleFileEntry> entries =
                        table.store()
                                .newScan()
                                .onlyReadRealBuckets()
                                .withPartitionFilter(Collections.singletonList(partition))
                                .readSimpleEntries();
                for (SimpleFileEntry entry : entries) {
                    int totalBuckets = entry.totalBuckets();
                    if (totalBuckets > 0) {
                        cache.put(partition.copy(), totalBuckets);
                        return totalBuckets;
                    }
                }
            } catch (Exception ignored) {
                // Fall through to return null, same as the eager fallback behavior
            }
            // Partition not found or has no totalBuckets — return null so
            // resolveNumBuckets falls back to the default. We intentionally do not
            // cache absent results: the partition may appear in a later snapshot.
            return null;
        }

        @Override
        public boolean containsKey(Object key) {
            return get(key) != null;
        }

        @Override
        public Set<Entry<BinaryRow, Integer>> entrySet() {
            // Return only the currently cached entries. This does not trigger lazy loading.
            return Collections.unmodifiableMap(new HashMap<>(cache)).entrySet();
        }

        @Override
        public int size() {
            return cache.size();
        }

        @Override
        public boolean isEmpty() {
            return cache.isEmpty();
        }
    }
}
