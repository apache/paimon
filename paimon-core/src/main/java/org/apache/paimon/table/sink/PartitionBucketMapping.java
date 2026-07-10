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
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

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
 * @see #loadFromTable(FileStoreTable)
 * @see #resolveNumBuckets(BinaryRow)
 */
public class PartitionBucketMapping implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final ConcurrentMap<CacheKey, PartitionBucketMapping> CACHE =
            new ConcurrentHashMap<>();
    private static final ConcurrentMap<CacheKey, Object> CACHE_LOCKS = new ConcurrentHashMap<>();

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
        this.partitionBucketMap = Collections.unmodifiableMap(new HashMap<>(partitionBucketMap));
    }

    /**
     * Loads a {@link PartitionBucketMapping} by scanning the manifest entries of the given table.
     *
     * <p>For non-partitioned tables, this returns a mapping with only the schema-defined default
     * bucket count and an empty partition map.
     *
     * <p>For partitioned tables, the method scans all manifest entries and records the {@code
     * totalBuckets} value for each partition. If the scan fails for any reason, a fallback mapping
     * with only the default bucket count is returned.
     *
     * @param table the {@link FileStoreTable} to load the mapping from
     * @return a {@link PartitionBucketMapping} reflecting the current bucket layout of the table
     */
    public static PartitionBucketMapping loadFromTable(FileStoreTable table) {
        if (!table.coreOptions().partitionBucketMappingCacheEnabled()) {
            return loadFromTableWithoutCache(table);
        }

        CacheKey key = CacheKey.from(table);
        return getOrLoad(key, () -> loadFromTableWithoutCache(table));
    }

    static PartitionBucketMapping getOrLoad(
            CacheKey key, Supplier<PartitionBucketMapping> mappingLoader) {
        PartitionBucketMapping cached = CACHE.get(key);
        if (cached != null) {
            return cached;
        }

        Object lock = CACHE_LOCKS.computeIfAbsent(key, ignored -> new Object());
        synchronized (lock) {
            try {
                cached = CACHE.get(key);
                if (cached != null) {
                    return cached;
                }

                PartitionBucketMapping loaded = mappingLoader.get();
                CACHE.put(key, loaded);
                return loaded;
            } finally {
                CACHE_LOCKS.remove(key);
            }
        }
    }

    static PartitionBucketMapping loadFromTableWithoutCache(FileStoreTable table) {
        int defaultBuckets = table.schema().numBuckets();
        if (table.partitionKeys().isEmpty()) {
            return new PartitionBucketMapping(defaultBuckets, Collections.emptyMap());
        }

        List<SimpleFileEntry> entries = table.store().newScan().readSimpleEntries();
        return loadFromEntries(entries, table.schema());
    }

    static void clearCache() {
        CACHE.clear();
        CACHE_LOCKS.clear();
    }

    static PartitionBucketMapping getCachedMapping(FileStoreTable table) {
        return CACHE.get(CacheKey.from(table));
    }

    public static PartitionBucketMapping loadFromEntries(
            List<? extends FileEntry> entries, TableSchema tableSchema) {
        int defaultBuckets = tableSchema.numBuckets();
        if (tableSchema.partitionKeys().isEmpty()) {
            return new PartitionBucketMapping(defaultBuckets, Collections.emptyMap());
        }

        Map<BinaryRow, Integer> partitionBucketMap = new HashMap<>();
        for (FileEntry entry : entries) {
            int totalBuckets = entry.totalBuckets();
            // Only store partitions whose bucket count differs from the default.
            // This keeps the map empty for partitions that have never been rescaled,
            // avoiding per-partition BinaryRow copies and Integer allocations entirely.
            if (totalBuckets > 0 && totalBuckets != defaultBuckets) {
                BinaryRow partition = entry.partition();
                partitionBucketMap.putIfAbsent(partition.copy(), totalBuckets);
            }
        }
        return new PartitionBucketMapping(defaultBuckets, partitionBucketMap);
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

    static class CacheKey {
        private final String tablePath;
        private final long latestSnapshotId;
        private final long schemaId;
        private final int defaultBucketCount;

        CacheKey(String tablePath, long latestSnapshotId, long schemaId, int defaultBucketCount) {
            this.tablePath = tablePath;
            this.latestSnapshotId = latestSnapshotId;
            this.schemaId = schemaId;
            this.defaultBucketCount = defaultBucketCount;
        }

        private static CacheKey from(FileStoreTable table) {
            Long latestSnapshotId = table.snapshotManager().latestSnapshotId();
            return new CacheKey(
                    table.location().toString(),
                    latestSnapshotId == null ? -1L : latestSnapshotId,
                    table.schema().id(),
                    table.schema().numBuckets());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof CacheKey)) {
                return false;
            }
            CacheKey cacheKey = (CacheKey) o;
            return latestSnapshotId == cacheKey.latestSnapshotId
                    && schemaId == cacheKey.schemaId
                    && defaultBucketCount == cacheKey.defaultBucketCount
                    && Objects.equals(tablePath, cacheKey.tablePath);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tablePath, latestSnapshotId, schemaId, defaultBucketCount);
        }
    }
}
