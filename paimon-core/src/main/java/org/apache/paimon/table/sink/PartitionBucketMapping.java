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
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.table.FileStoreTable;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        int defaultBuckets = table.schema().numBuckets();
        if (table.partitionKeys().isEmpty()) {
            return new PartitionBucketMapping(defaultBuckets, Collections.emptyMap());
        }

        try {
            List<ManifestEntry> entries = table.store().newScan().plan().files();
            Map<BinaryRow, Integer> partitionBucketMap = new HashMap<>();
            for (ManifestEntry entry : entries) {
                int totalBuckets = entry.totalBuckets();
                if (totalBuckets > 0) {
                    BinaryRow partition = entry.partition();
                    partitionBucketMap.putIfAbsent(partition.copy(), totalBuckets);
                }
            }

            return new PartitionBucketMapping(defaultBuckets, partitionBucketMap);
        } catch (Exception e) {
            return new PartitionBucketMapping(defaultBuckets, Collections.emptyMap());
        }
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
}
