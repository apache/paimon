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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PartitionBucketMapping}. */
public class PartitionBucketMappingTest {

    @Test
    public void testDefaultBucketCount() {
        PartitionBucketMapping mapping = new PartitionBucketMapping(16);

        // Any partition should resolve to the default
        assertThat(mapping.resolveNumBuckets(BinaryRow.EMPTY_ROW)).isEqualTo(16);
        assertThat(mapping.resolveNumBuckets(partition(1))).isEqualTo(16);
        assertThat(mapping.resolveNumBuckets(partition(42))).isEqualTo(16);
    }

    @Test
    public void testExplicitPartitionMapping() {
        BinaryRow partA = partition(1);
        BinaryRow partB = partition(2);
        BinaryRow partC = partition(3);

        Map<BinaryRow, Integer> partitionMap = new HashMap<>();
        partitionMap.put(partA, 32);
        partitionMap.put(partB, 64);

        PartitionBucketMapping mapping = new PartitionBucketMapping(16, partitionMap);

        // Mapped partitions return their specific bucket counts
        assertThat(mapping.resolveNumBuckets(partA)).isEqualTo(32);
        assertThat(mapping.resolveNumBuckets(partB)).isEqualTo(64);

        // Unmapped partition falls back to the default
        assertThat(mapping.resolveNumBuckets(partC)).isEqualTo(16);
    }

    @Test
    public void testLoadFromEntries_emptyList() {
        PartitionBucketMapping mapping =
                PartitionBucketMapping.loadFromEntries(
                        Collections.emptyList(), partitionedSchema(16));

        // With no entries, no per-partition mapping exists; everything returns the default.
        assertThat(mapping.resolveNumBuckets(partition(1))).isEqualTo(16);
        assertThat(mapping.resolveNumBuckets(BinaryRow.EMPTY_ROW)).isEqualTo(16);
    }

    @Test
    public void testLoadFromEntries_nonPartitionedTableSkipsScan() {
        // For non-partitioned tables, loadFromEntries must short-circuit and never
        // populate the per-partition map, even if entries report a different
        // totalBuckets value (which can happen mid-rescale or with stale snapshots).
        // Otherwise it would trigger spurious "bucket changed without overwrite"
        // errors at commit time on non-partitioned tables.
        List<SimpleFileEntry> entries =
                Arrays.asList(entry(BinaryRow.EMPTY_ROW, 0, 1), entry(BinaryRow.EMPTY_ROW, 0, 4));

        PartitionBucketMapping mapping =
                PartitionBucketMapping.loadFromEntries(entries, nonPartitionedSchema(2));

        // Should resolve to the schema default, not anything from the entries.
        assertThat(mapping.resolveNumBuckets(BinaryRow.EMPTY_ROW)).isEqualTo(2);
    }

    @Test
    public void testLoadFromEntries_allDefaultBuckets() {
        // Entries whose totalBuckets matches the default are intentionally not stored
        // in the per-partition map (memory optimisation), but resolveNumBuckets must
        // still return the default for those partitions.
        List<SimpleFileEntry> entries =
                Arrays.asList(
                        entry(partition(1), 0, 16),
                        entry(partition(2), 0, 16),
                        entry(partition(3), 1, 16));

        PartitionBucketMapping mapping =
                PartitionBucketMapping.loadFromEntries(entries, partitionedSchema(16));

        assertThat(mapping.resolveNumBuckets(partition(1))).isEqualTo(16);
        assertThat(mapping.resolveNumBuckets(partition(2))).isEqualTo(16);
        assertThat(mapping.resolveNumBuckets(partition(3))).isEqualTo(16);
        // Unseen partition still returns default.
        assertThat(mapping.resolveNumBuckets(partition(99))).isEqualTo(16);
    }

    @Test
    public void testLoadFromEntries_heterogeneousBuckets() {
        // Reproduces the scenario from the FileSystemWriteRestore bug fix:
        // table default = 32, but partition A has been rescaled to 2 buckets
        // and partition B to 64. Partition C uses default (no entry needed).
        BinaryRow partA = partition(1);
        BinaryRow partB = partition(2);
        BinaryRow partC = partition(3);

        List<SimpleFileEntry> entries =
                Arrays.asList(
                        entry(partA, 0, 2),
                        entry(partA, 1, 2),
                        entry(partB, 0, 64),
                        entry(partC, 0, 32));

        PartitionBucketMapping mapping =
                PartitionBucketMapping.loadFromEntries(entries, partitionedSchema(32));

        assertThat(mapping.resolveNumBuckets(partA)).isEqualTo(2);
        assertThat(mapping.resolveNumBuckets(partB)).isEqualTo(64);
        // partC matches default and was skipped from the map; resolves to default.
        assertThat(mapping.resolveNumBuckets(partC)).isEqualTo(32);
        // unseen partition resolves to default.
        assertThat(mapping.resolveNumBuckets(partition(99))).isEqualTo(32);
    }

    @Test
    public void testLoadFromEntries_zeroOrNegativeTotalBucketsIgnored() {
        // Entries with totalBuckets <= 0 represent metadata/legacy entries that
        // should not influence the mapping.
        BinaryRow partA = partition(1);

        List<SimpleFileEntry> entries = Arrays.asList(entry(partA, 0, 0), entry(partA, 1, -1));

        PartitionBucketMapping mapping =
                PartitionBucketMapping.loadFromEntries(entries, partitionedSchema(32));

        // Nothing was stored; partA resolves to the default.
        assertThat(mapping.resolveNumBuckets(partA)).isEqualTo(32);
    }

    @Test
    public void testLoadFromEntries_putIfAbsentSemantics() {
        // If multiple entries for the same partition somehow report different
        // totalBuckets values, the first observed value is kept (putIfAbsent
        // semantics in loadFromEntries). This is a defensive contract test.
        BinaryRow partA = partition(1);

        List<SimpleFileEntry> entries = Arrays.asList(entry(partA, 0, 2), entry(partA, 1, 4));

        PartitionBucketMapping mapping =
                PartitionBucketMapping.loadFromEntries(entries, partitionedSchema(32));

        assertThat(mapping.resolveNumBuckets(partA)).isEqualTo(2);
    }

    @Test
    public void testGetOrLoadCachesMappingByKey() {
        PartitionBucketMapping.clearCache();
        PartitionBucketMapping.CacheKey key =
                new PartitionBucketMapping.CacheKey("table", 1, 0, 32);
        AtomicInteger loadCount = new AtomicInteger();

        PartitionBucketMapping first =
                PartitionBucketMapping.getOrLoad(
                        key,
                        () -> {
                            loadCount.incrementAndGet();
                            return new PartitionBucketMapping(32);
                        });
        PartitionBucketMapping second =
                PartitionBucketMapping.getOrLoad(
                        key,
                        () -> {
                            loadCount.incrementAndGet();
                            return new PartitionBucketMapping(64);
                        });

        assertThat(second).isSameAs(first);
        assertThat(second.resolveNumBuckets(partition(1))).isEqualTo(32);
        assertThat(loadCount).hasValue(1);
        PartitionBucketMapping.clearCache();
    }

    @Test
    public void testGetOrLoadSingleFlightForConcurrentWriters() throws Exception {
        PartitionBucketMapping.clearCache();
        PartitionBucketMapping.CacheKey key =
                new PartitionBucketMapping.CacheKey("table", 1, 0, 32);
        AtomicInteger loadCount = new AtomicInteger();
        CountDownLatch loaderStarted = new CountDownLatch(1);
        CountDownLatch releaseLoader = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<PartitionBucketMapping> first =
                    executor.submit(
                            () ->
                                    PartitionBucketMapping.getOrLoad(
                                            key,
                                            () -> {
                                                loadCount.incrementAndGet();
                                                loaderStarted.countDown();
                                                await(releaseLoader);
                                                return new PartitionBucketMapping(32);
                                            }));

            loaderStarted.await();

            Future<PartitionBucketMapping> second =
                    executor.submit(
                            () ->
                                    PartitionBucketMapping.getOrLoad(
                                            key,
                                            () -> {
                                                loadCount.incrementAndGet();
                                                return new PartitionBucketMapping(64);
                                            }));

            releaseLoader.countDown();

            PartitionBucketMapping firstMapping = first.get();
            PartitionBucketMapping secondMapping = second.get();
            assertThat(secondMapping).isSameAs(firstMapping);
            assertThat(loadCount).hasValue(1);
        } finally {
            executor.shutdownNow();
            PartitionBucketMapping.clearCache();
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static BinaryRow partition(int value) {
        return BinaryRow.singleColumn(value);
    }

    private static SimpleFileEntry entry(BinaryRow partition, int bucket, int totalBuckets) {
        return new SimpleFileEntry(
                FileKind.ADD,
                partition,
                bucket,
                totalBuckets,
                0,
                "data-" + partition.hashCode() + "-" + bucket + ".parquet",
                Collections.emptyList(),
                null,
                BinaryRow.EMPTY_ROW,
                BinaryRow.EMPTY_ROW,
                null);
    }

    /**
     * Builds a minimal {@link TableSchema} for a partitioned table with the given default bucket
     * count. The schema declares a single partition column ("p") and a single value column ("v").
     */
    private static TableSchema partitionedSchema(int defaultBuckets) {
        return schema(Collections.singletonList("p"), defaultBuckets);
    }

    /** Builds a minimal {@link TableSchema} for a non-partitioned table. */
    private static TableSchema nonPartitionedSchema(int defaultBuckets) {
        return schema(Collections.emptyList(), defaultBuckets);
    }

    private static TableSchema schema(List<String> partitionKeys, int defaultBuckets) {
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "p", new IntType()), new DataField(1, "v", new IntType()));
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BUCKET.key(), String.valueOf(defaultBuckets));
        return new TableSchema(
                0,
                fields,
                RowType.currentHighestFieldId(fields),
                partitionKeys,
                Collections.emptyList(),
                options,
                "");
    }
}
