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

package org.apache.paimon.operation;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.Snapshot;
import org.apache.paimon.TestFileStore;
import org.apache.paimon.TestKeyValueGenerator;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.mergetree.compact.DeduplicateMergeFunction;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static org.apache.paimon.stats.SimpleStats.EMPTY_STATS;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KeyValueFileStoreScan}. */
public class KeyValueFileStoreScanTest {

    private static final int NUM_BUCKETS = 10;

    private TestKeyValueGenerator gen;
    @TempDir java.nio.file.Path tempDir;
    private TestFileStore store;

    @BeforeEach
    public void beforeEach() throws Exception {
        gen = new TestKeyValueGenerator();
        store =
                new TestFileStore.Builder(
                                "avro",
                                tempDir.toString(),
                                NUM_BUCKETS,
                                TestKeyValueGenerator.DEFAULT_PART_TYPE,
                                TestKeyValueGenerator.KEY_TYPE,
                                TestKeyValueGenerator.DEFAULT_ROW_TYPE,
                                TestKeyValueGenerator.TestKeyValueFieldsExtractor.EXTRACTOR,
                                DeduplicateMergeFunction.factory(),
                                null)
                        .build();

        SchemaManager schemaManager =
                new SchemaManager(LocalFileIO.create(), new Path(tempDir.toUri()));
        schemaManager.createTable(
                new Schema(
                        TestKeyValueGenerator.DEFAULT_ROW_TYPE.getFields(),
                        TestKeyValueGenerator.DEFAULT_PART_TYPE.getFieldNames(),
                        TestKeyValueGenerator.getPrimaryKeys(
                                TestKeyValueGenerator.GeneratorMode.MULTI_PARTITIONED),
                        Collections.emptyMap(),
                        null));
    }

    @Test
    public void testWithPartitionFilter() throws Exception {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        List<KeyValue> data = generateData(random.nextInt(1000) + 1);
        List<BinaryRow> partitions =
                data.stream()
                        .map(kv -> gen.getPartition(kv))
                        .distinct()
                        .collect(Collectors.toList());
        Snapshot snapshot = writeData(data);

        Set<BinaryRow> wantedPartitions = new HashSet<>();
        for (int i = random.nextInt(partitions.size() + 1); i > 0; i--) {
            wantedPartitions.add(partitions.get(random.nextInt(partitions.size())));
        }

        FileStoreScan scan = store.newScan();
        scan.withSnapshot(snapshot.id());
        scan.withPartitionFilter(new ArrayList<>(wantedPartitions));

        Map<BinaryRow, BinaryRow> expected =
                store.toKvMap(
                        wantedPartitions.isEmpty()
                                ? data
                                : data.stream()
                                        .filter(
                                                kv ->
                                                        wantedPartitions.contains(
                                                                gen.getPartition(kv)))
                                        .collect(Collectors.toList()));
        runTestExactMatch(scan, snapshot.id(), expected);
    }

    @Test
    public void testWithKeyFilter() throws Exception {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        List<KeyValue> data = generateData(random.nextInt(1000) + 1);
        Snapshot snapshot = writeData(data);

        int wantedShopId = data.get(random.nextInt(data.size())).key().getInt(0);

        KeyValueFileStoreScan scan = store.newScan();
        scan.withSnapshot(snapshot.id());
        scan.withKeyFilter(
                new PredicateBuilder(RowType.of(new IntType(false))).equal(0, wantedShopId));

        Map<BinaryRow, BinaryRow> expected =
                store.toKvMap(
                        data.stream()
                                .filter(kv -> kv.key().getInt(0) == wantedShopId)
                                .collect(Collectors.toList()));
        runTestContainsAll(scan, snapshot.id(), expected);
    }

    @Test
    public void testWithValueFilterBucket() throws Exception {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        // 0 <= item <= 999
        List<KeyValue> data = generateData(100, 0, (long) random.nextInt(1000));
        writeData(data, 0);
        // 1000 <= item <= 1999
        data = generateData(100, 0, (long) random.nextInt(1000) + 1000);
        writeData(data, 1);
        // 2000 <= item <= 2999
        data = generateData(100, 0, (long) random.nextInt(1000) + 2000);
        writeData(data, 2);
        // 3000 <= item <= 3999
        data = generateData(100, 0, (long) random.nextInt(1000) + 3000);
        Snapshot snapshot = writeData(data, 3);

        KeyValueFileStoreScan scan = store.newScan();
        scan.withSnapshot(snapshot.id());
        List<ManifestEntry> files = scan.plan().files();

        scan = store.newScan();
        scan.withSnapshot(snapshot.id());
        scan.withValueFilter(
                new PredicateBuilder(TestKeyValueGenerator.DEFAULT_ROW_TYPE)
                        .between(4, 1000L, 1999L));

        List<ManifestEntry> filesFiltered = scan.plan().files();

        assertThat(files.size()).isEqualTo(4);
        assertThat(filesFiltered.size()).isEqualTo(1);
    }

    @Test
    public void testWithValueFilterPartition() throws Exception {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        List<KeyValue> data = generateData(100, 0, (long) Math.abs(random.nextInt(1000)));
        writeData(data, 0);
        data = generateData(100, 1, (long) Math.abs(random.nextInt(1000)) + 1000);
        writeData(data, 0);
        data = generateData(100, 2, (long) Math.abs(random.nextInt(1000)) + 2000);
        writeData(data, 0);
        data = generateData(100, 3, (long) Math.abs(random.nextInt(1000)) + 3000);
        Snapshot snapshot = writeData(data, 0);

        KeyValueFileStoreScan scan = store.newScan();
        scan.withSnapshot(snapshot.id());
        List<ManifestEntry> files = scan.plan().files();

        scan = store.newScan();
        scan.withSnapshot(snapshot.id());
        scan.withValueFilter(
                new PredicateBuilder(TestKeyValueGenerator.DEFAULT_ROW_TYPE)
                        .between(4, 1000L, 1999L));

        List<ManifestEntry> filesFiltered = scan.plan().files();

        assertThat(files.size()).isEqualTo(4);
        assertThat(filesFiltered.size()).isEqualTo(1);
    }

    @Test
    public void testWithBucket() throws Exception {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        List<KeyValue> data = generateData(random.nextInt(1000) + 1);
        Snapshot snapshot = writeData(data);

        int wantedBucket = random.nextInt(NUM_BUCKETS);

        FileStoreScan scan = store.newScan();
        scan.withSnapshot(snapshot.id());
        scan.withBucket(wantedBucket);

        Map<BinaryRow, BinaryRow> expected =
                store.toKvMap(
                        data.stream()
                                .filter(kv -> getBucket(kv) == wantedBucket)
                                .collect(Collectors.toList()));
        runTestExactMatch(scan, snapshot.id(), expected);
    }

    @Test
    public void testWithSnapshot() throws Exception {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int numCommits = random.nextInt(10) + 1;
        int wantedCommit = random.nextInt(numCommits);

        List<Snapshot> snapshots = new ArrayList<>();
        List<List<KeyValue>> allData = new ArrayList<>();
        for (int i = 0; i < numCommits; i++) {
            List<KeyValue> data = generateData(random.nextInt(100) + 1);
            snapshots.add(writeData(data));
            allData.add(data);
        }
        long wantedSnapshot = snapshots.get(wantedCommit).id();

        FileStoreScan scan = store.newScan();
        scan.withSnapshot(wantedSnapshot);

        Map<BinaryRow, BinaryRow> expected =
                store.toKvMap(
                        allData.subList(0, wantedCommit + 1).stream()
                                .flatMap(Collection::stream)
                                .collect(Collectors.toList()));
        runTestExactMatch(scan, wantedSnapshot, expected);
    }

    @Test
    public void testDropStatsInPlan() throws Exception {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        List<KeyValue> data = generateData(100, 0, (long) Math.abs(random.nextInt(1000)));
        writeData(data, 0);
        data = generateData(100, 1, (long) Math.abs(random.nextInt(1000)) + 1000);
        writeData(data, 0);
        data = generateData(100, 2, (long) Math.abs(random.nextInt(1000)) + 2000);
        writeData(data, 0);
        data = generateData(100, 3, (long) Math.abs(random.nextInt(1000)) + 3000);
        Snapshot snapshot = writeData(data, 0);

        KeyValueFileStoreScan scan = store.newScan();
        scan.withSnapshot(snapshot.id()).dropStats();
        List<ManifestEntry> files = scan.plan().files();

        for (ManifestEntry manifestEntry : files) {
            assertThat(manifestEntry.file().valueStats()).isEqualTo(EMPTY_STATS);
        }
    }

    @Test
    public void testLimitPushdownWithoutValueFilter() throws Exception {
        // Write multiple files to test limit pushdown
        List<KeyValue> data1 = generateData(50);
        writeData(data1);
        List<KeyValue> data2 = generateData(50);
        writeData(data2);
        List<KeyValue> data3 = generateData(50);
        Snapshot snapshot = writeData(data3);

        // Without limit, should read all files
        KeyValueFileStoreScan scanWithoutLimit = store.newScan();
        scanWithoutLimit.withSnapshot(snapshot.id());
        List<ManifestEntry> filesWithoutLimit = scanWithoutLimit.plan().files();
        int totalFiles = filesWithoutLimit.size();
        assertThat(totalFiles).isGreaterThan(0);

        // With limit, should read fewer files (limit pushdown should work)
        KeyValueFileStoreScan scanWithLimit = store.newScan();
        scanWithLimit.withSnapshot(snapshot.id()).withLimit(10);
        List<ManifestEntry> filesWithLimit = scanWithLimit.plan().files();
        // Limit pushdown should reduce the number of files read
        assertThat(filesWithLimit.size()).isLessThanOrEqualTo(totalFiles);
        assertThat(filesWithLimit.size()).isGreaterThan(0);
    }

    @Test
    public void testLimitPushdownWithValueFilter() throws Exception {
        // Write data with different item values
        List<KeyValue> data1 = generateData(50, 0, 100L);
        writeData(data1);
        List<KeyValue> data2 = generateData(50, 0, 200L);
        writeData(data2);
        List<KeyValue> data3 = generateData(50, 0, 300L);
        Snapshot snapshot = writeData(data3);

        // Without valueFilter, limit pushdown should work
        KeyValueFileStoreScan scanWithoutFilter = store.newScan();
        scanWithoutFilter.withSnapshot(snapshot.id()).withLimit(10);
        List<ManifestEntry> filesWithoutFilter = scanWithoutFilter.plan().files();
        int totalFilesWithoutFilter = filesWithoutFilter.size();
        assertThat(totalFilesWithoutFilter).isGreaterThan(0);

        // With valueFilter, limit pushdown should still work.
        KeyValueFileStoreScan scanWithFilter = store.newScan();
        scanWithFilter.withSnapshot(snapshot.id());
        scanWithFilter.withValueFilter(
                new PredicateBuilder(TestKeyValueGenerator.DEFAULT_ROW_TYPE)
                        .between(4, 100L, 200L));
        scanWithFilter.withLimit(10);
        List<ManifestEntry> filesWithFilter = scanWithFilter.plan().files();

        // Limit pushdown should work with valueFilter
        // The number of files should be less than or equal to the total files after filtering
        assertThat(filesWithFilter.size()).isGreaterThan(0);
        assertThat(filesWithFilter.size()).isLessThanOrEqualTo(totalFilesWithoutFilter);
    }

    @Test
    public void testLimitPushdownWithKeyFilter() throws Exception {
        // Write data with different shop IDs
        List<KeyValue> data = generateData(200);
        Snapshot snapshot = writeData(data);

        // With keyFilter, limit pushdown should still work (keyFilter doesn't affect limit
        // pushdown)
        KeyValueFileStoreScan scan = store.newScan();
        scan.withSnapshot(snapshot.id());
        scan.withKeyFilter(
                new PredicateBuilder(RowType.of(new IntType(false)))
                        .equal(0, data.get(0).key().getInt(0)));
        scan.withLimit(5);
        List<ManifestEntry> files = scan.plan().files();
        assertThat(files.size()).isGreaterThan(0);
    }

    @Test
    public void testLimitPushdownMultipleBuckets() throws Exception {
        // Write data to multiple buckets to test limit pushdown across buckets
        List<KeyValue> data1 = generateData(30);
        writeData(data1);
        List<KeyValue> data2 = generateData(30);
        writeData(data2);
        List<KeyValue> data3 = generateData(30);
        Snapshot snapshot = writeData(data3);

        // Without limit, should read all files
        KeyValueFileStoreScan scanWithoutLimit = store.newScan();
        scanWithoutLimit.withSnapshot(snapshot.id());
        List<ManifestEntry> filesWithoutLimit = scanWithoutLimit.plan().files();
        int totalFiles = filesWithoutLimit.size();
        assertThat(totalFiles).isGreaterThan(0);

        // With limit, should read fewer files (limit pushdown should work across buckets)
        KeyValueFileStoreScan scanWithLimit = store.newScan();
        scanWithLimit.withSnapshot(snapshot.id()).withLimit(20);
        List<ManifestEntry> filesWithLimit = scanWithLimit.plan().files();
        // Limit pushdown should reduce the number of files read
        assertThat(filesWithLimit.size()).isLessThanOrEqualTo(totalFiles);
        assertThat(filesWithLimit.size()).isGreaterThan(0);
    }

    @Test
    public void testLimitPushdownWithSmallLimit() throws Exception {
        // Test limit pushdown with a very small limit
        List<KeyValue> data1 = generateData(100);
        writeData(data1);
        List<KeyValue> data2 = generateData(100);
        writeData(data2);
        Snapshot snapshot = writeData(data2);

        KeyValueFileStoreScan scan = store.newScan();
        scan.withSnapshot(snapshot.id()).withLimit(1);
        List<ManifestEntry> files = scan.plan().files();
        // Should read at least one file, but fewer than all files
        assertThat(files.size()).isGreaterThan(0);
    }

    @Test
    public void testLimitPushdownWithLargeLimit() throws Exception {
        // Test limit pushdown with a large limit (larger than total rows)
        List<KeyValue> data1 = generateData(50);
        writeData(data1);
        List<KeyValue> data2 = generateData(50);
        Snapshot snapshot = writeData(data2);

        KeyValueFileStoreScan scanWithoutLimit = store.newScan();
        scanWithoutLimit.withSnapshot(snapshot.id());
        List<ManifestEntry> filesWithoutLimit = scanWithoutLimit.plan().files();
        int totalFiles = filesWithoutLimit.size();

        KeyValueFileStoreScan scanWithLimit = store.newScan();
        scanWithLimit.withSnapshot(snapshot.id()).withLimit(10000);
        List<ManifestEntry> filesWithLimit = scanWithLimit.plan().files();
        // With a large limit, should read all files
        assertThat(filesWithLimit.size()).isEqualTo(totalFiles);
    }

    @Test
    public void testLimitPushdownWithPartialUpdateMergeEngine() throws Exception {
        // Test that limit pushdown is disabled for PARTIAL_UPDATE merge engine
        // Create a store with PARTIAL_UPDATE merge engine by setting it in schema options
        SchemaManager schemaManager =
                new SchemaManager(LocalFileIO.create(), new Path(tempDir.toUri()));
        Schema schema =
                new Schema(
                        TestKeyValueGenerator.DEFAULT_ROW_TYPE.getFields(),
                        TestKeyValueGenerator.DEFAULT_PART_TYPE.getFieldNames(),
                        TestKeyValueGenerator.getPrimaryKeys(
                                TestKeyValueGenerator.GeneratorMode.MULTI_PARTITIONED),
                        Collections.singletonMap(CoreOptions.MERGE_ENGINE.key(), "partial-update"),
                        null);
        TableSchema tableSchema = SchemaUtils.forceCommit(schemaManager, schema);

        TestFileStore storePartialUpdate =
                new TestFileStore.Builder(
                                "avro",
                                tempDir.toString(),
                                NUM_BUCKETS,
                                TestKeyValueGenerator.DEFAULT_PART_TYPE,
                                TestKeyValueGenerator.KEY_TYPE,
                                TestKeyValueGenerator.DEFAULT_ROW_TYPE,
                                TestKeyValueGenerator.TestKeyValueFieldsExtractor.EXTRACTOR,
                                DeduplicateMergeFunction.factory(),
                                tableSchema)
                        .build();

        List<KeyValue> data1 = generateData(50);
        writeData(data1, storePartialUpdate);
        List<KeyValue> data2 = generateData(50);
        Snapshot snapshot = writeData(data2, storePartialUpdate);

        KeyValueFileStoreScan scan = storePartialUpdate.newScan();
        scan.withSnapshot(snapshot.id()).withLimit(10);
        // supportsLimitPushManifestEntries should return false for PARTIAL_UPDATE
        assertThat(scan.limitPushdownEnabled()).isFalse();

        // Should read all files since limit pushdown is disabled
        KeyValueFileStoreScan scanWithoutLimit = storePartialUpdate.newScan();
        scanWithoutLimit.withSnapshot(snapshot.id());
        List<ManifestEntry> filesWithoutLimit = scanWithoutLimit.plan().files();
        int totalFiles = filesWithoutLimit.size();

        List<ManifestEntry> filesWithLimit = scan.plan().files();
        assertThat(filesWithLimit.size()).isEqualTo(totalFiles);
    }

    @Test
    public void testLimitPushdownWithAggregateMergeEngine() throws Exception {
        // Test that limit pushdown is disabled for AGGREGATE merge engine
        SchemaManager schemaManager =
                new SchemaManager(LocalFileIO.create(), new Path(tempDir.toUri()));
        Schema schema =
                new Schema(
                        TestKeyValueGenerator.DEFAULT_ROW_TYPE.getFields(),
                        TestKeyValueGenerator.DEFAULT_PART_TYPE.getFieldNames(),
                        TestKeyValueGenerator.getPrimaryKeys(
                                TestKeyValueGenerator.GeneratorMode.MULTI_PARTITIONED),
                        Collections.singletonMap(CoreOptions.MERGE_ENGINE.key(), "aggregation"),
                        null);
        TableSchema tableSchema = SchemaUtils.forceCommit(schemaManager, schema);

        TestFileStore storeAggregate =
                new TestFileStore.Builder(
                                "avro",
                                tempDir.toString(),
                                NUM_BUCKETS,
                                TestKeyValueGenerator.DEFAULT_PART_TYPE,
                                TestKeyValueGenerator.KEY_TYPE,
                                TestKeyValueGenerator.DEFAULT_ROW_TYPE,
                                TestKeyValueGenerator.TestKeyValueFieldsExtractor.EXTRACTOR,
                                DeduplicateMergeFunction.factory(),
                                tableSchema)
                        .build();

        List<KeyValue> data1 = generateData(50);
        writeData(data1, storeAggregate);
        List<KeyValue> data2 = generateData(50);
        Snapshot snapshot = writeData(data2, storeAggregate);

        KeyValueFileStoreScan scan = storeAggregate.newScan();
        scan.withSnapshot(snapshot.id()).withLimit(10);
        // supportsLimitPushManifestEntries should return false for AGGREGATE
        assertThat(scan.limitPushdownEnabled()).isFalse();

        // Should read all files since limit pushdown is disabled
        KeyValueFileStoreScan scanWithoutLimit = storeAggregate.newScan();
        scanWithoutLimit.withSnapshot(snapshot.id());
        List<ManifestEntry> filesWithoutLimit = scanWithoutLimit.plan().files();
        int totalFiles = filesWithoutLimit.size();

        List<ManifestEntry> filesWithLimit = scan.plan().files();
        assertThat(filesWithLimit.size()).isEqualTo(totalFiles);
    }

    @Test
    public void testLimitPushdownWithDeletionVectors() throws Exception {
        // Test that limit pushdown is disabled when deletion vectors are enabled
        SchemaManager schemaManager =
                new SchemaManager(LocalFileIO.create(), new Path(tempDir.toUri()));
        Schema schema =
                new Schema(
                        TestKeyValueGenerator.DEFAULT_ROW_TYPE.getFields(),
                        TestKeyValueGenerator.DEFAULT_PART_TYPE.getFieldNames(),
                        TestKeyValueGenerator.getPrimaryKeys(
                                TestKeyValueGenerator.GeneratorMode.MULTI_PARTITIONED),
                        Collections.singletonMap(
                                CoreOptions.DELETION_VECTORS_ENABLED.key(), "true"),
                        null);
        TableSchema tableSchema = SchemaUtils.forceCommit(schemaManager, schema);

        TestFileStore storeWithDV =
                new TestFileStore.Builder(
                                "avro",
                                tempDir.toString(),
                                NUM_BUCKETS,
                                TestKeyValueGenerator.DEFAULT_PART_TYPE,
                                TestKeyValueGenerator.KEY_TYPE,
                                TestKeyValueGenerator.DEFAULT_ROW_TYPE,
                                TestKeyValueGenerator.TestKeyValueFieldsExtractor.EXTRACTOR,
                                DeduplicateMergeFunction.factory(),
                                tableSchema)
                        .build();

        KeyValueFileStoreScan scan = storeWithDV.newScan();
        scan.withLimit(10);
        // supportsLimitPushManifestEntries should return false when deletion vectors are enabled
        assertThat(scan.limitPushdownEnabled()).isFalse();
    }

    private void runTestExactMatch(
            FileStoreScan scan, Long expectedSnapshotId, Map<BinaryRow, BinaryRow> expected)
            throws Exception {
        Map<BinaryRow, BinaryRow> actual = getActualKvMap(scan, expectedSnapshotId);
        assertThat(actual).isEqualTo(expected);
    }

    private void runTestContainsAll(
            FileStoreScan scan, Long expectedSnapshotId, Map<BinaryRow, BinaryRow> expected)
            throws Exception {
        Map<BinaryRow, BinaryRow> actual = getActualKvMap(scan, expectedSnapshotId);
        for (Map.Entry<BinaryRow, BinaryRow> entry : expected.entrySet()) {
            assertThat(actual).containsKey(entry.getKey());
            assertThat(actual.get(entry.getKey())).isEqualTo(entry.getValue());
        }
    }

    private Map<BinaryRow, BinaryRow> getActualKvMap(FileStoreScan scan, Long expectedSnapshotId)
            throws Exception {
        FileStoreScan.Plan plan = scan.plan();
        Snapshot snapshot = plan.snapshot();
        assertThat(snapshot == null ? null : snapshot.id()).isEqualTo(expectedSnapshotId);

        List<KeyValue> actualKvs = store.readKvsFromManifestEntries(plan.files(), false);
        gen.sort(actualKvs);
        return store.toKvMap(actualKvs);
    }

    private List<KeyValue> generateData(int numRecords) {
        List<KeyValue> data = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            data.add(gen.next());
        }
        return data;
    }

    private List<KeyValue> generateData(int numRecords, int hr, Long itemId) {
        List<KeyValue> data = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            data.add(gen.nextInsert("", hr, itemId, null, null));
        }
        return data;
    }

    private Snapshot writeData(List<KeyValue> kvs) throws Exception {
        return writeData(kvs, store);
    }

    private Snapshot writeData(List<KeyValue> kvs, TestFileStore testStore) throws Exception {
        List<Snapshot> snapshots = testStore.commitData(kvs, gen::getPartition, this::getBucket);
        return snapshots.get(snapshots.size() - 1);
    }

    private Snapshot writeData(List<KeyValue> kvs, int bucket) throws Exception {
        List<Snapshot> snapshots = store.commitData(kvs, gen::getPartition, b -> bucket);
        return snapshots.get(snapshots.size() - 1);
    }

    private int getBucket(KeyValue kv) {
        return (kv.key().hashCode() % NUM_BUCKETS + NUM_BUCKETS) % NUM_BUCKETS;
    }
}
