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

package org.apache.flink.table.store.file.operation;

import org.apache.flink.table.store.data.BinaryRow;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.Snapshot;
import org.apache.flink.table.store.file.TestFileStore;
import org.apache.flink.table.store.file.TestKeyValueGenerator;
import org.apache.flink.table.store.file.manifest.ManifestFileMeta;
import org.apache.flink.table.store.file.manifest.ManifestList;
import org.apache.flink.table.store.file.mergetree.compact.DeduplicateMergeFunction;
import org.apache.flink.table.store.file.predicate.PredicateBuilder;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.schema.UpdateSchema;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.fs.Path;
import org.apache.flink.table.store.fs.local.LocalFileIO;
import org.apache.flink.table.store.types.IntType;
import org.apache.flink.table.store.types.RowType;

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

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KeyValueFileStoreScan}. */
public class KeyValueFileStoreScanTest {

    private static final int NUM_BUCKETS = 10;

    private TestKeyValueGenerator gen;
    @TempDir java.nio.file.Path tempDir;
    private TestFileStore store;
    private SnapshotManager snapshotManager;

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
                                DeduplicateMergeFunction.factory())
                        .build();
        snapshotManager = store.snapshotManager();

        SchemaManager schemaManager =
                new SchemaManager(LocalFileIO.create(), new Path(tempDir.toUri()));
        schemaManager.commitNewVersion(
                new UpdateSchema(
                        TestKeyValueGenerator.DEFAULT_ROW_TYPE,
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
    public void testWithManifestList() throws Exception {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int numCommits = random.nextInt(10) + 1;
        for (int i = 0; i < numCommits; i++) {
            List<KeyValue> data = generateData(random.nextInt(100) + 1);
            writeData(data);
        }

        ManifestList manifestList = store.manifestListFactory().create();
        long wantedSnapshotId = random.nextLong(snapshotManager.latestSnapshotId()) + 1;
        Snapshot wantedSnapshot = snapshotManager.snapshot(wantedSnapshotId);
        List<ManifestFileMeta> wantedManifests = wantedSnapshot.readAllDataManifests(manifestList);

        FileStoreScan scan = store.newScan();
        scan.withManifestList(wantedManifests);

        List<KeyValue> expectedKvs = store.readKvsFromSnapshot(wantedSnapshotId);
        gen.sort(expectedKvs);
        Map<BinaryRow, BinaryRow> expected = store.toKvMap(expectedKvs);
        runTestExactMatch(scan, null, expected);
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
        assertThat(plan.snapshotId()).isEqualTo(expectedSnapshotId);

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

    private Snapshot writeData(List<KeyValue> kvs) throws Exception {
        List<Snapshot> snapshots = store.commitData(kvs, gen::getPartition, this::getBucket);
        return snapshots.get(snapshots.size() - 1);
    }

    private int getBucket(KeyValue kv) {
        return (kv.key().hashCode() % NUM_BUCKETS + NUM_BUCKETS) % NUM_BUCKETS;
    }
}
