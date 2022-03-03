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

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.Snapshot;
import org.apache.flink.table.store.file.TestFileStore;
import org.apache.flink.table.store.file.TestKeyValueGenerator;
import org.apache.flink.table.store.file.manifest.ManifestFileMeta;
import org.apache.flink.table.store.file.manifest.ManifestList;
import org.apache.flink.table.store.file.mergetree.compact.DeduplicateAccumulator;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FileStoreScanImpl}. */
public class FileStoreScanTest {

    private static final int NUM_BUCKETS = 10;

    private TestKeyValueGenerator gen;
    @TempDir java.nio.file.Path tempDir;
    private TestFileStore store;
    private FileStorePathFactory pathFactory;

    @BeforeEach
    public void beforeEach() throws IOException {
        gen = new TestKeyValueGenerator();
        Path root = new Path(tempDir.toString());
        root.getFileSystem().mkdirs(new Path(root + "/snapshot"));
        store =
                TestFileStore.create(
                        "avro",
                        tempDir.toString(),
                        NUM_BUCKETS,
                        TestKeyValueGenerator.PARTITION_TYPE,
                        TestKeyValueGenerator.KEY_TYPE,
                        TestKeyValueGenerator.ROW_TYPE,
                        new DeduplicateAccumulator());
        pathFactory = store.pathFactory();
    }

    @Test
    public void testWithPartitionFilter() throws Exception {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        List<KeyValue> data = generateData(random.nextInt(1000) + 1);
        List<BinaryRowData> partitions =
                data.stream()
                        .map(kv -> gen.getPartition(kv))
                        .distinct()
                        .collect(Collectors.toList());
        Snapshot snapshot = writeData(data);

        Set<BinaryRowData> wantedPartitions = new HashSet<>();
        for (int i = random.nextInt(partitions.size() + 1); i > 0; i--) {
            wantedPartitions.add(partitions.get(random.nextInt(partitions.size())));
        }

        FileStoreScan scan = store.newScan();
        scan.withSnapshot(snapshot.id());
        scan.withPartitionFilter(new ArrayList<>(wantedPartitions));

        Map<BinaryRowData, BinaryRowData> expected =
                store.toKvMap(
                        wantedPartitions.isEmpty()
                                ? data
                                : data.stream()
                                        .filter(
                                                kv ->
                                                        wantedPartitions.contains(
                                                                gen.getPartition(kv)))
                                        .collect(Collectors.toList()));
        runTest(scan, snapshot.id(), expected);
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

        Map<BinaryRowData, BinaryRowData> expected =
                store.toKvMap(
                        data.stream()
                                .filter(kv -> getBucket(kv) == wantedBucket)
                                .collect(Collectors.toList()));
        runTest(scan, snapshot.id(), expected);
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

        Map<BinaryRowData, BinaryRowData> expected =
                store.toKvMap(
                        allData.subList(0, wantedCommit + 1).stream()
                                .flatMap(Collection::stream)
                                .collect(Collectors.toList()));
        runTest(scan, wantedSnapshot, expected);
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
        long wantedSnapshotId = random.nextLong(pathFactory.latestSnapshotId()) + 1;
        Snapshot wantedSnapshot = Snapshot.fromPath(pathFactory.toSnapshotPath(wantedSnapshotId));
        List<ManifestFileMeta> wantedManifests = wantedSnapshot.readAllManifests(manifestList);

        FileStoreScan scan = store.newScan();
        scan.withManifestList(wantedManifests);

        List<KeyValue> expectedKvs = store.readKvsFromSnapshot(wantedSnapshotId);
        gen.sort(expectedKvs);
        Map<BinaryRowData, BinaryRowData> expected = store.toKvMap(expectedKvs);
        runTest(scan, null, expected);
    }

    private void runTest(
            FileStoreScan scan, Long expectedSnapshotId, Map<BinaryRowData, BinaryRowData> expected)
            throws Exception {
        FileStoreScan.Plan plan = scan.plan();
        assertThat(plan.snapshotId()).isEqualTo(expectedSnapshotId);

        List<KeyValue> actualKvs = store.readKvsFromManifestEntries(plan.files());
        gen.sort(actualKvs);
        Map<BinaryRowData, BinaryRowData> actual = store.toKvMap(actualKvs);
        assertThat(actual).isEqualTo(expected);
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
