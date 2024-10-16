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
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.mergetree.compact.DeduplicateMergeFunction;
import org.apache.paimon.options.ExpireConfig;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.ExpireSnapshots;
import org.apache.paimon.table.ExpireSnapshotsImpl;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.RecordWriter;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.SNAPSHOT_CLEAN_EMPTY_DIRECTORIES;
import static org.apache.paimon.operation.FileStoreCommitImpl.mustConflictCheck;
import static org.apache.paimon.operation.FileStoreTestUtils.assertPathExists;
import static org.apache.paimon.operation.FileStoreTestUtils.assertPathNotExists;
import static org.apache.paimon.operation.FileStoreTestUtils.commitData;
import static org.apache.paimon.operation.FileStoreTestUtils.partitionedData;
import static org.apache.paimon.utils.BranchManager.DEFAULT_MAIN_BRANCH;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for file deletion when expiring snapshot and deleting tag. It also tests that after
 * expiration, empty data file directories (buckets and partitions) are deleted. It didn't extend
 * {@link ExpireSnapshotsTest} because there are not too many codes can be reused.
 */
public class FileDeletionTest {

    @TempDir java.nio.file.Path tempDir;

    private final FileIO fileIO = new LocalFileIO();

    private long commitIdentifier;
    private String root;
    private TagManager tagManager;

    @BeforeEach
    public void setup() throws Exception {
        commitIdentifier = 0L;
        root = tempDir.toString();
        tagManager = null;
    }

    /**
     * This test checks FileStoreExpire can delete empty partition directories in multiple partition
     * situation. The partition keys are (dt, hr). Test process:
     *
     * <ul>
     *   <li>1. Generate snapshot 1 with (0401, 8/12), (0402, 8/12). Each partition has two buckets.
     *   <li>2. Generate snapshot 2 by deleting all data of partition dt=0401 (thus directory
     *       dt=0401 will be deleted after expiring).
     *   <li>3. Generate snapshot 3 by deleting all data of partition dt=0402/hr=8 (thus directory
     *       dt=0402/hr=8 will be deleted after expiring).
     *   <li>4. Generate snapshot 4 by deleting all data of partition dt=0402/hr=12/bucket-0 (thus
     *       directory dt=0402/hr=12/bucket-0 will be deleted after expiring).
     *   <li>5. Expire snapshot 1-3 (dt=0402/hr=12/bucket-1 survives) and check.
     * </ul>
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testMultiPartitions(boolean cleanEmptyDirs) throws Exception {
        TestFileStore store = createStore(TestKeyValueGenerator.GeneratorMode.MULTI_PARTITIONED);
        TestKeyValueGenerator gen =
                new TestKeyValueGenerator(TestKeyValueGenerator.GeneratorMode.MULTI_PARTITIONED);
        FileStorePathFactory pathFactory = store.pathFactory();

        // step 1: generate snapshot 1 by writing 5 randomly generated records to each bucket
        // writers for each bucket
        Map<BinaryRow, Map<Integer, RecordWriter<KeyValue>>> writers = new HashMap<>();

        List<BinaryRow> partitions = new ArrayList<>();
        for (String dt : Arrays.asList("0401", "0402")) {
            for (int hr : Arrays.asList(8, 12)) {
                for (int bucket : Arrays.asList(0, 1)) {
                    List<KeyValue> kvs = partitionedData(5, gen, dt, hr);
                    BinaryRow partition = gen.getPartition(kvs.get(0));
                    partitions.add(partition);
                    writeData(store, kvs, partition, bucket, writers);
                }
            }
        }

        commitData(store, commitIdentifier++, writers);
        // check all paths exist
        for (BinaryRow partition : partitions) {
            for (int bucket : Arrays.asList(0, 1)) {
                assertPathExists(fileIO, pathFactory.bucketPath(partition, bucket));
            }
        }

        // step 2: generate snapshot 2 by cleaning partition dt=0401 (through overwriting with an
        // empty ManifestCommittable)
        FileStoreCommitImpl commit = store.newCommit();
        Map<String, String> partitionSpec = new HashMap<>();
        partitionSpec.put("dt", "0401");
        commit.overwrite(
                partitionSpec, new ManifestCommittable(commitIdentifier++), Collections.emptyMap());

        // step 3: generate snapshot 3 by cleaning partition dt=0402/hr=10
        partitionSpec.put("dt", "0402");
        partitionSpec.put("hr", "8");
        commit.overwrite(
                partitionSpec, new ManifestCommittable(commitIdentifier++), Collections.emptyMap());
        commit.close();

        // step 4: generate snapshot 4 by cleaning dt=0402/hr=12/bucket-0
        BinaryRow partition = partitions.get(7);
        cleanBucket(store, partition, 0);

        // step 5: expire and check file paths
        store.options().toConfiguration().set(SNAPSHOT_CLEAN_EMPTY_DIRECTORIES, true);
        store.newExpire(1, 1, Long.MAX_VALUE).expire();

        // check dt=0401
        if (cleanEmptyDirs) {
            assertPathNotExists(fileIO, new Path(root, "dt=0401"));
        } else {
            for (String hr : new String[] {"hr=8", "hr=12"}) {
                for (String bucket : new String[] {"bucket-0", "bucket-1"}) {
                    Path path = new Path(root, String.format("dt=0401/%s/%s", hr, bucket));
                    assertThat(fileIO.listStatus(path).length).isEqualTo(0);
                }
            }
        }

        // check dt=0402
        assertPathExists(fileIO, new Path(root, "dt=0402/hr=12/bucket-1"));
        if (cleanEmptyDirs) {
            assertPathNotExists(fileIO, new Path(root, "dt=0402/hr=12/bucket-0"));
            assertPathNotExists(fileIO, new Path(root, "dt=0402/hr-8"));
        } else {
            assertThat(fileIO.listStatus(new Path("dt=0402/hr=8/bucket-0")).length).isEqualTo(0);
            assertThat(fileIO.listStatus(new Path("dt=0402/hr=8/bucket-1")).length).isEqualTo(0);
            assertThat(fileIO.listStatus(new Path("dt=0402/hr=12/bucket-0")).length).isEqualTo(0);
        }
    }

    // only exists bucket directories
    @Test
    public void testNoPartitions() throws Exception {
        TestFileStore store = createStore(TestKeyValueGenerator.GeneratorMode.NON_PARTITIONED);
        TestKeyValueGenerator gen =
                new TestKeyValueGenerator(TestKeyValueGenerator.GeneratorMode.NON_PARTITIONED);
        FileStorePathFactory pathFactory = store.pathFactory();

        Map<BinaryRow, Map<Integer, RecordWriter<KeyValue>>> writers = new HashMap<>();
        for (int bucket : Arrays.asList(0, 1)) {
            List<KeyValue> kvs = partitionedData(5, gen);
            BinaryRow partition = gen.getPartition(kvs.get(0));
            writeData(store, kvs, partition, bucket, writers);
        }
        commitData(store, commitIdentifier++, writers);

        // cleaning bucket 0
        BinaryRow partition = gen.getPartition(gen.next());
        cleanBucket(store, partition, 0);

        // check before expiring
        assertPathExists(fileIO, pathFactory.bucketPath(partition, 0));
        assertPathExists(fileIO, pathFactory.bucketPath(partition, 1));

        // check after expiring
        store.newExpire(1, 1, Long.MAX_VALUE).expire();

        assertPathNotExists(fileIO, pathFactory.bucketPath(partition, 0));
        assertPathExists(fileIO, pathFactory.bucketPath(partition, 1));
    }

    /**
     * This test checks FileStoreExpire won't delete data files and manifests that are used by tags.
     * Test process:
     *
     * <ul>
     *   <li>1. Generate snapshot 1 with +A, +B.
     *   <li>2. Generate snapshot 2 with -A, then create tag1.
     *   <li>3. Generate snapshot 3 with +C.
     *   <li>4. Generate snapshot 4 with -B, then create tag2.
     *   <li>5. Generate snapshot 5 with +D.
     *   <li>6. Generate snapshot 6 with -D.
     *   <li>5. Expire snapshot 1 to 5 respectively.
     * </ul>
     *
     * <p>To identify different data files, this test use 4 buckets to store A, B, C and D, so we
     * can check bucket path to assert whether the data file is reserved. The expiration result
     * should be:
     *
     * <ul>
     *   <li>Expiring snapshot 1 will delete file A because snapshot 2 has committed -A and A is not
     *       used by tag1.
     *   <li>Expiring snapshot 2 won't delete any file because snapshot 3 hasn't committed DELETE
     *       files.
     *   <li>Expiring snapshot 3 won't delete B although snapshot 4 has committed -B and is not used
     *       by tag2 because B is used by tag1.
     *   <li>Expiring snapshot 4 won't delete any file like snapshot 2.
     *   <li>Expiring snapshot 5 will delete file D because snapshot 6 has committed -D and D is not
     *       used by tag2.
     * </ul>
     */
    @Test
    public void testExpireWithExistingTags() throws Exception {
        TestFileStore store = createStore(TestKeyValueGenerator.GeneratorMode.NON_PARTITIONED, 4);
        tagManager = new TagManager(fileIO, store.options().path());
        SnapshotManager snapshotManager = store.snapshotManager();
        TestKeyValueGenerator gen =
                new TestKeyValueGenerator(TestKeyValueGenerator.GeneratorMode.NON_PARTITIONED);
        BinaryRow partition = gen.getPartition(gen.next());

        // step 1: commit A to bucket 0 and B to bucket 1
        Map<BinaryRow, Map<Integer, RecordWriter<KeyValue>>> writers = new HashMap<>();
        for (int bucket : Arrays.asList(0, 1)) {
            List<KeyValue> kvs = partitionedData(5, gen);
            writeData(store, kvs, partition, bucket, writers);
        }
        commitData(store, commitIdentifier++, writers);

        // step 2: commit -A (by clean bucket 0) and create tag1
        cleanBucket(store, gen.getPartition(gen.next()), 0);
        createTag(snapshotManager.snapshot(2), "tag1", store.options().tagDefaultTimeRetained());
        assertThat(tagManager.tagExists("tag1")).isTrue();

        // step 3: commit C to bucket 2
        writers.clear();
        List<KeyValue> kvs = partitionedData(5, gen);
        writeData(store, kvs, partition, 2, writers);
        commitData(store, commitIdentifier++, writers);

        // step 4: commit -B (by clean bucket 1) and create tag2
        cleanBucket(store, partition, 1);
        createTag(snapshotManager.snapshot(4), "tag2", store.options().tagDefaultTimeRetained());
        assertThat(tagManager.tagExists("tag2")).isTrue();

        // step 5: commit D to bucket 3
        writers.clear();
        kvs = partitionedData(5, gen);
        writeData(store, kvs, partition, 3, writers);
        commitData(store, commitIdentifier++, writers);

        // step 6: commit -D (by clean bucket 3)
        cleanBucket(store, partition, 3);

        // check before expiring
        FileStorePathFactory pathFactory = store.pathFactory();
        for (int i = 0; i < 4; i++) {
            assertPathExists(fileIO, pathFactory.bucketPath(partition, i));
        }

        // check expiring results
        store.newExpire(1, 1, Long.MAX_VALUE).expire();

        // expiring snapshot 1 will delete file A
        assertPathNotExists(fileIO, pathFactory.bucketPath(partition, 0));
        // expiring snapshot 2 & 3 won't delete file B
        assertPathExists(fileIO, pathFactory.bucketPath(partition, 1));
        // expiring snapshot 4 & 5 will delete file D
        assertPathNotExists(fileIO, pathFactory.bucketPath(partition, 3));
        // file C survives
        assertPathExists(fileIO, pathFactory.bucketPath(partition, 2));

        // check manifests
        ManifestList manifestList = store.manifestListFactory().create();
        for (String tagName : Arrays.asList("tag1", "tag2")) {
            Snapshot snapshot = tagManager.taggedSnapshot(tagName);
            List<Path> manifestFilePaths =
                    manifestList.readDataManifests(snapshot).stream()
                            .map(ManifestFileMeta::fileName)
                            .map(pathFactory::toManifestFilePath)
                            .collect(Collectors.toList());
            for (Path path : manifestFilePaths) {
                assertPathExists(fileIO, path);
            }

            assertPathExists(fileIO, pathFactory.toManifestListPath(snapshot.baseManifestList()));
            assertPathExists(fileIO, pathFactory.toManifestListPath(snapshot.deltaManifestList()));
        }
    }

    @Test
    public void testExpireWithUpgradeAndTags() throws Exception {
        TestFileStore store = createStore(TestKeyValueGenerator.GeneratorMode.NON_PARTITIONED);
        tagManager = new TagManager(fileIO, store.options().path());
        SnapshotManager snapshotManager = store.snapshotManager();
        TestKeyValueGenerator gen =
                new TestKeyValueGenerator(TestKeyValueGenerator.GeneratorMode.NON_PARTITIONED);
        BinaryRow partition = gen.getPartition(gen.next());

        // snapshot 1: commit A to bucket 0
        Map<BinaryRow, Map<Integer, RecordWriter<KeyValue>>> writers = new HashMap<>();
        List<KeyValue> kvs = partitionedData(5, gen);
        writeData(store, kvs, partition, 0, writers);
        commitData(store, commitIdentifier++, writers);

        // snapshot 2: compact
        writers.values().stream()
                .flatMap(m -> m.values().stream())
                .forEach(
                        writer -> {
                            try {
                                writer.compact(true);
                                writer.sync();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
        FileStoreTestUtils.commitData(store, commitIdentifier++, writers);

        // snapshot 3: commit -A (by clean bucket 0)
        cleanBucket(store, gen.getPartition(gen.next()), 0);

        createTag(snapshotManager.snapshot(1), "tag1", store.options().tagDefaultTimeRetained());
        store.newExpire(1, 1, Long.MAX_VALUE).expire();

        // check data file and manifests
        FileStorePathFactory pathFactory = store.pathFactory();
        assertPathExists(fileIO, pathFactory.bucketPath(partition, 0));

        Snapshot tag1 = tagManager.taggedSnapshot("tag1");
        ManifestList manifestList = store.manifestListFactory().create();
        List<Path> manifestFilePaths =
                manifestList.readDataManifests(tag1).stream()
                        .map(ManifestFileMeta::fileName)
                        .map(pathFactory::toManifestFilePath)
                        .collect(Collectors.toList());
        for (Path path : manifestFilePaths) {
            assertPathExists(fileIO, path);
        }

        assertPathExists(fileIO, pathFactory.toManifestListPath(tag1.baseManifestList()));
        assertPathExists(fileIO, pathFactory.toManifestListPath(tag1.deltaManifestList()));
    }

    @Test
    public void testDeleteTagWithSnapshot() throws Exception {
        TestFileStore store = createStore(TestKeyValueGenerator.GeneratorMode.NON_PARTITIONED, 3);
        tagManager = new TagManager(fileIO, store.options().path());
        SnapshotManager snapshotManager = store.snapshotManager();
        TestKeyValueGenerator gen =
                new TestKeyValueGenerator(TestKeyValueGenerator.GeneratorMode.NON_PARTITIONED);
        BinaryRow partition = gen.getPartition(gen.next());

        // snapshot 1: commit A to bucket 0 and B to bucket 1
        Map<BinaryRow, Map<Integer, RecordWriter<KeyValue>>> writers = new HashMap<>();
        for (int bucket : Arrays.asList(0, 1)) {
            List<KeyValue> kvs = partitionedData(5, gen);
            writeData(store, kvs, partition, bucket, writers);
        }
        commitData(store, commitIdentifier++, writers);

        // snapshot 2: commit -A (by cleaning bucket 0)
        cleanBucket(store, partition, 0);

        // snapshot 3: commit C to bucket 2
        writers.clear();
        writeData(store, partitionedData(5, gen), partition, 2, writers);
        commitData(store, commitIdentifier++, writers);

        ManifestList manifestList = store.manifestListFactory().create();
        Snapshot snapshot1 = snapshotManager.snapshot(1);
        List<ManifestFileMeta> snapshot1Data = manifestList.readDataManifests(snapshot1);
        Snapshot snapshot3 = snapshotManager.snapshot(3);
        List<ManifestFileMeta> snapshot3Data = manifestList.readDataManifests(snapshot3);

        List<String> manifestLists =
                Arrays.asList(snapshot1.baseManifestList(), snapshot1.deltaManifestList());

        // create tag1
        createTag(snapshot1, "tag1", store.options().tagDefaultTimeRetained());

        // expire snapshot 1, 2
        store.newExpire(1, 1, Long.MAX_VALUE).expire();

        // check before deleting tag1
        FileStorePathFactory pathFactory = store.pathFactory();
        for (int i = 0; i < 3; i++) {
            assertPathExists(fileIO, pathFactory.bucketPath(partition, i));
        }
        for (ManifestFileMeta manifestFileMeta : snapshot1Data) {
            assertPathExists(fileIO, pathFactory.toManifestFilePath(manifestFileMeta.fileName()));
        }
        for (String manifestListName : manifestLists) {
            assertPathExists(fileIO, pathFactory.toManifestListPath(manifestListName));
        }

        tagManager.deleteTag(
                "tag1", store.newTagDeletion(), snapshotManager, Collections.emptyList());

        // check data files
        assertPathNotExists(fileIO, pathFactory.bucketPath(partition, 0));
        assertPathExists(fileIO, pathFactory.bucketPath(partition, 1));
        assertPathExists(fileIO, pathFactory.bucketPath(partition, 2));

        // check manifests
        for (ManifestFileMeta manifestFileMeta : snapshot1Data) {
            Path path = pathFactory.toManifestFilePath(manifestFileMeta.fileName());
            if (snapshot3Data.contains(manifestFileMeta)) {
                assertPathExists(fileIO, path);
            } else {
                assertPathNotExists(fileIO, path);
            }
        }
        for (String manifestListName : manifestLists) {
            assertPathNotExists(fileIO, pathFactory.toManifestListPath(manifestListName));
        }
    }

    @Test
    public void testDeleteTagWithOtherTag() throws Exception {
        TestFileStore store = createStore(TestKeyValueGenerator.GeneratorMode.NON_PARTITIONED, 3);
        tagManager = new TagManager(fileIO, store.options().path());
        SnapshotManager snapshotManager = store.snapshotManager();
        TestKeyValueGenerator gen =
                new TestKeyValueGenerator(TestKeyValueGenerator.GeneratorMode.NON_PARTITIONED);
        BinaryRow partition = gen.getPartition(gen.next());

        // snapshot 1: commit A to bucket 0
        Map<BinaryRow, Map<Integer, RecordWriter<KeyValue>>> writers = new HashMap<>();
        writeData(store, partitionedData(5, gen), partition, 0, writers);
        commitData(store, commitIdentifier++, writers);

        // snapshot 2: commit B to bucket 1
        writers.clear();
        writeData(store, partitionedData(5, gen), partition, 1, writers);
        commitData(store, commitIdentifier++, writers);

        // snapshot 3: commit -A (by cleaning bucket 0)
        cleanBucket(store, partition, 0);

        // snapshot 4: commit -B (by cleaning bucket 1)
        cleanBucket(store, partition, 1);

        // snapshot 5: commit C to snapshot 2 (used as the last snapshot)
        cleanBucket(store, partition, 2);

        ManifestList manifestList = store.manifestListFactory().create();
        Snapshot snapshot2 = snapshotManager.snapshot(2);
        List<ManifestFileMeta> snapshot2Data = manifestList.readDataManifests(snapshot2);

        List<String> manifestLists =
                Arrays.asList(snapshot2.baseManifestList(), snapshot2.deltaManifestList());

        // create tags
        createTag(snapshotManager.snapshot(1), "tag1", store.options().tagDefaultTimeRetained());
        createTag(snapshotManager.snapshot(2), "tag2", store.options().tagDefaultTimeRetained());
        createTag(snapshotManager.snapshot(4), "tag3", store.options().tagDefaultTimeRetained());

        // expire snapshot 1, 2, 3, 4
        store.newExpire(1, 1, Long.MAX_VALUE).expire();

        // check before deleting tag2
        FileStorePathFactory pathFactory = store.pathFactory();
        assertPathExists(fileIO, pathFactory.bucketPath(partition, 0));
        assertPathExists(fileIO, pathFactory.bucketPath(partition, 1));

        for (ManifestFileMeta manifestFileMeta : snapshot2Data) {
            assertPathExists(fileIO, pathFactory.toManifestFilePath(manifestFileMeta.fileName()));
        }
        for (String manifestListName : manifestLists) {
            assertPathExists(fileIO, pathFactory.toManifestListPath(manifestListName));
        }

        tagManager.deleteTag(
                "tag2", store.newTagDeletion(), snapshotManager, Collections.emptyList());

        // check data files
        assertPathExists(fileIO, pathFactory.bucketPath(partition, 0));
        assertPathNotExists(fileIO, pathFactory.bucketPath(partition, 1));

        // check manifests
        Snapshot tag1 = tagManager.taggedSnapshot("tag1");
        Snapshot tag3 = tagManager.taggedSnapshot("tag3");
        List<ManifestFileMeta> existing = manifestList.readDataManifests(tag1);
        existing.addAll(manifestList.readDataManifests(tag3));
        for (ManifestFileMeta manifestFileMeta : snapshot2Data) {
            Path path = pathFactory.toManifestFilePath(manifestFileMeta.fileName());
            if (existing.contains(manifestFileMeta)) {
                assertPathExists(fileIO, path);
            } else {
                assertPathNotExists(fileIO, path);
            }
        }

        for (String manifestListName : manifestLists) {
            assertPathNotExists(fileIO, pathFactory.toManifestListPath(manifestListName));
        }
    }

    /**
     * When a data file is upgraded, it will have a {@link FileKind#ADD} and a {@link
     * FileKind#DELETE} entries. This test ensure that if the ADD entry cannot be read correctly
     * when expiring, the data file won't be deleted. In this test we manually separate the ADD
     * entry and delete entry into two manifest file and delete the ADD entry manifest file to
     * simulate the read exception.
     */
    @Test
    public void testExpireWithMissingManifest() throws Exception {
        TestFileStore store = createStore(TestKeyValueGenerator.GeneratorMode.NON_PARTITIONED);
        SnapshotManager snapshotManager = store.snapshotManager();
        TestKeyValueGenerator gen =
                new TestKeyValueGenerator(TestKeyValueGenerator.GeneratorMode.NON_PARTITIONED);
        BinaryRow partition = gen.getPartition(gen.next());

        // snapshot 1: commit A to bucket 0
        Map<BinaryRow, Map<Integer, RecordWriter<KeyValue>>> writers = new HashMap<>();
        List<KeyValue> kvs = partitionedData(5, gen);
        writeData(store, kvs, partition, 0, writers);
        commitData(store, commitIdentifier++, writers);

        // snapshot 2: compact
        writers.values().stream()
                .flatMap(m -> m.values().stream())
                .forEach(
                        writer -> {
                            try {
                                writer.compact(true);
                                writer.sync();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
        FileStoreTestUtils.commitData(store, commitIdentifier++, writers);

        // check that there are one data file and get its path
        FileStorePathFactory pathFactory = store.pathFactory();
        Path bucket0 = pathFactory.bucketPath(partition, 0);
        List<Path> datafiles =
                Files.walk(Paths.get(bucket0.toString()))
                        .filter(Files::isRegularFile)
                        .filter(p -> p.getFileName().toString().startsWith("data"))
                        .map(p -> new Path(p.toString()))
                        .collect(Collectors.toList());
        assertThat(datafiles.size()).isEqualTo(1);
        Path dataFileA = datafiles.get(0);

        // check the snapshot 2 has two delta manifests entry (-A, level=0), (+A, level=5)
        Snapshot snapshot2 = snapshotManager.snapshot(2);
        ManifestList manifestList = store.manifestListFactory().create();
        ManifestFile manifestFile = store.manifestFileFactory().create();

        String deltaManifestList = snapshot2.deltaManifestList();
        List<ManifestFileMeta> manifestFileMetas = manifestList.read(snapshot2.deltaManifestList());
        assertThat(manifestFileMetas.size()).isEqualTo(1);

        String deltaManifestFile = manifestFileMetas.get(0).fileName();
        List<ManifestEntry> entries = manifestFile.read(deltaManifestFile);
        assertThat(entries.size()).isEqualTo(2);

        ManifestEntry addEntry = null, deleteEntry = null;
        for (ManifestEntry entry : entries) {
            assertThat(entry.file().fileName()).isEqualTo(dataFileA.getName());
            if (entry.kind() == FileKind.ADD) {
                assertThat(addEntry).isNull();
                addEntry = entry;
                assertThat(entry.file().level()).isGreaterThan(0);
            } else {
                assertThat(deleteEntry).isNull();
                deleteEntry = entry;
                assertThat(entry.file().level()).isEqualTo(0);
            }
        }
        assertThat(addEntry).isNotNull();
        assertThat(deleteEntry).isNotNull();

        // separate two entries to two manifest files and delete the (+A, level=5) manifest
        fileIO.deleteQuietly(pathFactory.toManifestListPath(deltaManifestList));
        fileIO.deleteQuietly(pathFactory.toManifestFilePath(deltaManifestFile));

        List<ManifestFileMeta> newAddManifests =
                manifestFile.write(Collections.singletonList(addEntry));
        assertThat(newAddManifests.size()).isEqualTo(1);
        String newAddManifestFileName = newAddManifests.get(0).fileName();
        fileIO.deleteQuietly(pathFactory.toManifestFilePath(newAddManifestFileName));

        List<ManifestFileMeta> newDeleteManifests =
                manifestFile.write(Collections.singletonList(deleteEntry));
        assertThat(newDeleteManifests.size()).isEqualTo(1);

        List<ManifestFileMeta> newManifests =
                Arrays.asList(newAddManifests.get(0), newDeleteManifests.get(0));

        String newManifestListName = manifestList.write(newManifests);

        fileIO.rename(
                pathFactory.toManifestListPath(newManifestListName),
                pathFactory.toManifestListPath(deltaManifestList));

        store.newExpire(1, 1, Long.MAX_VALUE).expire();

        // check data file
        assertPathExists(fileIO, dataFileA);
    }

    @Test
    public void testExpireWithDeletingTags() throws Exception {
        TestFileStore store = createStore(TestKeyValueGenerator.GeneratorMode.NON_PARTITIONED, 2);
        tagManager = new TagManager(fileIO, store.options().path());
        SnapshotManager snapshotManager = store.snapshotManager();
        TestKeyValueGenerator gen =
                new TestKeyValueGenerator(TestKeyValueGenerator.GeneratorMode.NON_PARTITIONED);
        BinaryRow partition = gen.getPartition(gen.next());

        // step 1: commit A to bucket 0 and create tag1
        Map<BinaryRow, Map<Integer, RecordWriter<KeyValue>>> writers = new HashMap<>();
        List<KeyValue> kvs = partitionedData(5, gen);
        writeData(store, kvs, partition, 0, writers);
        commitData(store, commitIdentifier++, writers);
        createTag(snapshotManager.snapshot(1), "tag1", store.options().tagDefaultTimeRetained());

        // step 2: commit B to bucket 1 and create tag2
        writers.clear();
        kvs = partitionedData(5, gen);
        writeData(store, kvs, partition, 1, writers);
        commitData(store, commitIdentifier++, writers);
        createTag(snapshotManager.snapshot(2), "tag2", store.options().tagDefaultTimeRetained());

        // step 3: commit -B
        cleanBucket(store, partition, 1);

        // step 4: commit -A
        cleanBucket(store, partition, 0);

        // action: expire snapshot 1 -> delete tag1 -> expire snapshot 2
        // result: exist A & B (because of tag2)
        ExpireSnapshots expireSnapshots =
                new ExpireSnapshotsImpl(snapshotManager, store.newSnapshotDeletion(), tagManager);
        expireSnapshots
                .config(
                        ExpireConfig.builder()
                                .snapshotRetainMax(3)
                                .snapshotRetainMin(3)
                                .snapshotTimeRetain(Duration.ofMillis(Long.MAX_VALUE))
                                .build())
                .expire();
        tagManager.deleteTag(
                "tag1", store.newTagDeletion(), snapshotManager, Collections.emptyList());
        expireSnapshots
                .config(
                        ExpireConfig.builder()
                                .snapshotRetainMax(2)
                                .snapshotRetainMin(2)
                                .snapshotTimeRetain(Duration.ofMillis(Long.MAX_VALUE))
                                .build())
                .expire();

        assertThat(snapshotManager.snapshotCount()).isEqualTo(2);
        FileStorePathFactory pathFactory = store.pathFactory();
        assertPathExists(fileIO, pathFactory.bucketPath(partition, 0));
        assertPathExists(fileIO, pathFactory.bucketPath(partition, 1));
    }

    private TestFileStore createStore(TestKeyValueGenerator.GeneratorMode mode) throws Exception {
        return createStore(mode, 2);
    }

    private TestFileStore createStore(TestKeyValueGenerator.GeneratorMode mode, int buckets)
            throws Exception {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        CoreOptions.ChangelogProducer changelogProducer;
        if (random.nextBoolean()) {
            changelogProducer = CoreOptions.ChangelogProducer.INPUT;
        } else {
            changelogProducer = CoreOptions.ChangelogProducer.NONE;
        }

        RowType rowType, partitionType;
        switch (mode) {
            case NON_PARTITIONED:
                rowType = TestKeyValueGenerator.NON_PARTITIONED_ROW_TYPE;
                partitionType = TestKeyValueGenerator.NON_PARTITIONED_PART_TYPE;
                break;
            case SINGLE_PARTITIONED:
                rowType = TestKeyValueGenerator.SINGLE_PARTITIONED_ROW_TYPE;
                partitionType = TestKeyValueGenerator.SINGLE_PARTITIONED_PART_TYPE;
                break;
            case MULTI_PARTITIONED:
                rowType = TestKeyValueGenerator.DEFAULT_ROW_TYPE;
                partitionType = TestKeyValueGenerator.DEFAULT_PART_TYPE;
                break;
            default:
                throw new UnsupportedOperationException("Unsupported generator mode: " + mode);
        }

        SchemaManager schemaManager = new SchemaManager(fileIO, new Path(root));

        TableSchema tableSchema =
                schemaManager.createTable(
                        new Schema(
                                rowType.getFields(),
                                partitionType.getFieldNames(),
                                TestKeyValueGenerator.getPrimaryKeys(mode),
                                Collections.singletonMap(
                                        SNAPSHOT_CLEAN_EMPTY_DIRECTORIES.key(), "true"),
                                null));

        return new TestFileStore.Builder(
                        "avro",
                        root,
                        buckets,
                        partitionType,
                        TestKeyValueGenerator.KEY_TYPE,
                        rowType,
                        TestKeyValueGenerator.TestKeyValueFieldsExtractor.EXTRACTOR,
                        DeduplicateMergeFunction.factory(),
                        tableSchema)
                .changelogProducer(changelogProducer)
                .build();
    }

    private void writeData(
            TestFileStore store,
            List<KeyValue> kvs,
            BinaryRow partition,
            int bucket,
            Map<BinaryRow, Map<Integer, RecordWriter<KeyValue>>> writers)
            throws Exception {
        writers.computeIfAbsent(partition, p -> new HashMap<>())
                .put(bucket, FileStoreTestUtils.writeData(store, kvs, partition, bucket));
    }

    // clean given bucket and generate a new snapshot
    private void cleanBucket(TestFileStore store, BinaryRow partition, int bucket) {
        // manually make delete ManifestEntry
        List<ManifestEntry> bucketEntries =
                store.newScan().withPartitionBucket(partition, bucket).plan().files();
        List<ManifestEntry> delete =
                bucketEntries.stream()
                        .map(
                                entry ->
                                        new ManifestEntry(
                                                FileKind.DELETE,
                                                partition,
                                                bucket,
                                                entry.totalBuckets(),
                                                entry.file()))
                        .collect(Collectors.toList());
        // commit
        try (FileStoreCommitImpl commit = store.newCommit()) {
            commit.tryCommitOnce(
                    null,
                    delete,
                    Collections.emptyList(),
                    Collections.emptyList(),
                    commitIdentifier++,
                    null,
                    Collections.emptyMap(),
                    Snapshot.CommitKind.APPEND,
                    store.snapshotManager().latestSnapshot(),
                    mustConflictCheck(),
                    DEFAULT_MAIN_BRANCH,
                    null);
        }
    }

    private void createTag(Snapshot snapshot, String tagName, Duration timeRetained) {
        tagManager.createTag(snapshot, tagName, timeRetained, Collections.emptyList());
    }
}
