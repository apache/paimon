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

package org.apache.paimon;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOFinder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.IndexIncrement;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.memory.MemoryOwner;
import org.apache.paimon.mergetree.compact.MergeFunctionFactory;
import org.apache.paimon.operation.AbstractFileStoreWrite;
import org.apache.paimon.operation.FileStoreCommit;
import org.apache.paimon.operation.FileStoreCommitImpl;
import org.apache.paimon.operation.SplitRead;
import org.apache.paimon.options.ExpireConfig;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.KeyValueFieldsExtractor;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.ExpireChangelogImpl;
import org.apache.paimon.table.ExpireSnapshots;
import org.apache.paimon.table.ExpireSnapshotsImpl;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.RecordWriter;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** {@link FileStore} for tests. */
public class TestFileStore extends KeyValueFileStore {

    private static final Logger LOG = LoggerFactory.getLogger(TestFileStore.class);

    public static final MemorySize WRITE_BUFFER_SIZE = MemorySize.parse("16 kb");

    public static final MemorySize PAGE_SIZE = MemorySize.parse("4 kb");

    private final String root;
    private final FileIO fileIO;
    private final InternalRowSerializer keySerializer;
    private final InternalRowSerializer valueSerializer;
    private final String commitUser;

    private long commitIdentifier;

    private TestFileStore(
            String root,
            CoreOptions options,
            RowType partitionType,
            RowType keyType,
            RowType valueType,
            KeyValueFieldsExtractor keyValueFieldsExtractor,
            MergeFunctionFactory<KeyValue> mfFactory,
            TableSchema tableSchema) {
        super(
                FileIOFinder.find(new Path(root)),
                schemaManager(root, options),
                tableSchema != null
                        ? tableSchema
                        : new TableSchema(
                                0L,
                                valueType.getFields(),
                                valueType.getFieldCount(),
                                partitionType.getFieldNames(),
                                keyType.getFieldNames(),
                                Collections.emptyMap(),
                                null),
                false,
                options,
                partitionType,
                keyType,
                keyType,
                valueType,
                keyValueFieldsExtractor,
                mfFactory,
                (new Path(root)).getName(),
                CatalogEnvironment.empty());
        this.root = root;
        this.fileIO = FileIOFinder.find(new Path(root));
        this.keySerializer = new InternalRowSerializer(keyType);
        this.valueSerializer = new InternalRowSerializer(valueType);
        this.commitUser = UUID.randomUUID().toString();

        this.commitIdentifier = 0L;
    }

    private static SchemaManager schemaManager(String root, CoreOptions options) {
        return new SchemaManager(FileIOFinder.find(new Path(root)), options.path());
    }

    public AbstractFileStoreWrite<KeyValue> newWrite() {
        return super.newWrite(commitUser);
    }

    public FileStoreCommitImpl newCommit() {
        return super.newCommit(commitUser);
    }

    public ExpireSnapshots newExpire(int numRetainedMin, int numRetainedMax, long millisRetained) {
        return new ExpireSnapshotsImpl(
                        snapshotManager(),
                        newSnapshotDeletion(),
                        new TagManager(fileIO, options.path()))
                .config(
                        ExpireConfig.builder()
                                .snapshotRetainMax(numRetainedMax)
                                .snapshotRetainMin(numRetainedMin)
                                .snapshotTimeRetain(Duration.ofMillis(millisRetained))
                                .build());
    }

    public ExpireSnapshots newExpire(ExpireConfig expireConfig) {
        return new ExpireSnapshotsImpl(
                        snapshotManager(),
                        newSnapshotDeletion(),
                        new TagManager(fileIO, options.path()))
                .config(expireConfig);
    }

    public ExpireSnapshots newChangelogExpire(ExpireConfig config) {
        ExpireChangelogImpl impl =
                new ExpireChangelogImpl(
                        snapshotManager(),
                        new TagManager(fileIO, options.path()),
                        newChangelogDeletion());
        impl.config(config);
        return impl;
    }

    public List<Snapshot> commitData(
            List<KeyValue> kvs,
            Function<KeyValue, BinaryRow> partitionCalculator,
            Function<KeyValue, Integer> bucketCalculator)
            throws Exception {
        return commitData(kvs, partitionCalculator, bucketCalculator, new HashMap<>());
    }

    public List<Snapshot> commitDataWatermark(
            List<KeyValue> kvs, Function<KeyValue, BinaryRow> partitionCalculator, Long watermark)
            throws Exception {
        return commitDataImpl(
                kvs,
                partitionCalculator,
                kv -> 0,
                false,
                null,
                watermark,
                Collections.emptyList(),
                (commit, committable) -> commit.commit(committable, Collections.emptyMap()));
    }

    public List<Snapshot> commitData(
            List<KeyValue> kvs,
            Function<KeyValue, BinaryRow> partitionCalculator,
            Function<KeyValue, Integer> bucketCalculator,
            Map<Integer, Long> logOffsets)
            throws Exception {
        return commitDataImpl(
                kvs,
                partitionCalculator,
                bucketCalculator,
                false,
                null,
                null,
                Collections.emptyList(),
                (commit, committable) -> {
                    logOffsets.forEach(
                            (bucket, offset) -> committable.addLogOffset(bucket, offset, false));
                    commit.commit(committable, Collections.emptyMap());
                });
    }

    public List<Snapshot> overwriteData(
            List<KeyValue> kvs,
            Function<KeyValue, BinaryRow> partitionCalculator,
            Function<KeyValue, Integer> bucketCalculator,
            Map<String, String> partition)
            throws Exception {
        return commitDataImpl(
                kvs,
                partitionCalculator,
                bucketCalculator,
                true,
                null,
                null,
                Collections.emptyList(),
                (commit, committable) ->
                        commit.overwrite(partition, committable, Collections.emptyMap()));
    }

    public Snapshot dropPartitions(List<Map<String, String>> partitions) {
        SnapshotManager snapshotManager = snapshotManager();
        Long snapshotIdBeforeCommit = snapshotManager.latestSnapshotId();
        if (snapshotIdBeforeCommit == null) {
            snapshotIdBeforeCommit = Snapshot.FIRST_SNAPSHOT_ID - 1;
        }

        try (FileStoreCommit commit = newCommit(commitUser)) {
            commit.dropPartitions(partitions, Long.MAX_VALUE);
        }

        Long snapshotIdAfterCommit = snapshotManager.latestSnapshotId();
        assertThat(snapshotIdAfterCommit).isNotNull();
        assertThat(snapshotIdBeforeCommit + 1).isEqualTo(snapshotIdAfterCommit);

        return snapshotManager.snapshot(snapshotIdAfterCommit);
    }

    public List<Snapshot> commitDataIndex(
            KeyValue kv,
            Function<KeyValue, BinaryRow> partitionCalculator,
            int bucket,
            IndexFileMeta... indexFiles)
            throws Exception {
        return commitDataImpl(
                Collections.singletonList(kv),
                partitionCalculator,
                ignore -> bucket,
                false,
                null,
                null,
                Arrays.asList(indexFiles),
                (commit, committable) -> commit.commit(committable, Collections.emptyMap()));
    }

    public List<Snapshot> commitDataImpl(
            List<KeyValue> kvs,
            Function<KeyValue, BinaryRow> partitionCalculator,
            Function<KeyValue, Integer> bucketCalculator,
            boolean ignorePreviousFiles,
            Long identifier,
            Long watermark,
            List<IndexFileMeta> indexFiles,
            BiConsumer<FileStoreCommit, ManifestCommittable> commitFunction)
            throws Exception {
        AbstractFileStoreWrite<KeyValue> write = newWrite();
        Map<BinaryRow, Map<Integer, RecordWriter<KeyValue>>> writers = new HashMap<>();
        for (KeyValue kv : kvs) {
            BinaryRow partition = partitionCalculator.apply(kv);
            int bucket = bucketCalculator.apply(kv);
            writers.computeIfAbsent(partition, p -> new HashMap<>())
                    .compute(
                            bucket,
                            (b, w) -> {
                                if (w == null) {
                                    RecordWriter<KeyValue> writer =
                                            write.createWriterContainer(
                                                            partition, bucket, ignorePreviousFiles)
                                                    .writer;
                                    ((MemoryOwner) writer)
                                            .setMemoryPool(
                                                    new HeapMemorySegmentPool(
                                                            WRITE_BUFFER_SIZE.getBytes(),
                                                            (int) PAGE_SIZE.getBytes()));
                                    return writer;
                                } else {
                                    return w;
                                }
                            })
                    .write(kv);
        }

        ManifestCommittable committable =
                new ManifestCommittable(
                        identifier == null ? commitIdentifier++ : identifier, watermark);
        for (Map.Entry<BinaryRow, Map<Integer, RecordWriter<KeyValue>>> entryWithPartition :
                writers.entrySet()) {
            for (Map.Entry<Integer, RecordWriter<KeyValue>> entryWithBucket :
                    entryWithPartition.getValue().entrySet()) {
                CommitIncrement increment =
                        entryWithBucket.getValue().prepareCommit(ignorePreviousFiles);
                committable.addFileCommittable(
                        new CommitMessageImpl(
                                entryWithPartition.getKey(),
                                entryWithBucket.getKey(),
                                increment.newFilesIncrement(),
                                increment.compactIncrement(),
                                new IndexIncrement(indexFiles)));
            }
        }

        SnapshotManager snapshotManager = snapshotManager();
        Long snapshotIdBeforeCommit = snapshotManager.latestSnapshotId();
        if (snapshotIdBeforeCommit == null) {
            snapshotIdBeforeCommit = Snapshot.FIRST_SNAPSHOT_ID - 1;
        }

        try (FileStoreCommit commit = newCommit(commitUser)) {
            commitFunction.accept(commit, committable);
        }

        Long snapshotIdAfterCommit = snapshotManager.latestSnapshotId();
        if (snapshotIdAfterCommit == null) {
            snapshotIdAfterCommit = Snapshot.FIRST_SNAPSHOT_ID - 1;
        }

        writers.values().stream()
                .flatMap(m -> m.values().stream())
                .forEach(
                        w -> {
                            try {
                                // wait for compaction to end, otherwise orphan files may occur
                                // see CompactManager#cancelCompaction for more info
                                w.sync();
                                w.close();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        List<Snapshot> snapshots = new ArrayList<>();
        for (long id = snapshotIdBeforeCommit + 1; id <= snapshotIdAfterCommit; id++) {
            snapshots.add(snapshotManager.snapshot(id));
        }
        return snapshots;
    }

    public List<KeyValue> readKvsFromSnapshot(long snapshotId) throws Exception {
        List<ManifestEntry> entries = newScan().withSnapshot(snapshotId).plan().files();
        return readKvsFromManifestEntries(entries, false);
    }

    public List<KeyValue> readAllChangelogUntilSnapshot(long endInclusive) throws Exception {
        List<KeyValue> result = new ArrayList<>();
        for (long snapshotId = Snapshot.FIRST_SNAPSHOT_ID;
                snapshotId <= endInclusive;
                snapshotId++) {
            List<ManifestEntry> entries =
                    newScan()
                            .withKind(
                                    options.changelogProducer()
                                                    == CoreOptions.ChangelogProducer.NONE
                                            ? ScanMode.DELTA
                                            : ScanMode.CHANGELOG)
                            .withSnapshot(snapshotId)
                            .plan()
                            .files();
            result.addAll(readKvsFromManifestEntries(entries, true));
        }
        return result;
    }

    public List<KeyValue> readKvsFromManifestEntries(
            List<ManifestEntry> entries, boolean isStreaming) throws Exception {
        if (LOG.isDebugEnabled()) {
            for (ManifestEntry entry : entries) {
                LOG.debug("reading from " + entry.toString());
            }
        }

        Map<BinaryRow, Map<Integer, List<DataFileMeta>>> filesPerPartitionAndBucket =
                new HashMap<>();
        for (ManifestEntry entry : entries) {
            filesPerPartitionAndBucket
                    .computeIfAbsent(entry.partition(), p -> new HashMap<>())
                    .computeIfAbsent(entry.bucket(), b -> new ArrayList<>())
                    .add(entry.file());
        }

        List<KeyValue> kvs = new ArrayList<>();
        SplitRead<KeyValue> read = newRead();
        for (Map.Entry<BinaryRow, Map<Integer, List<DataFileMeta>>> entryWithPartition :
                filesPerPartitionAndBucket.entrySet()) {
            for (Map.Entry<Integer, List<DataFileMeta>> entryWithBucket :
                    entryWithPartition.getValue().entrySet()) {
                RecordReaderIterator<KeyValue> iterator =
                        new RecordReaderIterator<>(
                                read.createReader(
                                        DataSplit.builder()
                                                .withPartition(entryWithPartition.getKey())
                                                .withBucket(entryWithBucket.getKey())
                                                .withDataFiles(entryWithBucket.getValue())
                                                .isStreaming(isStreaming)
                                                .rawConvertible(false)
                                                .withBucketPath("not used")
                                                .build()));
                while (iterator.hasNext()) {
                    kvs.add(iterator.next().copy(keySerializer, valueSerializer));
                }
                iterator.close();
            }
        }
        return kvs;
    }

    public Map<BinaryRow, BinaryRow> toKvMap(List<KeyValue> kvs) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Compacting list of key values to kv map\n"
                            + kvs.stream()
                                    .map(
                                            kv ->
                                                    kv.toString(
                                                            TestKeyValueGenerator.KEY_TYPE,
                                                            TestKeyValueGenerator.DEFAULT_ROW_TYPE))
                                    .collect(Collectors.joining("\n")));
        }

        Map<BinaryRow, BinaryRow> result = new HashMap<>();
        for (KeyValue kv : kvs) {
            BinaryRow key = keySerializer.toBinaryRow(kv.key()).copy();
            BinaryRow value = valueSerializer.toBinaryRow(kv.value()).copy();
            switch (kv.valueKind()) {
                case INSERT:
                case UPDATE_AFTER:
                    result.put(key, value);
                    break;
                case UPDATE_BEFORE:
                case DELETE:
                    result.remove(key);
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Unknown value kind " + kv.valueKind().name());
            }
        }
        return result;
    }

    public void assertCleaned() throws IOException {
        Set<Path> filesInUse = getFilesInUse();
        Set<Path> actualFiles =
                Files.walk(Paths.get(root))
                        .filter(Files::isRegularFile)
                        .map(p -> new Path(p.toString()))
                        .collect(Collectors.toSet());

        // remove best effort latest and earliest hint files
        // Consider concurrency test, it will not be possible to check here because the hint_file is
        // possibly not the most accurate, so this check is only.
        // - latest should < true_latest
        // - earliest should < true_earliest
        SnapshotManager snapshotManager = snapshotManager();
        Path snapshotDir = snapshotManager.snapshotDirectory();
        Path earliest = new Path(snapshotDir, SnapshotManager.EARLIEST);
        Path latest = new Path(snapshotDir, SnapshotManager.LATEST);
        if (actualFiles.remove(earliest)) {
            long earliestId = snapshotManager.readHint(SnapshotManager.EARLIEST);
            fileIO.delete(earliest, false);
            assertThat(earliestId <= snapshotManager.earliestSnapshotId()).isTrue();
        }
        if (actualFiles.remove(latest)) {
            long latestId = snapshotManager.readHint(SnapshotManager.LATEST);
            fileIO.delete(latest, false);
            assertThat(latestId <= snapshotManager.latestSnapshotId()).isTrue();
        }
        Path changelogDir = snapshotManager.changelogDirectory();
        Path earliestChangelog = new Path(changelogDir, SnapshotManager.EARLIEST);
        Path latestChangelog = new Path(changelogDir, SnapshotManager.LATEST);

        if (actualFiles.remove(earliestChangelog)) {
            long earliestId = snapshotManager.readHint(SnapshotManager.EARLIEST, changelogDir);
            fileIO.delete(earliest, false);
            assertThat(earliestId <= snapshotManager.earliestLongLivedChangelogId()).isTrue();
        }
        if (actualFiles.remove(latestChangelog)) {
            long latestId = snapshotManager.readHint(SnapshotManager.LATEST, changelogDir);
            fileIO.delete(latest, false);
            assertThat(latestId <= snapshotManager.latestLongLivedChangelogId()).isTrue();
        }

        // for easier debugging
        String expectedString =
                filesInUse.stream().map(Path::toString).sorted().collect(Collectors.joining(",\n"));
        String actualString =
                actualFiles.stream()
                        .map(Path::toString)
                        .sorted()
                        .collect(Collectors.joining(",\n"));
        if (!actualString.equals(expectedString)) {
            System.out.println("");
        }
        assertThat(actualString).isEqualTo(expectedString);
    }

    private Set<Path> getFilesInUse() {
        Set<Path> result = new HashSet<>();

        SchemaManager schemaManager = new SchemaManager(fileIO, options.path());
        schemaManager.listAllIds().forEach(id -> result.add(schemaManager.toSchemaPath(id)));

        SnapshotManager snapshotManager = snapshotManager();
        Long latestSnapshotId = snapshotManager.latestSnapshotId();

        if (latestSnapshotId == null) {
            return result;
        }

        long firstInUseSnapshotId = Snapshot.FIRST_SNAPSHOT_ID;
        for (long id = latestSnapshotId - 1; id >= Snapshot.FIRST_SNAPSHOT_ID; id--) {
            if (!snapshotManager.snapshotExists(id)
                    && !snapshotManager.longLivedChangelogExists(id)) {
                firstInUseSnapshotId = id + 1;
                break;
            }
        }

        for (long id = firstInUseSnapshotId; id <= latestSnapshotId; id++) {
            result.addAll(getFilesInUse(id));
        }

        return result;
    }

    public Set<Path> getFilesInUse(long snapshotId) {
        return getFilesInUse(
                snapshotId,
                snapshotManager(),
                fileIO,
                pathFactory(),
                manifestListFactory().create(),
                manifestFileFactory().create());
    }

    public static Set<Path> getFilesInUse(
            long snapshotId,
            SnapshotManager snapshotManager,
            FileIO fileIO,
            FileStorePathFactory pathFactory,
            ManifestList manifestList,
            ManifestFile manifestFile) {
        Set<Path> result = new HashSet<>();

        if (snapshotManager.snapshotExists(snapshotId)) {
            Set<Path> files =
                    getSnapshotFileInUse(
                            snapshotId,
                            snapshotManager,
                            fileIO,
                            pathFactory,
                            manifestList,
                            manifestFile);
            result.addAll(files);
        } else if (snapshotManager.longLivedChangelogExists(snapshotId)) {
            Set<Path> files =
                    getChangelogFileInUse(
                            snapshotId,
                            snapshotManager,
                            fileIO,
                            pathFactory,
                            manifestList,
                            manifestFile);
            result.addAll(files);
        } else {
            throw new RuntimeException(
                    String.format("The snapshot %s does not exist.", snapshotId));
        }

        return result;
    }

    private static Set<Path> getSnapshotFileInUse(
            long snapshotId,
            SnapshotManager snapshotManager,
            FileIO fileIO,
            FileStorePathFactory pathFactory,
            ManifestList manifestList,
            ManifestFile manifestFile) {
        Set<Path> result = new HashSet<>();
        SchemaManager schemaManager = new SchemaManager(fileIO, snapshotManager.tablePath());
        CoreOptions options = new CoreOptions(schemaManager.latest().get().options());
        boolean produceChangelog =
                options.changelogProducer() != CoreOptions.ChangelogProducer.NONE;
        // The option from the table may not align with the expiration config
        boolean changelogDecoupled = snapshotManager.earliestLongLivedChangelogId() != null;

        Path snapshotPath = snapshotManager.snapshotPath(snapshotId);
        Snapshot snapshot = Snapshot.fromPath(fileIO, snapshotPath);

        // snapshot file
        result.add(snapshotPath);

        // manifest lists
        result.add(pathFactory.toManifestListPath(snapshot.baseManifestList()));
        result.add(pathFactory.toManifestListPath(snapshot.deltaManifestList()));
        if (snapshot.changelogManifestList() != null) {
            result.add(pathFactory.toManifestListPath(snapshot.changelogManifestList()));
        }

        // manifests
        List<ManifestFileMeta> manifests = manifestList.readAllManifests(snapshot);
        manifests.forEach(m -> result.add(pathFactory.toManifestFilePath(m.fileName())));

        // data file
        List<ManifestEntry> entries =
                manifests.stream()
                        .flatMap(m -> manifestFile.read(m.fileName()).stream())
                        .collect(Collectors.toList());
        entries = new ArrayList<>(FileEntry.mergeEntries(entries));
        for (ManifestEntry entry : entries) {
            result.add(
                    new Path(
                            pathFactory.bucketPath(entry.partition(), entry.bucket()),
                            entry.file().fileName()));
        }

        // Add 'DELETE' 'APPEND' file in snapshot
        // These 'delete' files can be merged by the plan#splits,
        // so it's not shown in the entries above.
        // In other words, these files are not used (by snapshot or changelog) now,
        // but it can only be cleaned after this snapshot expired, so we should add it to the file
        // use list.
        if (changelogDecoupled && !produceChangelog) {
            entries =
                    manifestList.readDeltaManifests(snapshot).stream()
                            .flatMap(m -> manifestFile.read(m.fileName()).stream())
                            .collect(Collectors.toList());
            for (ManifestEntry entry : entries) {
                // append delete file are delayed to delete
                if (entry.kind() == FileKind.DELETE
                        && entry.file().fileSource().orElse(FileSource.APPEND)
                                == FileSource.APPEND) {
                    result.add(
                            new Path(
                                    pathFactory.bucketPath(entry.partition(), entry.bucket()),
                                    entry.file().fileName()));
                }
            }
        }

        return result;
    }

    private static Set<Path> getChangelogFileInUse(
            long changelogId,
            SnapshotManager snapshotManager,
            FileIO fileIO,
            FileStorePathFactory pathFactory,
            ManifestList manifestList,
            ManifestFile manifestFile) {
        Set<Path> result = new HashSet<>();
        SchemaManager schemaManager = new SchemaManager(fileIO, snapshotManager.tablePath());
        CoreOptions options = new CoreOptions(schemaManager.latest().get().options());

        Path changelogPath = snapshotManager.longLivedChangelogPath(changelogId);
        Changelog changelog = Changelog.fromPath(fileIO, changelogPath);

        // changelog file
        result.add(changelogPath);

        // data file
        // not all manifests contains useful data file
        // (1) produceChangelog = 'true': data file in changelog manifests
        // (2) produceChangelog = 'false': 'APPEND' data file in delta manifests

        // delta file
        if (options.changelogProducer() == CoreOptions.ChangelogProducer.NONE) {
            // TODO why we need to keep base manifests?
            result.add(pathFactory.toManifestListPath(changelog.baseManifestList()));
            manifestList
                    .readDataManifests(changelog)
                    .forEach(m -> result.add(pathFactory.toManifestFilePath(m.fileName())));

            result.add(pathFactory.toManifestListPath(changelog.deltaManifestList()));
            List<ManifestFileMeta> manifests = manifestList.readDeltaManifests(changelog);
            manifests.forEach(m -> result.add(pathFactory.toManifestFilePath(m.fileName())));
            List<ManifestEntry> files =
                    manifests.stream()
                            .flatMap(m -> manifestFile.read(m.fileName()).stream())
                            .collect(Collectors.toList());
            for (ManifestEntry entry : files) {
                if (entry.file().fileSource().orElse(FileSource.APPEND) == FileSource.APPEND) {
                    result.add(
                            new Path(
                                    pathFactory.bucketPath(entry.partition(), entry.bucket()),
                                    entry.file().fileName()));
                }
            }
        } else if (changelog.changelogManifestList() != null) {
            result.add(pathFactory.toManifestListPath(changelog.changelogManifestList()));
            List<ManifestFileMeta> manifests = manifestList.readChangelogManifests(changelog);
            manifests.forEach(m -> result.add(pathFactory.toManifestFilePath(m.fileName())));
            List<ManifestEntry> files =
                    manifests.stream()
                            .flatMap(m -> manifestFile.read(m.fileName()).stream())
                            .collect(Collectors.toList());
            for (ManifestEntry entry : files) {
                result.add(
                        new Path(
                                pathFactory.bucketPath(entry.partition(), entry.bucket()),
                                entry.file().fileName()));
            }
        }
        return result;
    }

    /** Builder of {@link TestFileStore}. */
    public static class Builder {

        private final String format;
        private final String root;
        private final int numBuckets;
        private final RowType partitionType;
        private final RowType keyType;
        private final RowType valueType;
        private final KeyValueFieldsExtractor keyValueFieldsExtractor;
        private final MergeFunctionFactory<KeyValue> mfFactory;
        private final TableSchema tableSchema;

        private CoreOptions.ChangelogProducer changelogProducer;

        public Builder(
                String format,
                String root,
                int numBuckets,
                RowType partitionType,
                RowType keyType,
                RowType valueType,
                KeyValueFieldsExtractor keyValueFieldsExtractor,
                MergeFunctionFactory<KeyValue> mfFactory,
                TableSchema tableSchema) {
            this.format = format;
            this.root = root;
            this.numBuckets = numBuckets;
            this.partitionType = partitionType;
            this.keyType = keyType;
            this.valueType = valueType;
            this.keyValueFieldsExtractor = keyValueFieldsExtractor;
            this.mfFactory = mfFactory;
            this.tableSchema = tableSchema;

            this.changelogProducer = CoreOptions.ChangelogProducer.NONE;
        }

        public Builder changelogProducer(CoreOptions.ChangelogProducer changelogProducer) {
            this.changelogProducer = changelogProducer;
            return this;
        }

        public TestFileStore build() {
            Options conf =
                    tableSchema == null ? new Options() : Options.fromMap(tableSchema.options());

            conf.set(CoreOptions.WRITE_BUFFER_SIZE, WRITE_BUFFER_SIZE);
            conf.set(CoreOptions.PAGE_SIZE, PAGE_SIZE);
            conf.set(CoreOptions.TARGET_FILE_SIZE, MemorySize.parse("1 kb"));

            conf.set(
                    CoreOptions.MANIFEST_TARGET_FILE_SIZE,
                    MemorySize.parse((ThreadLocalRandom.current().nextInt(16) + 1) + "kb"));

            conf.set(CoreOptions.FILE_FORMAT, format);
            conf.set(CoreOptions.MANIFEST_FORMAT, format);
            conf.set(CoreOptions.PATH, root);
            conf.set(CoreOptions.BUCKET, numBuckets);

            conf.set(CoreOptions.CHANGELOG_PRODUCER, changelogProducer);

            // disable dynamic-partition-overwrite in FileStoreCommit layer test
            conf.set(CoreOptions.DYNAMIC_PARTITION_OVERWRITE, false);

            return new TestFileStore(
                    root,
                    new CoreOptions(conf),
                    partitionType,
                    keyType,
                    valueType,
                    keyValueFieldsExtractor,
                    mfFactory,
                    tableSchema);
        }
    }
}
