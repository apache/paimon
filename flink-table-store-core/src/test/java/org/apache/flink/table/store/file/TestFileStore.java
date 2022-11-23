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

package org.apache.flink.table.store.file;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.file.io.DataFileMeta;
import org.apache.flink.table.store.file.manifest.ManifestCommittable;
import org.apache.flink.table.store.file.manifest.ManifestEntry;
import org.apache.flink.table.store.file.manifest.ManifestFileMeta;
import org.apache.flink.table.store.file.manifest.ManifestList;
import org.apache.flink.table.store.file.memory.HeapMemorySegmentPool;
import org.apache.flink.table.store.file.memory.MemoryOwner;
import org.apache.flink.table.store.file.mergetree.compact.MergeFunction;
import org.apache.flink.table.store.file.operation.AbstractFileStoreWrite;
import org.apache.flink.table.store.file.operation.FileStoreCommit;
import org.apache.flink.table.store.file.operation.FileStoreCommitImpl;
import org.apache.flink.table.store.file.operation.FileStoreExpireImpl;
import org.apache.flink.table.store.file.operation.FileStoreRead;
import org.apache.flink.table.store.file.operation.FileStoreScan;
import org.apache.flink.table.store.file.schema.KeyFieldsExtractor;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.RecordReaderIterator;
import org.apache.flink.table.store.file.utils.RecordWriter;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.table.sink.FileCommittable;
import org.apache.flink.table.store.table.source.DataSplit;
import org.apache.flink.table.types.logical.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
    private final RowDataSerializer keySerializer;
    private final RowDataSerializer valueSerializer;
    private final String commitUser;

    private long commitIdentifier;

    private TestFileStore(
            String root,
            CoreOptions options,
            RowType partitionType,
            RowType keyType,
            RowType valueType,
            KeyFieldsExtractor keyFieldsExtractor,
            MergeFunction<KeyValue> mergeFunction) {
        super(
                new SchemaManager(options.path()),
                0L,
                options,
                partitionType,
                keyType,
                keyType,
                valueType,
                keyFieldsExtractor,
                p -> mergeFunction);
        this.root = root;
        this.keySerializer = new RowDataSerializer(keyType);
        this.valueSerializer = new RowDataSerializer(valueType);
        this.commitUser = UUID.randomUUID().toString();

        this.commitIdentifier = 0L;
    }

    public FileStoreCommitImpl newCommit() {
        return super.newCommit(commitUser);
    }

    public FileStoreExpireImpl newExpire(
            int numRetainedMin, int numRetainedMax, long millisRetained) {
        return new FileStoreExpireImpl(
                numRetainedMin,
                numRetainedMax,
                millisRetained,
                pathFactory(),
                snapshotManager(),
                manifestFileFactory(),
                manifestListFactory());
    }

    public List<Snapshot> commitData(
            List<KeyValue> kvs,
            Function<KeyValue, BinaryRowData> partitionCalculator,
            Function<KeyValue, Integer> bucketCalculator)
            throws Exception {
        return commitData(kvs, partitionCalculator, bucketCalculator, new HashMap<>());
    }

    public List<Snapshot> commitData(
            List<KeyValue> kvs,
            Function<KeyValue, BinaryRowData> partitionCalculator,
            Function<KeyValue, Integer> bucketCalculator,
            Map<Integer, Long> logOffsets)
            throws Exception {
        return commitDataImpl(
                kvs,
                partitionCalculator,
                bucketCalculator,
                false,
                null,
                (commit, committable) -> {
                    logOffsets.forEach(committable::addLogOffset);
                    commit.commit(committable, Collections.emptyMap());
                });
    }

    public List<Snapshot> overwriteData(
            List<KeyValue> kvs,
            Function<KeyValue, BinaryRowData> partitionCalculator,
            Function<KeyValue, Integer> bucketCalculator,
            Map<String, String> partition)
            throws Exception {
        return commitDataImpl(
                kvs,
                partitionCalculator,
                bucketCalculator,
                true,
                null,
                (commit, committable) ->
                        commit.overwrite(partition, committable, Collections.emptyMap()));
    }

    public List<Snapshot> commitDataImpl(
            List<KeyValue> kvs,
            Function<KeyValue, BinaryRowData> partitionCalculator,
            Function<KeyValue, Integer> bucketCalculator,
            boolean emptyWriter,
            Long identifier,
            BiConsumer<FileStoreCommit, ManifestCommittable> commitFunction)
            throws Exception {
        AbstractFileStoreWrite<KeyValue> write = newWrite(commitUser);
        Map<BinaryRowData, Map<Integer, RecordWriter<KeyValue>>> writers = new HashMap<>();
        for (KeyValue kv : kvs) {
            BinaryRowData partition = partitionCalculator.apply(kv);
            int bucket = bucketCalculator.apply(kv);
            writers.computeIfAbsent(partition, p -> new HashMap<>())
                    .compute(
                            bucket,
                            (b, w) -> {
                                if (w == null) {
                                    ExecutorService service = Executors.newSingleThreadExecutor();
                                    RecordWriter<KeyValue> writer =
                                            emptyWriter
                                                    ? write.createEmptyWriter(
                                                            partition, bucket, service)
                                                    : write.createWriter(
                                                            partition, bucket, service);
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

        FileStoreCommit commit = newCommit(commitUser);
        ManifestCommittable committable =
                new ManifestCommittable(identifier == null ? commitIdentifier++ : identifier);
        for (Map.Entry<BinaryRowData, Map<Integer, RecordWriter<KeyValue>>> entryWithPartition :
                writers.entrySet()) {
            for (Map.Entry<Integer, RecordWriter<KeyValue>> entryWithBucket :
                    entryWithPartition.getValue().entrySet()) {
                RecordWriter.CommitIncrement increment =
                        entryWithBucket.getValue().prepareCommit(emptyWriter);
                committable.addFileCommittable(
                        new FileCommittable(
                                entryWithPartition.getKey(),
                                entryWithBucket.getKey(),
                                increment.newFilesIncrement(),
                                increment.compactIncrement()));
            }
        }

        SnapshotManager snapshotManager = snapshotManager();
        Long snapshotIdBeforeCommit = snapshotManager.latestSnapshotId();
        if (snapshotIdBeforeCommit == null) {
            snapshotIdBeforeCommit = Snapshot.FIRST_SNAPSHOT_ID - 1;
        }
        commitFunction.accept(commit, committable);
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
                    newScan().withIncremental(true).withSnapshot(snapshotId).plan().files();
            result.addAll(readKvsFromManifestEntries(entries, true));
        }
        return result;
    }

    public List<KeyValue> readKvsFromManifestEntries(
            List<ManifestEntry> entries, boolean isIncremental) throws Exception {
        if (LOG.isDebugEnabled()) {
            for (ManifestEntry entry : entries) {
                LOG.debug("reading from " + entry.toString());
            }
        }

        Map<BinaryRowData, Map<Integer, List<DataFileMeta>>> filesPerPartitionAndBucket =
                new HashMap<>();
        for (ManifestEntry entry : entries) {
            filesPerPartitionAndBucket
                    .computeIfAbsent(entry.partition(), p -> new HashMap<>())
                    .computeIfAbsent(entry.bucket(), b -> new ArrayList<>())
                    .add(entry.file());
        }

        List<KeyValue> kvs = new ArrayList<>();
        FileStoreRead<KeyValue> read = newRead();
        for (Map.Entry<BinaryRowData, Map<Integer, List<DataFileMeta>>> entryWithPartition :
                filesPerPartitionAndBucket.entrySet()) {
            for (Map.Entry<Integer, List<DataFileMeta>> entryWithBucket :
                    entryWithPartition.getValue().entrySet()) {
                RecordReaderIterator<KeyValue> iterator =
                        new RecordReaderIterator<>(
                                read.createReader(
                                        new DataSplit(
                                                entryWithPartition.getKey(),
                                                entryWithBucket.getKey(),
                                                entryWithBucket.getValue(),
                                                isIncremental)));
                while (iterator.hasNext()) {
                    kvs.add(iterator.next().copy(keySerializer, valueSerializer));
                }
                iterator.close();
            }
        }
        return kvs;
    }

    public Map<BinaryRowData, BinaryRowData> toKvMap(List<KeyValue> kvs) {
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

        Map<BinaryRowData, BinaryRowData> result = new HashMap<>();
        for (KeyValue kv : kvs) {
            BinaryRowData key = keySerializer.toBinaryRow(kv.key()).copy();
            BinaryRowData value = valueSerializer.toBinaryRow(kv.value()).copy();
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
            earliest.getFileSystem().delete(earliest, false);
            assertThat(earliestId <= snapshotManager.earliestSnapshotId()).isTrue();
        }
        if (actualFiles.remove(latest)) {
            long latestId = snapshotManager.readHint(SnapshotManager.LATEST);
            latest.getFileSystem().delete(latest, false);
            assertThat(latestId <= snapshotManager.latestSnapshotId()).isTrue();
        }
        actualFiles.remove(latest);

        // for easier debugging
        String expectedString =
                filesInUse.stream().map(Path::toString).sorted().collect(Collectors.joining(",\n"));
        String actualString =
                actualFiles.stream()
                        .map(Path::toString)
                        .sorted()
                        .collect(Collectors.joining(",\n"));
        assertThat(actualString).isEqualTo(expectedString);
    }

    private Set<Path> getFilesInUse() {
        Set<Path> result = new HashSet<>();
        FileStorePathFactory pathFactory = pathFactory();
        ManifestList manifestList = manifestListFactory().create();
        FileStoreScan scan = newScan();

        SchemaManager schemaManager = new SchemaManager(options.path());
        schemaManager.listAllIds().forEach(id -> result.add(schemaManager.toSchemaPath(id)));

        SnapshotManager snapshotManager = snapshotManager();
        Long latestSnapshotId = snapshotManager.latestSnapshotId();

        if (latestSnapshotId == null) {
            return result;
        }

        long firstInUseSnapshotId = Snapshot.FIRST_SNAPSHOT_ID;
        for (long id = latestSnapshotId - 1; id >= Snapshot.FIRST_SNAPSHOT_ID; id--) {
            if (!snapshotManager.snapshotExists(id)) {
                firstInUseSnapshotId = id + 1;
                break;
            }
        }

        for (long id = firstInUseSnapshotId; id <= latestSnapshotId; id++) {
            Path snapshotPath = snapshotManager.snapshotPath(id);
            Snapshot snapshot = Snapshot.fromPath(snapshotPath);

            // snapshot file
            result.add(snapshotPath);

            // manifest lists
            result.add(pathFactory.toManifestListPath(snapshot.baseManifestList()));
            result.add(pathFactory.toManifestListPath(snapshot.deltaManifestList()));
            if (snapshot.changelogManifestList() != null) {
                result.add(pathFactory.toManifestListPath(snapshot.changelogManifestList()));
            }

            // manifests
            List<ManifestFileMeta> manifests = snapshot.readAllDataManifests(manifestList);
            if (snapshot.changelogManifestList() != null) {
                manifests.addAll(manifestList.read(snapshot.changelogManifestList()));
            }
            manifests.forEach(m -> result.add(pathFactory.toManifestFilePath(m.fileName())));

            // data file
            List<ManifestEntry> entries = scan.withManifestList(manifests).plan().files();
            for (ManifestEntry entry : entries) {
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
        private final KeyFieldsExtractor keyFieldsExtractor;
        private final MergeFunction<KeyValue> mergeFunction;

        private CoreOptions.ChangelogProducer changelogProducer;

        public Builder(
                String format,
                String root,
                int numBuckets,
                RowType partitionType,
                RowType keyType,
                RowType valueType,
                KeyFieldsExtractor keyFieldsExtractor,
                MergeFunction<KeyValue> mergeFunction) {
            this.format = format;
            this.root = root;
            this.numBuckets = numBuckets;
            this.partitionType = partitionType;
            this.keyType = keyType;
            this.valueType = valueType;
            this.keyFieldsExtractor = keyFieldsExtractor;
            this.mergeFunction = mergeFunction;

            this.changelogProducer = CoreOptions.ChangelogProducer.NONE;
        }

        public Builder changelogProducer(CoreOptions.ChangelogProducer changelogProducer) {
            this.changelogProducer = changelogProducer;
            return this;
        }

        public TestFileStore build() {
            Configuration conf = new Configuration();

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

            return new TestFileStore(
                    root,
                    new CoreOptions(conf),
                    partitionType,
                    keyType,
                    valueType,
                    keyFieldsExtractor,
                    mergeFunction);
        }
    }
}
