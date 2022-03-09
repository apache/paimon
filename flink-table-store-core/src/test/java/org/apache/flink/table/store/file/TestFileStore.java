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
import org.apache.flink.table.store.file.manifest.ManifestCommittable;
import org.apache.flink.table.store.file.manifest.ManifestEntry;
import org.apache.flink.table.store.file.manifest.ManifestFileMeta;
import org.apache.flink.table.store.file.manifest.ManifestList;
import org.apache.flink.table.store.file.mergetree.Increment;
import org.apache.flink.table.store.file.mergetree.MergeTreeOptions;
import org.apache.flink.table.store.file.mergetree.compact.Accumulator;
import org.apache.flink.table.store.file.mergetree.sst.SstFileMeta;
import org.apache.flink.table.store.file.operation.FileStoreCommit;
import org.apache.flink.table.store.file.operation.FileStoreExpireImpl;
import org.apache.flink.table.store.file.operation.FileStoreRead;
import org.apache.flink.table.store.file.operation.FileStoreScan;
import org.apache.flink.table.store.file.operation.FileStoreWrite;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.RecordReaderIterator;
import org.apache.flink.table.store.file.utils.RecordWriter;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.function.QuadFunction;

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
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** {@link FileStore} for tests. */
public class TestFileStore extends FileStoreImpl {

    private static final Logger LOG = LoggerFactory.getLogger(TestFileStore.class);

    private final String root;
    private final RowDataSerializer keySerializer;
    private final RowDataSerializer valueSerializer;

    private static final AtomicInteger ID = new AtomicInteger();

    public static TestFileStore create(
            String format,
            String root,
            int numBuckets,
            RowType partitionType,
            RowType keyType,
            RowType valueType,
            Accumulator accumulator) {
        Configuration conf = new Configuration();

        conf.set(MergeTreeOptions.WRITE_BUFFER_SIZE, MemorySize.parse("16 kb"));
        conf.set(MergeTreeOptions.PAGE_SIZE, MemorySize.parse("4 kb"));
        conf.set(MergeTreeOptions.TARGET_FILE_SIZE, MemorySize.parse("1 kb"));

        conf.set(
                FileStoreOptions.MANIFEST_TARGET_FILE_SIZE,
                MemorySize.parse((ThreadLocalRandom.current().nextInt(16) + 1) + "kb"));

        conf.set(FileStoreOptions.FILE_FORMAT, format);
        conf.set(FileStoreOptions.MANIFEST_FORMAT, format);
        conf.set(FileStoreOptions.FILE_PATH, root);
        conf.set(FileStoreOptions.BUCKET, numBuckets);

        return new TestFileStore(conf, partitionType, keyType, valueType, accumulator);
    }

    public TestFileStore(
            Configuration conf,
            RowType partitionType,
            RowType keyType,
            RowType valueType,
            Accumulator accumulator) {
        super(conf, UUID.randomUUID().toString(), partitionType, keyType, valueType, accumulator);
        this.root = conf.getString(FileStoreOptions.FILE_PATH);
        this.keySerializer = new RowDataSerializer(keyType);
        this.valueSerializer = new RowDataSerializer(valueType);
    }

    public FileStoreExpireImpl newExpire(int numRetained, long millisRetained) {
        return new FileStoreExpireImpl(
                numRetained,
                millisRetained,
                pathFactory(),
                manifestFileFactory(),
                manifestListFactory());
    }

    public List<Snapshot> commitData(
            List<KeyValue> kvs,
            Function<KeyValue, BinaryRowData> partitionCalculator,
            Function<KeyValue, Integer> bucketCalculator)
            throws Exception {
        return commitDataImpl(
                kvs,
                partitionCalculator,
                bucketCalculator,
                FileStoreWrite::createWriter,
                (commit, committable) -> commit.commit(committable, Collections.emptyMap()));
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
                FileStoreWrite::createEmptyWriter,
                (commit, committable) ->
                        commit.overwrite(partition, committable, Collections.emptyMap()));
    }

    private List<Snapshot> commitDataImpl(
            List<KeyValue> kvs,
            Function<KeyValue, BinaryRowData> partitionCalculator,
            Function<KeyValue, Integer> bucketCalculator,
            QuadFunction<FileStoreWrite, BinaryRowData, Integer, ExecutorService, RecordWriter>
                    createWriterFunction,
            BiConsumer<FileStoreCommit, ManifestCommittable> commitFunction)
            throws Exception {
        FileStoreWrite write = newWrite();
        Map<BinaryRowData, Map<Integer, RecordWriter>> writers = new HashMap<>();
        for (KeyValue kv : kvs) {
            BinaryRowData partition = partitionCalculator.apply(kv);
            int bucket = bucketCalculator.apply(kv);
            writers.compute(partition, (p, m) -> m == null ? new HashMap<>() : m)
                    .compute(
                            bucket,
                            (b, w) -> {
                                if (w == null) {
                                    ExecutorService service = Executors.newSingleThreadExecutor();
                                    return createWriterFunction.apply(
                                            write, partition, bucket, service);
                                } else {
                                    return w;
                                }
                            })
                    .write(kv.valueKind(), kv.key(), kv.value());
        }

        FileStoreCommit commit = newCommit();
        ManifestCommittable committable =
                new ManifestCommittable(String.valueOf(new Random().nextLong()));
        for (Map.Entry<BinaryRowData, Map<Integer, RecordWriter>> entryWithPartition :
                writers.entrySet()) {
            for (Map.Entry<Integer, RecordWriter> entryWithBucket :
                    entryWithPartition.getValue().entrySet()) {
                Increment increment = entryWithBucket.getValue().prepareCommit();
                committable.addFileCommittable(
                        entryWithPartition.getKey(), entryWithBucket.getKey(), increment);
                if (!committable.logOffsets().containsKey(entryWithBucket.getKey())) {
                    committable.addLogOffset(entryWithBucket.getKey(), ID.getAndIncrement());
                }
            }
        }

        FileStorePathFactory pathFactory = pathFactory();
        Long snapshotIdBeforeCommit = pathFactory.latestSnapshotId();
        if (snapshotIdBeforeCommit == null) {
            snapshotIdBeforeCommit = Snapshot.FIRST_SNAPSHOT_ID - 1;
        }
        commitFunction.accept(commit, committable);
        Long snapshotIdAfterCommit = pathFactory.latestSnapshotId();
        if (snapshotIdAfterCommit == null) {
            snapshotIdAfterCommit = Snapshot.FIRST_SNAPSHOT_ID - 1;
        }

        writers.values().stream()
                .flatMap(m -> m.values().stream())
                .forEach(
                        w -> {
                            try {
                                w.close();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        List<Snapshot> snapshots = new ArrayList<>();
        for (long id = snapshotIdBeforeCommit + 1; id <= snapshotIdAfterCommit; id++) {
            snapshots.add(Snapshot.fromPath(pathFactory.toSnapshotPath(id)));
        }
        return snapshots;
    }

    public List<KeyValue> readKvsFromSnapshot(long snapshotId) throws IOException {
        List<ManifestEntry> entries = newScan().withSnapshot(snapshotId).plan().files();
        return readKvsFromManifestEntries(entries);
    }

    public List<KeyValue> readKvsFromManifestEntries(List<ManifestEntry> entries)
            throws IOException {
        if (LOG.isDebugEnabled()) {
            for (ManifestEntry entry : entries) {
                LOG.debug("reading from " + entry.toString());
            }
        }

        Map<BinaryRowData, Map<Integer, List<SstFileMeta>>> filesPerPartitionAndBucket =
                new HashMap<>();
        for (ManifestEntry entry : entries) {
            filesPerPartitionAndBucket
                    .compute(entry.partition(), (p, m) -> m == null ? new HashMap<>() : m)
                    .compute(entry.bucket(), (b, l) -> l == null ? new ArrayList<>() : l)
                    .add(entry.file());
        }

        List<KeyValue> kvs = new ArrayList<>();
        FileStoreRead read = newRead();
        for (Map.Entry<BinaryRowData, Map<Integer, List<SstFileMeta>>> entryWithPartition :
                filesPerPartitionAndBucket.entrySet()) {
            for (Map.Entry<Integer, List<SstFileMeta>> entryWithBucket :
                    entryWithPartition.getValue().entrySet()) {
                RecordReaderIterator iterator =
                        new RecordReaderIterator(
                                read.createReader(
                                        entryWithPartition.getKey(),
                                        entryWithBucket.getKey(),
                                        entryWithBucket.getValue()));
                while (iterator.hasNext()) {
                    kvs.add(iterator.next().copy(keySerializer, valueSerializer));
                }
            }
        }
        return kvs;
    }

    public Map<BinaryRowData, BinaryRowData> toKvMap(List<KeyValue> kvs) {
        Map<BinaryRowData, BinaryRowData> result = new HashMap<>();
        for (KeyValue kv : kvs) {
            BinaryRowData key = keySerializer.toBinaryRow(kv.key()).copy();
            BinaryRowData value = valueSerializer.toBinaryRow(kv.value()).copy();
            switch (kv.valueKind()) {
                case ADD:
                    result.put(key, value);
                    break;
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

    public void assertCleaned() {
        Set<Path> filesInUse = getFilesInUse();
        Set<Path> actualFiles;
        try {
            actualFiles =
                    Files.walk(Paths.get(root))
                            .filter(p -> Files.isRegularFile(p))
                            .map(p -> new Path(p.toString()))
                            .collect(Collectors.toSet());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        assertThat(actualFiles).isEqualTo(filesInUse);
    }

    private Set<Path> getFilesInUse() {
        FileStorePathFactory pathFactory = pathFactory();
        ManifestList manifestList = manifestListFactory().create();
        FileStoreScan scan = newScan();
        FileStorePathFactory.SstPathFactoryCache sstPathFactoryCache =
                new FileStorePathFactory.SstPathFactoryCache(pathFactory);

        Long latestSnapshotId = pathFactory.latestSnapshotId();
        if (latestSnapshotId == null) {
            return Collections.emptySet();
        }

        long firstInUseSnapshotId = Snapshot.FIRST_SNAPSHOT_ID;
        for (long id = latestSnapshotId - 1; id >= Snapshot.FIRST_SNAPSHOT_ID; id--) {
            Path snapshotPath = pathFactory.toSnapshotPath(id);
            try {
                if (!snapshotPath.getFileSystem().exists(snapshotPath)) {
                    firstInUseSnapshotId = id + 1;
                    break;
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        Set<Path> result = new HashSet<>();
        for (long id = firstInUseSnapshotId; id <= latestSnapshotId; id++) {
            Path snapshotPath = pathFactory.toSnapshotPath(id);
            Snapshot snapshot = Snapshot.fromPath(snapshotPath);

            // snapshot file
            result.add(snapshotPath);

            // manifest lists
            result.add(pathFactory.toManifestListPath(snapshot.baseManifestList()));
            result.add(pathFactory.toManifestListPath(snapshot.deltaManifestList()));

            // manifests
            List<ManifestFileMeta> manifests = snapshot.readAllManifests(manifestList);
            manifests.forEach(m -> result.add(pathFactory.toManifestFilePath(m.fileName())));

            // sst
            List<ManifestEntry> entries = scan.withManifestList(manifests).plan().files();
            for (ManifestEntry entry : entries) {
                result.add(
                        sstPathFactoryCache
                                .getSstPathFactory(entry.partition(), entry.bucket())
                                .toPath(entry.file().fileName()));
            }
        }
        return result;
    }
}
