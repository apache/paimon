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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.FileFormat;
import org.apache.flink.table.store.file.FileStoreOptions;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.Snapshot;
import org.apache.flink.table.store.file.TestKeyValueGenerator;
import org.apache.flink.table.store.file.manifest.ManifestCommittable;
import org.apache.flink.table.store.file.manifest.ManifestEntry;
import org.apache.flink.table.store.file.manifest.ManifestFile;
import org.apache.flink.table.store.file.manifest.ManifestList;
import org.apache.flink.table.store.file.mergetree.Increment;
import org.apache.flink.table.store.file.mergetree.MergeTreeOptions;
import org.apache.flink.table.store.file.mergetree.compact.DeduplicateAccumulator;
import org.apache.flink.table.store.file.mergetree.sst.SstFileMeta;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.RecordReaderIterator;
import org.apache.flink.table.store.file.utils.RecordWriter;
import org.apache.flink.util.function.QuadFunction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;
import java.util.function.Function;

/** Utils for operation tests. */
public class OperationTestUtils {

    public static MergeTreeOptions getMergeTreeOptions(boolean forceCompact) {
        Configuration conf = new Configuration();
        conf.set(MergeTreeOptions.WRITE_BUFFER_SIZE, MemorySize.parse("16 kb"));
        conf.set(MergeTreeOptions.PAGE_SIZE, MemorySize.parse("4 kb"));
        conf.set(MergeTreeOptions.TARGET_FILE_SIZE, MemorySize.parse("1 kb"));
        conf.set(MergeTreeOptions.COMMIT_FORCE_COMPACT, forceCompact);
        return new MergeTreeOptions(conf);
    }

    private static FileStoreOptions getFileStoreOptions() {
        Configuration conf = new Configuration();
        conf.set(FileStoreOptions.BUCKET, 1);
        conf.set(
                FileStoreOptions.MANIFEST_TARGET_FILE_SIZE,
                MemorySize.parse((ThreadLocalRandom.current().nextInt(16) + 1) + "kb"));
        return new FileStoreOptions(conf);
    }

    public static FileStoreScan createScan(
            FileFormat fileFormat, FileStorePathFactory pathFactory) {
        return new FileStoreScanImpl(
                TestKeyValueGenerator.PARTITION_TYPE,
                pathFactory,
                createManifestFileFactory(fileFormat, pathFactory),
                createManifestListFactory(fileFormat, pathFactory));
    }

    public static FileStoreCommit createCommit(
            FileFormat fileFormat, FileStorePathFactory pathFactory) {
        ManifestFile.Factory testManifestFileFactory =
                createManifestFileFactory(fileFormat, pathFactory);
        ManifestList.Factory testManifestListFactory =
                createManifestListFactory(fileFormat, pathFactory);
        return new FileStoreCommitImpl(
                UUID.randomUUID().toString(),
                TestKeyValueGenerator.PARTITION_TYPE,
                pathFactory,
                testManifestFileFactory,
                testManifestListFactory,
                createScan(fileFormat, pathFactory),
                getFileStoreOptions());
    }

    public static FileStoreWrite createWrite(
            FileFormat fileFormat, FileStorePathFactory pathFactory) {
        return new FileStoreWriteImpl(
                TestKeyValueGenerator.KEY_TYPE,
                TestKeyValueGenerator.ROW_TYPE,
                TestKeyValueGenerator.KEY_COMPARATOR,
                new DeduplicateAccumulator(),
                fileFormat,
                pathFactory,
                createScan(fileFormat, pathFactory),
                getMergeTreeOptions(false));
    }

    public static FileStoreExpire createExpire(
            int numRetained,
            long millisRetained,
            FileFormat fileFormat,
            FileStorePathFactory pathFactory) {
        return new FileStoreExpireImpl(
                numRetained,
                millisRetained,
                pathFactory,
                createManifestListFactory(fileFormat, pathFactory),
                createScan(fileFormat, pathFactory));
    }

    public static FileStoreRead createRead(
            FileFormat fileFormat, FileStorePathFactory pathFactory) {
        return new FileStoreReadImpl(
                TestKeyValueGenerator.KEY_TYPE,
                TestKeyValueGenerator.ROW_TYPE,
                TestKeyValueGenerator.KEY_COMPARATOR,
                new DeduplicateAccumulator(),
                fileFormat,
                pathFactory);
    }

    public static FileStorePathFactory createPathFactory(String scheme, String root) {
        return new FileStorePathFactory(
                new Path(scheme + "://" + root), TestKeyValueGenerator.PARTITION_TYPE, "default");
    }

    private static ManifestFile.Factory createManifestFileFactory(
            FileFormat fileFormat, FileStorePathFactory pathFactory) {
        return new ManifestFile.Factory(
                TestKeyValueGenerator.PARTITION_TYPE,
                TestKeyValueGenerator.KEY_TYPE,
                TestKeyValueGenerator.ROW_TYPE,
                fileFormat,
                pathFactory);
    }

    private static ManifestList.Factory createManifestListFactory(
            FileFormat fileFormat, FileStorePathFactory pathFactory) {
        return new ManifestList.Factory(
                TestKeyValueGenerator.PARTITION_TYPE, fileFormat, pathFactory);
    }

    public static List<Snapshot> commitData(
            List<KeyValue> kvs,
            Function<KeyValue, BinaryRowData> partitionCalculator,
            Function<KeyValue, Integer> bucketCalculator,
            FileFormat fileFormat,
            FileStorePathFactory pathFactory)
            throws Exception {
        return commitDataImpl(
                kvs,
                partitionCalculator,
                bucketCalculator,
                fileFormat,
                pathFactory,
                FileStoreWrite::createWriter,
                (commit, committable) -> commit.commit(committable, Collections.emptyMap()));
    }

    public static List<Snapshot> overwriteData(
            List<KeyValue> kvs,
            Function<KeyValue, BinaryRowData> partitionCalculator,
            Function<KeyValue, Integer> bucketCalculator,
            FileFormat fileFormat,
            FileStorePathFactory pathFactory,
            Map<String, String> partition)
            throws Exception {
        return commitDataImpl(
                kvs,
                partitionCalculator,
                bucketCalculator,
                fileFormat,
                pathFactory,
                FileStoreWrite::createEmptyWriter,
                (commit, committable) ->
                        commit.overwrite(partition, committable, Collections.emptyMap()));
    }

    private static List<Snapshot> commitDataImpl(
            List<KeyValue> kvs,
            Function<KeyValue, BinaryRowData> partitionCalculator,
            Function<KeyValue, Integer> bucketCalculator,
            FileFormat fileFormat,
            FileStorePathFactory pathFactory,
            QuadFunction<FileStoreWrite, BinaryRowData, Integer, ExecutorService, RecordWriter>
                    createWriterFunction,
            BiConsumer<FileStoreCommit, ManifestCommittable> commitFunction)
            throws Exception {
        FileStoreWrite write = createWrite(fileFormat, pathFactory);
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

        FileStoreCommit commit = createCommit(fileFormat, pathFactory);
        ManifestCommittable committable =
                new ManifestCommittable(String.valueOf(new Random().nextLong()));
        for (Map.Entry<BinaryRowData, Map<Integer, RecordWriter>> entryWithPartition :
                writers.entrySet()) {
            for (Map.Entry<Integer, RecordWriter> entryWithBucket :
                    entryWithPartition.getValue().entrySet()) {
                Increment increment = entryWithBucket.getValue().prepareCommit();
                committable.addFileCommittable(
                        entryWithPartition.getKey(), entryWithBucket.getKey(), increment);
            }
        }

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

    public static List<KeyValue> readKvsFromSnapshot(
            long snapshotId, FileFormat fileFormat, FileStorePathFactory pathFactory)
            throws IOException {
        List<ManifestEntry> entries =
                createScan(fileFormat, pathFactory).withSnapshot(snapshotId).plan().files();
        return readKvsFromManifestEntries(entries, fileFormat, pathFactory);
    }

    public static List<KeyValue> readKvsFromManifestEntries(
            List<ManifestEntry> entries, FileFormat fileFormat, FileStorePathFactory pathFactory)
            throws IOException {
        Map<BinaryRowData, Map<Integer, List<SstFileMeta>>> filesPerPartitionAndBucket =
                new HashMap<>();
        for (ManifestEntry entry : entries) {
            filesPerPartitionAndBucket
                    .compute(entry.partition(), (p, m) -> m == null ? new HashMap<>() : m)
                    .compute(entry.bucket(), (b, l) -> l == null ? new ArrayList<>() : l)
                    .add(entry.file());
        }

        List<KeyValue> kvs = new ArrayList<>();
        FileStoreRead read = createRead(fileFormat, pathFactory);
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
                    kvs.add(
                            iterator.next()
                                    .copy(
                                            TestKeyValueGenerator.KEY_SERIALIZER,
                                            TestKeyValueGenerator.ROW_SERIALIZER));
                }
            }
        }
        return kvs;
    }

    public static Map<BinaryRowData, BinaryRowData> toKvMap(List<KeyValue> kvs) {
        Map<BinaryRowData, BinaryRowData> result = new HashMap<>();
        for (KeyValue kv : kvs) {
            BinaryRowData key = TestKeyValueGenerator.KEY_SERIALIZER.toBinaryRow(kv.key()).copy();
            BinaryRowData value =
                    TestKeyValueGenerator.ROW_SERIALIZER.toBinaryRow(kv.value()).copy();
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
}
