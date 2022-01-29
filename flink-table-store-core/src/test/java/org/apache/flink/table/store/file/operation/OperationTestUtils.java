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
import org.apache.flink.table.store.file.mergetree.MergeTreeFactory;
import org.apache.flink.table.store.file.mergetree.MergeTreeOptions;
import org.apache.flink.table.store.file.mergetree.compact.DeduplicateAccumulator;
import org.apache.flink.table.store.file.mergetree.sst.SstFile;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.RecordReaderIterator;
import org.apache.flink.table.store.file.utils.RecordWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
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
        MergeTreeFactory mergeTreeFactory =
                new MergeTreeFactory(
                        TestKeyValueGenerator.KEY_TYPE,
                        TestKeyValueGenerator.ROW_TYPE,
                        TestKeyValueGenerator.KEY_COMPARATOR,
                        new DeduplicateAccumulator(),
                        fileFormat,
                        pathFactory,
                        getMergeTreeOptions(false));
        return new FileStoreWriteImpl(
                pathFactory, mergeTreeFactory, createScan(fileFormat, pathFactory));
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

    public static List<Snapshot> writeAndCommitData(
            List<KeyValue> kvs,
            Function<KeyValue, BinaryRowData> partitionCalculator,
            Function<KeyValue, Integer> bucketCalculator,
            FileFormat fileFormat,
            FileStorePathFactory pathFactory)
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
                                    return write.createWriter(partition, bucket, service);
                                } else {
                                    return w;
                                }
                            })
                    .write(kv.valueKind(), kv.key(), kv.value());
        }

        FileStoreCommit commit = createCommit(fileFormat, pathFactory);
        ManifestCommittable committable = new ManifestCommittable();
        for (Map.Entry<BinaryRowData, Map<Integer, RecordWriter>> entryWithPartition :
                writers.entrySet()) {
            for (Map.Entry<Integer, RecordWriter> entryWithBucket :
                    entryWithPartition.getValue().entrySet()) {
                Increment increment = entryWithBucket.getValue().prepareCommit();
                committable.add(entryWithPartition.getKey(), entryWithBucket.getKey(), increment);
            }
        }

        Long snapshotIdBeforeCommit = pathFactory.latestSnapshotId();
        if (snapshotIdBeforeCommit == null) {
            snapshotIdBeforeCommit = Snapshot.FIRST_SNAPSHOT_ID - 1;
        }
        commit.commit(committable, Collections.emptyMap());
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
        List<KeyValue> kvs = new ArrayList<>();
        SstFile.Factory sstFileFactory =
                new SstFile.Factory(
                        TestKeyValueGenerator.KEY_TYPE,
                        TestKeyValueGenerator.ROW_TYPE,
                        fileFormat,
                        pathFactory,
                        1024 * 1024 // not used
                        );
        for (ManifestEntry entry : entries) {
            SstFile sstFile = sstFileFactory.create(entry.partition(), entry.bucket());
            RecordReaderIterator iterator =
                    new RecordReaderIterator(sstFile.read(entry.file().fileName()));
            while (iterator.hasNext()) {
                kvs.add(
                        iterator.next()
                                .copy(
                                        TestKeyValueGenerator.KEY_SERIALIZER,
                                        TestKeyValueGenerator.ROW_SERIALIZER));
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
