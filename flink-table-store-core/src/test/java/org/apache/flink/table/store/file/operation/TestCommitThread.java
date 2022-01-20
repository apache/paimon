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
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.FileFormat;
import org.apache.flink.table.store.file.FileStoreOptions;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.TestKeyValueGenerator;
import org.apache.flink.table.store.file.manifest.ManifestCommittable;
import org.apache.flink.table.store.file.manifest.ManifestCommittableSerializer;
import org.apache.flink.table.store.file.manifest.ManifestEntry;
import org.apache.flink.table.store.file.manifest.ManifestFile;
import org.apache.flink.table.store.file.manifest.ManifestList;
import org.apache.flink.table.store.file.mergetree.MergeTree;
import org.apache.flink.table.store.file.mergetree.MergeTreeOptions;
import org.apache.flink.table.store.file.mergetree.MergeTreeWriter;
import org.apache.flink.table.store.file.mergetree.compact.DeduplicateAccumulator;
import org.apache.flink.table.store.file.mergetree.sst.SstFile;
import org.apache.flink.table.store.file.mergetree.sst.SstFileMetaSerializer;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

/** Testing {@link Thread}s to perform concurrent commits. */
public class TestCommitThread extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(TestCommitThread.class);

    private static final MergeTreeOptions MERGE_TREE_OPTIONS;
    private static final long SUGGESTED_SST_FILE_SIZE = 1024;

    static {
        Configuration mergeTreeConf = new Configuration();
        mergeTreeConf.set(MergeTreeOptions.WRITE_BUFFER_SIZE, MemorySize.parse("16 kb"));
        mergeTreeConf.set(MergeTreeOptions.PAGE_SIZE, MemorySize.parse("4 kb"));
        MERGE_TREE_OPTIONS = new MergeTreeOptions(mergeTreeConf);
    }

    private final FileFormat avro =
            FileFormat.fromIdentifier(
                    FileStoreCommitTestBase.class.getClassLoader(), "avro", new Configuration());

    private final Map<BinaryRowData, List<KeyValue>> data;
    private final FileStorePathFactory safePathFactory;

    private final Map<BinaryRowData, MergeTreeWriter> writers;

    private final FileStoreScan scan;
    private final FileStoreCommit commit;

    public TestCommitThread(
            Map<BinaryRowData, List<KeyValue>> data,
            FileStorePathFactory testPathFactory,
            FileStorePathFactory safePathFactory) {
        this.data = data;
        this.safePathFactory = safePathFactory;

        this.writers = new HashMap<>();

        this.scan =
                new FileStoreScanImpl(
                        safePathFactory,
                        createManifestFile(safePathFactory),
                        createManifestList(safePathFactory));

        ManifestCommittableSerializer serializer =
                new ManifestCommittableSerializer(
                        TestKeyValueGenerator.PARTITION_TYPE,
                        new SstFileMetaSerializer(
                                TestKeyValueGenerator.KEY_TYPE, TestKeyValueGenerator.ROW_TYPE));
        ManifestFile testManifestFile = createManifestFile(testPathFactory);
        ManifestList testManifestList = createManifestList(testPathFactory);
        Configuration fileStoreConf = new Configuration();
        fileStoreConf.set(FileStoreOptions.BUCKET, 1);
        fileStoreConf.set(
                FileStoreOptions.MANIFEST_TARGET_FILE_SIZE,
                MemorySize.parse((ThreadLocalRandom.current().nextInt(16) + 1) + "kb"));
        FileStoreOptions fileStoreOptions = new FileStoreOptions(fileStoreConf);
        FileStoreScanImpl testScan =
                new FileStoreScanImpl(testPathFactory, testManifestFile, testManifestList);
        this.commit =
                new FileStoreCommitImpl(
                        UUID.randomUUID().toString(),
                        serializer,
                        testPathFactory,
                        testManifestFile,
                        testManifestList,
                        fileStoreOptions,
                        testScan);
    }

    private ManifestFile createManifestFile(FileStorePathFactory pathFactory) {
        return new ManifestFile(
                TestKeyValueGenerator.PARTITION_TYPE,
                TestKeyValueGenerator.KEY_TYPE,
                TestKeyValueGenerator.ROW_TYPE,
                avro,
                pathFactory);
    }

    private ManifestList createManifestList(FileStorePathFactory pathFactory) {
        return new ManifestList(TestKeyValueGenerator.PARTITION_TYPE, avro, pathFactory);
    }

    @Override
    public void run() {
        while (!data.isEmpty()) {
            ManifestCommittable committable;
            try {
                committable = createCommittable();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            boolean shouldCheckFilter = false;
            while (true) {
                try {
                    if (shouldCheckFilter) {
                        if (commit.filterCommitted(Collections.singletonList(committable))
                                .isEmpty()) {
                            break;
                        }
                    }
                    commit.commit(committable, Collections.emptyMap());
                    break;
                } catch (Throwable e) {
                    if (LOG.isDebugEnabled()) {
                        LOG.warn(
                                "["
                                        + Thread.currentThread().getName()
                                        + "] Failed to commit because of exception, try again",
                                e);
                    }
                    writers.clear();
                    shouldCheckFilter = true;
                }
            }
        }

        for (MergeTreeWriter writer : writers.values()) {
            try {
                writer.sync();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            writer.close();
        }
    }

    private ManifestCommittable createCommittable() throws Exception {
        int numWrites = ThreadLocalRandom.current().nextInt(3) + 1;
        for (int i = 0; i < numWrites && !data.isEmpty(); i++) {
            writeData();
        }

        ManifestCommittable committable = new ManifestCommittable();
        for (Map.Entry<BinaryRowData, MergeTreeWriter> entry : writers.entrySet()) {
            committable.add(entry.getKey(), 0, entry.getValue().prepareCommit());
        }
        return committable;
    }

    private void writeData() throws Exception {
        List<KeyValue> changes = new ArrayList<>();
        BinaryRowData partition = pickData(changes);
        MergeTreeWriter writer =
                writers.compute(partition, (k, v) -> v == null ? createWriter(k) : v);
        for (KeyValue kv : changes) {
            writer.write(kv.valueKind(), kv.key(), kv.value());
        }
    }

    private BinaryRowData pickData(List<KeyValue> changes) {
        List<BinaryRowData> keys = new ArrayList<>(data.keySet());
        BinaryRowData partition = keys.get(ThreadLocalRandom.current().nextInt(keys.size()));
        List<KeyValue> remaining = data.get(partition);
        int numChanges = ThreadLocalRandom.current().nextInt(Math.min(100, remaining.size() + 1));
        changes.addAll(remaining.subList(0, numChanges));
        if (numChanges == remaining.size()) {
            data.remove(partition);
        } else {
            remaining.subList(0, numChanges).clear();
        }
        return partition;
    }

    private MergeTreeWriter createWriter(BinaryRowData partition) {
        SstFile sstFile =
                new SstFile(
                        TestKeyValueGenerator.KEY_TYPE,
                        TestKeyValueGenerator.ROW_TYPE,
                        avro,
                        safePathFactory.createSstPathFactory(partition, 0),
                        SUGGESTED_SST_FILE_SIZE);
        ExecutorService service =
                Executors.newSingleThreadExecutor(
                        r -> {
                            Thread t = new Thread(r);
                            t.setName(Thread.currentThread().getName() + "-writer-service-pool");
                            return t;
                        });
        MergeTree mergeTree =
                new MergeTree(
                        MERGE_TREE_OPTIONS,
                        sstFile,
                        TestKeyValueGenerator.KEY_COMPARATOR,
                        service,
                        new DeduplicateAccumulator());
        Long latestSnapshotId = safePathFactory.latestSnapshotId();
        if (latestSnapshotId == null) {
            return (MergeTreeWriter) mergeTree.createWriter(Collections.emptyList());
        } else {
            return (MergeTreeWriter)
                    mergeTree.createWriter(
                            scan.withSnapshot(latestSnapshotId).plan().files().stream()
                                    .filter(e -> partition.equals(e.partition()))
                                    .map(ManifestEntry::file)
                                    .collect(Collectors.toList()));
        }
    }
}
