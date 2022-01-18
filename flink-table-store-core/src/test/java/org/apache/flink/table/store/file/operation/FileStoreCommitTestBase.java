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
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.FileFormat;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.TestKeyValueGenerator;
import org.apache.flink.table.store.file.ValueKind;
import org.apache.flink.table.store.file.manifest.ManifestEntry;
import org.apache.flink.table.store.file.manifest.ManifestFile;
import org.apache.flink.table.store.file.manifest.ManifestList;
import org.apache.flink.table.store.file.mergetree.sst.SstFile;
import org.apache.flink.table.store.file.utils.FailingAtomicRenameFileSystem;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.RecordReaderIterator;
import org.apache.flink.table.store.file.utils.TestAtomicRenameFileSystem;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FileStoreCommitImpl}. */
public abstract class FileStoreCommitTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(FileStoreCommitTestBase.class);

    private final FileFormat avro =
            FileFormat.fromIdentifier(
                    FileStoreCommitTestBase.class.getClassLoader(), "avro", new Configuration());

    private TestKeyValueGenerator gen;
    @TempDir java.nio.file.Path tempDir;

    @BeforeEach
    public void beforeEach() throws IOException {
        gen = new TestKeyValueGenerator();
        Path root = new Path(tempDir.toString());
        root.getFileSystem().mkdirs(new Path(root + "/snapshot"));
    }

    protected abstract String getSchema();

    @RepeatedTest(10)
    public void testSingleCommitter() throws Exception {
        testRandomConcurrentNoConflict(1);
    }

    @RepeatedTest(10)
    public void testManyCommittersNoConflict() throws Exception {
        testRandomConcurrentNoConflict(ThreadLocalRandom.current().nextInt(3) + 2);
    }

    protected void testRandomConcurrentNoConflict(int numThreads) throws Exception {
        // prepare test data
        Map<BinaryRowData, List<KeyValue>> data =
                generateData(ThreadLocalRandom.current().nextInt(1000) + 1);
        logData(
                () ->
                        data.values().stream()
                                .flatMap(Collection::stream)
                                .collect(Collectors.toList()),
                "input");
        Map<BinaryRowData, BinaryRowData> expected =
                toKvMap(
                        data.values().stream()
                                .flatMap(Collection::stream)
                                .collect(Collectors.toList()));

        List<Map<BinaryRowData, List<KeyValue>>> dataPerThread = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            dataPerThread.add(new HashMap<>());
        }
        for (Map.Entry<BinaryRowData, List<KeyValue>> entry : data.entrySet()) {
            dataPerThread
                    .get(ThreadLocalRandom.current().nextInt(numThreads))
                    .put(entry.getKey(), entry.getValue());
        }

        // concurrent commits
        List<TestCommitThread> threads = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            TestCommitThread thread =
                    new TestCommitThread(
                            dataPerThread.get(i), createTestPathFactory(), createSafePathFactory());
            thread.start();
            threads.add(thread);
        }
        for (TestCommitThread thread : threads) {
            thread.join();
        }

        // read actual data and compare
        Map<BinaryRowData, BinaryRowData> actual = toKvMap(readKvsFromLatestSnapshot());
        logData(() -> kvMapToKvList(expected), "expected");
        logData(() -> kvMapToKvList(actual), "actual");
        assertThat(actual).isEqualTo(expected);
    }

    private Map<BinaryRowData, List<KeyValue>> generateData(int numRecords) {
        Map<BinaryRowData, List<KeyValue>> data = new HashMap<>();
        for (int i = 0; i < numRecords; i++) {
            KeyValue kv = gen.next();
            data.compute(gen.getPartition(kv), (p, l) -> l == null ? new ArrayList<>() : l).add(kv);
        }
        return data;
    }

    private List<KeyValue> readKvsFromLatestSnapshot() throws IOException {
        FileStorePathFactory pathFactory = createSafePathFactory();
        Long latestSnapshotId = pathFactory.latestSnapshotId();
        assertThat(latestSnapshotId).isNotNull();

        ManifestFile manifestFile =
                new ManifestFile(
                        TestKeyValueGenerator.PARTITION_TYPE,
                        TestKeyValueGenerator.KEY_TYPE,
                        TestKeyValueGenerator.ROW_TYPE,
                        avro,
                        pathFactory);
        ManifestList manifestList =
                new ManifestList(TestKeyValueGenerator.PARTITION_TYPE, avro, pathFactory);

        List<KeyValue> kvs = new ArrayList<>();
        List<ManifestEntry> entries =
                new FileStoreScanImpl(pathFactory, manifestFile, manifestList)
                        .withSnapshot(latestSnapshotId)
                        .plan()
                        .files();
        for (ManifestEntry entry : entries) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Reading actual key-values from file " + entry.file().fileName());
            }
            SstFile sstFile =
                    new SstFile(
                            TestKeyValueGenerator.KEY_TYPE,
                            TestKeyValueGenerator.ROW_TYPE,
                            avro,
                            pathFactory.createSstPathFactory(entry.partition(), 0),
                            1024 * 1024 // not used
                            );
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

        gen.sort(kvs);
        logData(() -> kvs, "raw read results");
        return kvs;
    }

    private Map<BinaryRowData, BinaryRowData> toKvMap(List<KeyValue> kvs) {
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

    private FileStorePathFactory createTestPathFactory() {
        return new FileStorePathFactory(
                new Path(getSchema() + "://" + tempDir.toString()),
                TestKeyValueGenerator.PARTITION_TYPE,
                "default");
    }

    private FileStorePathFactory createSafePathFactory() {
        return new FileStorePathFactory(
                new Path(TestAtomicRenameFileSystem.SCHEME + "://" + tempDir.toString()),
                TestKeyValueGenerator.PARTITION_TYPE,
                "default");
    }

    private List<KeyValue> kvMapToKvList(Map<BinaryRowData, BinaryRowData> map) {
        return map.entrySet().stream()
                .map(e -> new KeyValue().replace(e.getKey(), -1, ValueKind.ADD, e.getValue()))
                .collect(Collectors.toList());
    }

    private void logData(Supplier<List<KeyValue>> supplier, String name) {
        if (!LOG.isDebugEnabled()) {
            return;
        }

        LOG.debug("========== Beginning of " + name + " ==========");
        for (KeyValue kv : supplier.get()) {
            LOG.debug(kv.toString(TestKeyValueGenerator.KEY_TYPE, TestKeyValueGenerator.ROW_TYPE));
        }
        LOG.debug("========== End of " + name + " ==========");
    }

    /** Tests for {@link FileStoreCommitImpl} with {@link TestAtomicRenameFileSystem}. */
    public static class WithTestAtomicRenameFileSystem extends FileStoreCommitTestBase {

        @Override
        protected String getSchema() {
            return TestAtomicRenameFileSystem.SCHEME;
        }
    }

    /** Tests for {@link FileStoreCommitImpl} with {@link FailingAtomicRenameFileSystem}. */
    public static class WithFailingAtomicRenameFileSystem extends FileStoreCommitTestBase {

        @BeforeEach
        @Override
        public void beforeEach() throws IOException {
            super.beforeEach();
            FailingAtomicRenameFileSystem.resetFailCounter(100);
            FailingAtomicRenameFileSystem.setFailPossibility(5000);
        }

        @Override
        protected String getSchema() {
            return FailingAtomicRenameFileSystem.SCHEME;
        }
    }
}
