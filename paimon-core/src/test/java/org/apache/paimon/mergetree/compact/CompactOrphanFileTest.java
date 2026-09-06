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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.mergetree.Levels;
import org.apache.paimon.mergetree.MergeSorter;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.KeyValueFieldsExtractor;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.SchemaEvolutionTableTestBase;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.apache.paimon.utils.FileStorePathFactoryTest.createNonPartFactory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A compaction task writes real files before its result reaches the writer. These tests run a real
 * merge tree compaction, break it half way through, and check that the files the finished part has
 * already written to the bucket directory do not survive as orphans.
 */
public class CompactOrphanFileTest {

    @TempDir java.nio.file.Path tempDir;

    private final LocalFileIO fileIO = LocalFileIO.create();
    private final RowType keyType =
            new RowType(singletonList(new DataField(0, "k", new IntType())));
    private final RowType valueType =
            new RowType(singletonList(new DataField(1, "v", new IntType())));

    private CoreOptions options;
    private Comparator<InternalRow> comparator;
    private KeyValueFileReaderFactory readerFactory;
    private KeyValueFileWriterFactory writerFactory;
    private Path bucketDir;
    private ExecutorService executor;
    private long sequenceNumber = 0;

    @BeforeEach
    public void before() throws IOException {
        Path root = new Path(tempDir.toString());
        FileStorePathFactory pathFactory = createNonPartFactory(root);
        comparator = Comparator.comparingInt(o -> o.getInt(0));
        executor = Executors.newSingleThreadExecutor();

        Options conf = new Options();
        conf.set(CoreOptions.TARGET_FILE_SIZE, MemorySize.ofMebiBytes(1));
        options = new CoreOptions(conf);

        FileFormat avro = FileFormat.fromIdentifier("avro", new Options());
        SchemaManager schemaManager = testingSchemaManager(root);
        KeyValueFileReaderFactory.Builder readerBuilder =
                KeyValueFileReaderFactory.builder(
                        fileIO,
                        schemaManager,
                        schemaManager.schema(0),
                        keyType,
                        valueType,
                        ignore -> avro,
                        pathFactory,
                        new TestKeyValueFieldsExtractor(),
                        new CoreOptions(new HashMap<>()));
        readerFactory =
                readerBuilder.build(
                        org.apache.paimon.data.BinaryRow.EMPTY_ROW,
                        0,
                        DeletionVector.emptyFactory());

        Function<String, FileStorePathFactory> pathFactoryMap = k -> pathFactory;
        writerFactory =
                KeyValueFileWriterFactory.builder(
                                fileIO,
                                0,
                                keyType,
                                valueType,
                                avro,
                                pathFactoryMap,
                                options.targetFileSize(true))
                        .build(org.apache.paimon.data.BinaryRow.EMPTY_ROW, 0, options);

        bucketDir = writerFactory.pathFactory(0).newPath().getParent();
        fileIO.mkdirs(bucketDir);
    }

    @AfterEach
    public void after() {
        executor.shutdownNow();
    }

    /**
     * The compaction is cancelled while it is still running. Everything the sections it already
     * rewrote wrote to the bucket directory is only reachable through the result of the task, which
     * the cancelled future drops.
     */
    @Test
    @Timeout(60)
    public void testCancelledCompactionLeavesNoOrphanFile() throws Exception {
        List<DataFileMeta> inputs = writeInputFiles();

        CountDownLatch reachedSecondRewrite = new CountDownLatch(1);
        CountDownLatch neverReleased = new CountDownLatch(1);
        BreakingRewriter rewriter =
                new BreakingRewriter(
                        realRewriter(),
                        2,
                        () -> {
                            reachedSecondRewrite.countDown();
                            neverReleased.await();
                        });

        MergeTreeCompactManager manager = createCompactManager(inputs, rewriter);
        manager.triggerCompaction(true);
        assertThat(reachedSecondRewrite.await(60, TimeUnit.SECONDS)).isTrue();

        // the first section has been rewritten by now, its files are on disk
        assertThat(orphanFiles(inputs)).isNotEmpty();

        manager.cancelCompaction();

        assertThat(orphanFiles(inputs)).isEmpty();
    }

    /**
     * The same files are left behind when a later section of the same task fails: the task never
     * returns the result that names them.
     */
    @Test
    @Timeout(60)
    public void testFailedCompactionLeavesNoOrphanFile() throws Exception {
        List<DataFileMeta> inputs = writeInputFiles();

        BreakingRewriter rewriter =
                new BreakingRewriter(
                        realRewriter(),
                        2,
                        () -> {
                            throw new IOException("rewriting the second section failed");
                        });

        MergeTreeCompactManager manager = createCompactManager(inputs, rewriter);
        manager.triggerCompaction(true);

        assertThatThrownBy(() -> manager.getCompactionResult(true))
                .hasRootCauseMessage("rewriting the second section failed");

        assertThat(orphanFiles(inputs)).isEmpty();
    }

    /** A compaction that runs to the end keeps its files - they are about to be committed. */
    @Test
    @Timeout(60)
    public void testSuccessfulCompactionKeepsItsFiles() throws Exception {
        List<DataFileMeta> inputs = writeInputFiles();

        MergeTreeCompactManager manager = createCompactManager(inputs, realRewriter());
        manager.triggerCompaction(true);

        CompactResult result = manager.getCompactionResult(true).orElseThrow(AssertionError::new);
        assertThat(result.after()).isNotEmpty();

        Set<String> onDisk = filesOnDisk();
        for (DataFileMeta file : result.after()) {
            assertThat(onDisk).contains(file.fileName());
        }
    }

    /**
     * Five level 0 files: two overlapping small ones, one large one on its own, then two more
     * overlapping small ones. {@link MergeTreeCompactTask} rewrites the first pair, upgrades the
     * large file in between, and rewrites the last pair - two separate rewrites in one task.
     */
    private List<DataFileMeta> writeInputFiles() throws Exception {
        List<DataFileMeta> files = new ArrayList<>();
        files.add(writeFile(1, 10));
        files.add(writeFile(5, 15));
        files.add(writeFile(100, 700));
        files.add(writeFile(2000, 2010));
        files.add(writeFile(2005, 2015));
        return files;
    }

    /** Between the small files and the large one, so the large one is upgraded, not rewritten. */
    private long compactionFileSize(List<DataFileMeta> inputs) {
        long large = inputs.get(2).fileSize();
        long smallest =
                inputs.stream()
                        .mapToLong(DataFileMeta::fileSize)
                        .min()
                        .orElseThrow(AssertionError::new);
        assertThat(large).isGreaterThan(smallest * 2);
        return (large + smallest) / 2;
    }

    private MergeTreeCompactManager createCompactManager(
            List<DataFileMeta> inputs, CompactRewriter rewriter) {
        return new MergeTreeCompactManager(
                executor,
                new Levels(comparator, inputs, options.numLevels()),
                new UniversalCompaction(
                        options.maxSizeAmplificationPercent(),
                        options.sortedRunSizeRatio(),
                        options.numSortedRunCompactionTrigger(),
                        null,
                        null),
                comparator,
                compactionFileSize(inputs),
                options.numSortedRunStopTrigger(),
                rewriter,
                null,
                null,
                false,
                false,
                null,
                false,
                false,
                "");
    }

    private MergeTreeCompactRewriter realRewriter() {
        return new MergeTreeCompactRewriter(
                readerFactory,
                writerFactory,
                comparator,
                null,
                DeduplicateMergeFunction.factory(),
                new MergeSorter(options, keyType, valueType, null));
    }

    private DataFileMeta writeFile(int minKey, int maxKey) throws Exception {
        RollingFileWriter<KeyValue, DataFileMeta> writer =
                writerFactory.createRollingMergeTreeFileWriter(0, FileSource.APPEND);
        for (int k = minKey; k <= maxKey; k++) {
            writer.write(
                    new KeyValue()
                            .replace(
                                    GenericRow.of(k),
                                    sequenceNumber++,
                                    RowKind.INSERT,
                                    GenericRow.of(k)));
        }
        writer.close();
        List<DataFileMeta> result = writer.result();
        assertThat(result).hasSize(1);
        return result.get(0);
    }

    private Set<String> filesOnDisk() throws IOException {
        FileStatus[] statuses = fileIO.listStatus(bucketDir);
        return Arrays.stream(statuses).map(s -> s.getPath().getName()).collect(Collectors.toSet());
    }

    /** Files present in the bucket directory that nothing refers to any more. */
    private Set<String> orphanFiles(List<DataFileMeta> inputs) throws IOException {
        Set<String> orphans = new HashSet<>(filesOnDisk());
        inputs.forEach(f -> orphans.remove(f.fileName()));
        return orphans;
    }

    private SchemaManager testingSchemaManager(Path path) {
        TableSchema schema =
                new TableSchema(
                        0,
                        new ArrayList<>(),
                        -1,
                        new ArrayList<>(),
                        new ArrayList<>(),
                        new HashMap<>(),
                        "");
        Map<Long, TableSchema> schemas = new HashMap<>();
        schemas.put(schema.id(), schema);
        return new SchemaEvolutionTableTestBase.TestingSchemaManager(path, schemas);
    }

    private class TestKeyValueFieldsExtractor implements KeyValueFieldsExtractor {

        private static final long serialVersionUID = 1L;

        @Override
        public List<DataField> keyFields(TableSchema schema) {
            return keyType.getFields();
        }

        @Override
        public List<DataField> valueFields(TableSchema schema) {
            return valueType.getFields();
        }
    }

    /** Runs a real rewriter, but breaks on the n-th call to {@link #rewrite}. */
    private static class BreakingRewriter implements CompactRewriter {

        private final CompactRewriter delegate;
        private final int breakOnCall;
        private final Break onBreak;

        private int calls = 0;

        private BreakingRewriter(CompactRewriter delegate, int breakOnCall, Break onBreak) {
            this.delegate = delegate;
            this.breakOnCall = breakOnCall;
            this.onBreak = onBreak;
        }

        @Override
        public CompactResult rewrite(
                int outputLevel, boolean dropDelete, List<List<SortedRun>> sections)
                throws Exception {
            if (++calls == breakOnCall) {
                onBreak.run();
            }
            return delegate.rewrite(outputLevel, dropDelete, sections);
        }

        @Override
        public CompactResult upgrade(int outputLevel, DataFileMeta file) throws Exception {
            return delegate.upgrade(outputLevel, file);
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        interface Break {
            void run() throws Exception;
        }
    }
}
