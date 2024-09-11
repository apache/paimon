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

package org.apache.paimon.mergetree;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.format.FlushingFileFormat;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.FileReaderFactory;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.mergetree.compact.ChangelogMergeTreeRewriter;
import org.apache.paimon.mergetree.compact.ChangelogResult;
import org.apache.paimon.mergetree.compact.DeduplicateMergeFunction;
import org.apache.paimon.mergetree.compact.FullChangelogMergeFunctionWrapper;
import org.apache.paimon.mergetree.compact.MergeFunctionWrapper;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.KeyValueFieldsExtractor;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.paimon.options.MemorySize.VALUE_128_MB;
import static org.apache.paimon.utils.FileStorePathFactoryTest.createNonPartFactory;
import static org.junit.jupiter.api.Assertions.fail;

/** Tests for {@link ChangelogMergeTreeRewriter}. */
public class ChangelogMergeTreeRewriterTest {
    @TempDir java.nio.file.Path tempDir;
    private Path path;
    private Comparator<InternalRow> comparator;
    private SchemaManager schemaManager;
    private TableSchema tableSchema;
    private RowType keyType;
    private RowType valueType;

    @BeforeEach
    public void beforeEach() throws Exception {
        path = new Path(tempDir.toString());
        comparator = Comparator.comparingInt(o -> o.getInt(0));
        schemaManager = new SchemaManager(LocalFileIO.create(), path);
        RowType recordType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "key", DataTypes.INT()),
                        DataTypes.FIELD(1, "value", DataTypes.INT()));
        tableSchema =
                schemaManager.createTable(
                        new Schema(
                                recordType.getFields(),
                                Collections.emptyList(),
                                Collections.singletonList("key"),
                                Collections.emptyMap(),
                                ""));
        keyType = recordType.project(Collections.singletonList("key"));
        valueType = recordType.project(Collections.singletonList("value"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testRewriteFailAndCleanupFiles(boolean rewriteChangelog) throws Exception {
        List<List<SortedRun>> sections = createTestSections(2);
        Path testPath = new Path(path, UUID.randomUUID().toString());
        try (ChangelogMergeTreeRewriter rewriter =
                new TestRewriter(
                        createReaderFactory(schemaManager, tableSchema, keyType, valueType),
                        createWriterFactory(testPath, keyType, valueType),
                        comparator,
                        new MergeSorter(
                                new CoreOptions(new Options()),
                                tableSchema.logicalPrimaryKeysType(),
                                tableSchema.logicalRowType(),
                                null),
                        rewriteChangelog,
                        true)) {
            try {
                rewriter.rewrite(5, true, sections);
                fail();
            } catch (IOException ignore) {
                // ignore
            }

            List<java.nio.file.Path> files =
                    Files.walk(Paths.get(testPath.toString()))
                            .filter(Files::isRegularFile)
                            .filter(
                                    p ->
                                            p.getFileName()
                                                            .toString()
                                                            .startsWith(
                                                                    DataFilePathFactory
                                                                            .DATA_FILE_PREFIX)
                                                    || p.getFileName()
                                                            .toString()
                                                            .startsWith(
                                                                    DataFilePathFactory
                                                                            .CHANGELOG_FILE_PREFIX))
                            .collect(Collectors.toList());
            Assertions.assertEquals(0, files.size());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testRewriteSuccess(boolean rewriteChangelog) throws Exception {
        List<List<SortedRun>> sections = createTestSections(2);
        Path testPath = new Path(path, UUID.randomUUID().toString());
        try (ChangelogMergeTreeRewriter rewriter =
                new TestRewriter(
                        createReaderFactory(schemaManager, tableSchema, keyType, valueType),
                        createWriterFactory(testPath, keyType, valueType),
                        comparator,
                        new MergeSorter(
                                new CoreOptions(new Options()),
                                tableSchema.logicalPrimaryKeysType(),
                                tableSchema.logicalRowType(),
                                null),
                        rewriteChangelog,
                        false)) {

            rewriter.rewrite(5, true, sections);
            List<java.nio.file.Path> files =
                    Files.walk(Paths.get(testPath.toString()))
                            .filter(Files::isRegularFile)
                            .filter(
                                    p ->
                                            p.getFileName()
                                                            .toString()
                                                            .startsWith(
                                                                    DataFilePathFactory
                                                                            .DATA_FILE_PREFIX)
                                                    || p.getFileName()
                                                            .toString()
                                                            .startsWith(
                                                                    DataFilePathFactory
                                                                            .CHANGELOG_FILE_PREFIX))
                            .collect(Collectors.toList());
            if (rewriteChangelog) {
                Assertions.assertEquals(2, files.size()); // changelog + data file
            } else {
                Assertions.assertEquals(1, files.size()); // data file
            }
        }
    }

    private KeyValueFileWriterFactory createWriterFactory(
            Path path, RowType keyType, RowType valueType) {
        return KeyValueFileWriterFactory.builder(
                        LocalFileIO.create(),
                        0,
                        keyType,
                        valueType,
                        new FlushingFileFormat("avro"),
                        Collections.singletonMap("avro", createNonPartFactory(path)),
                        VALUE_128_MB.getBytes())
                .build(BinaryRow.EMPTY_ROW, 0, new CoreOptions(new Options()));
    }

    private KeyValueFileReaderFactory createReaderFactory(
            SchemaManager schemaManager, TableSchema schema, RowType keyType, RowType valueType) {
        return KeyValueFileReaderFactory.builder(
                        LocalFileIO.create(),
                        schemaManager,
                        schema,
                        keyType,
                        valueType,
                        ignore -> new FlushingFileFormat("avro"),
                        createNonPartFactory(path),
                        new KeyValueFieldsExtractor() {
                            @Override
                            public List<DataField> keyFields(TableSchema schema) {
                                return keyType.getFields();
                            }

                            @Override
                            public List<DataField> valueFields(TableSchema schema) {
                                return valueType.getFields();
                            }
                        },
                        new CoreOptions(new HashMap<>()))
                .build(BinaryRow.EMPTY_ROW, 0, DeletionVector.emptyFactory());
    }

    private List<List<SortedRun>> createTestSections(int numSections) throws IOException {
        List<List<SortedRun>> sections = new ArrayList<>();
        createFile(Collections.singletonMap(1, 1), keyType, valueType);
        for (int i = 0; i < numSections; i++) {
            sections.add(
                    Collections.singletonList(
                            SortedRun.fromSorted(
                                    createFile(
                                            Collections.singletonMap(1, numSections),
                                            keyType,
                                            valueType))));
        }
        return sections;
    }

    private List<DataFileMeta> createFile(
            Map<Integer, Integer> kvs, RowType keyType, RowType valueType) throws IOException {
        KeyValueFileWriterFactory writerFactory = createWriterFactory(path, keyType, valueType);
        RollingFileWriter<KeyValue, DataFileMeta> writer =
                writerFactory.createRollingChangelogFileWriter(0);

        try {
            for (Map.Entry<Integer, Integer> kv : kvs.entrySet()) {
                GenericRow key = new GenericRow(1);
                key.setField(0, kv.getKey());

                GenericRow value = new GenericRow(1);
                value.setField(0, kv.getValue());

                writer.write(new KeyValue().replace(key, RowKind.INSERT, value));
            }
        } finally {
            writer.close();
        }
        return writer.result();
    }

    private static class TestRewriter extends ChangelogMergeTreeRewriter {
        private static final int MAX_LEVEL = 5;
        private final boolean rewriteChangelog;
        private final boolean closeWithException;

        public TestRewriter(
                FileReaderFactory<KeyValue> readerFactory,
                KeyValueFileWriterFactory writerFactory,
                Comparator<InternalRow> keyComparator,
                MergeSorter mergeSorter,
                boolean rewriteChangelog,
                boolean closeWithException) {
            super(
                    MAX_LEVEL,
                    CoreOptions.MergeEngine.DEDUPLICATE,
                    readerFactory,
                    writerFactory,
                    keyComparator,
                    null,
                    DeduplicateMergeFunction.factory(),
                    mergeSorter,
                    true,
                    true);
            this.rewriteChangelog = rewriteChangelog;
            this.closeWithException = closeWithException;
        }

        @Override
        protected boolean rewriteChangelog(
                int outputLevel, boolean dropDelete, List<List<SortedRun>> sections) {
            return rewriteChangelog;
        }

        @Override
        protected UpgradeStrategy upgradeStrategy(int outputLevel, DataFileMeta file) {
            return UpgradeStrategy.CHANGELOG_WITH_REWRITE;
        }

        @Override
        protected MergeFunctionWrapper<ChangelogResult> createMergeWrapper(int outputLevel) {
            return new FullChangelogMergeFunctionWrapper(
                    mfFactory.create(), MAX_LEVEL, null, false);
        }

        @Override
        protected <T> RecordReader<T> readerForMergeTree(
                List<List<SortedRun>> sections, MergeFunctionWrapper<T> mergeFunctionWrapper)
                throws IOException {
            RecordReader<T> reader = super.readerForMergeTree(sections, mergeFunctionWrapper);
            return new RecordReader<T>() {
                @Nullable
                @Override
                public RecordIterator<T> readBatch() throws IOException {
                    return reader.readBatch();
                }

                @Override
                public void close() throws IOException {
                    reader.close();
                    if (closeWithException) {
                        throw new IOException("Test exception during closing.");
                    }
                }
            };
        }
    }
}
