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

package org.apache.paimon.append.dataevolution;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.BlobData;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import static org.apache.paimon.format.blob.BlobFileFormat.isBlobFile;
import static org.apache.paimon.types.VectorType.isVectorStoreFile;
import static org.apache.paimon.utils.DataEvolutionUtils.fileFields;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for splitting and column sequence propagation in {@link DataEvolutionNormalCompactTask}.
 */
public class DataEvolutionNormalCompactTaskTest extends TableTestBase {

    private static final int ROW_COUNT = 100;

    @Override
    public Schema schemaDefault() {
        return Schema.newBuilder()
                .column("dt", DataTypes.STRING())
                .column("f0", DataTypes.INT())
                .column("f1", DataTypes.STRING())
                .partitionKeys(Collections.singletonList("dt"))
                .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                .build();
    }

    @Test
    public void testPropagateColumnSequencesAcrossCompactions() throws Exception {
        write();

        int f0Id = getTableDefault().rowType().getField("f0").id();
        int f1Id = getTableDefault().rowType().getField("f1").id();
        DataFileMeta firstCompact =
                updateColumnsAndCompact(
                        Collections.singletonList("f1"),
                        1,
                        CoreOptions.GlobalIndexColumnUpdateAction.IGNORE);

        long f0Sequence = columnSequence(firstCompact, f0Id);
        assertThat(f0Sequence).isLessThan(firstCompact.maxSequenceNumber());
        assertThat(columnSequence(firstCompact, f1Id)).isEqualTo(firstCompact.maxSequenceNumber());

        DataFileMeta secondCompact =
                updateColumnsAndCompact(
                        Collections.singletonList("f1"),
                        2,
                        CoreOptions.GlobalIndexColumnUpdateAction.IGNORE);
        assertThat(columnSequence(secondCompact, f0Id)).isEqualTo(f0Sequence);
        assertThat(columnSequence(secondCompact, f1Id))
                .isEqualTo(secondCompact.maxSequenceNumber());
    }

    @Test
    public void testOmitAndReconstructRedundantColumnSequences() throws Exception {
        write();

        DataFileMeta fullUpdate =
                updateColumnsAndCompact(
                        Arrays.asList("f0", "f1"),
                        1,
                        CoreOptions.GlobalIndexColumnUpdateAction.IGNORE);
        assertThat(fullUpdate.columnMaxSequenceNumbers()).isNull();

        int f0Id = getTableDefault().rowType().getField("f0").id();
        DataFileMeta partialUpdate =
                updateColumnsAndCompact(
                        Collections.singletonList("f1"),
                        2,
                        CoreOptions.GlobalIndexColumnUpdateAction.IGNORE);
        assertThat(columnSequence(partialUpdate, f0Id))
                .isEqualTo(fullUpdate.maxSequenceNumber())
                .isLessThan(partialUpdate.maxSequenceNumber());
    }

    @Test
    public void testOmitColumnSequencesUnlessUpdatesAreIgnored() throws Exception {
        write();

        DataFileMeta compacted =
                updateColumnsAndCompact(
                        Collections.singletonList("f1"),
                        1,
                        CoreOptions.GlobalIndexColumnUpdateAction.THROW_ERROR);
        assertThat(compacted.columnMaxSequenceNumbers()).isNull();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testSplitHistoricalLargeFile(boolean updateColumn) throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        int rowCount = 12000;
        Random random = new Random(42);
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            for (int i = 0; i < rowCount; i++) {
                StringBuilder value = new StringBuilder();
                for (int j = 0; j < 8; j++) {
                    value.append(Long.toHexString(random.nextLong()));
                }
                write.write(
                        GenericRow.of(
                                BinaryString.fromString("p0"),
                                i,
                                BinaryString.fromString(value.toString())));
            }
            commit.commit(write.prepareCommit());
        }
        DataFileMeta original = table.store().newScan().plan().files().get(0).file();
        if (updateColumn) {
            builder = table.newBatchWriteBuilder();
            try (BatchTableWrite write =
                            builder.newWrite()
                                    .withWriteType(
                                            table.rowType().project(Arrays.asList("dt", "f0")));
                    BatchTableCommit commit = builder.newCommit()) {
                for (int i = 0; i < rowCount; i++) {
                    write.write(GenericRow.of(BinaryString.fromString("p0"), i + rowCount));
                }
                List<CommitMessage> messages = write.prepareCommit();
                assignFirstRowId(messages, original.nonNullFirstRowId());
                commit.commit(messages);
            }
        }
        List<InternalRow> expected = read(table);
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.TARGET_FILE_SIZE.key(), "128 kb");
        options.put(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "10");
        options.put(CoreOptions.GLOBAL_INDEX_COLUMN_UPDATE_ACTION.key(), "ignore");
        table = table.copy(options);
        long targetSize = table.coreOptions().targetFileSize(false);
        assertThat(original.fileSize()).isGreaterThan(3 * targetSize);
        Snapshot snapshot = table.snapshotManager().latestSnapshot();
        assertThat(new DataEvolutionCompactCoordinator(table, false, false, snapshot).plan())
                .isEmpty();

        options.put(CoreOptions.DATA_EVOLUTION_COMPACTION_SPLIT_LARGE_FILES.key(), "true");
        options.put(CoreOptions.DATA_EVOLUTION_COMPACTION_LARGE_FILE_RATIO.key(), "1000.0");
        table = table.copy(options);
        assertThat(new DataEvolutionCompactCoordinator(table, false, false, snapshot).plan())
                .isEmpty();
        options.put(CoreOptions.DATA_EVOLUTION_COMPACTION_LARGE_FILE_RATIO.key(), "3.0");
        table = table.copy(options);
        List<DataEvolutionCompactTask> tasks =
                new DataEvolutionCompactCoordinator(table, false, false, snapshot).plan();
        assertThat(tasks).hasSize(1);
        DataEvolutionCompactTaskSerializer serializer = new DataEvolutionCompactTaskSerializer();
        DataEvolutionCompactTask task =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(tasks.get(0)));
        List<CommitMessage> messages = new ArrayList<>();
        messages.add(task.doCompact(table, "split-large-file"));
        List<DataFileMeta> output = task.compactAfter();
        assertThat(output.size()).isGreaterThan(1);
        long nextRowId = original.nonNullFirstRowId();
        long maxSequenceNumber =
                task.compactBefore().stream()
                        .mapToLong(DataFileMeta::maxSequenceNumber)
                        .max()
                        .getAsLong();
        for (DataFileMeta file : output) {
            assertThat(file.nonNullFirstRowId()).isEqualTo(nextRowId);
            assertThat(file.fileSize()).isLessThan(2 * targetSize);
            assertThat(file.minSequenceNumber()).isEqualTo(original.minSequenceNumber());
            assertThat(file.maxSequenceNumber()).isEqualTo(maxSequenceNumber);
            if (updateColumn) {
                assertThat(columnSequence(file, table.rowType().getField("f1").id()))
                        .isEqualTo(original.maxSequenceNumber());
                assertThat(columnSequence(file, table.rowType().getField("f0").id()))
                        .isEqualTo(maxSequenceNumber);
            }
            nextRowId += file.rowCount();
        }
        assertThat(nextRowId).isEqualTo(original.nonNullFirstRowId() + rowCount);
        messages.addAll(
                new DataEvolutionCompactionCommitPreparation(table, snapshot).prepare(messages));
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(messages);
        }
        assertThat(read(table)).containsExactlyInAnyOrderElementsOf(expected);
        assertThat(
                        new DataEvolutionCompactCoordinator(
                                        table,
                                        false,
                                        false,
                                        table.snapshotManager().latestSnapshot())
                                .plan())
                .isEmpty();
    }

    @Test
    public void testSplitRetainsMultipleBlobColumnsAndVectorFiles() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("b1", DataTypes.BLOB())
                        .column("b2", DataTypes.BLOB())
                        .column("v", DataTypes.VECTOR(2, DataTypes.FLOAT()))
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .option(CoreOptions.BLOB_TARGET_FILE_SIZE.key(), "128 kb")
                        .option(CoreOptions.VECTOR_TARGET_FILE_SIZE.key(), "128 kb")
                        .option(CoreOptions.VECTOR_FILE_FORMAT.key(), "json")
                        .option(CoreOptions.FILE_COMPRESSION.key(), "none")
                        .build();
        catalog.createTable(identifier(), schema, false);
        FileStoreTable table = getTableDefault();
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            for (int i = 0; i < 2500; i++) {
                write.write(
                        GenericRow.of(
                                i,
                                new BlobData(new byte[] {(byte) i}),
                                new BlobData(new byte[] {(byte) (i + 1)}),
                                BinaryVector.fromPrimitiveArray(new float[] {i, i + 1})));
            }
            commit.commit(write.prepareCommit());
        }
        catalog.alterTable(
                identifier(),
                Collections.singletonList(SchemaChange.renameColumn("v", "renamed_v")),
                false);
        table = getTableDefault();
        List<DataFileMeta> dedicatedFiles =
                table.store().newScan().plan().files().stream()
                        .map(ManifestEntry::file)
                        .filter(
                                file ->
                                        isBlobFile(file.fileName())
                                                || isVectorStoreFile(file.fileName()))
                        .collect(Collectors.toList());
        assertThat(dedicatedFiles).hasSize(3);
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.TARGET_FILE_SIZE.key(), "1 b");
        options.put(CoreOptions.DATA_EVOLUTION_COMPACTION_SPLIT_LARGE_FILES.key(), "true");
        table = table.copy(options);
        List<DataEvolutionCompactTask> tasks =
                new DataEvolutionCompactCoordinator(
                                table, false, false, table.snapshotManager().latestSnapshot())
                        .plan();
        assertThat(tasks).hasSize(1);
        DataEvolutionCompactTask task = tasks.get(0);
        assertThat(task.compactBefore()).hasSize(1);
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(Collections.singletonList(task.doCompact(table, "split-dedicated")));
        }
        List<Range> normalRanges =
                task.compactAfter().stream()
                        .filter(
                                file ->
                                        !isBlobFile(file.fileName())
                                                && !isVectorStoreFile(file.fileName()))
                        .map(DataFileMeta::nonNullRowIdRange)
                        .collect(Collectors.toList());
        assertThat(normalRanges.size()).isGreaterThan(1);
        assertThat(task.compactAfter())
                .allSatisfy(
                        file ->
                                assertThat(
                                                isBlobFile(file.fileName())
                                                        || isVectorStoreFile(file.fileName()))
                                        .isFalse());
        assertThat(
                        table.store().newScan().plan().files().stream()
                                .map(ManifestEntry::file)
                                .filter(
                                        file ->
                                                isBlobFile(file.fileName())
                                                        || isVectorStoreFile(file.fileName()))
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrderElementsOf(dedicatedFiles);

        // Reading the new layout does not depend on the compaction option remaining enabled.
        options.put(CoreOptions.DATA_EVOLUTION_COMPACTION_SPLIT_LARGE_FILES.key(), "false");
        table = table.copy(options);
        assertDedicatedValues(table);

        // Merging split normal files must also leave spanning dedicated files untouched.
        options.put(CoreOptions.TARGET_FILE_SIZE.key(), "128 mb");
        options.put(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "2");
        table = table.copy(options);
        tasks =
                new DataEvolutionCompactCoordinator(
                                table, false, false, table.snapshotManager().latestSnapshot())
                        .plan();
        assertThat(tasks).hasSize(1);
        task = tasks.get(0);
        assertThat(task.compactBefore()).hasSize(normalRanges.size());
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(Collections.singletonList(task.doCompact(table, "merge-split-normal")));
        }
        assertThat(task.compactAfter()).hasSize(1);
        assertThat(
                        table.store().newScan().plan().files().stream()
                                .map(ManifestEntry::file)
                                .filter(
                                        file ->
                                                isBlobFile(file.fileName())
                                                        || isVectorStoreFile(file.fileName()))
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrderElementsOf(dedicatedFiles);
        assertDedicatedValues(table);
    }

    private void assertDedicatedValues(FileStoreTable table) throws Exception {
        ReadBuilder readBuilder = table.newReadBuilder();
        List<Integer> ids = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan())) {
            reader.forEachRemaining(
                    row -> {
                        int id = row.getInt(0);
                        ids.add(id);
                        assertThat(row.getBlob(1).toData()).containsExactly((byte) id);
                        assertThat(row.getBlob(2).toData()).containsExactly((byte) (id + 1));
                        assertThat(row.getVector(3).toFloatArray()).containsExactly(id, id + 1);
                    });
        }
        assertThat(ids)
                .containsExactlyElementsOf(
                        java.util.stream.IntStream.range(0, 2500)
                                .boxed()
                                .collect(Collectors.toList()));
    }

    private void write() throws Exception {
        createTableDefault();

        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite()) {
            for (int i = 0; i < ROW_COUNT; i++) {
                write.write(
                        GenericRow.of(
                                BinaryString.fromString("p0"),
                                i,
                                BinaryString.fromString("f1_" + i)));
            }
            try (BatchTableCommit commit = builder.newCommit()) {
                commit.commit(write.prepareCommit());
            }
        }
    }

    private DataFileMeta updateColumnsAndCompact(
            List<String> columns,
            int updateRound,
            CoreOptions.GlobalIndexColumnUpdateAction updateAction)
            throws Exception {
        Map<String, String> writeOptions = new HashMap<>();
        writeOptions.put(
                CoreOptions.GLOBAL_INDEX_COLUMN_UPDATE_ACTION.key(), updateAction.toString());
        FileStoreTable table = getTableDefault().copy(writeOptions);
        List<String> writeColumns = new ArrayList<>();
        writeColumns.add("dt");
        writeColumns.addAll(columns);
        RowType writeType = table.rowType().project(writeColumns);
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite batchWrite = writeBuilder.newWrite().withWriteType(writeType)) {
            for (int i = 0; i < ROW_COUNT; i++) {
                List<Object> values = new ArrayList<>();
                values.add(BinaryString.fromString("p0"));
                for (String column : columns) {
                    values.add(
                            "f0".equals(column)
                                    ? i + updateRound * ROW_COUNT
                                    : BinaryString.fromString("updated_" + updateRound + "_" + i));
                }
                batchWrite.write(GenericRow.of(values.toArray()));
            }
            List<CommitMessage> messages = batchWrite.prepareCommit();
            assignFirstRowId(messages, 0L);
            try (BatchTableCommit commit = writeBuilder.newCommit()) {
                commit.commit(messages);
            }
        }

        writeOptions.put(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "2");
        table = getTableDefault().copy(writeOptions);
        Snapshot compactSnapshot = table.snapshotManager().latestSnapshot();
        DataEvolutionCompactCoordinator coordinator =
                new DataEvolutionCompactCoordinator(table, false, false, compactSnapshot);
        List<CommitMessage> compactMessages = new ArrayList<>();
        for (DataEvolutionCompactTask task : coordinator.plan()) {
            compactMessages.add(task.doCompact(table, "test-compact"));
        }
        assertThat(compactMessages).isNotEmpty();
        compactMessages.addAll(
                new DataEvolutionCompactionCommitPreparation(table, compactSnapshot)
                        .prepare(compactMessages));
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(compactMessages);
        }

        List<DataFileMeta> rowRangeFiles =
                getTableDefault().store().newScan().plan().files().stream()
                        .map(ManifestEntry::file)
                        .filter(file -> file.firstRowId() != null && file.firstRowId() == 0L)
                        .collect(Collectors.toList());
        assertThat(rowRangeFiles).hasSize(1);
        return rowRangeFiles.get(0);
    }

    private long columnSequence(DataFileMeta file, int fieldId) throws Exception {
        TableSchema fileSchema = getTableDefault().schemaManager().schema(file.schemaId());
        boolean nestedFieldEnabled =
                new CoreOptions(fileSchema.options()).dataEvolutionNestedFieldEnabled();
        List<DataField> fields = fileFields(fileSchema.fields(), file, nestedFieldEnabled);
        long[] sequences = file.columnMaxSequenceNumbers();
        assertThat(sequences).hasSize(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).id() == fieldId) {
                return sequences[i];
            }
        }
        throw new IllegalArgumentException("Field not found in data file: " + fieldId);
    }

    private void assignFirstRowId(List<CommitMessage> messages, long firstRowId) {
        for (CommitMessage message : messages) {
            CommitMessageImpl impl = (CommitMessageImpl) message;
            List<DataFileMeta> files = new ArrayList<>(impl.newFilesIncrement().newFiles());
            impl.newFilesIncrement().newFiles().clear();
            impl.newFilesIncrement()
                    .newFiles()
                    .addAll(
                            files.stream()
                                    .map(file -> file.assignFirstRowId(firstRowId))
                                    .collect(Collectors.toList()));
        }
    }
}
