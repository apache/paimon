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

    @ParameterizedTest
    @ValueSource(ints = {1, 2000})
    public void testSplitAtBlobBoundariesRetainsDedicatedFiles(int estimatedRows) throws Exception {
        FileStoreTable table = createBlobSegmentsTable();
        List<DataFileMeta> dedicated = dedicatedFiles(table);
        assertThat(dedicated).hasSize(3);
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "2");
        DataEvolutionCompactTask merged = compactSingleTask(table.copy(options));
        assertThat(merged.compactAfter()).hasSize(1);
        assertThat(merged.compactAfter().get(0).nonNullRowIdRange()).isEqualTo(new Range(0, 3749));

        options.put(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "10");
        long targetSize =
                Math.max(1L, merged.compactAfter().get(0).fileSize() * estimatedRows / 3750);
        options.put(CoreOptions.TARGET_FILE_SIZE.key(), targetSize + " b");
        options.put(CoreOptions.WRITE_BUFFER_FOR_APPEND.key(), "true");
        options.put(CoreOptions.TARGET_FILE_ROW_NUM.key(), "10");
        options.put(CoreOptions.DATA_EVOLUTION_COMPACTION_SPLIT_LARGE_FILES.key(), "true");
        options.put(CoreOptions.DATA_EVOLUTION_COMPACTION_LARGE_FILE_RATIO.key(), "1.0");
        table = table.copy(options);
        DataEvolutionCompactTask split = compactSingleTask(table);
        // Estimated cuts move to BLOB ends; adjacent ranges can share a normal output.
        assertThat(split.compactAfter().stream().map(DataFileMeta::nonNullRowIdRange))
                .containsExactlyElementsOf(
                        estimatedRows == 1
                                ? Arrays.asList(
                                        new Range(0, 1249),
                                        new Range(1250, 2499),
                                        new Range(2500, 3749))
                                : Arrays.asList(new Range(0, 2499), new Range(2500, 3749)));
        assertDedicatedFilesContained(table, dedicated);
        assertBlobValues(table);
        // Completed outputs must not be rewritten even if estimates leave them oversized.
        assertThat(
                        new DataEvolutionCompactCoordinator(
                                        table,
                                        false,
                                        false,
                                        table.snapshotManager().latestSnapshot())
                                .withCompletedNormalFiles(
                                        split.compactAfter().stream()
                                                .map(DataFileMeta::fileName)
                                                .collect(Collectors.toSet()))
                                .plan())
                .isEmpty();

        options.put(CoreOptions.TARGET_FILE_SIZE.key(), "128 mb");
        options.put(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "2");
        table = table.copy(options);
        Snapshot snapshot = table.snapshotManager().latestSnapshot();
        assertThat(new DataEvolutionCompactCoordinator(table, false, false, snapshot).plan())
                .hasSize(1);
        assertThat(
                        new DataEvolutionCompactCoordinator(table, false, false, snapshot)
                                .withCompletedNormalFiles(
                                        split.compactAfter().stream()
                                                .map(DataFileMeta::fileName)
                                                .collect(Collectors.toSet()))
                                .plan())
                .isEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testNormalSplitRespectsBlobCompactionOutputRange(boolean mergeNormalVersions)
            throws Exception {
        FileStoreTable table = createBlobSegmentsTable();
        List<DataFileMeta> originalBlobs = dedicatedFiles(table);
        assertThat(originalBlobs).hasSize(3);
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "2");
        DataEvolutionCompactTask initialMerge = compactSingleTask(table.copy(options));
        assertThat(initialMerge.compactAfter()).hasSize(1);
        DataFileMeta originalNormal = initialMerge.compactAfter().get(0);
        if (mergeNormalVersions) {
            writeProjectedRange(table, "id", 0, 3750, 0, true);
        }

        options.put(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "2");
        options.put(CoreOptions.TARGET_FILE_SIZE.key(), "1 b");
        options.put(CoreOptions.DATA_EVOLUTION_COMPACTION_SPLIT_LARGE_FILES.key(), "true");
        table = table.copy(options);
        Snapshot snapshot = table.snapshotManager().latestSnapshot();
        List<DataEvolutionCompactTask> tasks =
                new DataEvolutionCompactCoordinator(table, true, false, snapshot).plan();
        if (mergeNormalVersions) {
            assertThat(tasks)
                    .extracting(DataEvolutionCompactTask::type)
                    .containsExactly(
                            DataEvolutionCompactTask.TaskType.NORMAL,
                            DataEvolutionCompactTask.TaskType.BLOB);
        } else {
            assertThat(tasks)
                    .extracting(DataEvolutionCompactTask::type)
                    .containsExactly(DataEvolutionCompactTask.TaskType.BLOB);
        }
        DataEvolutionCompactTaskSerializer serializer = new DataEvolutionCompactTaskSerializer();
        List<CommitMessage> messages = new ArrayList<>();
        List<DataEvolutionCompactTask> restored = new ArrayList<>();
        for (DataEvolutionCompactTask planned : tasks) {
            DataEvolutionCompactTask task =
                    serializer.deserialize(serializer.getVersion(), serializer.serialize(planned));
            restored.add(task);
            messages.add(task.doCompact(table, "compact-normal-and-blob"));
        }
        if (mergeNormalVersions) {
            assertThat(restored.get(0).compactBefore()).hasSize(2);
        }
        assertThat(restored.get(restored.size() - 1).compactBefore())
                .containsExactlyInAnyOrderElementsOf(originalBlobs);
        // The old BLOB boundaries are safe individually, but the planned BLOB merge removes them.
        // Any normal rewrite must therefore retain the planned BLOB output's complete range.
        for (DataEvolutionCompactTask task : restored) {
            assertThat(task.compactAfter()).hasSize(1);
            assertThat(task.compactAfter().get(0).nonNullRowIdRange())
                    .isEqualTo(new Range(0, 3749));
        }
        messages.addAll(
                new DataEvolutionCompactionCommitPreparation(table, snapshot).prepare(messages));
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(messages);
        }
        if (!mergeNormalVersions) {
            assertThat(
                            table.store().newScan().plan().files().stream()
                                    .map(ManifestEntry::file)
                                    .filter(
                                            file ->
                                                    !isBlobFile(file.fileName())
                                                            && !isVectorStoreFile(file.fileName())))
                    .containsExactly(originalNormal);
        }
        List<DataFileMeta> compactedBlobs = dedicatedFiles(table);
        assertThat(compactedBlobs).hasSize(1).doesNotContainAnyElementsOf(originalBlobs);
        assertDedicatedFilesContained(table, compactedBlobs);
        assertBlobValues(table);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testOverlappingDedicatedRangesPreventSplit(boolean sameColumn) throws Exception {
        catalog.createTable(
                identifier(),
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("b1", DataTypes.BLOB())
                        .column("b2", DataTypes.BLOB())
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .option(CoreOptions.BLOB_TARGET_FILE_SIZE.key(), "128 mb")
                        .build(),
                false);
        FileStoreTable table = getTableDefault();
        writeProjectedRange(table, "id", 0, 3750, 0, false);
        writeProjectedRange(table, "b1", 0, 1250, 0, true);
        writeProjectedRange(table, "b1", 1250, 2500, 0, true);
        String overlappingColumn = sameColumn ? "b1" : "b2";
        writeProjectedRange(table, overlappingColumn, 0, 2500, 1, true);
        writeProjectedRange(table, overlappingColumn, 2500, 1250, 1, true);
        List<DataFileMeta> dedicated = dedicatedFiles(table);
        assertThat(dedicated).hasSize(4);
        assertThat(dedicated).allSatisfy(file -> assertThat(file.rowCount()).isLessThan(3750));
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "10");
        options.put(CoreOptions.TARGET_FILE_SIZE.key(), "1 b");
        options.put(CoreOptions.DATA_EVOLUTION_COMPACTION_SPLIT_LARGE_FILES.key(), "true");
        FileStoreTable splitTable = table.copy(options);
        assertThat(
                        new DataEvolutionCompactCoordinator(
                                        splitTable,
                                        false,
                                        false,
                                        table.snapshotManager().latestSnapshot())
                                .plan())
                .isEmpty();

        // Ordinary version merging remains useful even though no dedicated-safe split is possible.
        writeProjectedRange(table, "id", 0, 3750, 0, true);
        options.put(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "2");
        DataEvolutionCompactTask merged = compactSingleTask(table.copy(options));
        assertThat(merged.compactBefore()).hasSize(2);
        assertThat(merged.compactAfter()).hasSize(1);
        assertThat(merged.compactAfter().get(0).nonNullRowIdRange()).isEqualTo(new Range(0, 3749));
        assertDedicatedFilesContained(table, dedicated);
        ReadBuilder readBuilder = table.newReadBuilder();
        List<Integer> ids = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan())) {
            reader.forEachRemaining(
                    row -> {
                        int id = row.getInt(0);
                        ids.add(id);
                        assertThat(row.getBlob(1).toData())
                                .containsExactly((byte) (id + (sameColumn ? 1 : 0)));
                        if (sameColumn) {
                            assertThat(row.isNullAt(2)).isTrue();
                        } else {
                            assertThat(row.getBlob(2).toData()).containsExactly((byte) (id + 1));
                        }
                    });
        }
        assertThat(ids)
                .containsExactlyElementsOf(
                        java.util.stream.IntStream.range(0, 3750)
                                .boxed()
                                .collect(Collectors.toList()));
    }

    @Test
    public void testFullRangeVectorPreventsSplit() throws Exception {
        catalog.createTable(
                identifier(),
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("vector", DataTypes.VECTOR(2, DataTypes.FLOAT()))
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .option(CoreOptions.VECTOR_TARGET_FILE_SIZE.key(), "128 mb")
                        .option(CoreOptions.VECTOR_FILE_FORMAT.key(), "json")
                        .option(CoreOptions.FILE_COMPRESSION.key(), "none")
                        .build(),
                false);
        FileStoreTable table = getTableDefault();
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            for (int i = 0; i < 2500; i++) {
                write.write(
                        GenericRow.of(i, BinaryVector.fromPrimitiveArray(new float[] {i, i + 1})));
            }
            commit.commit(write.prepareCommit());
        }
        catalog.alterTable(
                identifier(),
                Collections.singletonList(SchemaChange.renameColumn("vector", "renamed_vector")),
                false);
        table = getTableDefault();
        List<DataFileMeta> dedicated = dedicatedFiles(table);
        assertThat(dedicated).hasSize(1);
        assertThat(dedicated.get(0).nonNullRowIdRange()).isEqualTo(new Range(0, 2499));
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.TARGET_FILE_SIZE.key(), "1 b");
        options.put(CoreOptions.DATA_EVOLUTION_COMPACTION_SPLIT_LARGE_FILES.key(), "true");
        assertThat(
                        new DataEvolutionCompactCoordinator(
                                        table.copy(options),
                                        false,
                                        false,
                                        table.snapshotManager().latestSnapshot())
                                .plan())
                .isEmpty();

        writeProjectedRange(table, "id", 0, 2500, 0, true);
        options.put(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "2");
        DataEvolutionCompactTask merged = compactSingleTask(table.copy(options));
        assertThat(merged.compactAfter()).hasSize(1);
        assertDedicatedFilesContained(table, dedicated);
        ReadBuilder readBuilder = table.newReadBuilder();
        List<Integer> ids = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan())) {
            reader.forEachRemaining(
                    row -> {
                        int id = row.getInt(0);
                        ids.add(id);
                        assertThat(row.getVector(1).toFloatArray()).containsExactly(id, id + 1);
                    });
        }
        assertThat(ids)
                .containsExactlyElementsOf(
                        java.util.stream.IntStream.range(0, 2500)
                                .boxed()
                                .collect(Collectors.toList()));
    }

    @Test
    public void testEstimatedRangesIgnoreParquetBufferSize() throws Exception {
        catalog.createTable(
                identifier(),
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("payload", DataTypes.STRING())
                        .column("blob", DataTypes.BLOB())
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .option(CoreOptions.FILE_FORMAT.key(), "parquet")
                        .option(CoreOptions.FILE_COMPRESSION.key(), "snappy")
                        .option(CoreOptions.TARGET_FILE_SIZE.key(), "8 mb")
                        .option(CoreOptions.BLOB_TARGET_FILE_SIZE.key(), "128 mb")
                        .option(CoreOptions.SOURCE_SPLIT_OPEN_FILE_COST.key(), "4 mb")
                        .option(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "2")
                        .option(
                                CoreOptions.DATA_EVOLUTION_COMPACTION_SPLIT_LARGE_FILES.key(),
                                "true")
                        .option("parquet.enable.dictionary", "false")
                        .option("parquet.page.size", String.valueOf(16 * 1024 * 1024))
                        .build(),
                false);
        FileStoreTable table = getTableDefault();
        char[] payloadChars = new char[10 * 1024];
        Arrays.fill(payloadChars, 'a');
        BinaryString payload = BinaryString.fromString(new String(payloadChars));
        for (int batch = 0; batch < 3; batch++) {
            BatchWriteBuilder builder = table.newBatchWriteBuilder();
            try (BatchTableWrite write = builder.newWrite();
                    BatchTableCommit commit = builder.newCommit()) {
                for (int i = batch * 1000; i < (batch + 1) * 1000; i++) {
                    write.write(GenericRow.of(i, payload, new BlobData(new byte[] {(byte) i})));
                }
                commit.commit(write.prepareCommit());
            }
        }
        List<DataFileMeta> dedicated = dedicatedFiles(table);
        assertThat(dedicated).hasSize(3);

        DataEvolutionCompactTask compacted = compactSingleTask(table);
        assertThat(compacted.compactBefore()).hasSize(3);
        List<DataFileMeta> outputs = compacted.compactAfter();
        // The input files fit in the target based on their compressed sizes, even though
        // Parquet's uncompressed page buffer crosses that target every 1000 rows.
        assertThat(outputs).extracting(DataFileMeta::rowCount).containsExactly(3000L);
        assertThat(outputs)
                .allSatisfy(
                        file ->
                                assertThat(file.fileSize())
                                        .isLessThan(table.coreOptions().splitOpenFileCost()));
        assertDedicatedFilesContained(table, dedicated);

        Snapshot snapshot = table.snapshotManager().latestSnapshot();
        assertThat(new DataEvolutionCompactCoordinator(table, false, false, snapshot).plan())
                .isEmpty();

        List<Integer> ids = new ArrayList<>();
        ReadBuilder readBuilder = table.newReadBuilder();
        try (RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan())) {
            reader.forEachRemaining(
                    row -> {
                        int id = row.getInt(0);
                        ids.add(id);
                        assertThat(row.getString(1)).isEqualTo(payload);
                        assertThat(row.getBlob(2).toData()).containsExactly((byte) id);
                    });
        }
        assertThat(ids)
                .containsExactlyElementsOf(
                        java.util.stream.IntStream.range(0, 3000)
                                .boxed()
                                .collect(Collectors.toList()));
    }

    private FileStoreTable createBlobSegmentsTable() throws Exception {
        catalog.createTable(
                identifier(),
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("blob", DataTypes.BLOB())
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .option(CoreOptions.BLOB_TARGET_FILE_SIZE.key(), "128 mb")
                        .build(),
                false);
        FileStoreTable table = getTableDefault();
        for (int batch = 0; batch < 3; batch++) {
            BatchWriteBuilder builder = table.newBatchWriteBuilder();
            try (BatchTableWrite write = builder.newWrite();
                    BatchTableCommit commit = builder.newCommit()) {
                for (int i = batch * 1250; i < (batch + 1) * 1250; i++) {
                    write.write(GenericRow.of(i, new BlobData(new byte[] {(byte) i})));
                }
                commit.commit(write.prepareCommit());
            }
        }
        return table;
    }

    private void assertBlobValues(FileStoreTable table) throws Exception {
        List<Integer> ids = new ArrayList<>();
        ReadBuilder readBuilder = table.newReadBuilder();
        try (RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan())) {
            reader.forEachRemaining(
                    row -> {
                        int id = row.getInt(0);
                        ids.add(id);
                        assertThat(row.getBlob(1).toData()).containsExactly((byte) id);
                    });
        }
        assertThat(ids)
                .containsExactlyElementsOf(
                        java.util.stream.IntStream.range(0, 3750)
                                .boxed()
                                .collect(Collectors.toList()));
    }

    private void writeProjectedRange(
            FileStoreTable table,
            String column,
            int from,
            int count,
            int valueOffset,
            boolean existingRows)
            throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write =
                        builder.newWrite().withWriteType(table.rowType().project(column));
                BatchTableCommit commit = builder.newCommit()) {
            for (int i = from; i < from + count; i++) {
                write.write(
                        GenericRow.of(
                                "id".equals(column)
                                        ? i
                                        : new BlobData(new byte[] {(byte) (i + valueOffset)})));
            }
            List<CommitMessage> messages = write.prepareCommit();
            if (existingRows) {
                assignFirstRowId(messages, from);
            }
            commit.commit(messages);
        }
    }

    private DataEvolutionCompactTask compactSingleTask(FileStoreTable table) throws Exception {
        Snapshot snapshot = table.snapshotManager().latestSnapshot();
        List<DataEvolutionCompactTask> tasks =
                new DataEvolutionCompactCoordinator(table, false, false, snapshot).plan();
        assertThat(tasks).hasSize(1);
        DataEvolutionCompactTaskSerializer serializer = new DataEvolutionCompactTaskSerializer();
        DataEvolutionCompactTask task =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(tasks.get(0)));
        List<CommitMessage> messages = new ArrayList<>();
        messages.add(task.doCompact(table, "compact-dedicated-boundaries"));
        messages.addAll(
                new DataEvolutionCompactionCommitPreparation(table, snapshot).prepare(messages));
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(messages);
        }
        return task;
    }

    private List<DataFileMeta> dedicatedFiles(FileStoreTable table) {
        return table.store().newScan().plan().files().stream()
                .map(ManifestEntry::file)
                .filter(file -> isBlobFile(file.fileName()) || isVectorStoreFile(file.fileName()))
                .collect(Collectors.toList());
    }

    private void assertDedicatedFilesContained(FileStoreTable table, List<DataFileMeta> expected) {
        assertThat(dedicatedFiles(table)).containsExactlyInAnyOrderElementsOf(expected);
        List<Range> normalRanges =
                table.store().newScan().plan().files().stream()
                        .map(ManifestEntry::file)
                        .filter(
                                file ->
                                        !isBlobFile(file.fileName())
                                                && !isVectorStoreFile(file.fileName()))
                        .map(DataFileMeta::nonNullRowIdRange)
                        .collect(Collectors.toList());
        for (DataFileMeta file : expected) {
            Range dedicated = file.nonNullRowIdRange();
            assertThat(
                            normalRanges.stream()
                                    .anyMatch(
                                            normal ->
                                                    normal.from <= dedicated.from
                                                            && normal.to >= dedicated.to))
                    .isTrue();
        }
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
