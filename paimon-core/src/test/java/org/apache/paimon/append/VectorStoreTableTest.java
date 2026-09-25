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

package org.apache.paimon.append;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.append.dataevolution.DataEvolutionCompactCoordinator;
import org.apache.paimon.append.dataevolution.DataEvolutionCompactTask;
import org.apache.paimon.append.dataevolution.DataEvolutionCompactionCommitPreparation;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.BlobData;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.BitmapDeletionVector;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.deletionvectors.DeletionVectorsIndexFile;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.DataEvolutionSplitRead;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.DataEvolutionTestBase;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.DataEvolutionUtils.retrieveAnchorFile;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for table with vector-store and data evolution. */
public class VectorStoreTableTest extends DataEvolutionTestBase {

    private static final int VECTOR_DIM = 12;

    private final AtomicInteger uniqueIdGen = new AtomicInteger(0);
    private final Map<Integer, InternalRow> rowsWritten = new HashMap<>();

    @Test
    public void testPartialUpdateOfSharedVectorFile() throws Exception {
        Schema schema =
                vectorSchema("json")
                        .column("embedding_v2", DataTypes.VECTOR(2, DataTypes.FLOAT()))
                        .build();
        catalog.createTable(identifier(), schema, false);
        BinaryVector original = vector(1);
        BinaryVector updated = vector(2);
        write(
                getTableDefault(),
                GenericRow.of(0, original, null),
                GenericRow.of(1, original, original));

        List<DataFileMeta> vectorFiles =
                getTableDefault().store().newScan().plan().files().stream()
                        .map(ManifestEntry::file)
                        .filter(file -> file.fileName().contains(".vector."))
                        .collect(Collectors.toList());
        assertThat(vectorFiles.size()).isEqualTo(1);
        assertThat(vectorFiles.get(0).writeCols())
                .isEqualTo(Arrays.asList("embedding", "embedding_v2"));

        updateVectors(
                0,
                Collections.singletonList("embedding_v2"),
                GenericRow.of(updated),
                GenericRow.of((Object) null));

        List<InternalRow> actual = read(getTableDefault());
        assertThat(actual).extracting(row -> row.getInt(0)).containsExactly(0, 1);
        actual.forEach(
                row ->
                        assertThat(row.getVector(1).toFloatArray())
                                .isEqualTo(original.toFloatArray()));
        assertThat(actual.get(0).getVector(2).toFloatArray()).isEqualTo(updated.toFloatArray());
        assertThat(actual.get(1).isNullAt(2)).isTrue();

        List<InternalRow> projected = read(getTableDefault(), new int[] {2});
        assertThat(projected.size()).isEqualTo(2);
        assertThat(projected.get(0).getVector(0).toFloatArray()).isEqualTo(updated.toFloatArray());
        assertThat(projected.get(1).isNullAt(0)).isTrue();

        Table historical =
                getTableDefault()
                        .copy(Collections.singletonMap(CoreOptions.SCAN_SNAPSHOT_ID.key(), "1"));
        List<InternalRow> beforeUpdate = read(historical);
        assertThat(beforeUpdate.size()).isEqualTo(2);
        assertThat(beforeUpdate.get(0).isNullAt(2)).isTrue();
        assertThat(beforeUpdate.get(1).getVector(2).toFloatArray())
                .isEqualTo(original.toFloatArray());
    }

    @ParameterizedTest
    @ValueSource(strings = {"json", "parquet"})
    public void testPartialUpdateAcrossCompactedRanges(String format) throws Exception {
        catalog.createTable(
                identifier(),
                vectorSchema(format)
                        .column("embedding_v2", DataTypes.VECTOR(2, DataTypes.FLOAT()))
                        .build(),
                false);
        write(
                getTableDefault(),
                GenericRow.of(0, vector(1), vector(10)),
                GenericRow.of(1, vector(1), vector(10)));
        write(
                getTableDefault(),
                GenericRow.of(2, vector(1), vector(10)),
                GenericRow.of(3, vector(1), vector(10)));
        updateVectors(
                2,
                Collections.singletonList("embedding_v2"),
                GenericRow.of(vector(100)),
                GenericRow.of(vector(100)));
        updateVectors(
                2,
                Arrays.asList("embedding", "embedding_v2"),
                GenericRow.of(vector(2), vector(200)),
                GenericRow.of(vector(2), vector(200)));
        updateVectors(
                0,
                Collections.singletonList("embedding_v2"),
                GenericRow.of(vector(300)),
                GenericRow.of(vector(300)));
        compactVectorTable();
        catalog.alterTable(
                identifier(),
                Collections.singletonList(
                        SchemaChange.renameColumn("embedding_v2", "query_embedding")),
                false);

        // The single-column file wins for rows 0-1, but the shared file wins for rows 2-3.
        // Sorting whole column groups by their maximum sequence would return 100 for rows 2-3.
        List<InternalRow> rows = read(getTableDefault());
        assertThat(rows).extracting(row -> row.getInt(0)).containsExactly(0, 1, 2, 3);
        assertThat(rows)
                .extracting(row -> row.getVector(1).toFloatArray()[0])
                .containsExactly(1F, 1F, 2F, 2F);
        assertThat(rows)
                .extracting(row -> row.getVector(2).toFloatArray()[0])
                .containsExactly(300F, 300F, 200F, 200F);
        assertThat(read(getTableDefault(), new int[] {2}))
                .extracting(row -> row.getVector(0).toFloatArray()[0])
                .containsExactly(300F, 300F, 200F, 200F);
        assertThat(read(getTableDefault(), new int[] {1}))
                .extracting(row -> row.getVector(0).toFloatArray()[0])
                .containsExactly(1F, 1F, 2F, 2F);

        ReadBuilder builder =
                getTableDefault()
                        .newReadBuilder()
                        .withProjection(new int[] {2})
                        .withRowRanges(Arrays.asList(new Range(1, 1), new Range(2, 2)));
        List<Float> selected = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                builder.newRead().createReader(builder.newScan().plan())) {
            reader.forEachRemaining(row -> selected.add(row.getVector(0).toFloatArray()[0]));
        }
        assertThat(selected).containsExactly(300F, 200F);
    }

    @Test
    public void testPartialUpdateWithMissingVectorRange() throws Exception {
        catalog.createTable(
                identifier(),
                vectorSchema("json")
                        // Keep the normal anchor when pruning columns to preserve missing rows.
                        .option(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true")
                        .build(),
                false);
        write(getTableDefault(), GenericRow.of(0, vector(1)), GenericRow.of(1, vector(1)));
        catalog.alterTable(
                identifier(),
                Collections.singletonList(
                        SchemaChange.addColumn(
                                "embedding_v2", DataTypes.VECTOR(2, DataTypes.FLOAT()))),
                false);
        write(
                getTableDefault(),
                GenericRow.of(2, vector(1), vector(10)),
                GenericRow.of(3, vector(1), vector(10)));
        updateVectors(
                2,
                Collections.singletonList("embedding_v2"),
                GenericRow.of(vector(200)),
                GenericRow.of(vector(200)));
        compactVectorTable();

        ReadBuilder builder = getTableDefault().newReadBuilder().withProjection(new int[] {2});
        List<Split> splits = builder.newScan().plan().splits();
        assertThat(splits).hasSize(1);
        DataSplit split = (DataSplit) splits.get(0);
        assertThat(readVectorValues(builder.newRead().createReader(split)))
                .containsExactly(null, null, 200F, 200F);
        assertThat(
                        readVectorValues(
                                builder.newRead()
                                        .createReader(
                                                new IndexedSplit(
                                                        split,
                                                        Arrays.asList(
                                                                new Range(1, 1), new Range(3, 3)),
                                                        null))))
                .containsExactly(null, 200F);

        // Delete a row from both the missing and populated vector ranges.
        DeletionVector deletionVector = new BitmapDeletionVector();
        deletionVector.delete(0);
        deletionVector.delete(2);
        DeletionVectorsIndexFile indexFile =
                getTableDefault()
                        .store()
                        .newIndexFileHandler()
                        .dvIndex(split.partition(), split.bucket());
        String anchor = retrieveAnchorFile(split.dataFiles(), Function.identity()).fileName();
        Map<String, DeletionFile> deletionFiles =
                indexFile.toDeletionFiles(
                        Collections.singletonList(
                                indexFile.writeSingleFile(
                                        Collections.singletonMap(anchor, deletionVector))));
        DataSplit deletedSplit =
                DataSplit.builder()
                        .withPartition(split.partition())
                        .withBucket(split.bucket())
                        .withBucketPath(split.bucketPath())
                        .withDataFiles(split.dataFiles())
                        .withDataDeletionFiles(
                                split.dataFiles().stream()
                                        .map(file -> deletionFiles.get(file.fileName()))
                                        .collect(Collectors.toList()))
                        .build();
        assertThat(readVectorValues(builder.newRead().createReader(deletedSplit)))
                .containsExactly(null, 200F);
        assertThat(
                        readVectorValues(
                                builder.newRead()
                                        .createReader(
                                                new IndexedSplit(
                                                        deletedSplit,
                                                        Collections.singletonList(new Range(0, 2)),
                                                        null))))
                .containsExactly((Float) null);
    }

    @Test
    public void testPartialUpdateWithMissingVectorRangeWithoutDeletionVectors() throws Exception {
        // Same partial-vector layout as testPartialUpdateWithMissingVectorRange but without
        // deletion vectors: embedding_v2 is populated only for rows [2, 3], so projecting it must
        // still emit rows [0, 1] as NULL. Without the fix the anchor is dropped and the vector read
        // planner takes the sequential path, which rejects the mismatched row count.
        catalog.createTable(identifier(), vectorSchema("json").build(), false);
        write(getTableDefault(), GenericRow.of(0, vector(1)), GenericRow.of(1, vector(1)));
        catalog.alterTable(
                identifier(),
                Collections.singletonList(
                        SchemaChange.addColumn(
                                "embedding_v2", DataTypes.VECTOR(2, DataTypes.FLOAT()))),
                false);
        write(
                getTableDefault(),
                GenericRow.of(2, vector(1), vector(10)),
                GenericRow.of(3, vector(1), vector(10)));
        updateVectors(
                2,
                Collections.singletonList("embedding_v2"),
                GenericRow.of(vector(200)),
                GenericRow.of(vector(200)));
        compactVectorTable();

        ReadBuilder builder = getTableDefault().newReadBuilder().withProjection(new int[] {2});
        List<Split> splits = builder.newScan().plan().splits();
        assertThat(splits).hasSize(1);
        DataSplit split = (DataSplit) splits.get(0);
        assertThat(readVectorValues(builder.newRead().createReader(split)))
                .containsExactly(null, null, 200F, 200F);
    }

    private List<Float> readVectorValues(RecordReader<InternalRow> reader) throws IOException {
        List<Float> actual = new ArrayList<>();
        try (RecordReader<InternalRow> closeable = reader) {
            closeable.forEachRemaining(
                    row -> {
                        // Fail promptly if an all-missing projection produces unbounded NULL rows.
                        assertThat(actual.size()).isLessThan(4);
                        actual.add(row.isNullAt(0) ? null : row.getVector(0).toFloatArray()[0]);
                    });
        }
        return actual;
    }

    private Schema.Builder vectorSchema(String format) {
        return Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("embedding", DataTypes.VECTOR(2, DataTypes.FLOAT()))
                .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                .option(CoreOptions.VECTOR_FILE_FORMAT.key(), format)
                .option(CoreOptions.FILE_COMPRESSION.key(), "none")
                .option(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "2");
    }

    private static BinaryVector vector(float value) {
        return BinaryVector.fromPrimitiveArray(new float[] {value, 0});
    }

    private void updateVectors(long firstRowId, List<String> columns, InternalRow... rows)
            throws Exception {
        FileStoreTable table = getTableDefault();
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        long readSnapshotId = table.latestSnapshot().get().id();
        try (BatchTableWrite writer =
                        builder.newWrite().withWriteType(table.rowType().project(columns));
                BatchTableCommit commit = builder.newCommit()) {
            for (InternalRow row : rows) {
                writer.write(row);
            }
            List<CommitMessage> messages = writer.prepareCommit();
            setFirstRowId(messages, firstRowId);
            commit.commit(
                    messages.stream()
                            .map(
                                    message ->
                                            ((CommitMessageImpl) message)
                                                    .withCheckFromSnapshot(readSnapshotId))
                            .collect(Collectors.toList()));
        }
    }

    private void compactVectorTable() throws Exception {
        FileStoreTable table = getTableDefault();
        Snapshot snapshot = table.latestSnapshot().get();
        List<CommitMessage> messages = new ArrayList<>();
        for (DataEvolutionCompactTask task :
                new DataEvolutionCompactCoordinator(table, false, false, snapshot).plan()) {
            messages.add(task.doCompact(table, commitUser));
        }
        messages.addAll(
                new DataEvolutionCompactionCommitPreparation(table, snapshot).prepare(messages));
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(messages);
        }
        List<DataFileMeta> normalFiles =
                getTableDefault().store().newScan().plan().files().stream()
                        .map(ManifestEntry::file)
                        .filter(file -> !file.fileName().contains(".vector."))
                        .collect(Collectors.toList());
        assertThat(normalFiles).hasSize(1);
        assertThat(normalFiles.get(0).nonNullRowIdRange()).isEqualTo(new Range(0, 3));
    }

    @Test
    public void testBasic() throws Exception {
        int rowNum = RANDOM.nextInt(64) + 1;

        createTableDefault();

        commitDefault(writeDataDefault(rowNum, 1));

        AtomicInteger counter = new AtomicInteger(0);

        List<DataFileMeta> filesMetas =
                getTableDefault().store().newScan().plan().files().stream()
                        .map(ManifestEntry::file)
                        .collect(Collectors.toList());

        List<DataEvolutionSplitRead.FieldBunch> fieldGroups =
                DataEvolutionSplitRead.splitFieldBunches(
                        filesMetas, key -> makeBlobRowType(key.writeCols(), f -> 0));

        assertThat(fieldGroups.size()).isEqualTo(3);
        assertThat(fieldGroups.get(0).files().size()).isEqualTo(1);
        assertThat(fieldGroups.get(1).files().size()).isEqualTo(1);
        assertThat(fieldGroups.get(2).files().size()).isEqualTo(1);

        readDefault(
                row -> {
                    counter.getAndIncrement();
                    InternalRow expected = rowsWritten.get(row.getInt(0));
                    assertThat(row.getString(1)).isEqualTo(expected.getString(1));
                    assertThat(row.getBlob(2)).isEqualTo(expected.getBlob(2));
                    assertThat(row.getVector(3).toFloatArray())
                            .isEqualTo(expected.getVector(3).toFloatArray());
                    assertThat(row.getInt(4)).isEqualTo(expected.getInt(4));
                });

        assertThat(counter.get()).isEqualTo(rowNum);
    }

    @Test
    public void testOmitWriteColsForAllNonDedicatedColumns() throws Exception {
        catalog.createTable(identifier(), schemaDefault(true), true);
        commitDefault(writeDataDefault(1, 1));

        List<DataFileMeta> files =
                getTableDefault().store().newScan().plan().files().stream()
                        .map(ManifestEntry::file)
                        .collect(Collectors.toList());
        assertThat(
                        files.stream()
                                .filter(file -> !file.fileName().contains(".vector."))
                                .filter(file -> !file.fileName().endsWith(".blob"))
                                .findFirst()
                                .get()
                                .writeCols())
                .isNull();
        assertThat(
                        files.stream()
                                .filter(file -> file.fileName().endsWith(".blob"))
                                .findFirst()
                                .get()
                                .writeCols())
                .isEqualTo(Collections.singletonList("f2"));
        assertThat(
                        files.stream()
                                .filter(file -> file.fileName().contains(".vector."))
                                .findFirst()
                                .get()
                                .writeCols())
                .isEqualTo(Collections.singletonList("f3"));

        AtomicInteger count = new AtomicInteger();
        readDefault(row -> count.incrementAndGet());
        assertThat(count.get()).isEqualTo(1);
    }

    @Test
    public void testMultiBatch() throws Exception {
        int rowNum = (RANDOM.nextInt(64) + 1) * 2;

        createTableDefault();

        commitDefault(writeDataDefault(rowNum / 2, 2));

        AtomicInteger counter = new AtomicInteger(0);

        List<DataFileMeta> filesMetas =
                getTableDefault().store().newScan().plan().files().stream()
                        .map(ManifestEntry::file)
                        .collect(Collectors.toList());

        List<List<DataFileMeta>> batches = DataEvolutionSplitRead.mergeRangesAndSort(filesMetas);
        assertThat(batches.size()).isEqualTo(2);
        for (List<DataFileMeta> batch : batches) {
            List<DataEvolutionSplitRead.FieldBunch> fieldGroups =
                    DataEvolutionSplitRead.splitFieldBunches(
                            batch, file -> makeBlobRowType(file.writeCols(), f -> 0));
            assertThat(fieldGroups.size()).isEqualTo(3);
            assertThat(fieldGroups.get(0).files().size()).isEqualTo(1);
            assertThat(fieldGroups.get(1).files().size()).isEqualTo(1);
            assertThat(fieldGroups.get(2).files().size()).isEqualTo(1);
        }

        readDefault(
                row -> {
                    counter.getAndIncrement();
                    InternalRow expected = rowsWritten.get(row.getInt(0));
                    assertThat(row.getString(1)).isEqualTo(expected.getString(1));
                    assertThat(row.getBlob(2)).isEqualTo(expected.getBlob(2));
                    assertThat(row.getVector(3).toFloatArray())
                            .isEqualTo(expected.getVector(3).toFloatArray());
                    assertThat(row.getInt(4)).isEqualTo(expected.getInt(4));
                });
        assertThat(counter.get()).isEqualTo(rowNum);
    }

    @Test
    public void testRolling() throws Exception {
        // 100k vector-store data would create 1 normal, 1 blob, and 3 vector-store files
        int rowNum = 100 * 1000 * 3;

        createTableDefault();

        commitDefault(writeDataDefault(rowNum / 3, 3));

        AtomicInteger counter = new AtomicInteger(0);

        List<DataFileMeta> filesMetas =
                getTableDefault().store().newScan().plan().files().stream()
                        .map(ManifestEntry::file)
                        .collect(Collectors.toList());

        List<List<DataFileMeta>> batches = DataEvolutionSplitRead.mergeRangesAndSort(filesMetas);
        assertThat(batches.size()).isEqualTo(3);
        for (List<DataFileMeta> batch : batches) {
            List<DataEvolutionSplitRead.FieldBunch> fieldGroups =
                    DataEvolutionSplitRead.splitFieldBunches(
                            batch, file -> makeBlobRowType(file.writeCols(), f -> 0));
            assertThat(fieldGroups.size()).isEqualTo(3);
            assertThat(fieldGroups.get(0).files().size()).isEqualTo(1);
            assertThat(fieldGroups.get(1).files().size()).isEqualTo(1);
            assertThat(fieldGroups.get(2).files().size()).isEqualTo(3);
        }

        readDefault(
                row -> {
                    counter.getAndIncrement();
                    InternalRow expected = rowsWritten.get(row.getInt(0));
                    assertThat(row.getString(1)).isEqualTo(expected.getString(1));
                    assertThat(row.getBlob(2)).isEqualTo(expected.getBlob(2));
                    assertThat(row.getVector(3).toFloatArray())
                            .isEqualTo(expected.getVector(3).toFloatArray());
                    assertThat(row.getInt(4)).isEqualTo(expected.getInt(4));
                });

        assertThat(counter.get()).isEqualTo(rowNum);
    }

    @Test
    public void testWithoutBlob() throws Exception {
        // 100k vector-store data would create 1 normal, 1 blob, and 3 vector-store files
        int rowNum = 100 * 1000 * 3;

        catalog.createTable(identifier(), schemaWithoutBlob(), true);

        commitDefault(writeDataWithoutBlob(rowNum / 3, 3));

        AtomicInteger counter = new AtomicInteger(0);

        List<DataFileMeta> filesMetas =
                getTableDefault().store().newScan().plan().files().stream()
                        .map(ManifestEntry::file)
                        .collect(Collectors.toList());

        List<List<DataFileMeta>> batches = DataEvolutionSplitRead.mergeRangesAndSort(filesMetas);
        assertThat(batches.size()).isEqualTo(3);
        for (List<DataFileMeta> batch : batches) {
            List<DataEvolutionSplitRead.FieldBunch> fieldGroups =
                    DataEvolutionSplitRead.splitFieldBunches(
                            batch, file -> makeBlobRowType(file.writeCols(), f -> 0));
            assertThat(fieldGroups.size()).isEqualTo(2);
            assertThat(fieldGroups.get(0).files().size()).isEqualTo(1);
            assertThat(fieldGroups.get(1).files().size()).isEqualTo(3);
        }

        readDefault(
                row -> {
                    counter.getAndIncrement();
                    InternalRow expected = rowsWritten.get(row.getInt(0));
                    assertThat(row.getString(1)).isEqualTo(expected.getString(1));
                    assertThat(row.getVector(2).toFloatArray())
                            .isEqualTo(expected.getVector(3).toFloatArray());
                    assertThat(row.getInt(3)).isEqualTo(expected.getInt(4));
                });

        assertThat(counter.get()).isEqualTo(rowNum);
    }

    @Override
    protected Schema schemaDefault() {
        return schemaDefault(false);
    }

    private Schema schemaDefault(boolean optimizeWriteCols) {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.STRING());
        schemaBuilder.column("f2", DataTypes.BLOB());
        schemaBuilder.column("f3", DataTypes.VECTOR(VECTOR_DIM, DataTypes.FLOAT()));
        schemaBuilder.column("f4", DataTypes.INT());
        schemaBuilder.option(CoreOptions.TARGET_FILE_SIZE.key(), "2 MB");
        schemaBuilder.option(CoreOptions.VECTOR_TARGET_FILE_SIZE.key(), "4 MB");
        schemaBuilder.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.VECTOR_FIELD.key(), "f3");
        schemaBuilder.option(CoreOptions.VECTOR_FILE_FORMAT.key(), "json");
        schemaBuilder.option(CoreOptions.FILE_COMPRESSION.key(), "none");
        if (optimizeWriteCols) {
            schemaBuilder.option(
                    CoreOptions.DATA_EVOLUTION_WRITE_COLS_OPTIMIZATION_ENABLED.key(), "true");
        }
        return schemaBuilder.build();
    }

    private Schema schemaWithoutBlob() {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.STRING());
        schemaBuilder.column("f2", DataTypes.VECTOR(VECTOR_DIM, DataTypes.FLOAT()));
        schemaBuilder.column("f3", DataTypes.INT());
        schemaBuilder.option(CoreOptions.TARGET_FILE_SIZE.key(), "2 MB");
        schemaBuilder.option(CoreOptions.VECTOR_TARGET_FILE_SIZE.key(), "4 MB");
        schemaBuilder.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.VECTOR_FIELD.key(), "f2");
        schemaBuilder.option(CoreOptions.VECTOR_FILE_FORMAT.key(), "json");
        schemaBuilder.option(CoreOptions.FILE_COMPRESSION.key(), "none");
        return schemaBuilder.build();
    }

    protected List<CommitMessage> writeDataWithoutBlob(int size, int times) throws Exception {
        Table table = getTableDefault();
        List<CommitMessage> messages = new ArrayList<>();
        for (int time = 0; time < times; time++) {
            StreamWriteBuilder builder = table.newStreamWriteBuilder();
            builder.withCommitUser(commitUser);
            try (StreamTableWrite streamTableWrite = builder.newWrite()) {
                for (int j = 0; j < size; j++) {
                    InternalRow row = dataDefault(time, j);
                    streamTableWrite.write(
                            GenericRow.of(
                                    row.getInt(0),
                                    row.getString(1),
                                    row.getVector(3),
                                    row.getInt(4)));
                }
                messages.addAll(streamTableWrite.prepareCommit(false, Long.MAX_VALUE));
            }
        }
        return messages;
    }

    @Override
    protected InternalRow dataDefault(int time, int size) {
        byte[] stringBytes = new byte[1];
        RANDOM.nextBytes(stringBytes);
        byte[] blobBytes = new byte[1];
        RANDOM.nextBytes(blobBytes);
        byte[] vectorBytes = new byte[VECTOR_DIM];
        RANDOM.nextBytes(vectorBytes);
        float[] vector = new float[VECTOR_DIM];
        for (int i = 0; i < VECTOR_DIM; i++) {
            vector[i] = vectorBytes[i];
        }
        int id = uniqueIdGen.getAndIncrement();
        InternalRow row =
                GenericRow.of(
                        id,
                        BinaryString.fromBytes(stringBytes),
                        new BlobData(blobBytes),
                        BinaryVector.fromPrimitiveArray(vector),
                        RANDOM.nextInt(32) + 1);
        rowsWritten.put(id, row);
        return row;
    }

    private static RowType makeBlobRowType(
            List<String> fieldNames, Function<String, Integer> fieldIdFunc) {
        List<DataField> fields = new ArrayList<>();
        if (fieldNames == null) {
            fieldNames = Collections.emptyList();
        }
        for (String fieldName : fieldNames) {
            int fieldId = fieldIdFunc.apply(fieldName);
            DataField blobField = new DataField(fieldId, fieldName, DataTypes.BLOB());
            fields.add(blobField);
        }
        return new RowType(fields);
    }
}
