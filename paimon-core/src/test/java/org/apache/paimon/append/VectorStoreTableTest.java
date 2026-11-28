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
import org.apache.paimon.table.SpecialFields;
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
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.TableScan;
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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.DataEvolutionUtils.retrieveAnchorFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for table with vector-store and data evolution. */
public class VectorStoreTableTest extends DataEvolutionTestBase {

    private static final int VECTOR_DIM = 12;

    private final AtomicInteger uniqueIdGen = new AtomicInteger(0);
    private final Map<Integer, InternalRow> rowsWritten = new HashMap<>();

    @ParameterizedTest
    @ValueSource(strings = {"json", "parquet"})
    public void testVectorRowIdsAfterNormalOnlyFiles(String format) throws Exception {
        catalog.createTable(
                identifier(),
                vectorSchema(format).option(CoreOptions.TARGET_FILE_ROW_NUM.key(), "1").build(),
                false);
        FileStoreTable table = getTableDefault();
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        List<CommitMessage> messages = new ArrayList<>();
        try (BatchTableWrite normalWrite =
                        builder.newWrite()
                                .withWriteType(
                                        table.rowType().project(Collections.singletonList("id")));
                BatchTableWrite vectorWrite = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            normalWrite.write(GenericRow.of(0));
            normalWrite.write(GenericRow.of(1));
            vectorWrite.write(GenericRow.of(2, vector(22)));
            vectorWrite.write(GenericRow.of(3, vector(33)));
            messages.addAll(normalWrite.prepareCommit());
            messages.addAll(vectorWrite.prepareCommit());
            commit.commit(messages);
        }
        List<InternalRow> rows = read(getTableDefault());
        rows.sort(Comparator.comparingInt(row -> row.getInt(0)));
        assertThat(rows).extracting(row -> row.getInt(0)).containsExactly(0, 1, 2, 3);
        assertThat(rows)
                .extracting(row -> row.isNullAt(1) ? null : row.getVector(1).toFloatArray()[0])
                .containsExactly(null, null, 22F, 33F);
    }

    @Test
    public void testVectorFieldNameWithLeadingSpace() throws Exception {
        // Existing tables may name a vector field " embedding_v2" and list it as "embedding,
        // embedding_v2", so the exact name must win over its trimmed form.
        Schema schema =
                vectorSchema("json")
                        .column(" embedding_v2", DataTypes.VECTOR(2, DataTypes.FLOAT()))
                        .option(CoreOptions.VECTOR_FIELD.key(), "embedding, embedding_v2")
                        .build();
        catalog.createTable(identifier(), schema, false);
        write(getTableDefault(), GenericRow.of(0, vector(1), vector(2)));

        List<InternalRow> rows = read(getTableDefault());
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getVector(2).toFloatArray()[0]).isEqualTo(2F);
    }

    @Test
    public void testVectorFieldWithSpacesAfterAddingSimilarColumn() throws Exception {
        Schema schema =
                vectorSchema("json")
                        .column("embedding_v2", DataTypes.VECTOR(2, DataTypes.FLOAT()))
                        .option(CoreOptions.VECTOR_FIELD.key(), "embedding, embedding_v2")
                        .build();
        catalog.createTable(identifier(), schema, false);
        assertThat(getTableDefault().options())
                .containsEntry(CoreOptions.VECTOR_FIELD.key(), "embedding,embedding_v2");

        // A new string field named " embedding_v2" must not become one of the vector fields.
        catalog.alterTable(
                identifier(), SchemaChange.addColumn(" embedding_v2", DataTypes.STRING()), false);
        write(
                getTableDefault(),
                GenericRow.of(0, vector(1), vector(2), BinaryString.fromString("text")));

        List<InternalRow> rows = read(getTableDefault());
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getString(3).toString()).isEqualTo("text");
    }

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
        // still emit rows [0, 1] as NULL.
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

    @Test
    public void testNewerVectorUpdateOverPartOfTheColumn() throws Exception {
        // embedding is written for rows [0, 3], then updated for [0, 1] only. Together the two
        // files cover the group, but the newer one overlaps the older, so the sequential reader
        // would keep just [0, 1].
        catalog.createTable(identifier(), vectorSchema("json").build(), false);
        write(
                getTableDefault(),
                GenericRow.of(0, vector(1)),
                GenericRow.of(1, vector(1)),
                GenericRow.of(2, vector(1)),
                GenericRow.of(3, vector(1)));
        updateVectors(
                0,
                Collections.singletonList("embedding"),
                GenericRow.of(vector(100)),
                GenericRow.of(vector(100)));

        ReadBuilder builder = getTableDefault().newReadBuilder().withProjection(new int[] {1});
        assertThat(readVectorValues(builder.newRead().createReader(builder.newScan().plan())))
                .containsExactly(100F, 100F, 1F, 1F);
    }

    @Test
    public void testNewerVectorUpdateOverPartOfTheColumnWithDeletionVectors() throws Exception {
        catalog.createTable(
                identifier(),
                vectorSchema("json")
                        .option(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true")
                        .build(),
                false);
        write(
                getTableDefault(),
                GenericRow.of(0, vector(10)),
                GenericRow.of(1, vector(11)),
                GenericRow.of(2, vector(12)),
                GenericRow.of(3, vector(13)));
        updateVectors(
                0,
                Collections.singletonList("embedding"),
                GenericRow.of(vector(100)),
                GenericRow.of(vector(101)));

        ReadBuilder builder = getTableDefault().newReadBuilder().withProjection(new int[] {1});
        List<Split> splits = builder.newScan().plan().splits();
        assertThat(splits).hasSize(1);
        DataSplit split = (DataSplit) splits.get(0);

        // Delete one row from the updated range and one from the original range.
        DeletionVector deletionVector = new BitmapDeletionVector();
        deletionVector.delete(1);
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
                .containsExactly(100F, 13F);
    }

    @Test
    public void testOlderVectorUpdateNewerThanTheNextKeptFile() throws Exception {
        // [0, 9] s1, then [5, 9] s2, [2, 7] s3 and [0, 4] s4. The newest file and [5, 9] tile the
        // group, but rows [5, 7] must come from [2, 7], which is older than [0, 4] only.
        catalog.createTable(identifier(), vectorSchema("json").build(), false);
        InternalRow[] rows = new InternalRow[10];
        for (int i = 0; i < 10; i++) {
            rows[i] = GenericRow.of(i, vector(1));
        }
        write(getTableDefault(), rows);
        updateVectors(5, Collections.singletonList("embedding"), vectorRows(5, 2));
        updateVectors(2, Collections.singletonList("embedding"), vectorRows(6, 3));
        updateVectors(0, Collections.singletonList("embedding"), vectorRows(5, 4));

        ReadBuilder builder = getTableDefault().newReadBuilder().withProjection(new int[] {1});
        assertThat(readVectorValues(builder.newRead().createReader(builder.newScan().plan()), 10))
                .containsExactly(4F, 4F, 4F, 4F, 4F, 3F, 3F, 3F, 2F, 2F);
    }

    @Test
    public void testVectorColumnBackfilledByTwoUpdatesLeavesTailNull() throws Exception {
        // embedding_v2 is backfilled for [0, 4] and [5, 7] by two updates; [8, 9] stay NULL.
        catalog.createTable(identifier(), vectorSchema("json").build(), false);
        InternalRow[] rows = new InternalRow[10];
        for (int i = 0; i < 10; i++) {
            rows[i] = GenericRow.of(i, vector(1));
        }
        write(getTableDefault(), rows);
        catalog.alterTable(
                identifier(),
                Collections.singletonList(
                        SchemaChange.addColumn(
                                "embedding_v2", DataTypes.VECTOR(2, DataTypes.FLOAT()))),
                false);
        updateVectors(0, Collections.singletonList("embedding_v2"), vectorRows(5, 200));
        updateVectors(5, Collections.singletonList("embedding_v2"), vectorRows(3, 300));

        ReadBuilder builder = getTableDefault().newReadBuilder().withProjection(new int[] {2});
        assertThat(readVectorValues(builder.newRead().createReader(builder.newScan().plan()), 10))
                .containsExactly(200F, 200F, 200F, 200F, 200F, 300F, 300F, 300F, null, null);

        // [0, 4] has no selected row and is pruned by the scan; [8, 9] still needs NULL-filling.
        builder.withRowRanges(Collections.singletonList(new Range(6, 9)));
        assertThat(readVectorValues(builder.newRead().createReader(builder.newScan().plan())))
                .containsExactly(300F, 300F, null, null);

        // Every selected row is backfilled.
        builder.withRowRanges(Arrays.asList(new Range(1, 1), new Range(6, 6)));
        assertThat(readVectorValues(builder.newRead().createReader(builder.newScan().plan())))
                .containsExactly(200F, 300F);
    }

    @Test
    public void testVectorColumnWithAGapUnderRowRanges() throws Exception {
        // embedding_v2 is backfilled for [0, 2] and [6, 9]; rows [3, 5] stay NULL.
        catalog.createTable(identifier(), vectorSchema("json").build(), false);
        InternalRow[] rows = new InternalRow[10];
        for (int i = 0; i < 10; i++) {
            rows[i] = GenericRow.of(i, vector(1));
        }
        write(getTableDefault(), rows);
        catalog.alterTable(
                identifier(),
                Collections.singletonList(
                        SchemaChange.addColumn(
                                "embedding_v2", DataTypes.VECTOR(2, DataTypes.FLOAT()))),
                false);
        updateVectors(0, Collections.singletonList("embedding_v2"), vectorRows(3, 200));
        updateVectors(6, Collections.singletonList("embedding_v2"), vectorRows(4, 300));

        ReadBuilder builder = getTableDefault().newReadBuilder().withProjection(new int[] {0, 2});
        // No selected row is in the gap.
        builder.withRowRanges(Arrays.asList(new Range(1, 1), new Range(7, 7)));
        assertThat(readIdAndVector(builder)).containsExactly("1=200.0", "7=300.0");
        // Row 4 is selected and must be NULL-filled.
        builder.withRowRanges(Arrays.asList(new Range(1, 1), new Range(4, 4), new Range(7, 7)));
        assertThat(readIdAndVector(builder)).containsExactly("1=200.0", "4=null", "7=300.0");
    }

    @Test
    public void testReadWithoutAVectorColumnBackfilledFromALaterRow() throws Exception {
        // embedding_v2 is backfilled only for rows [2, 3]; reads that skip it must emit all rows.
        catalog.createTable(identifier(), vectorSchema("json").build(), false);
        InternalRow[] rows = new InternalRow[4];
        for (int i = 0; i < 4; i++) {
            rows[i] = GenericRow.of(i, vector(1));
        }
        write(getTableDefault(), rows);
        catalog.alterTable(
                identifier(),
                Collections.singletonList(
                        SchemaChange.addColumn(
                                "embedding_v2", DataTypes.VECTOR(2, DataTypes.FLOAT()))),
                false);
        updateVectors(2, Collections.singletonList("embedding_v2"), vectorRows(2, 200));

        // Splits planned for every column keep the embedding_v2 file.
        List<Split> splits = getTableDefault().newReadBuilder().newScan().plan().splits();
        ReadBuilder idOnly = getTableDefault().newReadBuilder().withProjection(new int[] {0});
        List<Integer> ids = new ArrayList<>();
        try (RecordReader<InternalRow> reader = idOnly.newRead().createReader(splits)) {
            reader.forEachRemaining(row -> ids.add(row.getInt(0)));
        }
        assertThat(ids).containsExactly(0, 1, 2, 3);

        // Read with embedding, the same splits keep embedding's vector file and drop
        // embedding_v2's.
        ReadBuilder idAndEmbedding =
                getTableDefault().newReadBuilder().withProjection(new int[] {0, 1});
        List<String> idsAndVectors = new ArrayList<>();
        try (RecordReader<InternalRow> reader = idAndEmbedding.newRead().createReader(splits)) {
            reader.forEachRemaining(
                    row ->
                            idsAndVectors.add(
                                    row.getInt(0) + "=" + row.getVector(1).toFloatArray()[0]));
        }
        assertThat(idsAndVectors).containsExactly("0=1.0", "1=1.0", "2=1.0", "3=1.0");

        // A row-id-only read type prunes no file in the scan.
        ReadBuilder rowIdOnly =
                getTableDefault().newReadBuilder().withReadType(RowType.of(SpecialFields.ROW_ID));
        assertThat(readRowIds(rowIdOnly)).containsExactly(0L, 1L, 2L, 3L);
    }

    @Test
    public void testReadOfARenamedVectorColumnNextToABackfilledOne() throws Exception {
        // Vector-store files are matched to the read type by field id, not by name.
        catalog.createTable(identifier(), vectorSchema("json").build(), false);
        InternalRow[] rows = new InternalRow[4];
        for (int i = 0; i < 4; i++) {
            rows[i] = GenericRow.of(i, vector(1));
        }
        write(getTableDefault(), rows);
        catalog.alterTable(
                identifier(),
                Collections.singletonList(
                        SchemaChange.addColumn(
                                "embedding_v2", DataTypes.VECTOR(2, DataTypes.FLOAT()))),
                false);
        updateVectors(2, Collections.singletonList("embedding_v2"), vectorRows(2, 200));
        catalog.alterTable(
                identifier(),
                Collections.singletonList(SchemaChange.renameColumn("embedding", "e")),
                false);

        List<Split> splits = getTableDefault().newReadBuilder().newScan().plan().splits();
        ReadBuilder idAndE = getTableDefault().newReadBuilder().withProjection(new int[] {0, 1});
        List<String> idsAndVectors = new ArrayList<>();
        try (RecordReader<InternalRow> reader = idAndE.newRead().createReader(splits)) {
            reader.forEachRemaining(
                    row ->
                            idsAndVectors.add(
                                    row.getInt(0)
                                            + "="
                                            + (row.isNullAt(1)
                                                    ? null
                                                    : row.getVector(1).toFloatArray()[0])));
        }
        assertThat(idsAndVectors).containsExactly("0=1.0", "1=1.0", "2=1.0", "3=1.0");
    }

    @Test
    public void testIncrementalRowIdReadOfVectorOnlyChanges() throws Exception {
        // Snapshots that only backfill vector columns give groups without a normal file.
        catalog.createTable(identifier(), vectorSchema("json").build(), false);
        InternalRow[] rows = new InternalRow[4];
        for (int i = 0; i < 4; i++) {
            rows[i] = GenericRow.of(i, vector(1));
        }
        write(getTableDefault(), rows);
        catalog.alterTable(
                identifier(),
                Collections.singletonList(
                        SchemaChange.addColumn(
                                "embedding_v2", DataTypes.VECTOR(2, DataTypes.FLOAT()))),
                false);
        long base = getTableDefault().latestSnapshot().get().id();
        updateVectors(2, Collections.singletonList("embedding_v2"), vectorRows(2, 200));
        updateVectors(2, Collections.singletonList("embedding"), vectorRows(2, 300));

        // Both vector columns cover rows [2, 3]; neither is read.
        assertThat(readRowIds(rowIdsBetween(base, base + 2))).containsExactly(2L, 3L);

        // embedding now covers rows [0, 3], embedding_v2 still only [2, 3].
        updateVectors(0, Collections.singletonList("embedding"), vectorRows(4, 400));
        assertThat(readRowIds(rowIdsBetween(base, base + 3))).containsExactly(0L, 1L, 2L, 3L);
    }

    @Test
    public void testIncrementalReadOfVectorOnlyChangesWithAPartialUnreadColumn() throws Exception {
        // embedding covers only rows [2, 3] and is never read; embedding_v2 covers [0, 3].
        catalog.createTable(identifier(), vectorSchema("json").build(), false);
        InternalRow[] rows = new InternalRow[4];
        for (int i = 0; i < 4; i++) {
            rows[i] = GenericRow.of(i, vector(1));
        }
        write(getTableDefault(), rows);
        catalog.alterTable(
                identifier(),
                Collections.singletonList(
                        SchemaChange.addColumn(
                                "embedding_v2", DataTypes.VECTOR(2, DataTypes.FLOAT()))),
                false);
        long base = getTableDefault().latestSnapshot().get().id();
        updateVectors(2, Collections.singletonList("embedding"), vectorRows(2, 300));
        updateVectors(0, Collections.singletonList("embedding_v2"), vectorRows(4, 400));
        List<Split> splits = rowIdsBetween(base, base + 2).newScan().plan().splits();

        // Pushed-down row ranges.
        ReadBuilder rowIdOnly =
                getTableDefault().newReadBuilder().withReadType(RowType.of(SpecialFields.ROW_ID));
        List<Long> rowIds = new ArrayList<>();
        for (Split split : splits) {
            IndexedSplit indexed =
                    new IndexedSplit(
                            (DataSplit) split, Collections.singletonList(new Range(0, 3)), null);
            try (RecordReader<InternalRow> reader = rowIdOnly.newRead().createReader(indexed)) {
                reader.forEachRemaining(row -> rowIds.add(row.getLong(0)));
            }
        }
        assertThat(rowIds).containsExactly(0L, 1L, 2L, 3L);

        // A read of embedding_v2.
        ReadBuilder v2Only = getTableDefault().newReadBuilder().withProjection(new int[] {2});
        assertThat(readVectorValues(v2Only.newRead().createReader(splits)))
                .containsExactly(400F, 400F, 400F, 400F);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testStreamingReadOfANormalColumnOverVectorOnlyChanges(boolean deletionVectors)
            throws Exception {
        // A snapshot that only backfills vector columns gives the delta scan a group without a
        // normal file.
        catalog.createTable(
                identifier(),
                vectorSchema("json")
                        .option(
                                CoreOptions.DELETION_VECTORS_ENABLED.key(),
                                String.valueOf(deletionVectors))
                        .build(),
                false);
        InternalRow[] rows = new InternalRow[4];
        for (int i = 0; i < 4; i++) {
            rows[i] = GenericRow.of(i, vector(1));
        }
        write(getTableDefault(), rows);
        catalog.alterTable(
                identifier(),
                Collections.singletonList(
                        SchemaChange.addColumn(
                                "embedding_v2", DataTypes.VECTOR(2, DataTypes.FLOAT()))),
                false);
        ReadBuilder builder =
                getTableDefault()
                        .newReadBuilder()
                        .withReadType(
                                RowType.of(
                                        getTableDefault().rowType().getField("id"),
                                        SpecialFields.ROW_ID));
        StreamTableScan scan = builder.newStreamScan();
        assertThat(readIdAndRowId(builder, scan.plan()))
                .containsExactly("0@0", "1@1", "2@2", "3@3");
        RowType v2ReadType =
                RowType.of(
                        getTableDefault().rowType().getField("id"),
                        getTableDefault().rowType().getField("embedding_v2"),
                        SpecialFields.ROW_ID);
        ReadBuilder v2Builder = getTableDefault().newReadBuilder().withReadType(v2ReadType);
        StreamTableScan v2Scan = v2Builder.newStreamScan();
        v2Scan.plan();

        // One commit backfills embedding_v2 for rows [2, 3] and rewrites embedding for [0, 3].
        List<CommitMessage> messages =
                new ArrayList<>(
                        writeVectors(
                                2, Collections.singletonList("embedding_v2"), vectorRows(2, 200)));
        messages.addAll(
                writeVectors(0, Collections.singletonList("embedding"), vectorRows(4, 300)));
        commitVectors(messages);

        // No changed file holds id, so id reads as NULL.
        assertThat(readIdAndRowId(builder, scan.plan()))
                .containsExactly("null@0", "null@1", "null@2", "null@3");

        // A read of embedding_v2 keeps only the file that holds it, as without deletion vectors.
        List<String> v2Rows = new ArrayList<>();
        try (RecordReader<InternalRow> reader = v2Builder.newRead().createReader(v2Scan.plan())) {
            reader.forEachRemaining(
                    row ->
                            v2Rows.add(
                                    (row.isNullAt(0) ? null : row.getInt(0))
                                            + "@"
                                            + (row.isNullAt(1)
                                                    ? null
                                                    : row.getVector(1).toFloatArray()[0])
                                            + "@"
                                            + row.getLong(2)));
        }
        assertThat(v2Rows).containsExactly("null@200.0@2", "null@200.0@3");
    }

    @Test
    public void testFileCreationTimeScanOfVectorOnlyChangesWithDeletionVectors() throws Exception {
        // A scan by file creation time can skip the normal file of a group, which deletion vectors
        // are looked up through. Reading a user column over the two vector files left fails at
        // planning.
        catalog.createTable(
                identifier(),
                vectorSchema("json")
                        .option(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true")
                        .build(),
                false);
        InternalRow[] rows = new InternalRow[4];
        for (int i = 0; i < 4; i++) {
            rows[i] = GenericRow.of(i, vector(1));
        }
        write(getTableDefault(), rows);
        // Start right after the first write, so the scan skips its files, the normal one too.
        long startMillis =
                getTableDefault().store().newScan().plan().files().stream()
                                .mapToLong(entry -> entry.file().creationTimeEpochMillis())
                                .max()
                                .getAsLong()
                        + 1;
        catalog.alterTable(
                identifier(),
                Collections.singletonList(
                        SchemaChange.addColumn(
                                "embedding_v2", DataTypes.VECTOR(2, DataTypes.FLOAT()))),
                false);
        List<CommitMessage> messages =
                new ArrayList<>(
                        writeVectors(
                                2, Collections.singletonList("embedding_v2"), vectorRows(2, 200)));
        messages.addAll(
                writeVectors(0, Collections.singletonList("embedding"), vectorRows(4, 300)));
        commitVectors(messages);
        assertThat(getTableDefault().store().newScan().plan().files())
                .filteredOn(entry -> entry.file().creationTimeEpochMillis() >= startMillis)
                .extracting(entry -> entry.file().fileName())
                .hasSize(2)
                .allMatch(name -> name.contains(".vector."));

        ReadBuilder builder =
                getTableDefault()
                        .copy(
                                Collections.singletonMap(
                                        CoreOptions.SCAN_FILE_CREATION_TIME_MILLIS.key(),
                                        String.valueOf(startMillis)))
                        .newReadBuilder()
                        .withProjection(new int[] {0});
        assertThatThrownBy(() -> builder.newScan().plan())
                .hasMessageContaining("must contain a normal");
    }

    @ParameterizedTest
    @ValueSource(strings = {"json", "parquet"})
    public void testCompactVectorFilesKeepsLatestColumnValues(String format) throws Exception {
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
        List<String> expected = readAllVectors(getTableDefault());
        assertThat(expected)
                .containsExactly("0|1.0|300.0", "1|1.0|300.0", "2|2.0|200.0", "3|2.0|200.0");
        assertThat(vectorFiles(getTableDefault())).hasSize(5);

        compactVectorFiles();

        List<DataFileMeta> vectorFiles = vectorFiles(getTableDefault());
        assertThat(vectorFiles).hasSize(1);
        assertThat(vectorFiles.get(0).nonNullRowIdRange()).isEqualTo(new Range(0, 3));
        assertThat(vectorFiles.get(0).writeCols()).containsExactly("embedding", "embedding_v2");
        assertThat(readAllVectors(getTableDefault())).isEqualTo(expected);
        assertThat(read(getTableDefault(), new int[] {2}))
                .extracting(row -> row.getVector(0).toFloatArray()[0])
                .containsExactly(300F, 300F, 200F, 200F);

        catalog.alterTable(
                identifier(),
                Collections.singletonList(
                        SchemaChange.renameColumn("embedding_v2", "query_embedding")),
                false);
        assertThat(readAllVectors(getTableDefault())).isEqualTo(expected);
    }

    @Test
    public void testCompactVectorFilesSkipsPartiallyPopulatedColumn() throws Exception {
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
        List<String> expected = readAllVectors(getTableDefault());
        assertThat(expected)
                .containsExactly("0|1.0|null", "1|1.0|null", "2|1.0|200.0", "3|1.0|200.0");

        compactVectorFiles();

        // embedding_v2 is missing for rows [0, 1], so the merged normal range keeps the vector
        // file of [0, 1] and only the two vector files of [2, 3] are compacted.
        List<DataFileMeta> vectorFiles = vectorFiles(getTableDefault());
        assertThat(vectorFiles)
                .extracting(DataFileMeta::nonNullRowIdRange)
                .containsExactlyInAnyOrder(new Range(0, 1), new Range(2, 3));
        assertThat(readAllVectors(getTableDefault())).isEqualTo(expected);
    }

    @Test
    public void testCompactVectorFilesIgnoresDroppedColumn() throws Exception {
        catalog.createTable(identifier(), vectorSchema("json").build(), false);
        write(getTableDefault(), GenericRow.of(0, vector(1)), GenericRow.of(1, vector(1)));
        write(getTableDefault(), GenericRow.of(2, vector(2)), GenericRow.of(3, vector(2)));
        catalog.alterTable(
                identifier(),
                Collections.singletonList(
                        SchemaChange.addColumn(
                                "embedding_v2", DataTypes.VECTOR(2, DataTypes.FLOAT()))),
                false);
        updateVectors(
                2,
                Collections.singletonList("embedding_v2"),
                GenericRow.of(vector(200)),
                GenericRow.of(vector(200)));
        catalog.alterTable(
                identifier(),
                Collections.singletonList(SchemaChange.dropColumn("embedding_v2")),
                false);
        List<String> expected = readAllVectors(getTableDefault());
        assertThat(expected).containsExactly("0|1.0", "1|1.0", "2|2.0", "3|2.0");

        compactVectorFiles();

        List<DataFileMeta> vectorFiles = vectorFiles(getTableDefault());
        assertThat(vectorFiles).hasSize(1);
        assertThat(vectorFiles.get(0).nonNullRowIdRange()).isEqualTo(new Range(0, 3));
        assertThat(vectorFiles.get(0).writeCols()).containsExactly("embedding");
        assertThat(readAllVectors(getTableDefault())).isEqualTo(expected);
    }

    @Test
    public void testCompactVectorFilesKeepsColumnSequencesWhenIgnoringIndexUpdates()
            throws Exception {
        catalog.createTable(
                identifier(),
                vectorSchema("json")
                        .column("embedding_v2", DataTypes.VECTOR(2, DataTypes.FLOAT()))
                        .option(CoreOptions.GLOBAL_INDEX_COLUMN_UPDATE_ACTION.key(), "IGNORE")
                        .build(),
                false);
        write(getTableDefault(), GenericRow.of(0, vector(1), vector(10)));
        write(getTableDefault(), GenericRow.of(1, vector(1), vector(10)));
        updateVectors(0, Collections.singletonList("embedding_v2"), GenericRow.of(vector(100)));
        long embeddingSequence =
                vectorFiles(getTableDefault()).stream()
                        .filter(file -> file.writeCols().contains("embedding"))
                        .mapToLong(DataFileMeta::maxSequenceNumber)
                        .max()
                        .getAsLong();
        long updateSequence =
                vectorFiles(getTableDefault()).stream()
                        .mapToLong(DataFileMeta::maxSequenceNumber)
                        .max()
                        .getAsLong();
        assertThat(embeddingSequence).isLessThan(updateSequence);
        List<String> expected = readAllVectors(getTableDefault());

        compactVectorFiles();

        List<DataFileMeta> vectorFiles = vectorFiles(getTableDefault());
        assertThat(vectorFiles).hasSize(1);
        assertThat(vectorFiles.get(0).writeCols()).containsExactly("embedding", "embedding_v2");
        assertThat(vectorFiles.get(0).maxSequenceNumber()).isEqualTo(updateSequence);
        assertThat(vectorFiles.get(0).writeColsSequences())
                .containsExactly(embeddingSequence, updateSequence);
        assertThat(readAllVectors(getTableDefault())).isEqualTo(expected);
    }

    @Test
    public void testCompactVectorFilesRollsByVectorTargetSize() throws Exception {
        catalog.createTable(identifier(), vectorSchema("parquet").build(), false);
        int rowsPerWrite = 1250;
        for (int write = 0; write < 2; write++) {
            InternalRow[] rows = new InternalRow[rowsPerWrite];
            for (int i = 0; i < rowsPerWrite; i++) {
                int id = write * rowsPerWrite + i;
                rows[i] = GenericRow.of(id, vector(id));
            }
            write(getTableDefault(), rows);
        }
        // Non-overlapping files are only rewritten when small; overlap them with an update.
        updateVectors(0, Collections.singletonList("embedding"), vectorRows(rowsPerWrite, 7));
        List<String> expected = readAllVectors(getTableDefault());
        catalog.alterTable(
                identifier(),
                Collections.singletonList(
                        SchemaChange.setOption(CoreOptions.VECTOR_TARGET_FILE_SIZE.key(), "1 b")),
                false);

        compactVectorFiles();

        // The rolling writer checks the target size every 1000 rows.
        List<DataFileMeta> vectorFiles = vectorFiles(getTableDefault());
        vectorFiles.sort(Comparator.comparingLong(DataFileMeta::nonNullFirstRowId));
        assertThat(vectorFiles)
                .extracting(DataFileMeta::nonNullRowIdRange)
                .containsExactly(new Range(0, 999), new Range(1000, 1999), new Range(2000, 2499));
        assertThat(readAllVectors(getTableDefault())).isEqualTo(expected);

        FileStoreTable table = getTableDefault();
        assertThat(
                        new DataEvolutionCompactCoordinator(
                                        table, false, true, table.latestSnapshot().get())
                                .plan())
                .isEmpty();
    }

    private List<String> readAllVectors(FileStoreTable table) throws Exception {
        List<String> result = new ArrayList<>();
        for (InternalRow row : read(table)) {
            StringBuilder builder = new StringBuilder().append(row.getInt(0));
            for (int i = 1; i < row.getFieldCount(); i++) {
                builder.append('|')
                        .append(row.isNullAt(i) ? null : row.getVector(i).toFloatArray()[0]);
            }
            result.add(builder.toString());
        }
        return result;
    }

    private static List<DataFileMeta> vectorFiles(FileStoreTable table) {
        return table.store().newScan().plan().files().stream()
                .map(ManifestEntry::file)
                .filter(file -> file.fileName().contains(".vector."))
                .collect(Collectors.toList());
    }

    private void compactVectorFiles() throws Exception {
        FileStoreTable table = getTableDefault();
        Snapshot snapshot = table.latestSnapshot().get();
        List<CommitMessage> messages = new ArrayList<>();
        for (DataEvolutionCompactTask task :
                new DataEvolutionCompactCoordinator(table, false, true, snapshot).plan()) {
            messages.add(task.doCompact(table, commitUser));
        }
        messages.addAll(
                new DataEvolutionCompactionCommitPreparation(table, snapshot).prepare(messages));
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(messages);
        }
    }

    private static List<String> readIdAndVector(ReadBuilder builder) throws IOException {
        List<String> actual = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                builder.newRead().createReader(builder.newScan().plan())) {
            reader.forEachRemaining(
                    row ->
                            actual.add(
                                    row.getInt(0)
                                            + "="
                                            + (row.isNullAt(1)
                                                    ? null
                                                    : row.getVector(1).toFloatArray()[0])));
        }
        return actual;
    }

    private ReadBuilder rowIdsBetween(long fromSnapshot, long toSnapshot) throws Exception {
        return getTableDefault()
                .copy(
                        Collections.singletonMap(
                                CoreOptions.INCREMENTAL_BETWEEN.key(),
                                fromSnapshot + "," + toSnapshot))
                .newReadBuilder()
                .withReadType(RowType.of(SpecialFields.ROW_ID));
    }

    private static List<String> readIdAndRowId(ReadBuilder builder, TableScan.Plan plan)
            throws IOException {
        List<String> actual = new ArrayList<>();
        try (RecordReader<InternalRow> reader = builder.newRead().createReader(plan)) {
            reader.forEachRemaining(
                    row ->
                            actual.add(
                                    (row.isNullAt(0) ? null : row.getInt(0))
                                            + "@"
                                            + row.getLong(1)));
        }
        return actual;
    }

    private static List<Long> readRowIds(ReadBuilder builder) throws IOException {
        List<Long> rowIds = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                builder.newRead().createReader(builder.newScan().plan())) {
            reader.forEachRemaining(row -> rowIds.add(row.getLong(0)));
        }
        return rowIds;
    }

    private static InternalRow[] vectorRows(int count, float value) {
        InternalRow[] rows = new InternalRow[count];
        for (int i = 0; i < count; i++) {
            rows[i] = GenericRow.of(vector(value));
        }
        return rows;
    }

    private List<Float> readVectorValues(RecordReader<InternalRow> reader) throws IOException {
        return readVectorValues(reader, 4);
    }

    private List<Float> readVectorValues(RecordReader<InternalRow> reader, int maxRows)
            throws IOException {
        List<Float> actual = new ArrayList<>();
        try (RecordReader<InternalRow> closeable = reader) {
            closeable.forEachRemaining(
                    row -> {
                        // Fail promptly if an all-missing projection produces unbounded NULL rows.
                        assertThat(actual.size()).isLessThan(maxRows);
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
        commitVectors(writeVectors(firstRowId, columns, rows));
    }

    private List<CommitMessage> writeVectors(
            long firstRowId, List<String> columns, InternalRow... rows) throws Exception {
        FileStoreTable table = getTableDefault();
        try (BatchTableWrite writer =
                table.newBatchWriteBuilder()
                        .newWrite()
                        .withWriteType(table.rowType().project(columns))) {
            for (InternalRow row : rows) {
                writer.write(row);
            }
            List<CommitMessage> messages = writer.prepareCommit();
            setFirstRowId(messages, firstRowId);
            return messages;
        }
    }

    private void commitVectors(List<CommitMessage> messages) throws Exception {
        FileStoreTable table = getTableDefault();
        long readSnapshotId = table.latestSnapshot().get().id();
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
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
