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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.append.ForceSingleBatchReader;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.variant.GenericVariant;
import org.apache.paimon.data.variant.VariantMetadataUtils;
import org.apache.paimon.deletionvectors.ApplyDeletionVectorReader;
import org.apache.paimon.deletionvectors.BitmapDeletionVector;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.deletionvectors.append.BaseAppendDeleteFileMaintainer;
import org.apache.paimon.fileindex.FileIndexFormat;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.fileindex.bitmap.ApplyBitmapIndexRecordReader;
import org.apache.paimon.fileindex.bitmap.BitmapFileIndexFactory;
import org.apache.paimon.fileindex.bloomfilter.BloomFilterFileIndexFactory;
import org.apache.paimon.fileindex.bsi.BitSliceIndexBitmapFileIndexFactory;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.globalindex.ScanResult;
import org.apache.paimon.globalindex.sorted.SortedGlobalIndexScanner;
import org.apache.paimon.globalindex.sorted.SortedGlobalIndexTestUtils;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.DataEvolutionFileReader;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.paimon.table.SpecialFields.rowTypeWithRowId;
import static org.apache.paimon.utils.DataEvolutionUtils.retrieveAnchorFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests file index and predicate push down in {@link
 * org.apache.paimon.operation.DataEvolutionSplitRead}, on two levels.
 *
 * <p>{@link #readWithFilter} creates the readers directly from the splits, without {@link
 * TableRead#executeFilter()}, so an empty result proves that the reader itself pruned the rows.
 *
 * <p>{@link #query} reads the whole plan through {@link TableRead#executeFilter()}, the residual
 * filter both Flink and Spark keep on top of the push down, so it returns the answer a query gives.
 * Those tests assert the rows themselves, and compare them with {@link #fullScanFiltered}: the push
 * down is an optimization and must never change the result. A row count alone can not say that, it
 * passes just as well when nothing is pushed down at all.
 *
 * <p>Filters on unprojected columns are also checked through {@link TableRead#executeFilter()},
 * which reads the filter operands before restoring the requested output projection.
 *
 * <p>Filter values are always picked inside the min/max range of the column, otherwise the group
 * level stats pruning in {@link org.apache.paimon.operation.DataEvolutionFileStoreScan} would drop
 * the split before the reader ever sees it.
 */
public class DataEvolutionFileIndexTest extends DataEvolutionTestBase {

    private static final int ROW_COUNT = 100;

    /** Inside the min/max range of f1 ("a000".."a099"), but not written. */
    private static final String MISSING_F1 = "a050x";

    /** Inside the min/max range of f2 ("b000".."b099"), but not written. */
    private static final String MISSING_F2 = "b050x";

    @ParameterizedTest
    @ValueSource(
            strings = {
                "file-bitmap-single",
                "file-bitmap-merged",
                "bitmap-single",
                "bitmap-merged",
                "btree-single",
                "btree-merged"
            })
    public void testVariantIndexQueryReadsRowSidecar(String mode) throws Exception {
        boolean merged = mode.endsWith("merged");
        boolean fileIndex = mode.startsWith("file-");
        String indexType = mode.substring(0, mode.indexOf('-'));
        Schema.Builder schemaBuilder =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("payload", DataTypes.VARIANT())
                        .column(
                                "nested",
                                DataTypes.ROW(
                                        DataTypes.FIELD(3, "label", DataTypes.STRING()),
                                        DataTypes.FIELD(4, "detail", DataTypes.VARIANT())))
                        .option(CoreOptions.FILE_FORMAT.key(), "parquet")
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ROW_SIDECAR_ENABLED.key(), "true")
                        .option(
                                CoreOptions.GLOBAL_INDEX_ENABLED.key(),
                                Boolean.toString(!fileIndex))
                        .option(CoreOptions.GLOBAL_INDEX_QUERY_IN_READER_ENABLED.key(), "false");
        Identifier identifier = identifier("variant_sidecar_" + mode);
        catalog.createTable(identifier, schemaBuilder.build(), false);
        FileStoreTable table = getTable(identifier);

        // A preceding group makes the selected global row ids differ from physical positions.
        write(table, variantSidecarRow(-2), variantSidecarRow(-1));
        RowType firstWriteType =
                merged ? table.rowType().project("id", "payload") : table.rowType();
        BatchWriteBuilder writeBuilder =
                table.copy(fileIndex ? bitmapOptions("id") : Collections.emptyMap())
                        .newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite().withWriteType(firstWriteType);
                BatchTableCommit commit = writeBuilder.newCommit()) {
            for (int i = 0; i < ROW_COUNT; i++) {
                GenericRow row = variantSidecarRow(i);
                write.write(merged ? GenericRow.of(i, row.getVariant(1)) : row);
            }
            commit.commit(write.prepareCommit());
        }
        if (merged) {
            table = getTable(identifier);
            writeBuilder = table.newBatchWriteBuilder();
            try (BatchTableWrite write =
                            writeBuilder
                                    .newWrite()
                                    .withWriteType(table.rowType().project("nested"));
                    BatchTableCommit commit = writeBuilder.newCommit()) {
                for (int i = 0; i < ROW_COUNT; i++) {
                    write.write(GenericRow.of(variantSidecarRow(i).getRow(2, 2)));
                }
                List<CommitMessage> messages = write.prepareCommit();
                setSingleFileFirstRowId(messages, 2L);
                commit.commit(messages);
            }
            table = getTable(identifier);
            writeBuilder = table.newBatchWriteBuilder();
            try (BatchTableWrite write =
                            writeBuilder
                                    .newWrite()
                                    .withWriteType(table.rowType().project("payload"));
                    BatchTableCommit commit = writeBuilder.newCommit()) {
                for (int i = 0; i < ROW_COUNT; i++) {
                    write.write(GenericRow.of(variantSidecarRow(i + 1000).getVariant(1)));
                }
                List<CommitMessage> messages = write.prepareCommit();
                setSingleFileFirstRowId(messages, 2L);
                commit.commit(messages);
            }
        }
        table = getTable(identifier);
        if (!fileIndex) {
            ScanResult<DataSplit> indexScan =
                    new SortedGlobalIndexScanner(table, indexType)
                            .withIndexFields(Collections.singletonList("id"))
                            .scan()
                            .get();
            List<CommitMessage> messages = new ArrayList<>();
            for (DataSplit split : indexScan.entries()) {
                messages.addAll(
                        SortedGlobalIndexTestUtils.buildIndex(
                                table, indexType, "id", split, indexScan.scanSnapshotId()));
            }
            try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
                commit.commit(messages);
            }
        }
        catalog.alterTable(
                identifier, SchemaChange.renameColumn("payload", "renamed_payload"), false);
        FileStoreTable latest = getTable(identifier);
        Predicate filter =
                new PredicateBuilder(latest.rowType()).in(0, Arrays.asList(50, 51, 52, 53, 54));
        RowType fullType =
                rowTypeWithRowId(latest.rowType())
                        .project("nested", "renamed_payload", "id", SpecialFields.ROW_ID.name());
        ReadBuilder fullRead = latest.newReadBuilder().withReadType(fullType).withFilter(filter);
        TableScan.Plan plan = fullRead.newScan().plan();
        assertThat(plan.splits()).hasSize(1);
        Split selected = plan.splits().get(0);
        if (!fileIndex) {
            assertThat(selected).isInstanceOf(IndexedSplit.class);
            assertThat(((IndexedSplit) selected).rowRanges()).containsExactly(new Range(52L, 56L));
        }
        DataSplit split =
                selected instanceof IndexedSplit
                        ? ((IndexedSplit) selected).dataSplit()
                        : (DataSplit) selected;
        assertThat(split.dataFiles()).hasSize(merged ? 3 : 1);
        DataFilePathFactory paths =
                latest.store()
                        .pathFactory()
                        .createDataFilePathFactory(split.partition(), split.bucket());
        for (DataFileMeta file : split.dataFiles()) {
            assertThat(file.fileName()).endsWith(".parquet");
            List<String> sidecars =
                    file.extraFiles().stream()
                            .filter(name -> name.endsWith(".row"))
                            .collect(Collectors.toList());
            assertThat(sidecars).hasSize(1);
            assertThat(latest.fileIO().exists(paths.toAlignedPath(sidecars.get(0), file))).isTrue();
            // Any main-file fallback or full scan now fails; only the sparse ROW path can work.
            assertThat(latest.fileIO().delete(paths.toPath(file), false)).isTrue();
        }
        InternalRowSerializer fullSerializer = new InternalRowSerializer(fullType);
        List<InternalRow> expected = new ArrayList<>();
        for (int i = 50; i <= 54; i++) {
            GenericRow row = variantSidecarRow(i);
            expected.add(
                    fullSerializer
                            .toBinaryRow(
                                    GenericRow.of(
                                            row.getRow(2, 2),
                                            merged
                                                    ? variantSidecarRow(i + 1000).getVariant(1)
                                                    : row.getVariant(1),
                                            i,
                                            i + 2L))
                            .copy());
        }
        assertThat(collect(fullRead.newRead().executeFilter(), plan, fullType))
                .containsExactlyInAnyOrderElementsOf(expected);

        RowType extraction =
                VariantMetadataUtils.VariantRowTypeBuilder.builder()
                        .field(DataTypes.INT(), "$.x", false, "UTC")
                        .field(DataTypes.VARIANT(), "$")
                        .build();
        RowType nestedType = (RowType) latest.rowType().getField("nested").type();
        RowType extractedType =
                DataTypes.ROW(
                        latest.rowType()
                                .getField("nested")
                                .newType(
                                        DataTypes.ROW(
                                                nestedType.getField("detail").newType(extraction))),
                        latest.rowType().getField("renamed_payload").newType(extraction),
                        latest.rowType().getField("id"));
        ReadBuilder extractedRead =
                latest.newReadBuilder().withReadType(extractedType).withFilter(filter);
        InternalRowSerializer extractedSerializer = new InternalRowSerializer(extractedType);
        expected.clear();
        for (int i = 50; i <= 54; i++) {
            GenericRow row = variantSidecarRow(i);
            InternalRow nested = row.getRow(2, 2);
            Object nestedValue = null;
            if (nested != null) {
                nestedValue =
                        GenericRow.of(
                                nested.isNullAt(1)
                                        ? null
                                        : GenericRow.of(
                                                i % 5 == 1 ? null : -i, nested.getVariant(1)));
            }
            expected.add(
                    extractedSerializer
                            .toBinaryRow(
                                    GenericRow.of(
                                            nestedValue,
                                            row.isNullAt(1)
                                                    ? null
                                                    : GenericRow.of(
                                                            i % 5 == 0
                                                                    ? i + (merged ? 1000 : 0)
                                                                    : null,
                                                            merged
                                                                    ? variantSidecarRow(i + 1000)
                                                                            .getVariant(1)
                                                                    : row.getVariant(1)),
                                            i))
                            .copy());
        }
        assertThat(
                        collect(
                                extractedRead.newRead().executeFilter(),
                                extractedRead.newScan().plan(),
                                extractedType))
                .containsExactlyInAnyOrderElementsOf(expected);
    }

    private static GenericRow variantSidecarRow(int id) {
        int kind = Math.floorMod(id, 5);
        GenericVariant payload =
                kind == 1
                        ? null
                        : GenericVariant.fromJson(
                                kind == 0
                                        ? "{\"x\":" + id + "}"
                                        : kind == 2 ? "null" : kind == 3 ? "\"text\"" : "[1,null]");
        GenericVariant detail =
                kind == 2
                        ? null
                        : GenericVariant.fromJson(kind == 1 ? "null" : "{\"x\":" + -id + "}");
        return GenericRow.of(
                id,
                payload,
                kind == 3 ? null : GenericRow.of(BinaryString.fromString("label"), detail));
    }

    @Test
    public void testBitmapFilterWithProjectedWriteSchemaAndRowSidecar() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.DATA_EVOLUTION_ROW_SIDECAR_ENABLED.key(), "true");
        FileStoreTable table = createTable("projected_schema_row_sidecar", options);
        writeSplitColumns(table, ROW_COUNT, Collections.emptyMap(), bitmapOptions("f2"));

        FileStoreTable latest = getTable(identifier(table.name()));
        RowType projection = latest.rowType().project("f2");
        Predicate filter = equalF2(f2(50));
        ReadBuilder readBuilder =
                latest.newReadBuilder().withReadType(projection).withFilter(filter);
        List<InternalRow> rows =
                collect(
                        readBuilder.newRead().executeFilter(),
                        readBuilder.newScan().plan(),
                        projection);

        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getString(0).toString()).isEqualTo(f2(50));

        ReadBuilder rangeReadBuilder =
                latest.newReadBuilder()
                        .withReadType(projection)
                        .withFilter(filter)
                        .withRowRanges(Collections.singletonList(new Range(0L, ROW_COUNT - 1L)));
        rows =
                collect(
                        rangeReadBuilder.newRead().executeFilter(),
                        rangeReadBuilder.newScan().plan(),
                        projection);
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getString(0).toString()).isEqualTo(f2(50));

        RowType rowIdType = rowTypeWithRowId(latest.rowType());
        RowType rowIdProjection = rowIdType.project(SpecialFields.ROW_ID.name(), "f2");
        PredicateBuilder rowIdBuilder = new PredicateBuilder(rowIdType);
        Predicate rowIdFilter =
                PredicateBuilder.and(
                        rowIdBuilder.between(3, 0L, ROW_COUNT - 1L),
                        rowIdBuilder.equal(2, BinaryString.fromString(f2(50))));
        ReadBuilder rowIdReadBuilder =
                latest.newReadBuilder().withReadType(rowIdProjection).withFilter(rowIdFilter);
        rows =
                collect(
                        rowIdReadBuilder.newRead().executeFilter(),
                        rowIdReadBuilder.newScan().plan(),
                        rowIdProjection);
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getLong(0)).isEqualTo(50L);
        assertThat(rows.get(0).getString(1).toString()).isEqualTo(f2(50));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testPositionalRowPreservesTrackingFieldsAfterSchemaCast(boolean ordinaryRowFormat)
            throws Exception {
        Map<String, String> options = bitmapOptions("f1");
        options.put(
                CoreOptions.DATA_EVOLUTION_ROW_SIDECAR_ENABLED.key(),
                Boolean.toString(!ordinaryRowFormat));
        if (ordinaryRowFormat) {
            options.put(CoreOptions.FILE_FORMAT.key(), "row");
        }
        FileStoreTable table = createTable("row_sidecar_tracking_cast", options);
        writeAllColumns(table, ROW_COUNT);
        catalog.alterTable(
                identifier("row_sidecar_tracking_cast"),
                SchemaChange.updateColumnType("f0", DataTypes.BIGINT()),
                false);

        FileStoreTable latest = getTable(identifier("row_sidecar_tracking_cast"));
        RowType readType =
                SpecialFields.rowTypeWithRowTracking(latest.rowType(), true, true)
                        .project(
                                "f0",
                                SpecialFields.ROW_ID.name(),
                                SpecialFields.SEQUENCE_NUMBER.name());
        Predicate filter =
                new PredicateBuilder(latest.rowType()).equal(1, BinaryString.fromString(f1(50)));

        ReadBuilder readBuilder = latest.newReadBuilder().withReadType(readType).withFilter(filter);
        TableScan.Plan plan = readBuilder.newScan().plan();
        long maxSequenceNumber =
                ((DataSplit) plan.splits().get(0)).dataFiles().get(0).maxSequenceNumber();
        List<InternalRow> rows = collect(readBuilder.newRead().executeFilter(), plan, readType);
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getLong(0)).isEqualTo(50L);
        assertThat(rows.get(0).getLong(1)).isEqualTo(50L);
        assertThat(rows.get(0).isNullAt(2)).isFalse();
        assertThat(rows.get(0).getLong(2)).isEqualTo(maxSequenceNumber);

        ReadBuilder rangeReadBuilder =
                latest.newReadBuilder()
                        .withReadType(readType)
                        .withFilter(filter)
                        .withRowRanges(Collections.singletonList(new Range(0L, ROW_COUNT - 1L)));
        rows =
                collect(
                        rangeReadBuilder.newRead().executeFilter(),
                        rangeReadBuilder.newScan().plan(),
                        readType);
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getLong(0)).isEqualTo(50L);
        assertThat(rows.get(0).getLong(1)).isEqualTo(50L);
        assertThat(rows.get(0).isNullAt(2)).isFalse();
        assertThat(rows.get(0).getLong(2)).isEqualTo(maxSequenceNumber);

        RowType trackedType = SpecialFields.rowTypeWithRowTracking(latest.rowType(), true, true);
        PredicateBuilder trackedBuilder = new PredicateBuilder(trackedType);
        Predicate rowIdFilter =
                PredicateBuilder.and(
                        filter,
                        trackedBuilder.between(
                                trackedType.getFieldIndex(SpecialFields.ROW_ID.name()),
                                0L,
                                ROW_COUNT - 1L));
        ReadBuilder rowIdReadBuilder =
                latest.newReadBuilder().withReadType(readType).withFilter(rowIdFilter);
        rows =
                collect(
                        rowIdReadBuilder.newRead().executeFilter(),
                        rowIdReadBuilder.newScan().plan(),
                        readType);
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getLong(0)).isEqualTo(50L);
        assertThat(rows.get(0).getLong(1)).isEqualTo(50L);
        assertThat(rows.get(0).getLong(2)).isEqualTo(maxSequenceNumber);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testRowSidecarRestoresMissingPartitionField(boolean merged) throws Exception {
        Schema.Builder schemaBuilder =
                Schema.newBuilder()
                        .column("pt", DataTypes.STRING())
                        .column("f1", DataTypes.STRING())
                        .column("f2", DataTypes.STRING())
                        .partitionKeys("pt")
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ROW_SIDECAR_ENABLED.key(), "true");
        Identifier identifier = identifier("row_sidecar_partition_restore");
        catalog.createTable(identifier, schemaBuilder.build(), false);
        FileStoreTable table = getTable(identifier);

        RowType writeType = table.rowType().project("f1");
        BatchWriteBuilder writeBuilder = table.copy(bitmapOptions("f1")).newBatchWriteBuilder();
        List<InternalRow> writtenRows = new ArrayList<>();
        for (int i = 0; i < ROW_COUNT; i++) {
            writtenRows.add(GenericRow.of(BinaryString.fromString(f1(i))));
        }
        try (BatchTableWrite write = writeBuilder.newWrite().withWriteType(writeType);
                BatchTableCommit commit = writeBuilder.newCommit()) {
            write.writeBundle(
                    new InternalRowSerializer(table.schema().logicalPartitionType())
                            .toBinaryRow(GenericRow.of(BinaryString.fromString("p1"))),
                    0,
                    new BundleRecords() {
                        @Override
                        public long rowCount() {
                            return writtenRows.size();
                        }

                        @Override
                        public Iterator<InternalRow> iterator() {
                            return writtenRows.iterator();
                        }
                    });
            commit.commit(write.prepareCommit());
        }

        if (merged) {
            BatchWriteBuilder mergeWriteBuilder = table.newBatchWriteBuilder();
            try (BatchTableWrite write =
                            mergeWriteBuilder
                                    .newWrite()
                                    .withWriteType(table.rowType().project("f2"));
                    BatchTableCommit commit = mergeWriteBuilder.newCommit()) {
                write.writeBundle(
                        new InternalRowSerializer(table.schema().logicalPartitionType())
                                .toBinaryRow(GenericRow.of(BinaryString.fromString("p1"))),
                        0,
                        new BundleRecords() {
                            @Override
                            public long rowCount() {
                                return ROW_COUNT;
                            }

                            @Override
                            public Iterator<InternalRow> iterator() {
                                return writtenRows.stream()
                                        .map(row -> (InternalRow) GenericRow.of(row.getString(0)))
                                        .iterator();
                            }
                        });
                List<CommitMessage> messages = write.prepareCommit();
                setFirstRowId(messages, 0L);
                commit.commit(messages);
            }
        }

        FileStoreTable latest = getTable(identifier);
        RowType readType =
                latest.rowType()
                        .project(
                                merged
                                        ? new String[] {"f2", "pt", "f1"}
                                        : new String[] {"pt", "f1"});
        Predicate filter =
                new PredicateBuilder(latest.rowType()).equal(1, BinaryString.fromString(f1(50)));
        for (List<Range> ranges : Arrays.<List<Range>>asList(null, fullRowRanges())) {
            ReadBuilder readBuilder =
                    latest.newReadBuilder().withReadType(readType).withFilter(filter);
            if (ranges != null) {
                readBuilder.withRowRanges(ranges);
            }
            List<InternalRow> rows =
                    collect(
                            readBuilder.newRead().executeFilter(),
                            readBuilder.newScan().plan(),
                            readType);

            InternalRow row = assertSingleRow(rows);
            assertThat(row.getString(readType.getFieldIndex("pt")).toString()).isEqualTo("p1");
            assertThat(row.getString(readType.getFieldIndex("f1")).toString()).isEqualTo(f1(50));
            if (merged) {
                assertThat(row.getString(0).toString()).isEqualTo(f1(50));
            }
        }

        RowType partitionOnly = latest.rowType().project("pt");
        ReadBuilder partitionReadBuilder = latest.newReadBuilder().withReadType(partitionOnly);
        List<InternalRow> partitions =
                collect(
                        partitionReadBuilder.newRead(),
                        partitionReadBuilder.newScan().plan(),
                        partitionOnly);
        assertThat(partitions).hasSize(ROW_COUNT);
        assertThat(partitions)
                .allSatisfy(row -> assertThat(row.getString(0).toString()).isEqualTo("p1"));
    }

    // Deleting the data files proves the rows come from the row sidecars: without them the read
    // would fail on the missing files.
    @Test
    public void testMergedGroupFinalSelectionChoosesRowSidecar() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        options.put(CoreOptions.DATA_EVOLUTION_ROW_SIDECAR_ENABLED.key(), "true");
        FileStoreTable table = createTable("merged_bitmap_dv_row_sidecar", options);
        writeSplitColumns(table, ROW_COUNT, bitmapOptions("f1"), Collections.emptyMap());
        deleteRows(table, 51, 52, 53, 54, 55);

        Predicate filter = equalAnyF1(50, 51, 52, 53, 54, 55);
        assertRow(assertSingleRow(readAfterDeletingDataFiles(table, filter, null)), 50);
    }

    @Test
    public void testMergedGroupResolvesRowSidecarOncePerFile() throws Exception {
        RowSidecarStatusCountingFileIO fileIO = new RowSidecarStatusCountingFileIO();
        catalog =
                new FileSystemCatalog(
                        fileIO,
                        new Path(tempPath.toUri().toString(), "counting-row-sidecar-status"));
        catalog.createDatabase(database, true);

        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.DATA_EVOLUTION_ROW_SIDECAR_ENABLED.key(), "true");
        FileStoreTable table = createTable("merged_row_sidecar_status", options);
        writeSplitColumns(table, ROW_COUNT, bitmapOptions("f1"), Collections.emptyMap());

        fileIO.rowSidecarStatusReads.clear();
        assertRow(assertSingleRow(readWithFilter(table, equalAnyF1(50))), 50);
        assertThat(fileIO.rowSidecarStatusReads).hasSize(2);
        assertThat(fileIO.rowSidecarStatusReads.values()).containsOnly(1);
    }

    @Test
    public void testMissingFieldsReaderResolvesRowSidecarOnce() throws Exception {
        RowSidecarStatusCountingFileIO fileIO = new RowSidecarStatusCountingFileIO();
        catalog =
                new FileSystemCatalog(
                        fileIO,
                        new Path(tempPath.toUri().toString(), "counting-missing-field-status"));
        catalog.createDatabase(database, true);

        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.DATA_EVOLUTION_ROW_SIDECAR_ENABLED.key(), "true");
        FileStoreTable table = createTable("missing_field_row_sidecar_status", options);
        writeSplitColumns(table, ROW_COUNT, Collections.emptyMap(), Collections.emptyMap());
        catalog.alterTable(
                identifier(table.name()), SchemaChange.addColumn("f3", DataTypes.STRING()), false);

        FileStoreTable latest = getTable(identifier(table.name()));
        RowType readType = latest.rowType().project("f3");
        ReadBuilder readBuilder =
                latest.newReadBuilder()
                        .withReadType(readType)
                        .withRowRanges(Collections.singletonList(new Range(50L, 50L)));
        fileIO.rowSidecarStatusReads.clear();
        List<InternalRow> rows =
                collect(readBuilder.newRead(), readBuilder.newScan().plan(), readType);

        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).isNullAt(0)).isTrue();
        assertThat(fileIO.rowSidecarStatusReads).hasSize(1);
        assertThat(fileIO.rowSidecarStatusReads.values()).containsOnly(1);
    }

    @Test
    public void testSingleFileFinalSelectionChoosesRowSidecar() throws Exception {
        Map<String, String> options = bitmapOptions("f1");
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        options.put(CoreOptions.DATA_EVOLUTION_ROW_SIDECAR_ENABLED.key(), "true");
        FileStoreTable table = createTable("single_bitmap_dv_row_sidecar", options);
        writeAllColumns(table, ROW_COUNT);
        deleteRows(table, 51, 52, 53, 54, 55);

        Predicate filter = equalAnyF1(50, 51, 52, 53, 54, 55);
        assertRow(assertSingleRow(readAfterDeletingDataFiles(table, filter, null)), 50);
    }

    @Test
    public void testMergedGroupFinalSelectionChoosesRowSidecarWithRowRanges() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        options.put(CoreOptions.DATA_EVOLUTION_ROW_SIDECAR_ENABLED.key(), "true");
        FileStoreTable table = createTable("merged_bitmap_dv_row_sidecar_ranges", options);
        writeSplitColumns(table, ROW_COUNT, bitmapOptions("f1"), Collections.emptyMap());
        deleteRows(table, 51, 52, 53, 54, 55);

        Predicate filter = equalAnyF1(50, 51, 52, 53, 54, 55);
        assertRow(assertSingleRow(readAfterDeletingDataFiles(table, filter, fullRowRanges())), 50);
    }

    @Test
    public void testSingleFileFinalSelectionChoosesRowSidecarWithRowRanges() throws Exception {
        Map<String, String> options = bitmapOptions("f1");
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        options.put(CoreOptions.DATA_EVOLUTION_ROW_SIDECAR_ENABLED.key(), "true");
        FileStoreTable table = createTable("single_bitmap_dv_row_sidecar_ranges", options);
        writeAllColumns(table, ROW_COUNT);
        deleteRows(table, 51, 52, 53, 54, 55);

        Predicate filter = equalAnyF1(50, 51, 52, 53, 54, 55);
        assertRow(assertSingleRow(readAfterDeletingDataFiles(table, filter, fullRowRanges())), 50);
    }

    private static List<Range> fullRowRanges() {
        return Collections.singletonList(new Range(0L, ROW_COUNT - 1L));
    }

    // The f0/f1 file is read from its row sidecar (its data file is deleted), the f2 file has no
    // sidecar and is read from its data file.
    @Test
    public void testMergedGroupMixedSidecarAndDataFileRead() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        options.put(CoreOptions.DATA_EVOLUTION_ROW_SIDECAR_ENABLED.key(), "true");
        FileStoreTable table = createTable("merged_mixed_sidecar_data_file", options);
        Map<String, String> withoutSidecar = new HashMap<>();
        withoutSidecar.put(CoreOptions.DATA_EVOLUTION_ROW_SIDECAR_ENABLED.key(), "false");
        writeSplitColumns(table, ROW_COUNT, bitmapOptions("f1"), withoutSidecar);
        deleteRows(table, 51, 52, 53, 54, 55);

        Predicate filter = equalAnyF1(50, 51, 52, 53, 54, 55);
        assertRow(
                assertSingleRow(
                        readAfterDeletingDataFiles(
                                table,
                                filter,
                                null,
                                file ->
                                        file.writeCols() != null
                                                && file.writeCols().contains("f1"))),
                50);
    }

    // The first file is read from its row sidecar (its data file is deleted), the second file has
    // no sidecar and is read from its data file.
    @Test
    public void testSingleFileMixedSidecarAndDataFileRead() throws Exception {
        Map<String, String> options = bitmapOptions("f1");
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        options.put(CoreOptions.DATA_EVOLUTION_ROW_SIDECAR_ENABLED.key(), "true");
        FileStoreTable table = createTable("single_mixed_sidecar_data_file", options);
        writeAllColumns(table, ROW_COUNT);
        Map<String, String> withoutSidecar = new HashMap<>(bitmapOptions("f1"));
        withoutSidecar.put(CoreOptions.DATA_EVOLUTION_ROW_SIDECAR_ENABLED.key(), "false");
        writeAllColumns(getTable(identifier(table.name())).copy(withoutSidecar), ROW_COUNT);
        // deleting the overlapping rows leaves the first file with a single-row bitmap selection,
        // sparse enough to be read from its sidecar, while the second file has no sidecar at all
        deleteRows(table, 51, 52, 53, 54, 55);

        Predicate filter = equalAnyF1(50, 51, 52, 53, 54, 55);
        List<InternalRow> rows =
                readAfterDeletingDataFiles(
                        table,
                        filter,
                        null,
                        file -> file.firstRowId() != null && file.firstRowId() == 0L);
        // row 50 of the first file comes from its sidecar, rows 50-55 of the second file come
        // from its data file
        assertThat(rows).hasSize(7);
        for (InternalRow row : rows) {
            int index = row.getInt(0);
            assertThat(index).isBetween(50, 55);
            assertThat(row.getString(1).toString()).isEqualTo(f1(index));
            assertThat(row.getString(2).toString()).isEqualTo(f2(index));
        }

        // the same mixed read with row tracking: the row id of the sidecar row comes from the
        // manifest entry, the row ids of the data file rows from its own range
        RowType readType =
                rowTypeWithRowId(getTable(identifier(table.name())).rowType())
                        .project(SpecialFields.ROW_ID.name(), "f0", "f1");
        List<InternalRow> trackedRows = readWithFilter(table, filter, readType);
        assertThat(trackedRows).hasSize(7);
        assertThat(trackedRows.stream().map(row -> row.getLong(0)))
                .containsExactlyInAnyOrder(50L, 150L, 151L, 152L, 153L, 154L, 155L);
    }

    // Ordinary row-format data files use the same physical schema mapping as row sidecars. The
    // row format has no format-level filter pushdown, so the bitmap index provides the selection.
    @Test
    public void testOrdinaryRowFormatDataFile() throws Exception {
        Map<String, String> options = bitmapOptions("f1");
        options.put(CoreOptions.FILE_FORMAT.key(), "row");
        FileStoreTable table = createTable("ordinary_row_format_file", options);
        writeAllColumns(table, ROW_COUNT);

        assertRow(assertSingleRow(readWithFilter(table, equalF1(f1(50)), null)), 50);

        RowType readType =
                rowTypeWithRowId(table.rowType()).project(SpecialFields.ROW_ID.name(), "f1");
        List<InternalRow> rows = readWithFilter(table, equalF1(f1(50)), readType);
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getLong(0)).isEqualTo(50L);
        assertThat(rows.get(0).getString(1).toString()).isEqualTo(f1(50));
    }

    @Test
    public void testFormatReaderCacheKeyIncludesPhysicalSchema() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.DATA_EVOLUTION_ROW_SIDECAR_ENABLED.key(), "true");
        FileStoreTable table = createTable("row_sidecar_physical_schema_key", options);
        writeSplitColumns(table, ROW_COUNT, Collections.emptyMap(), bitmapOptions("f2"));
        writeSecondSplitColumns(table, ROW_COUNT, bitmapOptions("f2"));

        FileStoreTable latest = getTable(identifier(table.name()));
        RowType projection = latest.rowType().project(Arrays.asList("f1", "f2"));
        List<InternalRow> rows = readWithFilter(table, equalF2(f2(50)), projection);

        assertThat(rows).hasSize(2);
        assertThat(rows)
                .extracting(row -> row.getString(0).toString())
                .containsExactlyInAnyOrder(f1(50), "second-" + f1(50));
        assertThat(rows).extracting(row -> row.getString(1).toString()).containsOnly(f2(50));
    }

    @Test
    public void testBitmapFilterWithWideWriteSchemaAndRowSidecar() throws Exception {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        for (int i = 0; i < 7; i++) {
            schemaBuilder.column("c" + i, DataTypes.INT());
        }
        schemaBuilder
                .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                .option(CoreOptions.DATA_EVOLUTION_ROW_SIDECAR_ENABLED.key(), "true");
        bitmapOptions("c6").forEach(schemaBuilder::option);
        Identifier identifier = identifier("wide_schema_row_sidecar");
        catalog.createTable(identifier, schemaBuilder.build(), false);
        FileStoreTable table = getTable(identifier);

        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            for (int i = 0; i < ROW_COUNT; i++) {
                write.write(GenericRow.of(i, i, i, i, i, i, i));
            }
            commit.commit(write.prepareCommit());
        }

        Predicate filter = new PredicateBuilder(table.rowType()).equal(6, 50);
        ReadBuilder readBuilder = table.newReadBuilder().withFilter(filter);
        List<InternalRow> rows =
                collect(
                        readBuilder.newRead().executeFilter(),
                        readBuilder.newScan().plan(),
                        table.rowType());

        InternalRow row = assertSingleRow(rows);
        for (int i = 0; i < 7; i++) {
            assertThat(row.getInt(i)).isEqualTo(50);
        }

        ReadBuilder rangeReadBuilder =
                table.newReadBuilder()
                        .withFilter(filter)
                        .withRowRanges(Collections.singletonList(new Range(0L, ROW_COUNT - 1L)));
        rows =
                collect(
                        rangeReadBuilder.newRead().executeFilter(),
                        rangeReadBuilder.newScan().plan(),
                        table.rowType());
        row = assertSingleRow(rows);
        for (int i = 0; i < 7; i++) {
            assertThat(row.getInt(i)).isEqualTo(50);
        }

        RowType rowIdType = rowTypeWithRowId(table.rowType());
        PredicateBuilder rowIdBuilder = new PredicateBuilder(rowIdType);
        Predicate rowIdFilter =
                PredicateBuilder.and(
                        rowIdBuilder.between(7, 0L, ROW_COUNT - 1L), rowIdBuilder.equal(6, 50));
        ReadBuilder rowIdReadBuilder =
                table.newReadBuilder().withReadType(rowIdType).withFilter(rowIdFilter);
        rows =
                collect(
                        rowIdReadBuilder.newRead().executeFilter(),
                        rowIdReadBuilder.newScan().plan(),
                        rowIdType);
        row = assertSingleRow(rows);
        for (int i = 0; i < 7; i++) {
            assertThat(row.getInt(i)).isEqualTo(50);
        }
        assertThat(row.getLong(7)).isEqualTo(50L);
    }

    @Test
    public void testSingleFileSkippedByFileIndex() throws Exception {
        // a standalone .index file, only the reader can evaluate it
        FileStoreTable table = createTable("single_file", bloomOptions("f1", "1 B"));
        writeAllColumns(table, ROW_COUNT);

        assertThat(readWithFilter(table, equalF1(MISSING_F1))).isEmpty();

        List<InternalRow> hit = readWithFilter(table, equalF1(f1(50)));
        assertThat(hit).isNotEmpty();
        assertThat(hit).anyMatch(row -> row.getInt(0) == 50);
        assertAligned(hit);
    }

    @Test
    public void testQuerySingleFileWithFileIndex() throws Exception {
        FileStoreTable table = createTable("single_file_query", bloomOptions("f1", "1 B"));
        writeAllColumns(table, ROW_COUNT);

        Predicate filter = equalF1(f1(50));
        List<InternalRow> rows = query(table, filter);
        // the push down is an optimization, it must not change the answer
        assertThat(rows).containsExactlyInAnyOrderElementsOf(fullScanFiltered(table, filter));
        assertRow(assertSingleRow(rows), 50);

        assertThat(query(table, equalF1(MISSING_F1))).isEmpty();
    }

    @Test
    public void testMergedGroupSkippedByFileIndex() throws Exception {
        // an embedded index, the default threshold keeps it in the manifest
        FileStoreTable table = createTable("merged_group", Collections.emptyMap());
        writeSplitColumns(table, ROW_COUNT, Collections.emptyMap(), bloomOptions("f2", null));
        assertMergedGroup(table);

        assertThat(readWithFilter(table, equalF2(MISSING_F2))).isEmpty();
    }

    @Test
    public void testMergedGroupKeepsColumnsAligned() throws Exception {
        FileStoreTable table = createTable("merged_aligned", Collections.emptyMap());
        writeSplitColumns(table, ROW_COUNT, Collections.emptyMap(), bloomOptions("f2", null));
        assertMergedGroup(table);

        // A non-bitmap index cannot select rows, but the merged fields must stay aligned.
        List<InternalRow> rows = readWithFilter(table, equalF2(f2(50)));
        assertThat(rows).hasSize(ROW_COUNT);
        assertAligned(rows);
    }

    @Test
    public void testMergedGroupBitmapIndexSelectsMatchingRowsOnly() throws Exception {
        FileStoreTable table = createTable("merged_bitmap", Collections.emptyMap());
        writeSplitColumns(table, ROW_COUNT, Collections.emptyMap(), bitmapOptions("f2"));
        assertMergedGroup(table);

        List<InternalRow> rows = readWithFilter(table, equalF2(f2(50)));
        assertThat(rows).hasSize(1);
        assertRow(rows.get(0), 50);
    }

    @Test
    public void testMergedGroupBitmapIndexIntersectsFieldSelections() throws Exception {
        FileStoreTable table = createTable("merged_bitmap_intersect", Collections.emptyMap());
        writeSplitColumns(table, ROW_COUNT, bitmapOptions("f1"), bitmapOptions("f2"));
        assertMergedGroup(table);

        Predicate filter = PredicateBuilder.and(equalF1(f1(50)), equalF2(f2(50)));
        List<InternalRow> rows = readWithFilter(table, filter);
        assertThat(rows).hasSize(1);
        assertRow(rows.get(0), 50);
    }

    @Test
    public void testMergedGroupBitmapIndexPreservesOrSelection() throws Exception {
        FileStoreTable table = createTable("merged_bitmap_or", Collections.emptyMap());
        writeSplitColumns(table, ROW_COUNT, Collections.emptyMap(), bitmapOptions("f2"));
        assertMergedGroup(table);

        Predicate filter = PredicateBuilder.or(equalF2(f2(50)), equalF2(f2(51)));
        List<InternalRow> rows = readWithFilter(table, filter);
        assertThat(rows).extracting(row -> row.getInt(0)).containsExactlyInAnyOrder(50, 51);
        assertAligned(rows);
    }

    @Test
    public void testQueryMergedGroup() throws Exception {
        FileStoreTable table = createTable("merged_group_query", Collections.emptyMap());
        writeSplitColumns(table, ROW_COUNT, Collections.emptyMap(), bloomOptions("f2", null));
        assertMergedGroup(table);

        // the group is read whole, the residual filter has to cut it down to the matching row
        Predicate filter = equalF2(f2(50));
        List<InternalRow> rows = query(table, filter);
        assertThat(rows).containsExactlyInAnyOrderElementsOf(fullScanFiltered(table, filter));
        assertRow(assertSingleRow(rows), 50);

        assertThat(query(table, equalF2(MISSING_F2))).isEmpty();
    }

    @Test
    public void testBitmapIndexSelectsMatchingRowsOnly() throws Exception {
        FileStoreTable table = createTable("bitmap", bitmapOptions("f1"));
        writeAllColumns(table, ROW_COUNT);

        // a bitmap index is exact, the reader returns the matching row and nothing else
        List<InternalRow> rows = readWithFilter(table, equalF1(f1(50)));
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getInt(0)).isEqualTo(50);
        assertAligned(rows);

        assertThat(readWithFilter(table, equalF1(MISSING_F1))).isEmpty();
    }

    @Test
    public void testQueryWithBitmapIndex() throws Exception {
        FileStoreTable table = createTable("bitmap_query", bitmapOptions("f1"));
        writeAllColumns(table, ROW_COUNT);

        Predicate filter = equalF1(f1(50));
        List<InternalRow> rows = query(table, filter);
        assertThat(rows).containsExactlyInAnyOrderElementsOf(fullScanFiltered(table, filter));
        assertRow(assertSingleRow(rows), 50);

        assertThat(query(table, equalF1(MISSING_F1))).isEmpty();
    }

    @Test
    public void testMergedGroupSkippedAfterColumnRename() throws Exception {
        FileStoreTable table = createTable("renamed", Collections.emptyMap());
        writeSplitColumns(table, ROW_COUNT, Collections.emptyMap(), bloomOptions("f2", null));
        catalog.alterTable(identifier("renamed"), SchemaChange.renameColumn("f2", "f3"), false);

        // the index of the file was written under the old column name, the filter has to be
        // devolved by field id before it can be evaluated
        FileStoreTable renamed = getTable(identifier("renamed"));
        PredicateBuilder builder = new PredicateBuilder(renamed.rowType());
        int f3 = renamed.rowType().getFieldIndex("f3");

        assertThat(readWithFilter(renamed, builder.equal(f3, BinaryString.fromString(MISSING_F2))))
                .isEmpty();
        assertThat(readWithFilter(renamed, builder.equal(f3, BinaryString.fromString(f2(50)))))
                .hasSize(ROW_COUNT);
    }

    @Test
    public void testQueryAfterColumnRename() throws Exception {
        FileStoreTable table = createTable("renamed_query", Collections.emptyMap());
        writeSplitColumns(table, ROW_COUNT, Collections.emptyMap(), bloomOptions("f2", null));
        catalog.alterTable(
                identifier("renamed_query"), SchemaChange.renameColumn("f2", "f3"), false);

        FileStoreTable renamed = getTable(identifier("renamed_query"));
        PredicateBuilder builder = new PredicateBuilder(renamed.rowType());
        int f3 = renamed.rowType().getFieldIndex("f3");

        Predicate filter = builder.equal(f3, BinaryString.fromString(f2(50)));
        List<InternalRow> rows = query(renamed, filter);
        assertThat(rows).containsExactlyInAnyOrderElementsOf(fullScanFiltered(renamed, filter));
        assertRow(assertSingleRow(rows), 50);

        assertThat(query(renamed, builder.equal(f3, BinaryString.fromString(MISSING_F2))))
                .isEmpty();
    }

    @Test
    public void testFileIndexReadDisabled() throws Exception {
        Map<String, String> options = bloomOptions("f1", "1 B");
        options.put(CoreOptions.FILE_INDEX_READ_ENABLED.key(), "false");
        FileStoreTable table = createTable("index_disabled", options);
        writeAllColumns(table, ROW_COUNT);

        assertThat(readWithFilter(table, equalF1(MISSING_F1))).hasSize(ROW_COUNT);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    public void testIgnoreUnknownFileIndexInSingleFile(int version) throws Exception {
        Map<String, String> options = bloomOptions("f1", "1 B");
        options.put(CoreOptions.FILE_INDEX_FORMAT_VERSION.key(), Integer.toString(version));
        FileStoreTable table = createTable("unknown_single_file", options);
        writeAllColumns(table, ROW_COUNT);

        DataSplit split = (DataSplit) table.newReadBuilder().newScan().plan().splits().get(0);
        assertThat(split.dataFiles()).hasSize(1);
        DataFileMeta file = split.dataFiles().get(0);
        DataFilePathFactory pathFactory =
                table.store()
                        .pathFactory()
                        .createDataFilePathFactory(split.partition(), split.bucket());
        List<String> indexes =
                file.extraFiles().stream()
                        .filter(name -> name.endsWith(DataFilePathFactory.INDEX_PATH_SUFFIX))
                        .collect(Collectors.toList());
        assertThat(indexes).hasSize(1);
        // Replace the sidecar with an index produced by a plugin unavailable to this reader.
        try (OutputStream out =
                        table.fileIO()
                                .newOutputStream(
                                        pathFactory.toAlignedPath(indexes.get(0), file), true);
                FileIndexFormat.Writer writer = FileIndexFormat.createWriter(out, version)) {
            writer.writeIndex("f1", "unknown", payload -> payload.write(42));
            writer.finish();
        }

        assertThatThrownBy(() -> readWithFilter(table, equalF1(MISSING_F1)))
                .hasMessage("Can't find file index for type: unknown");
        catalog.alterTable(
                identifier(table.name()),
                SchemaChange.setOption(
                        CoreOptions.FILE_INDEX_READ_IGNORE_UNKNOWN_TYPE.key(), "true"),
                false);
        assertThat(readWithFilter(table, equalF1(MISSING_F1))).hasSize(ROW_COUNT);
        assertThat(query(table, equalF1(MISSING_F1))).isEmpty();
        assertRow(assertSingleRow(query(table, equalF1(f1(50)))), 50);
    }

    @Test
    public void testQueryWithFileIndexReadDisabled() throws Exception {
        Map<String, String> options = bloomOptions("f1", "1 B");
        options.put(CoreOptions.FILE_INDEX_READ_ENABLED.key(), "false");
        FileStoreTable table = createTable("index_disabled_query", options);
        writeAllColumns(table, ROW_COUNT);

        // the index only decides how much is read, never what the query answers
        Predicate filter = equalF1(f1(50));
        List<InternalRow> rows = query(table, filter);
        assertThat(rows).containsExactlyInAnyOrderElementsOf(fullScanFiltered(table, filter));
        assertRow(assertSingleRow(rows), 50);

        assertThat(query(table, equalF1(MISSING_F1))).isEmpty();
    }

    @Test
    public void testRowTrackingFilterIsNotPushedDown() throws Exception {
        FileStoreTable table = createTable("row_tracking", Collections.emptyMap());
        writeSplitColumns(table, ROW_COUNT, bloomOptions("f1", "1 B"), Collections.emptyMap());

        // _ROW_ID is assigned from the manifest entry, a filter on it must not reach the file
        // index or the format reader, and must not break the filter devolution either, which
        // resolves predicate fields against the table schema and knows nothing about system fields
        PredicateBuilder builder = new PredicateBuilder(rowTypeWithRowId(rowType()));
        Predicate predicate =
                PredicateBuilder.and(
                        builder.between(3, 0L, (long) ROW_COUNT),
                        builder.equal(1, BinaryString.fromString(f1(50))));

        List<InternalRow> rows = readWithFilter(table, predicate);
        assertThat(rows).hasSize(ROW_COUNT);
        assertAligned(rows);
    }

    @ParameterizedTest
    @ValueSource(strings = {"parquet", "orc", "avro"})
    public void testFilterOnColumnMissingFromFileIsNotApplied(String format) throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.FILE_FORMAT.key(), format);
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "false");
        FileStoreTable table = createTable("missing_column_" + format, options);
        writeSplitColumns(table, ROW_COUNT, Collections.emptyMap(), Collections.emptyMap());

        // projecting f2 leaves only the file that does not contain f1, so the filter reaches a
        // format reader that knows nothing about the column and must not drop anything
        List<InternalRow> rows = readWithFilter(table, equalF1(f1(50)), rowType().project("f2"));
        assertThat(rows).hasSize(ROW_COUNT);
    }

    @ParameterizedTest
    @ValueSource(strings = {"parquet", "orc", "avro"})
    public void testReadOnlyRowIdWithFilterOnDataColumn(String format) throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.FILE_FORMAT.key(), format);
        FileStoreTable table = createTable("row_id_only_" + format, options);
        writeAllColumns(table, ROW_COUNT);

        // the read type holds no data column at all, so the filter can not be pushed to the
        // format: a format only evaluates a predicate over the columns it was asked to read and
        // treats the others as absent, which drops every row instead of none
        List<InternalRow> rows =
                readWithFilter(table, equalF1(f1(50)), RowType.of(SpecialFields.ROW_ID));
        assertThat(rowIds(rows)).containsExactlyInAnyOrderElementsOf(rowIds(0, ROW_COUNT));
    }

    @Test
    public void testProjectionWithoutFilterColumn() throws Exception {
        FileStoreTable table = createTable("projection_without_filter", Collections.emptyMap());
        writeAllColumns(table, ROW_COUNT);

        // f1 is filtered but not read, nothing proves anything about the rows, all of them stay
        List<InternalRow> rows = readWithFilter(table, equalF1(f1(50)), rowType().project("f0"));
        assertThat(rows).hasSize(ROW_COUNT);
        assertThat(rows).anyMatch(row -> row.getInt(0) == 50);
    }

    @Test
    public void testExecuteFilterWithUnprojectedOverwrittenColumn() throws Exception {
        FileStoreTable table = createTable("execute_filter_projection", Collections.emptyMap());
        writeThenOverwriteF1(table, ROW_COUNT);
        FileStoreTable latest = getTable(identifier(table.name()));
        RowType outputType = rowType().project("f0");
        ReadBuilder readBuilder =
                latest.newReadBuilder().withReadType(outputType).withFilter(equalF1(c1(50)));
        List<InternalRow> rows =
                collect(
                        readBuilder.newRead().executeFilter(),
                        readBuilder.newScan().plan(),
                        outputType);
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getInt(0)).isEqualTo(50);

        readBuilder = latest.newReadBuilder().withReadType(outputType).withFilter(equalF1(f1(50)));
        assertThat(
                        collect(
                                readBuilder.newRead().executeFilter(),
                                readBuilder.newScan().plan(),
                                outputType))
                .isEmpty();
    }

    @Test
    public void testFileIndexIsGivenUpForAColumnOutsideTheReadType() throws Exception {
        FileStoreTable table = createTable("projection_index", bloomOptions("f1", "1 B"));
        writeAllColumns(table, ROW_COUNT);

        // The split reader only evaluates filters over its read type. Without executeFilter,
        // f1 remains outside that type even though planning retains its files.
        assertThat(readWithFilter(table, equalF1(MISSING_F1), rowType().project("f0")))
                .hasSize(ROW_COUNT);

        // reading f1 puts it back in reach
        assertThat(readWithFilter(table, equalF1(MISSING_F1), rowType().project("f0", "f1")))
                .isEmpty();
    }

    @Test
    public void testBitmapSelectionReturnsExactRowId() throws Exception {
        FileStoreTable table = createTable("bitmap_row_id", bitmapOptions("f1"));
        writeAllColumns(table, ROW_COUNT);
        // a second row id range, so a row id can not be confused with a position inside a file
        writeAllColumns(table, ROW_COUNT);

        // the bitmap is exact, and the row id of what it selects comes from the position the row
        // has in its file, not from its position in the batch the selection left over
        RowType readType = rowTypeWithRowId(rowType()).project(SpecialFields.ROW_ID.name(), "f1");
        List<InternalRow> rows = readWithFilter(table, equalF1(f1(50)), readType);
        assertThat(rowIds(rows)).containsExactlyInAnyOrder(50L, ROW_COUNT + 50L);
    }

    @Test
    public void testUnprojectedOverwrittenFilterColumnWithoutExecution() throws Exception {
        FileStoreTable table = createTable("pruned_winner", Collections.emptyMap());
        writeThenOverwriteF1(table, ROW_COUNT);

        // f1 was rewritten as c* by a second file. Without executeFilter the reader still only
        // reads f0, so an unprojected filter must not reject the matching row.
        List<InternalRow> rows = readWithFilter(table, equalF1(c1(50)), rowType().project("f0"));
        assertThat(rows).anyMatch(row -> row.getInt(0) == 50);
    }

    @Test
    public void testEmptyReadTypeWithFilter() throws Exception {
        FileStoreTable table = createTable("empty_read_type", Collections.emptyMap());
        writeAllColumns(table, ROW_COUNT);

        // a count(*) read projects nothing, so nothing can be pushed to the format either
        assertThat(countWithFilter(table, equalF1(f1(50)), RowType.of())).isEqualTo(ROW_COUNT);
    }

    @Test
    public void testReaderMappingIsNotSharedBetweenReadTypes() throws Exception {
        FileStoreTable table = createTable("read_type_reuse", Collections.emptyMap());
        writeAllColumns(table, ROW_COUNT);

        FileStoreTable latest = getTable(identifier(table.name()));
        TableScan.Plan plan = latest.newReadBuilder().newScan().plan();
        InnerTableRead read = latest.newRead();

        // a read can be reconfigured after it created readers, see AppendTableRead#applyReadType,
        // so a reader mapping built for one read type must not answer for another
        RowType first = rowType().project("f1");
        read.withReadType(first);
        assertThat(collect(read, plan, first).get(0).getString(0).toString()).isEqualTo(f1(0));

        RowType second = rowType().project("f2");
        read.withReadType(second);
        assertThat(collect(read, plan, second).get(0).getString(0).toString()).isEqualTo(f2(0));
    }

    @Test
    public void testReaderMappingIsNotSharedBetweenFilters() throws Exception {
        FileStoreTable table = createTable("filter_reuse", Collections.emptyMap());
        writeAllColumns(table, ROW_COUNT);

        FileStoreTable latest = getTable(identifier(table.name()));
        // planned without a filter, so only the readers can drop anything here
        TableScan.Plan plan = latest.newReadBuilder().newScan().plan();
        InnerTableRead read = latest.newRead();

        // outside the min/max of f1, the format reader prunes the whole file
        read.withFilter(equalF1("z999"));
        assertThat(collect(read, plan, latest.rowType())).isEmpty();

        // the mapping above carries that filter, the second read must not inherit it
        read.withFilter(equalF1(f1(50)));
        assertThat(collect(read, plan, latest.rowType())).isNotEmpty();
    }

    @Test
    public void testBitmapSelectionComposesWithDeletionVector() throws Exception {
        Map<String, String> options = bitmapOptions("f1");
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        FileStoreTable table = createTable("bitmap_dv", options);
        writeAllColumns(table, ROW_COUNT);

        // both the bitmap selection and the deletion vector filter by position, so deleting the
        // very row the bitmap selects has to make it disappear
        deleteRows(table, 50);
        assertThat(readWithFilter(table, equalF1(f1(50)))).isEmpty();

        // deleting a neighbour must leave the selected row untouched
        FileStoreTable other = createTable("bitmap_dv_other", options);
        writeAllColumns(other, ROW_COUNT);
        deleteRows(other, 51);
        List<InternalRow> rows = readWithFilter(other, equalF1(f1(50)));
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getInt(0)).isEqualTo(50);
    }

    @Test
    public void testQueryComposesBitmapWithDeletionVector() throws Exception {
        Map<String, String> options = bitmapOptions("f1");
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        FileStoreTable table = createTable("bitmap_dv_query", options);
        writeAllColumns(table, ROW_COUNT);
        deleteRows(table, 50);

        assertThat(query(table, equalF1(f1(50)))).isEmpty();

        FileStoreTable other = createTable("bitmap_dv_other_query", options);
        writeAllColumns(other, ROW_COUNT);
        deleteRows(other, 51);

        Predicate filter = equalF1(f1(50));
        List<InternalRow> rows = query(other, filter);
        assertThat(rows).containsExactlyInAnyOrderElementsOf(fullScanFiltered(other, filter));
        assertRow(assertSingleRow(rows), 50);
    }

    @Test
    public void testBitmapSelectionComposesWithDeletionVectorForNonZeroRowId() throws Exception {
        Map<String, String> options = bitmapOptions("f1");
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        FileStoreTable table = createTable("bitmap_dv_non_zero_row_id", options);
        writeAllColumns(table, ROW_COUNT);
        writeAllColumns(table, ROW_COUNT);

        deleteRowsFrom(table, ROW_COUNT, 50);

        FileStoreTable latest = getTable(identifier(table.name()));
        DataSplit targetSplit =
                latest.newReadBuilder().newScan().plan().splits().stream()
                        .map(split -> (DataSplit) split)
                        .filter(
                                split ->
                                        split.dataFiles().stream()
                                                .anyMatch(
                                                        file ->
                                                                file.nonNullFirstRowId()
                                                                        == ROW_COUNT))
                        .findFirst()
                        .orElseThrow(IllegalStateException::new);
        DataFileMeta targetFile =
                targetSplit.dataFiles().stream()
                        .filter(file -> file.nonNullFirstRowId() == ROW_COUNT)
                        .findFirst()
                        .orElseThrow(IllegalStateException::new);
        Path targetPath =
                latest.store()
                        .pathFactory()
                        .createDataFilePathFactory(targetSplit.partition(), targetSplit.bucket())
                        .toPath(targetFile);
        assertThat(latest.fileIO().delete(targetPath, false)).isTrue();

        // The second group can only succeed now if the bitmap hit is intersected with its DV
        // before opening the missing data file. The first group remains visible.
        RowType readType = rowTypeWithRowId(rowType()).project(SpecialFields.ROW_ID.name(), "f1");
        List<InternalRow> rows = readWithFilter(table, equalF1(f1(50)), readType);
        assertThat(rowIds(rows)).containsExactly(50L);
    }

    @Test
    public void testSingleFileIndexSkipsBeforeReadingDeletionVector() throws Exception {
        Map<String, String> options = bitmapOptions("f1");
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        FileStoreTable table = createTable("single_bitmap_before_dv", options);
        writeAllColumns(table, ROW_COUNT);
        deleteRows(table, 50);

        FileStoreTable latest = getTable(identifier(table.name()));
        DataSplit split = (DataSplit) latest.newReadBuilder().newScan().plan().splits().get(0);
        Path deletionVectorPath =
                split.deletionFiles().get().stream()
                        .filter(Objects::nonNull)
                        .map(file -> new Path(file.path()))
                        .findFirst()
                        .orElseThrow(IllegalStateException::new);
        assertThat(latest.fileIO().delete(deletionVectorPath, false)).isTrue();

        // The bitmap index already rejects this value, so the missing DV file must not be read.
        assertThat(readWithFilter(table, equalF1(MISSING_F1))).isEmpty();
    }

    @Test
    public void testSingleFileEmptySelectionSkipsBeforeReadingDeletionVector() throws Exception {
        Map<String, String> options = bitmapOptions("f1");
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        FileStoreTable table = createTable("single_empty_selection_before_dv", options);
        writeAllColumns(table, ROW_COUNT);
        deleteRows(table, 75);

        FileStoreTable latest = getTable(identifier(table.name()));
        DataSplit split = (DataSplit) latest.newReadBuilder().newScan().plan().splits().get(0);
        Path deletionVectorPath =
                split.deletionFiles().get().stream()
                        .filter(Objects::nonNull)
                        .map(file -> new Path(file.path()))
                        .findFirst()
                        .orElseThrow(IllegalStateException::new);
        assertThat(latest.fileIO().delete(deletionVectorPath, false)).isTrue();

        ReadBuilder readBuilder =
                latest.newReadBuilder()
                        .withFilter(equalF1(f1(50)))
                        .withRowRanges(Collections.singletonList(new Range(0L, 49L)));
        TableScan.Plan plan = readBuilder.newScan().plan();
        assertThat(plan.splits()).hasSize(1);
        assertThat(collect(readBuilder.newRead().executeFilter(), plan, latest.rowType()))
                .isEmpty();
    }

    @Test
    public void testMergedGroupKeptWhenFilterColumnOverwritten() throws Exception {
        FileStoreTable table = createTable("overwritten", Collections.emptyMap());
        writeThenOverwriteF1(table, ROW_COUNT);

        // f1 was rewritten by a newer file (c*); the old file still holds a* and a bloom index that
        // knows nothing about c*, so its index must not veto the group
        Predicate filter = equalF1(c1(50));
        List<InternalRow> rows = query(table, filter);
        assertThat(rows).containsExactlyInAnyOrderElementsOf(fullScanFiltered(table, filter));

        InternalRow row = assertSingleRow(rows);
        assertThat(row.getInt(0)).isEqualTo(50);
        assertThat(row.getString(1).toString()).isEqualTo(c1(50));
        // no file of the group wrote f2
        assertThat(row.isNullAt(2)).isTrue();

        // and the overwritten value must be gone, the column merge has to take f1 from the new file
        assertThat(query(table, equalF1(f1(50)))).isEmpty();
    }

    @Test
    public void testMergedGroupFileIndexComposesWithDeletionVector() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        FileStoreTable table = createTable("merged_bitmap_dv", options);
        writeSplitColumns(table, ROW_COUNT, bitmapOptions("f1"), Collections.emptyMap());
        writeSplitColumns(table, ROW_COUNT, bitmapOptions("f1"), Collections.emptyMap());

        deleteRowsFrom(table, ROW_COUNT, 50);

        FileStoreTable latest = getTable(identifier(table.name()));
        DataSplit targetSplit =
                latest.newReadBuilder().newScan().plan().splits().stream()
                        .map(split -> (DataSplit) split)
                        .filter(
                                split ->
                                        split.dataFiles().stream()
                                                .anyMatch(
                                                        file ->
                                                                file.nonNullFirstRowId()
                                                                        == ROW_COUNT))
                        .findFirst()
                        .orElseThrow(IllegalStateException::new);
        DataFileMeta anchor =
                retrieveAnchorFile(
                        targetSplit.dataFiles().stream()
                                .filter(file -> file.nonNullFirstRowId() == ROW_COUNT)
                                .collect(Collectors.toList()),
                        file -> file);
        Path anchorPath =
                latest.store()
                        .pathFactory()
                        .createDataFilePathFactory(targetSplit.partition(), targetSplit.bucket())
                        .toPath(anchor);
        assertThat(latest.fileIO().delete(anchorPath, false)).isTrue();

        // The deleted row is the only bitmap hit in the second merged group. The missing anchor
        // file therefore proves that the group was skipped before any union reader opened it;
        // the first group contributes its matching row through the shared bitmap selection.
        RowType readType =
                rowTypeWithRowId(rowType()).project(SpecialFields.ROW_ID.name(), "f1", "f2");
        List<InternalRow> rows = readWithFilter(table, equalF1(f1(50)), readType);
        assertThat(rowIds(rows)).containsExactly(50L);

        FileStoreTable neighbour = createTable("merged_bitmap_dv_neighbour", options);
        writeSplitColumns(neighbour, ROW_COUNT, bitmapOptions("f1"), Collections.emptyMap());
        deleteRows(neighbour, 51);
        assertRow(assertSingleRow(query(neighbour, equalF1(f1(50)))), 50);
    }

    @Test
    public void testMergedGroupDvAwareBitmapSelectionIsPushedDown() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        FileStoreTable table = createTable("merged_bitmap_dv_partial", options);
        writeSplitColumns(table, ROW_COUNT, bitmapOptions("f1"), Collections.emptyMap());

        deleteRows(table, 51);

        Predicate filter = PredicateBuilder.or(equalF1(f1(50)), equalF1(f1(51)));
        FileStoreTable latest = getTable(identifier(table.name()));
        ReadBuilder readBuilder = latest.newReadBuilder().withFilter(filter);
        DataSplit split = (DataSplit) readBuilder.newScan().plan().splits().get(0);
        InternalRowSerializer serializer = new InternalRowSerializer(latest.rowType());
        List<InternalRow> rows = new ArrayList<>();
        try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(split)) {
            // Bypass the final DV filter. The result is still correct only when the DV-aware bitmap
            // {50} (rather than the original index bitmap {50, 51}) reached every field reader.
            removeFinalDeletionVectorReaders(reader);
            reader.forEachRemaining(row -> rows.add(serializer.copy(row)));
        }

        assertRow(assertSingleRow(rows), 50);
    }

    @Test
    public void testMergedGroupFileIndexSkipsBeforeReadingDeletionVector() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        FileStoreTable table = createTable("merged_bitmap_before_dv", options);
        writeSplitColumns(table, ROW_COUNT, bitmapOptions("f1"), Collections.emptyMap());
        deleteRows(table, 50);

        FileStoreTable latest = getTable(identifier(table.name()));
        DataSplit split = (DataSplit) latest.newReadBuilder().newScan().plan().splits().get(0);
        Path deletionVectorPath =
                split.deletionFiles().get().stream()
                        .filter(Objects::nonNull)
                        .map(file -> new Path(file.path()))
                        .findFirst()
                        .orElseThrow(IllegalStateException::new);
        assertThat(latest.fileIO().delete(deletionVectorPath, false)).isTrue();

        // The bitmap index already rejects this value, so the missing DV file must not be read.
        assertThat(readWithFilter(table, equalF1(MISSING_F1))).isEmpty();
    }

    @Test
    public void testMergedGroupEmptySelectionSkipsBeforeReadingDeletionVector() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        FileStoreTable table = createTable("merged_empty_selection_before_dv", options);
        writeSplitColumns(table, ROW_COUNT, bitmapOptions("f1"), Collections.emptyMap());
        deleteRows(table, 75);

        FileStoreTable latest = getTable(identifier(table.name()));
        DataSplit split = (DataSplit) latest.newReadBuilder().newScan().plan().splits().get(0);
        Path deletionVectorPath =
                split.deletionFiles().get().stream()
                        .filter(Objects::nonNull)
                        .map(file -> new Path(file.path()))
                        .findFirst()
                        .orElseThrow(IllegalStateException::new);
        assertThat(latest.fileIO().delete(deletionVectorPath, false)).isTrue();

        ReadBuilder readBuilder =
                latest.newReadBuilder()
                        .withFilter(equalF1(f1(50)))
                        .withRowRanges(Collections.singletonList(new Range(0L, 49L)));
        TableScan.Plan plan = readBuilder.newScan().plan();
        assertThat(plan.splits()).hasSize(1);
        assertThat(collect(readBuilder.newRead().executeFilter(), plan, latest.rowType()))
                .isEmpty();
    }

    /** Commits a deletion vector for the anchor file of the only row id group of {@code table}. */
    private void deleteRows(FileStoreTable table, long... positions) throws Exception {
        FileStoreTable latest = getTable(identifier(table.name()));
        List<DataFileMeta> dataFiles =
                ((DataSplit) latest.newReadBuilder().newScan().plan().splits().get(0)).dataFiles();
        deleteRows(latest, retrieveAnchorFile(dataFiles, file -> file), positions);
    }

    private void deleteRowsFrom(FileStoreTable table, long firstRowId, long... positions)
            throws Exception {
        FileStoreTable latest = getTable(identifier(table.name()));
        List<DataFileMeta> group =
                latest.newReadBuilder().newScan().plan().splits().stream()
                        .map(split -> (DataSplit) split)
                        .flatMap(split -> split.dataFiles().stream())
                        .filter(file -> file.nonNullFirstRowId() == firstRowId)
                        .collect(Collectors.toList());
        if (group.isEmpty()) {
            throw new IllegalArgumentException(
                    "Cannot find data file with first row id " + firstRowId);
        }
        DataFileMeta anchor = retrieveAnchorFile(group, file -> file);
        deleteRows(latest, anchor, positions);
    }

    private void deleteRows(FileStoreTable latest, DataFileMeta anchor, long... positions)
            throws Exception {

        BaseAppendDeleteFileMaintainer maintainer =
                BaseAppendDeleteFileMaintainer.forUnawareAppend(
                        latest.store().newIndexFileHandler(),
                        latest.latestSnapshot().get(),
                        BinaryRow.EMPTY_ROW);
        DeletionVector deletionVector = new BitmapDeletionVector();
        for (long position : positions) {
            deletionVector.delete(position);
        }
        maintainer.notifyNewDeletionVector(anchor.fileName(), deletionVector);

        List<IndexFileMeta> newIndexFiles = new ArrayList<>();
        for (IndexManifestEntry entry : maintainer.persist()) {
            if (entry.kind() == FileKind.ADD) {
                newIndexFiles.add(entry.indexFile());
            }
        }

        try (BatchTableCommit commit = latest.newBatchWriteBuilder().newCommit()) {
            commit.commit(
                    Collections.singletonList(
                            new CommitMessageImpl(
                                    BinaryRow.EMPTY_ROW,
                                    BucketMode.UNAWARE_BUCKET,
                                    null,
                                    new DataIncrement(
                                            Collections.emptyList(),
                                            Collections.emptyList(),
                                            Collections.emptyList(),
                                            newIndexFiles,
                                            Collections.emptyList()),
                                    CompactIncrement.emptyIncrement())));
        }
    }

    private FileStoreTable createTable(String name, Map<String, String> options) throws Exception {
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("f0", DataTypes.INT())
                        .column("f1", DataTypes.STRING())
                        .column("f2", DataTypes.STRING())
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        options.forEach(builder::option);
        Identifier identifier = identifier(name);
        catalog.createTable(identifier, builder.build(), false);
        return getTable(identifier);
    }

    private static Map<String, String> bloomOptions(String column, String inManifestThreshold) {
        String prefix =
                FileIndexOptions.FILE_INDEX + "." + BloomFilterFileIndexFactory.BLOOM_FILTER + ".";
        Map<String, String> options = new HashMap<>();
        options.put(prefix + CoreOptions.COLUMNS, column);
        options.put(prefix + column + ".items", "200");
        options.put(prefix + column + ".fpp", "0.001");
        if (inManifestThreshold != null) {
            options.put(CoreOptions.FILE_INDEX_IN_MANIFEST_THRESHOLD.key(), inManifestThreshold);
        }
        return options;
    }

    private static Map<String, String> bitmapOptions(String column) {
        Map<String, String> options = new HashMap<>();
        options.put(
                FileIndexOptions.FILE_INDEX
                        + "."
                        + BitmapFileIndexFactory.BITMAP_INDEX
                        + "."
                        + CoreOptions.COLUMNS,
                column);
        return options;
    }

    private static class RowSidecarStatusCountingFileIO extends LocalFileIO {

        private final Map<String, Integer> rowSidecarStatusReads = new HashMap<>();

        @Override
        public FileStatus getFileStatus(Path path) throws IOException {
            if (path.getName().endsWith(".row")) {
                rowSidecarStatusReads.merge(path.getName(), 1, Integer::sum);
            }
            return super.getFileStatus(path);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"parquet", "orc"})
    public void testSubMicrosecondTimestampBsiMatchesUnindexed(String format) throws Exception {
        // A BSI index maps TIMESTAMP through toMicros(), so on a TIMESTAMP(9) column two values in
        // the same microsecond share one indexed value. Answering <>, strict range or NOT IN from
        // that bitmap drops rows the residual filter can no longer recover. The fix makes the
        // indexed read (executeFilter) return exactly what an unindexed full scan does.
        FileStoreTable table = createTimestampBsiTable("bsi_ts9_" + format, format);

        long base = 1_704_067_200_000L;
        Timestamp tsA = Timestamp.fromEpochMillis(base, 123_000); // micro bucket base*1000+123
        Timestamp tsB = Timestamp.fromEpochMillis(base, 123_400); // same bucket, but tsB > tsA
        Timestamp tsC = Timestamp.fromEpochMillis(base, 999_000); // a different bucket
        write(
                table,
                GenericRow.of(0, tsA),
                GenericRow.of(1, tsB),
                GenericRow.of(2, tsC),
                GenericRow.of(3, null));

        PredicateBuilder b = new PredicateBuilder(table.rowType());

        // Guard: the sub-microsecond nanos must survive the write/read round trip on this format,
        // otherwise tsA and tsB collapse and the comparison below would pass vacuously.
        List<Timestamp> stored = new ArrayList<>();
        for (InternalRow row : fullScanFiltered(table, b.isNotNull(1))) {
            stored.add(row.getTimestamp(1, 9));
        }
        assertThat(stored).contains(tsA, tsB);

        // notEqual / strict lessThan / strict greaterThan are the ones that lose rows to the
        // micro-bucket collision; isNull/isNotNull still come from the index; equal/between only
        // over-select and are already corrected by the residual filter.
        List<Predicate> predicates =
                Arrays.asList(
                        b.notEqual(1, tsA),
                        b.lessThan(1, tsB),
                        b.greaterThan(1, tsA),
                        b.isNull(1),
                        b.isNotNull(1),
                        b.equal(1, tsA),
                        b.between(1, tsA, tsC));
        for (Predicate p : predicates) {
            assertThat(query(table, p))
                    .containsExactlyInAnyOrderElementsOf(fullScanFiltered(table, p));
        }
    }

    private FileStoreTable createTimestampBsiTable(String name, String format) throws Exception {
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("f0", DataTypes.INT())
                        .column("f1", DataTypes.TIMESTAMP(9))
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .option(CoreOptions.FILE_FORMAT.key(), format);
        bsiOptions("f1").forEach(builder::option);
        Identifier identifier = identifier(name);
        catalog.createTable(identifier, builder.build(), false);
        return getTable(identifier);
    }

    private static Map<String, String> bsiOptions(String column) {
        Map<String, String> options = new HashMap<>();
        options.put(
                FileIndexOptions.FILE_INDEX
                        + "."
                        + BitSliceIndexBitmapFileIndexFactory.BSI_INDEX
                        + "."
                        + CoreOptions.COLUMNS,
                column);
        return options;
    }

    @ParameterizedTest
    @ValueSource(strings = {"parquet", "orc"})
    public void testSubMicrosecondTimestampBitmapMatchesUnindexed(String format) throws Exception {
        // A bitmap index maps TIMESTAMP through toMicros(), so on a TIMESTAMP(9) column two values
        // in the same microsecond share one bitmap key. visitNotIn flips the matched rows over the
        // whole row count, so answering <> or NOT IN from that bitmap drops rows the residual
        // filter can no longer recover. The fix makes the indexed read (executeFilter) return
        // exactly what an unindexed full scan does.
        FileStoreTable table = createTimestampBitmapTable("bitmap_ts9_" + format, format);

        long base = 1_704_067_200_000L;
        Timestamp tsA = Timestamp.fromEpochMillis(base, 123_000); // micro bucket base*1000+123
        Timestamp tsB = Timestamp.fromEpochMillis(base, 123_400); // same bucket as tsA
        Timestamp tsC = Timestamp.fromEpochMillis(base, 999_000); // a different bucket
        write(
                table,
                GenericRow.of(0, tsA),
                GenericRow.of(1, tsB),
                GenericRow.of(2, tsC),
                GenericRow.of(3, null));

        PredicateBuilder b = new PredicateBuilder(table.rowType());

        // Guard: the sub-microsecond nanos must survive the write/read round trip on this format,
        // otherwise tsA and tsB collapse and the comparison below would pass vacuously.
        List<Timestamp> stored = new ArrayList<>();
        for (InternalRow row : fullScanFiltered(table, b.isNotNull(1))) {
            stored.add(row.getTimestamp(1, 9));
        }
        assertThat(stored).contains(tsA, tsB);

        // notEqual / NOT IN are the ones that lose rows to the micro-bucket collision;
        // isNull/isNotNull still come from the index; equal / IN only over-select and are
        // already corrected by the residual filter.
        List<Predicate> predicates =
                Arrays.asList(
                        b.notEqual(1, tsA),
                        b.notIn(1, Arrays.asList(tsA, tsC)),
                        b.isNull(1),
                        b.isNotNull(1),
                        b.equal(1, tsA),
                        b.in(1, Arrays.asList(tsA, tsC)),
                        PredicateBuilder.and(b.notEqual(1, tsA), b.notEqual(1, tsC)),
                        PredicateBuilder.or(b.equal(1, tsA), b.equal(1, tsC)));
        for (Predicate p : predicates) {
            assertThat(query(table, p))
                    .containsExactlyInAnyOrderElementsOf(fullScanFiltered(table, p));
        }
    }

    private FileStoreTable createTimestampBitmapTable(String name, String format) throws Exception {
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("f0", DataTypes.INT())
                        .column("f1", DataTypes.TIMESTAMP(9))
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .option(CoreOptions.FILE_FORMAT.key(), format);
        bitmapOptions("f1").forEach(builder::option);
        Identifier identifier = identifier(name);
        catalog.createTable(identifier, builder.build(), false);
        return getTable(identifier);
    }

    private void writeAllColumns(FileStoreTable table, int count) throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            for (int i = 0; i < count; i++) {
                write.write(
                        GenericRow.of(
                                i, BinaryString.fromString(f1(i)), BinaryString.fromString(f2(i))));
            }
            commit.commit(write.prepareCommit());
        }
    }

    /**
     * Writes f0/f1 and f2 into two files sharing one row id range, so they have to be merged. The
     * index options are per write, a file index can only be configured for columns the write
     * actually contains (see {@link org.apache.paimon.io.DataFileIndexWriter}).
     */
    private void writeSplitColumns(
            FileStoreTable table,
            int count,
            Map<String, String> firstOptions,
            Map<String, String> secondOptions)
            throws Exception {
        RowType writeType0 = table.rowType().project(Arrays.asList("f0", "f1"));
        RowType writeType1 = table.rowType().project(Collections.singletonList("f2"));

        BatchWriteBuilder builder = table.copy(firstOptions).newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite().withWriteType(writeType0);
                BatchTableCommit commit = builder.newCommit()) {
            for (int i = 0; i < count; i++) {
                write.write(GenericRow.of(i, BinaryString.fromString(f1(i))));
            }
            commit.commit(write.prepareCommit());
        }

        FileStoreTable latest = getTable(identifier(table.name()));
        long firstRowId = latest.snapshotManager().latestSnapshot().nextRowId() - count;
        builder = latest.copy(secondOptions).newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite().withWriteType(writeType1);
                BatchTableCommit commit = builder.newCommit()) {
            for (int i = 0; i < count; i++) {
                write.write(GenericRow.of(BinaryString.fromString(f2(i))));
            }
            List<CommitMessage> commitables = write.prepareCommit();
            setSingleFileFirstRowId(commitables, firstRowId);
            commit.commit(commitables);
        }
    }

    /** Writes f1 and f2 into a second merged group with a different physical schema. */
    private void writeSecondSplitColumns(
            FileStoreTable table, int count, Map<String, String> secondOptions) throws Exception {
        FileStoreTable latest = getTable(identifier(table.name()));
        RowType writeType0 = latest.rowType().project(Collections.singletonList("f1"));
        BatchWriteBuilder builder = latest.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite().withWriteType(writeType0);
                BatchTableCommit commit = builder.newCommit()) {
            for (int i = 0; i < count; i++) {
                write.write(GenericRow.of(BinaryString.fromString("second-" + f1(i))));
            }
            commit.commit(write.prepareCommit());
        }

        latest = getTable(identifier(table.name()));
        long firstRowId = latest.snapshotManager().latestSnapshot().nextRowId() - count;
        RowType writeType1 = latest.rowType().project(Collections.singletonList("f2"));
        builder = latest.copy(secondOptions).newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite().withWriteType(writeType1);
                BatchTableCommit commit = builder.newCommit()) {
            for (int i = 0; i < count; i++) {
                write.write(GenericRow.of(BinaryString.fromString(f2(i))));
            }
            List<CommitMessage> commitables = write.prepareCommit();
            setSingleFileFirstRowId(commitables, firstRowId);
            commit.commit(commitables);
        }
    }

    /**
     * Writes f0/f1 (a*) with a bloom index on f1, then overwrites f1 with c* in a second file that
     * shares the same row id range and has a higher sequence number, so the column merge takes f1
     * from the newer file.
     */
    private void writeThenOverwriteF1(FileStoreTable table, int count) throws Exception {
        RowType writeType0 = table.rowType().project(Arrays.asList("f0", "f1"));
        BatchWriteBuilder builder = table.copy(bloomOptions("f1", null)).newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite().withWriteType(writeType0);
                BatchTableCommit commit = builder.newCommit()) {
            for (int i = 0; i < count; i++) {
                write.write(GenericRow.of(i, BinaryString.fromString(f1(i))));
            }
            commit.commit(write.prepareCommit());
        }

        FileStoreTable latest = getTable(identifier(table.name()));
        long firstRowId = latest.snapshotManager().latestSnapshot().nextRowId() - count;
        RowType writeType1 = table.rowType().project(Collections.singletonList("f1"));
        builder = latest.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite().withWriteType(writeType1);
                BatchTableCommit commit = builder.newCommit()) {
            for (int i = 0; i < count; i++) {
                write.write(GenericRow.of(BinaryString.fromString(c1(i))));
            }
            List<CommitMessage> commitables = write.prepareCommit();
            setSingleFileFirstRowId(commitables, firstRowId);
            commit.commit(commitables);
        }
    }

    /**
     * {@link #setFirstRowId} stamps the same first row id on every file of the commit, which only
     * describes a valid layout when the write produced a single file.
     */
    private void setSingleFileFirstRowId(List<CommitMessage> commitables, long firstRowId) {
        List<DataFileMeta> files = new ArrayList<>();
        for (CommitMessage commitable : commitables) {
            files.addAll(((CommitMessageImpl) commitable).newFilesIncrement().newFiles());
        }
        assertThat(files).hasSize(1);
        setFirstRowId(commitables, firstRowId);
    }

    private List<InternalRow> readWithFilter(FileStoreTable table, Predicate predicate)
            throws Exception {
        return readWithFilter(table, predicate, null);
    }

    private List<InternalRow> readAfterDeletingDataFiles(
            FileStoreTable table, Predicate predicate, @Nullable List<Range> rowRanges)
            throws Exception {
        return readAfterDeletingDataFiles(table, predicate, rowRanges, null);
    }

    /**
     * Reads after deleting the data files. A non-null delete filter restricts the deletion to the
     * matching files, so part of the split is read from the row sidecars and the rest from the
     * remaining data files.
     */
    private List<InternalRow> readAfterDeletingDataFiles(
            FileStoreTable table,
            Predicate predicate,
            @Nullable List<Range> rowRanges,
            @Nullable java.util.function.Predicate<DataFileMeta> deleteFilter)
            throws Exception {
        FileStoreTable latest = getTable(identifier(table.name()));
        ReadBuilder readBuilder = latest.newReadBuilder().withFilter(predicate);
        if (rowRanges != null) {
            readBuilder.withRowRanges(rowRanges);
        }
        TableScan.Plan plan = readBuilder.newScan().plan();
        // with row ranges the scan wraps the split into an IndexedSplit
        Split first = plan.splits().get(0);
        DataSplit split =
                first instanceof IndexedSplit
                        ? ((IndexedSplit) first).dataSplit()
                        : (DataSplit) first;
        DataFilePathFactory pathFactory =
                latest.store()
                        .pathFactory()
                        .createDataFilePathFactory(split.partition(), split.bucket());
        for (DataFileMeta file : split.dataFiles()) {
            if (deleteFilter == null || deleteFilter.test(file)) {
                assertThat(latest.fileIO().delete(pathFactory.toPath(file), false)).isTrue();
            }
        }

        List<InternalRow> rows = new ArrayList<>();
        InternalRowSerializer serializer = new InternalRowSerializer(latest.rowType());
        try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(plan)) {
            reader.forEachRemaining(row -> rows.add(serializer.copy(row)));
        }
        return rows;
    }

    private List<InternalRow> readWithFilter(
            FileStoreTable table, Predicate predicate, @Nullable RowType readType)
            throws Exception {
        FileStoreTable latest = getTable(identifier(table.name()));
        ReadBuilder readBuilder = latest.newReadBuilder().withFilter(predicate);
        if (readType != null) {
            readBuilder.withReadType(readType);
        }
        TableRead read = readBuilder.newRead();
        InternalRowSerializer serializer =
                new InternalRowSerializer(readType == null ? latest.rowType() : readType);
        List<InternalRow> rows = new ArrayList<>();
        for (Split split : readBuilder.newScan().plan().splits()) {
            try (RecordReader<InternalRow> reader = read.createReader(split)) {
                reader.forEachRemaining(row -> rows.add(serializer.copy(row)));
            }
        }
        return rows;
    }

    /** Like {@link #readWithFilter}, but only counts, so an empty read type can be used. */
    private int countWithFilter(FileStoreTable table, Predicate predicate, RowType readType)
            throws Exception {
        FileStoreTable latest = getTable(identifier(table.name()));
        ReadBuilder readBuilder =
                latest.newReadBuilder().withFilter(predicate).withReadType(readType);
        TableRead read = readBuilder.newRead();
        int count = 0;
        for (Split split : readBuilder.newScan().plan().splits()) {
            try (RecordReader<InternalRow> reader = read.createReader(split)) {
                RecordReader.RecordIterator<InternalRow> batch;
                while ((batch = reader.readBatch()) != null) {
                    while (batch.next() != null) {
                        count++;
                    }
                    batch.releaseBatch();
                }
            }
        }
        return count;
    }

    /**
     * Reads the whole plan the way an engine does, with the residual filter on top of the push
     * down.
     */
    private List<InternalRow> query(FileStoreTable table, Predicate predicate) throws Exception {
        FileStoreTable latest = getTable(identifier(table.name()));
        ReadBuilder readBuilder = latest.newReadBuilder().withFilter(predicate);
        return collect(
                readBuilder.newRead().executeFilter(),
                readBuilder.newScan().plan(),
                latest.rowType());
    }

    /** What {@link #query} has to return: a full scan filtered in memory, with no push down. */
    private List<InternalRow> fullScanFiltered(FileStoreTable table, Predicate predicate)
            throws Exception {
        FileStoreTable latest = getTable(identifier(table.name()));
        ReadBuilder readBuilder = latest.newReadBuilder();
        List<InternalRow> rows = new ArrayList<>();
        for (InternalRow row :
                collect(readBuilder.newRead(), readBuilder.newScan().plan(), latest.rowType())) {
            if (predicate.test(row)) {
                rows.add(row);
            }
        }
        return rows;
    }

    /**
     * Materializes as {@link BinaryRow}, the reader hands out row implementations without a value
     * based equals.
     */
    private static List<InternalRow> collect(TableRead read, TableScan.Plan plan, RowType rowType)
            throws Exception {
        InternalRowSerializer serializer = new InternalRowSerializer(rowType);
        List<InternalRow> rows = new ArrayList<>();
        try (RecordReader<InternalRow> reader = read.createReader(plan)) {
            reader.forEachRemaining(row -> rows.add(serializer.toBinaryRow(row).copy()));
        }
        return rows;
    }

    @SuppressWarnings("unchecked")
    private static void removeFinalDeletionVectorReaders(RecordReader<InternalRow> reader)
            throws Exception {
        assertThat(reader).isInstanceOf(DataEvolutionFileReader.class);
        RecordReader<InternalRow>[] fieldReaders =
                (RecordReader<InternalRow>[]) readField(reader, "readers");
        for (int i = 0; i < fieldReaders.length; i++) {
            if (fieldReaders[i] == null) {
                continue;
            }

            assertThat(fieldReaders[i]).isInstanceOf(ForceSingleBatchReader.class);
            RecordReader<InternalRow> fieldReader =
                    (RecordReader<InternalRow>) readField(fieldReaders[i], "multiBatchReader");
            assertThat(fieldReader).isInstanceOf(ApplyDeletionVectorReader.class);
            RecordReader<InternalRow> bitmapReader =
                    ((ApplyDeletionVectorReader) fieldReader).reader();
            assertThat(bitmapReader).isInstanceOf(ApplyBitmapIndexRecordReader.class);
            fieldReaders[i] = new ForceSingleBatchReader(bitmapReader);
        }
    }

    private static Object readField(Object target, String name) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }

    private void assertMergedGroup(FileStoreTable table) throws Exception {
        FileStoreTable latest = getTable(identifier(table.name()));
        List<Split> splits = latest.newReadBuilder().newScan().plan().splits();
        assertThat(splits).hasSize(1);
        assertThat(((DataSplit) splits.get(0)).dataFiles()).hasSize(2);
    }

    /** Row ids of rows read with {@code RowType.of(SpecialFields.ROW_ID)}. */
    private static List<Long> rowIds(List<InternalRow> rows) {
        return rows.stream().map(row -> row.getLong(0)).collect(Collectors.toList());
    }

    private static List<Long> rowIds(long from, int count) {
        List<Long> ids = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            ids.add(from + i);
        }
        return ids;
    }

    private static InternalRow assertSingleRow(List<InternalRow> rows) {
        assertThat(rows).hasSize(1);
        return rows.get(0);
    }

    private static void assertRow(InternalRow row, int i) {
        assertThat(row.getInt(0)).isEqualTo(i);
        assertThat(row.getString(1).toString()).isEqualTo(f1(i));
        assertThat(row.getString(2).toString()).isEqualTo(f2(i));
    }

    private static void assertAligned(List<InternalRow> rows) {
        for (InternalRow row : rows) {
            int f0 = row.getInt(0);
            assertThat(row.getString(1).toString()).isEqualTo(f1(f0));
            assertThat(row.getString(2).toString()).isEqualTo(f2(f0));
        }
    }

    private static Predicate equalF1(String value) {
        return new PredicateBuilder(rowType()).equal(1, BinaryString.fromString(value));
    }

    private static Predicate equalAnyF1(int... values) {
        Predicate[] predicates = new Predicate[values.length];
        for (int i = 0; i < values.length; i++) {
            predicates[i] = equalF1(f1(values[i]));
        }
        return PredicateBuilder.or(predicates);
    }

    private static Predicate equalF2(String value) {
        return new PredicateBuilder(rowType()).equal(2, BinaryString.fromString(value));
    }

    private static RowType rowType() {
        return RowType.of(DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING());
    }

    private static String f1(int i) {
        return String.format("a%03d", i);
    }

    private static String f2(int i) {
        return String.format("b%03d", i);
    }

    private static String c1(int i) {
        return String.format("c%03d", i);
    }
}
