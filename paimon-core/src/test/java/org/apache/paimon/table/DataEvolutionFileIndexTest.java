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
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.deletionvectors.BitmapDeletionVector;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.deletionvectors.append.BaseAppendDeleteFileMaintainer;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.fileindex.bitmap.BitmapFileIndexFactory;
import org.apache.paimon.fileindex.bloomfilter.BloomFilterFileIndexFactory;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.table.SpecialFields.rowTypeWithRowId;
import static org.apache.paimon.utils.DataEvolutionUtils.retrieveAnchorFile;
import static org.assertj.core.api.Assertions.assertThat;

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
 * <p>A filter on a column that is not part of the read type has no {@link #query} counterpart:
 * {@link TableRead#executeFilter()} can not project such a predicate onto the read row and silently
 * keeps every row, so only the split level assertion says anything.
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

        // a merged group is never row filtered, but the rows it returns must stay aligned
        List<InternalRow> rows = readWithFilter(table, equalF2(f2(50)));
        assertThat(rows).hasSize(ROW_COUNT);
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
    public void testFileIndexIsGivenUpForAColumnOutsideTheReadType() throws Exception {
        FileStoreTable table = createTable("projection_index", bloomOptions("f1", "1 B"));
        writeAllColumns(table, ROW_COUNT);

        // the index of this file does prove that no row of it matches, but a file only owns the
        // columns of the read type: the file that owns f1 can be pruned out of the split, see
        // testProjectionPruningAwayTheWinnerOfTheFilterColumn, so the push down gives the column
        // up rather than trust whichever file is left holding it
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
    public void testProjectionPruningAwayTheWinnerOfTheFilterColumn() throws Exception {
        FileStoreTable table = createTable("pruned_winner", Collections.emptyMap());
        writeThenOverwriteF1(table, ROW_COUNT);

        // f1 was rewritten as c* by a second file, and projecting f0 prunes that file out of the
        // split, see DataEvolutionFileStoreScan#pruneByReadType. The old file is left holding a*
        // and a bloom index that knows nothing about c*, so nothing about it may be used to prove
        // that a row does not match: the row does match, through the file that is not there.
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

    /** Commits a deletion vector for the anchor file of the only row id group of {@code table}. */
    private void deleteRows(FileStoreTable table, long... positions) throws Exception {
        FileStoreTable latest = getTable(identifier(table.name()));
        List<DataFileMeta> dataFiles =
                ((DataSplit) latest.newReadBuilder().newScan().plan().splits().get(0)).dataFiles();
        String anchor = retrieveAnchorFile(dataFiles, file -> file).fileName();

        BaseAppendDeleteFileMaintainer maintainer =
                BaseAppendDeleteFileMaintainer.forUnawareAppend(
                        latest.store().newIndexFileHandler(),
                        latest.latestSnapshot().get(),
                        BinaryRow.EMPTY_ROW);
        DeletionVector deletionVector = new BitmapDeletionVector();
        for (long position : positions) {
            deletionVector.delete(position);
        }
        maintainer.notifyNewDeletionVector(anchor, deletionVector);

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
