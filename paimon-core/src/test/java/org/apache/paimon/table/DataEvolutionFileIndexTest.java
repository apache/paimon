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
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.fileindex.bitmap.BitmapFileIndexFactory;
import org.apache.paimon.fileindex.bloomfilter.BloomFilterFileIndexFactory;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.table.SpecialFields.rowTypeWithRowId;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests file index and predicate push down in {@link
 * org.apache.paimon.operation.DataEvolutionSplitRead}.
 *
 * <p>Readers are created directly from the splits, without {@link TableRead#executeFilter()}, so an
 * empty result proves that the reader itself pruned the rows. Filter values are always picked
 * inside the min/max range of the column, otherwise the group level stats pruning in {@link
 * org.apache.paimon.operation.DataEvolutionFileStoreScan} would drop the split before the reader
 * ever sees it.
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
    public void testFileIndexReadDisabled() throws Exception {
        Map<String, String> options = bloomOptions("f1", "1 B");
        options.put(CoreOptions.FILE_INDEX_READ_ENABLED.key(), "false");
        FileStoreTable table = createTable("index_disabled", options);
        writeAllColumns(table, ROW_COUNT);

        assertThat(readWithFilter(table, equalF1(MISSING_F1))).hasSize(ROW_COUNT);
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
            setFirstRowId(commitables, firstRowId);
            commit.commit(commitables);
        }
    }

    private List<InternalRow> readWithFilter(FileStoreTable table, Predicate predicate)
            throws Exception {
        FileStoreTable latest = getTable(identifier(table.name()));
        ReadBuilder readBuilder = latest.newReadBuilder().withFilter(predicate);
        TableRead read = readBuilder.newRead();
        InternalRowSerializer serializer = new InternalRowSerializer(latest.rowType());
        List<InternalRow> rows = new ArrayList<>();
        for (Split split : readBuilder.newScan().plan().splits()) {
            try (RecordReader<InternalRow> reader = read.createReader(split)) {
                reader.forEachRemaining(row -> rows.add(serializer.copy(row)));
            }
        }
        return rows;
    }

    private void assertMergedGroup(FileStoreTable table) throws Exception {
        FileStoreTable latest = getTable(identifier(table.name()));
        List<Split> splits = latest.newReadBuilder().newScan().plan().splits();
        assertThat(splits).hasSize(1);
        assertThat(((DataSplit) splits.get(0)).dataFiles()).hasSize(2);
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
}
