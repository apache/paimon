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
import org.apache.paimon.append.dataevolution.DataEvolutionCompactCoordinator;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.table.SpecialFields.rowTypeWithRowId;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A data-evolution table can hold files written before row tracking was enabled on it (they have no
 * first row id until {@code sys.enable_data_evolution} assigns one). Such files must stay readable
 * as complete-row files with a NULL row id, and every operation that depends on row ids must refuse
 * them with a clear message.
 */
public class DataEvolutionFilesWithoutRowIdTest extends TableTestBase {

    private static final Identifier TABLE = new Identifier("default", "t");
    private static final RowType ROW_TYPE =
            RowType.of(
                    new org.apache.paimon.types.DataType[] {DataTypes.INT(), DataTypes.STRING()},
                    new String[] {"id", "v"});

    @Test
    public void testReadFilesWithoutRowId() throws Exception {
        FileStoreTable plain = createAppendTable();
        writeRows(plain, row(1, "a"), row(2, "b"));
        writeRows(plain, row(3, "c"));
        FileStoreTable table = enableDataEvolutionDirectly(plain);
        assertThat(table.coreOptions().dataEvolutionEnabled()).isTrue();

        // full read: every row, all of them without a row id
        List<InternalRow> rows = read(table, null, rowTypeWithRowId(ROW_TYPE));
        assertThat(ids(rows)).containsExactlyInAnyOrder(1, 2, 3);
        assertThat(rows).allMatch(row -> row.isNullAt(2));

        // projection
        assertThat(read(table, null, ROW_TYPE.project("v")))
                .extracting(row -> row.getString(0).toString())
                .containsExactlyInAnyOrder("a", "b", "c");

        // predicate: file statistics prune the other file and the row filter applies
        PredicateBuilder builder = new PredicateBuilder(ROW_TYPE);
        List<InternalRow> filtered = read(table, builder.equal(0, 3), ROW_TYPE);
        assertThat(ids(filtered)).containsExactly(3);
        assertThat(splitsOf(table, builder.equal(0, 3))).hasSize(1);

        // every split is a raw-convertible group of complete-row files
        for (DataSplit split : splitsOf(table, null)) {
            assertThat(split.rawConvertible()).isTrue();
            assertThat(split.dataFiles()).allMatch(file -> file.firstRowId() == null);
        }
    }

    @Test
    public void testReadMixedFilesWithAndWithoutRowId() throws Exception {
        FileStoreTable plain = createAppendTable();
        writeRows(plain, row(1, "a"), row(2, "b"));
        FileStoreTable table = enableDataEvolutionDirectly(plain);
        // written with row tracking on: gets first row id 0
        writeRows(table, row(3, "c"), row(4, "d"));

        List<InternalRow> rows = read(table, null, rowTypeWithRowId(ROW_TYPE));
        assertThat(ids(rows)).containsExactlyInAnyOrder(1, 2, 3, 4);
        Map<Integer, Long> rowIds = new HashMap<>();
        for (InternalRow row : rows) {
            rowIds.put(row.getInt(0), row.isNullAt(2) ? null : row.getLong(2));
        }
        assertThat(rowIds.get(1)).isNull();
        assertThat(rowIds.get(2)).isNull();
        assertThat(rowIds.get(3)).isEqualTo(0L);
        assertThat(rowIds.get(4)).isEqualTo(1L);

        // the row-id file and the file without id never share a split
        List<DataSplit> splits = splitsOf(table, null);
        assertThat(splits).hasSize(2);
        for (DataSplit split : splits) {
            List<Long> firstRowIds =
                    split.dataFiles().stream()
                            .map(DataFileMeta::firstRowId)
                            .collect(Collectors.toList());
            assertThat(firstRowIds)
                    .satisfiesAnyOf(
                            ids -> assertThat(ids).containsOnlyNulls(),
                            ids -> assertThat(ids).doesNotContainNull());
        }

        // a predicate on the file without id and on the file with id
        PredicateBuilder builder = new PredicateBuilder(ROW_TYPE);
        assertThat(ids(read(table, builder.equal(0, 2), ROW_TYPE))).containsExactly(2);
        assertThat(ids(read(table, builder.equal(0, 4), ROW_TYPE))).containsExactly(4);
    }

    @Test
    public void testTimeTravelToSnapshotBeforeRowTracking() throws Exception {
        FileStoreTable plain = createAppendTable();
        writeRows(plain, row(1, "a"));
        long before = plain.snapshotManager().latestSnapshotId();
        FileStoreTable table = enableDataEvolutionDirectly(plain);
        writeRows(table, row(2, "b"));

        FileStoreTable travelled =
                table.copy(
                        java.util.Collections.singletonMap(
                                CoreOptions.SCAN_SNAPSHOT_ID.key(), String.valueOf(before)));
        List<InternalRow> rows = read(travelled, null, rowTypeWithRowId(ROW_TYPE));
        assertThat(ids(rows)).containsExactly(1);
        assertThat(rows.get(0).isNullAt(2)).isTrue();
    }

    @Test
    public void testCompactionRefusesFilesWithoutRowId() throws Exception {
        FileStoreTable plain = createAppendTable();
        writeRows(plain, row(1, "a"));
        FileStoreTable table = enableDataEvolutionDirectly(plain);

        assertThatThrownBy(
                        () ->
                                new DataEvolutionCompactCoordinator(
                                                table,
                                                false,
                                                false,
                                                table.snapshotManager().latestSnapshot())
                                        .plan())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Cannot compact data file")
                .hasMessageContaining("has no first row id")
                .hasMessageContaining("sys.enable_data_evolution");
    }

    @Test
    public void testMissingRowIdMessageNamesTheFile() throws Exception {
        FileStoreTable plain = createAppendTable();
        writeRows(plain, row(1, "a"));
        DataFileMeta file =
                splitsOf(enableDataEvolutionDirectly(plain), null).get(0).dataFiles().get(0);
        assertThatThrownBy(file::nonNullFirstRowId)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(file.fileName())
                .hasMessageContaining("sys.enable_data_evolution");
    }

    private FileStoreTable createAppendTable() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("v", DataTypes.STRING())
                        .build();
        catalog.createTable(TABLE, schema, false);
        return getTable(TABLE);
    }

    /** Flips the two options in a new schema the way the conversion procedure will. */
    private FileStoreTable enableDataEvolutionDirectly(FileStoreTable table) throws Exception {
        SchemaManager schemaManager = table.schemaManager();
        TableSchema latest = schemaManager.latest().get();
        Map<String, String> options = new HashMap<>(latest.options());
        options.put(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        options.put(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        TableSchema enabled =
                new TableSchema(
                        latest.id() + 1,
                        latest.fields(),
                        latest.highestFieldId(),
                        latest.partitionKeys(),
                        latest.primaryKeys(),
                        options,
                        latest.comment());
        assertThat(schemaManager.commit(enabled)).isTrue();
        return FileStoreTableFactory.create(table.fileIO(), table.location());
    }

    private static GenericRow row(int id, String v) {
        return GenericRow.of(id, BinaryString.fromString(v));
    }

    private static List<Integer> ids(List<InternalRow> rows) {
        return rows.stream().map(row -> row.getInt(0)).collect(Collectors.toList());
    }

    private List<DataSplit> splitsOf(FileStoreTable table, @Nullable Predicate predicate) {
        ReadBuilder readBuilder = table.newReadBuilder();
        if (predicate != null) {
            readBuilder.withFilter(predicate);
        }
        return readBuilder.newScan().plan().splits().stream()
                .map(split -> (DataSplit) split)
                .collect(Collectors.toList());
    }

    private List<InternalRow> read(
            FileStoreTable table, @Nullable Predicate predicate, RowType readType)
            throws Exception {
        ReadBuilder readBuilder = table.newReadBuilder().withReadType(readType);
        if (predicate != null) {
            readBuilder.withFilter(predicate);
        }
        TableRead read = readBuilder.newRead();
        if (predicate != null) {
            read.executeFilter();
        }
        InternalRowSerializer serializer = new InternalRowSerializer(readType);
        List<InternalRow> rows = new ArrayList<>();
        for (Split split : readBuilder.newScan().plan().splits()) {
            try (RecordReader<InternalRow> reader = read.createReader(split)) {
                reader.forEachRemaining(row -> rows.add(serializer.copy(row)));
            }
        }
        return rows;
    }

    private void writeRows(FileStoreTable table, GenericRow... rows) throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            for (GenericRow row : rows) {
                write.write(row);
            }
            commit.commit(write.prepareCommit());
        }
    }
}
