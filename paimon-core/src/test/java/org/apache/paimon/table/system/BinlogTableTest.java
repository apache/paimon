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

package org.apache.paimon.table.system;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.catalog.Identifier.SYSTEM_TABLE_SPLITTER;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link BinlogTable}. */
public class BinlogTableTest extends TableTestBase {

    @Test
    public void testReadBinlogFromLatest() throws Exception {
        BinlogTable binlogTable = createBinlogTable("binlog_table", false);
        assertThat(binlogTable.rowType().getFieldNames())
                .containsExactly("rowkind", "pk", "pt", "col1");

        List<InternalRow> result = read(binlogTable);
        List<InternalRow> expectRow = getExpectedResult();
        assertThat(result).containsExactlyInAnyOrderElementsOf(expectRow);
    }

    @Test
    public void testReadSequenceNumberWithTableOption() throws Exception {
        BinlogTable binlogTable = createBinlogTable("binlog_table_with_seq", true);
        assertThat(binlogTable.rowType().getFieldNames())
                .containsExactly("rowkind", "_SEQUENCE_NUMBER", "pk", "pt", "col1");

        List<InternalRow> result = read(binlogTable);
        List<InternalRow> expectRow = getExpectedResultWithSequenceNumber();
        assertThat(result).containsExactlyInAnyOrderElementsOf(expectRow);
    }

    @Test
    public void testReadSequenceNumberWithAlterTable() throws Exception {
        String tableName = "binlog_table_alter_seq";
        // Create table without sequence-number option
        BinlogTable binlogTable = createBinlogTable(tableName, false);
        assertThat(binlogTable.rowType().getFieldNames())
                .containsExactly("rowkind", "pk", "pt", "col1");

        // Add sequence-number option via alterTable
        catalog.alterTable(
                identifier(tableName),
                SchemaChange.setOption(
                        CoreOptions.TABLE_READ_SEQUENCE_NUMBER_ENABLED.key(), "true"),
                false);

        // Re-fetch the binlog table to get updated schema
        Identifier binlogTableId =
                identifier(tableName + SYSTEM_TABLE_SPLITTER + BinlogTable.BINLOG);
        BinlogTable updatedBinlogTable = (BinlogTable) catalog.getTable(binlogTableId);

        // Verify schema now includes _SEQUENCE_NUMBER
        assertThat(updatedBinlogTable.rowType().getFieldNames())
                .containsExactly("rowkind", "_SEQUENCE_NUMBER", "pk", "pt", "col1");

        List<InternalRow> result = read(updatedBinlogTable);
        List<InternalRow> expectRow = getExpectedResultWithSequenceNumber();
        assertThat(result).containsExactlyInAnyOrderElementsOf(expectRow);
    }

    private BinlogTable createBinlogTable(String tableName, boolean enableSequenceNumber)
            throws Exception {
        Path tablePath = new Path(String.format("%s/%s.db/%s", warehouse, database, tableName));
        FileIO fileIO = LocalFileIO.create();

        Schema.Builder schemaBuilder =
                Schema.newBuilder()
                        .column("pk", DataTypes.INT())
                        .column("pt", DataTypes.INT())
                        .column("col1", DataTypes.INT())
                        .partitionKeys("pt")
                        .primaryKey("pk", "pt")
                        .option(CoreOptions.CHANGELOG_PRODUCER.key(), "input")
                        .option("bucket", "1");
        if (enableSequenceNumber) {
            schemaBuilder.option(CoreOptions.TABLE_READ_SEQUENCE_NUMBER_ENABLED.key(), "true");
        }

        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new SchemaManager(fileIO, tablePath), schemaBuilder.build());
        FileStoreTable table =
                FileStoreTableFactory.create(LocalFileIO.create(), tablePath, tableSchema);

        writeTestData(table);

        Identifier binlogTableId =
                identifier(tableName + SYSTEM_TABLE_SPLITTER + BinlogTable.BINLOG);
        return (BinlogTable) catalog.getTable(binlogTableId);
    }

    private void writeTestData(FileStoreTable table) throws Exception {
        write(table, GenericRow.ofKind(RowKind.INSERT, 1, 1, 1));
        write(table, GenericRow.ofKind(RowKind.DELETE, 1, 1, 1));
        write(table, GenericRow.ofKind(RowKind.INSERT, 1, 2, 5));
        write(table, GenericRow.ofKind(RowKind.UPDATE_BEFORE, 1, 2, 5));
        write(table, GenericRow.ofKind(RowKind.UPDATE_AFTER, 1, 2, 6));
        write(table, GenericRow.ofKind(RowKind.INSERT, 2, 3, 1));
    }

    private List<InternalRow> getExpectedResult() {
        List<InternalRow> expectedRow = new ArrayList<>();
        expectedRow.add(
                GenericRow.of(
                        BinaryString.fromString(RowKind.DELETE.shortString()),
                        new GenericArray(new Object[] {1}),
                        new GenericArray(new Object[] {1}),
                        new GenericArray(new Object[] {1})));
        expectedRow.add(
                GenericRow.of(
                        BinaryString.fromString(RowKind.UPDATE_AFTER.shortString()),
                        new GenericArray(new Object[] {1}),
                        new GenericArray(new Object[] {2}),
                        new GenericArray(new Object[] {6})));
        expectedRow.add(
                GenericRow.of(
                        BinaryString.fromString(RowKind.INSERT.shortString()),
                        new GenericArray(new Object[] {2}),
                        new GenericArray(new Object[] {3}),
                        new GenericArray(new Object[] {1})));
        return expectedRow;
    }

    private List<InternalRow> getExpectedResultWithSequenceNumber() {
        List<InternalRow> expectedRow = new ArrayList<>();
        expectedRow.add(
                GenericRow.of(
                        BinaryString.fromString(RowKind.DELETE.shortString()),
                        1L,
                        new GenericArray(new Object[] {1}),
                        new GenericArray(new Object[] {1}),
                        new GenericArray(new Object[] {1})));
        expectedRow.add(
                GenericRow.of(
                        BinaryString.fromString(RowKind.UPDATE_AFTER.shortString()),
                        2L,
                        new GenericArray(new Object[] {1}),
                        new GenericArray(new Object[] {2}),
                        new GenericArray(new Object[] {6})));
        expectedRow.add(
                GenericRow.of(
                        BinaryString.fromString(RowKind.INSERT.shortString()),
                        0L,
                        new GenericArray(new Object[] {2}),
                        new GenericArray(new Object[] {3}),
                        new GenericArray(new Object[] {1})));
        return expectedRow;
    }
}
