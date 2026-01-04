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
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.paimon.catalog.Identifier.SYSTEM_TABLE_SPLITTER;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link AuditLogTable}. */
public class AuditLogTableTest extends TableTestBase {

    private FileStoreTable baseTable;
    private AuditLogTable auditLogTable;

    @BeforeEach
    public void before() throws Exception {
        baseTable = createTableWithData("AuditLogTestTable");
        Identifier auditLogId =
                identifier(baseTable.name() + SYSTEM_TABLE_SPLITTER + AuditLogTable.AUDIT_LOG);
        auditLogTable = (AuditLogTable) catalog.getTable(auditLogId);
    }

    @Test
    public void testBatchReadWithSystemFields() throws Exception {
        List<InternalRow> result = read(auditLogTable);

        // Verify we get 4 records (after compaction: 1 DELETE, 1 UPDATE_BEFORE, 1 UPDATE_AFTER, 1
        // INSERT)
        assertThat(result).hasSize(4);

        // Verify all rows have correct system fields (ROW_KIND at index 0, SEQUENCE_NUMBER at
        // index 1)
        for (InternalRow row : result) {
            // ROW_KIND should be a BinaryString
            assertThat(row.getString(0)).isNotNull();
            // SEQUENCE_NUMBER should be a Long
            assertThat(row.getLong(1)).isGreaterThanOrEqualTo(0L);
        }

        // Verify specific records
        assertThat(result)
                .containsExactlyInAnyOrder(
                        // DELETE: pk=1, pt=1, col1=1, seq=1
                        GenericRow.of(
                                BinaryString.fromString(RowKind.DELETE.shortString()), 1L, 1, 1, 1),
                        // UPDATE_BEFORE: pk=1, pt=2, col1=5, seq=1
                        GenericRow.of(
                                BinaryString.fromString(RowKind.UPDATE_BEFORE.shortString()),
                                1L,
                                1,
                                2,
                                5),
                        // UPDATE_AFTER: pk=1, pt=4, col1=6, seq=0 (new data)
                        GenericRow.of(
                                BinaryString.fromString(RowKind.UPDATE_AFTER.shortString()),
                                0L,
                                1,
                                4,
                                6),
                        // INSERT: pk=2, pt=3, col1=1, seq=0
                        GenericRow.of(
                                BinaryString.fromString(RowKind.INSERT.shortString()),
                                0L,
                                2,
                                3,
                                1));
    }

    @Test
    public void testReadWithPartialFieldsAndOutOfOrder() throws Exception {
        // Test reading with partial fields in a different order than table schema
        // AuditLogTable schema: [rowkind, _SEQUENCE_NUMBER, pk, pt, col1]
        // We want to read: [pt, rowkind, col1] (out of order, partial fields)

        List<InternalRow> result = readWithCustomProjection(auditLogTable);

        // Verify we get 4 records
        assertThat(result).hasSize(4);

        // Verify field ordering: [pt, rowkind, col1]
        for (InternalRow row : result) {
            // Index 0: pt (INT)
            assertThat(row.getInt(0)).isGreaterThan(0);
            // Index 1: rowkind (STRING)
            assertThat(row.getString(1)).isNotNull();
            // Index 2: col1 (INT)
            assertThat(row.getInt(2)).isGreaterThan(0);
        }

        // Verify specific records with the new field order
        assertThat(result)
                .containsExactlyInAnyOrder(
                        // pt=1, rowkind=DELETE, col1=1
                        GenericRow.of(1, BinaryString.fromString(RowKind.DELETE.shortString()), 1),
                        // pt=2, rowkind=UPDATE_BEFORE, col1=5
                        GenericRow.of(
                                2, BinaryString.fromString(RowKind.UPDATE_BEFORE.shortString()), 5),
                        // pt=4, rowkind=UPDATE_AFTER, col1=6
                        GenericRow.of(
                                4, BinaryString.fromString(RowKind.UPDATE_AFTER.shortString()), 6),
                        // pt=3, rowkind=INSERT, col1=1
                        GenericRow.of(3, BinaryString.fromString(RowKind.INSERT.shortString()), 1));
    }

    /**
     * Helper method to read with custom field projection in a specific order. Reads fields [pt,
     * rowkind, col1] from AuditLogTable.
     */
    private List<InternalRow> readWithCustomProjection(AuditLogTable table) throws Exception {
        RowType tableRowType = auditLogTable.rowType();
        List<DataField> customFields =
                Arrays.asList(
                        tableRowType.getField("pt"),
                        tableRowType.getField("rowkind"),
                        tableRowType.getField("col1"));
        RowType customReadType = new RowType(customFields);

        ReadBuilder readBuilder = table.newReadBuilder().withReadType(customReadType);
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());
        InternalRowSerializer serializer = new InternalRowSerializer(customReadType);

        List<InternalRow> rows = new ArrayList<>();
        reader.forEachRemaining(row -> rows.add(serializer.copy(row)));
        return rows;
    }

    @Test
    public void testAppendOnlyTableWithRowKind() throws Exception {
        FileStoreTable appendTable = createAppendOnlyTable("AppendOnlyAuditTest");

        TableWriteImpl<?> write = appendTable.newWrite("user0");
        StreamTableCommit commit = appendTable.newCommit("user0");

        write.write(GenericRow.of(1, BinaryString.fromString("Alice"), 100L));
        write.write(GenericRow.of(2, BinaryString.fromString("Bob"), 200L));
        commit.commit(1, write.prepareCommit(true, 1));

        write.close();
        commit.close();

        AuditLogTable auditLog = new AuditLogTable(appendTable);
        List<InternalRow> result = read(auditLog);

        // Verify specific records: [rowkind, seq, id, name, amount]
        // Note: append-only table has null sequence number
        assertThat(result)
                .containsExactlyInAnyOrder(
                        GenericRow.of(
                                BinaryString.fromString("+I"),
                                null,
                                1,
                                BinaryString.fromString("Alice"),
                                100L),
                        GenericRow.of(
                                BinaryString.fromString("+I"),
                                null,
                                2,
                                BinaryString.fromString("Bob"),
                                200L));
    }

    // ==================== Helper Methods ====================

    /** Creates a FileStoreTable with changelog producer and writes test data. */
    private FileStoreTable createTableWithData(String tableName) throws Exception {
        FileStoreTable table = createChangelogTable(tableName);

        // Write test data with different RowKinds
        write(table, GenericRow.ofKind(RowKind.INSERT, 1, 1, 1)); // Will be deleted
        write(table, GenericRow.ofKind(RowKind.DELETE, 1, 1, 1));
        write(table, GenericRow.ofKind(RowKind.INSERT, 1, 2, 5)); // Will be updated
        write(table, GenericRow.ofKind(RowKind.UPDATE_BEFORE, 1, 2, 5));
        write(table, GenericRow.ofKind(RowKind.UPDATE_AFTER, 1, 4, 6));
        write(table, GenericRow.ofKind(RowKind.INSERT, 2, 3, 1)); // Remains as insert

        return table;
    }

    /** Creates a table with changelog producer enabled. */
    private FileStoreTable createChangelogTable(String tableName) throws Exception {
        Path tablePath = new Path(String.format("%s/%s.db/%s", warehouse, database, tableName));
        FileIO fileIO = LocalFileIO.create();

        Schema schema =
                Schema.newBuilder()
                        .column("pk", DataTypes.INT())
                        .column("pt", DataTypes.INT())
                        .column("col1", DataTypes.INT())
                        .partitionKeys("pt")
                        .primaryKey("pk", "pt")
                        .option(CoreOptions.CHANGELOG_PRODUCER.key(), "input")
                        .option("bucket", "1")
                        .build();

        TableSchema tableSchema =
                SchemaUtils.forceCommit(new SchemaManager(fileIO, tablePath), schema);
        return FileStoreTableFactory.create(LocalFileIO.create(), tablePath, tableSchema);
    }

    /** Creates an append-only table (no primary key). */
    private FileStoreTable createAppendOnlyTable(String tableName) throws Exception {
        Path tablePath = new Path(String.format("%s/%s.db/%s", warehouse, database, tableName));
        FileIO fileIO = LocalFileIO.create();

        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("amount", DataTypes.BIGINT())
                        .option("bucket", "1")
                        .option("bucket-key", "id")
                        .build();

        TableSchema tableSchema =
                SchemaUtils.forceCommit(new SchemaManager(fileIO, tablePath), schema);
        return FileStoreTableFactory.create(LocalFileIO.create(), tablePath, tableSchema);
    }
}
