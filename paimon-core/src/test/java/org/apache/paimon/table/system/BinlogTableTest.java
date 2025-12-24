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
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.paimon.catalog.Identifier.SYSTEM_TABLE_SPLITTER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

/** Unit tests for {@link BinlogTable}. */
public class BinlogTableTest extends TableTestBase {

    private FileStoreTable baseTable;
    private BinlogTable binlogTable;

    @BeforeEach
    public void before() throws Exception {
        baseTable = createTableWithData("BinlogTestTable");
        Identifier binlogId =
                identifier(baseTable.name() + SYSTEM_TABLE_SPLITTER + BinlogTable.BINLOG);
        binlogTable = (BinlogTable) catalog.getTable(binlogId);
    }

    @Test
    public void testBatchRead() throws Exception {
        List<InternalRow> result = read(binlogTable);

        // Verify schema: system fields (BinaryString, Long) NOT packed into arrays,
        // physical fields ARE arrays, all rows output as INSERT
        for (InternalRow row : result) {
            assertThat(row.getString(0)).isInstanceOf(BinaryString.class);
            assertThat(row.getArray(2).size()).isEqualTo(1);
            assertThat(row.getRowKind()).isEqualTo(RowKind.INSERT);
        }

        // Verify specific records: [rowkind, seq, pk[0], pt[0], col1[0]]
        assertThat(result)
                .extracting(
                        row -> row.getString(0).toString(),
                        row -> row.getLong(1),
                        row -> row.getArray(2).getInt(0),
                        row -> row.getArray(3).getInt(0),
                        row -> row.getArray(4).getInt(0))
                .containsExactlyInAnyOrder(
                        tuple("-D", 1L, 1, 1, 1),
                        tuple("-U", 1L, 1, 2, 5),
                        tuple("+U", 0L, 1, 4, 6),
                        tuple("+I", 0L, 2, 3, 1));
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
}
