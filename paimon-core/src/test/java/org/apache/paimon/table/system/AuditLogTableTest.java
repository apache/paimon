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
import org.apache.paimon.catalog.Catalog;
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

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link AuditLogTable}. */
public class AuditLogTableTest extends TableTestBase {

    private static final String tableName = "MyTable";
    private AuditLogTable auditLogTable;

    @BeforeEach
    public void before() throws Exception {
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
        FileStoreTable table =
                FileStoreTableFactory.create(LocalFileIO.create(), tablePath, tableSchema);
        Identifier filesTableId =
                identifier(tableName + Catalog.SYSTEM_TABLE_SPLITTER + AuditLogTable.AUDIT_LOG);
        auditLogTable = (AuditLogTable) catalog.getTable(filesTableId);

        write(table, GenericRow.ofKind(RowKind.INSERT, 1, 1, 1));
        write(table, GenericRow.ofKind(RowKind.DELETE, 1, 1, 1));
        write(table, GenericRow.ofKind(RowKind.INSERT, 1, 2, 5));
        write(table, GenericRow.ofKind(RowKind.UPDATE_BEFORE, 1, 2, 5));
        write(table, GenericRow.ofKind(RowKind.UPDATE_AFTER, 1, 4, 6));
        write(table, GenericRow.ofKind(RowKind.INSERT, 2, 3, 1));
    }

    @Test
    public void testReadAuditLogFromLatest() throws Exception {
        List<InternalRow> expectRow = getExpectedResult();
        List<InternalRow> result = read(auditLogTable);
        assertThat(result).containsExactlyInAnyOrderElementsOf(expectRow);
    }

    private List<InternalRow> getExpectedResult() {
        List<InternalRow> expectedRow = new ArrayList<>();
        expectedRow.add(
                GenericRow.of(BinaryString.fromString(RowKind.DELETE.shortString()), 1, 1, 1));
        expectedRow.add(
                GenericRow.of(
                        BinaryString.fromString(RowKind.UPDATE_BEFORE.shortString()), 1, 2, 5));
        expectedRow.add(
                GenericRow.of(
                        BinaryString.fromString(RowKind.UPDATE_AFTER.shortString()), 1, 4, 6));
        expectedRow.add(
                GenericRow.of(BinaryString.fromString(RowKind.INSERT.shortString()), 2, 3, 1));
        return expectedRow;
    }
}
