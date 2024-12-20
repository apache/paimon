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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link PartitionsTable}. */
public class PartitionsTableTest extends TableTestBase {

    private static final String tableName = "MyTable";

    private FileStoreTable table;

    private PartitionsTable partitionsTable;

    @BeforeEach
    public void before() throws Exception {
        FileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(String.format("%s/%s.db/%s", warehouse, database, tableName));
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
        table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath, tableSchema);

        Identifier filesTableId =
                identifier(tableName + Catalog.SYSTEM_TABLE_SPLITTER + PartitionsTable.PARTITIONS);
        partitionsTable = (PartitionsTable) catalog.getTable(filesTableId);

        // snapshot 1: append
        write(table, GenericRow.of(1, 1, 1), GenericRow.of(1, 3, 5));

        write(table, GenericRow.of(1, 1, 3), GenericRow.of(1, 2, 4));
    }

    @Test
    public void testPartitionRecordCount() throws Exception {
        List<InternalRow> expectedRow = new ArrayList<>();
        expectedRow.add(GenericRow.of(BinaryString.fromString("[1]"), 2L));
        expectedRow.add(GenericRow.of(BinaryString.fromString("[2]"), 1L));
        expectedRow.add(GenericRow.of(BinaryString.fromString("[3]"), 1L));

        // Only read partition and record count, record size may not stable.
        List<InternalRow> result = read(partitionsTable, new int[] {0, 1});
        assertThat(result).containsExactlyInAnyOrderElementsOf(expectedRow);
    }

    @Test
    public void testPartitionTimeTravel() throws Exception {
        List<InternalRow> expectedRow = new ArrayList<>();
        expectedRow.add(GenericRow.of(BinaryString.fromString("[1]"), 1L));
        expectedRow.add(GenericRow.of(BinaryString.fromString("[3]"), 1L));

        // Only read partition and record count, record size may not stable.
        List<InternalRow> result =
                read(
                        partitionsTable.copy(
                                Collections.singletonMap(CoreOptions.SCAN_VERSION.key(), "1")),
                        new int[] {0, 1});
        assertThat(result).containsExactlyInAnyOrderElementsOf(expectedRow);
    }

    @Test
    public void testPartitionValue() throws Exception {
        write(table, GenericRow.of(2, 1, 3), GenericRow.of(3, 1, 4));
        List<InternalRow> expectedRow = new ArrayList<>();
        expectedRow.add(GenericRow.of(BinaryString.fromString("[1]"), 4L, 3L));
        expectedRow.add(GenericRow.of(BinaryString.fromString("[2]"), 1L, 1L));
        expectedRow.add(GenericRow.of(BinaryString.fromString("[3]"), 1L, 1L));

        List<InternalRow> result = read(partitionsTable, new int[] {0, 1, 3});
        assertThat(result).containsExactlyInAnyOrderElementsOf(expectedRow);
    }
}
