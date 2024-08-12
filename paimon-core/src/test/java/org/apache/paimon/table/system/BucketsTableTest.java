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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
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
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link BucketsTable}. */
public class BucketsTableTest extends TableTestBase {

    private static final String tableName = "MyTable";

    private FileStoreTable table;

    private BucketsTable bucketsTable;

    @BeforeEach
    public void before() throws Exception {
        FileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(String.format("%s/%s.db/%s", warehouse, database, tableName));
        Schema schema =
                Schema.newBuilder()
                        .column("pk", DataTypes.INT())
                        .column("pt", DataTypes.INT())
                        .column("col1", DataTypes.INT())
                        .primaryKey("pk")
                        .option("bucket", "2")
                        .build();
        TableSchema tableSchema =
                SchemaUtils.forceCommit(new SchemaManager(fileIO, tablePath), schema);
        table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath, tableSchema);

        Identifier filesTableId =
                identifier(tableName + Catalog.SYSTEM_TABLE_SPLITTER + BucketsTable.BUCKETS);
        bucketsTable = (BucketsTable) catalog.getTable(filesTableId);

        // snapshot 1: append
        write(table, GenericRow.of(1, 1, 1), GenericRow.of(2, 2, 1));

        write(table, GenericRow.of(3, 1, 1), GenericRow.of(4, 2, 2));
    }

    @Test
    public void testBucketRecordCount() throws Exception {
        List<InternalRow> expectedRow = new ArrayList<>();
        expectedRow.add(GenericRow.of(0));
        expectedRow.add(GenericRow.of(1));

        // Only read partition and record count, record size may not stable.
        List<InternalRow> result = read(bucketsTable, new int[][] {{0}});
        assertThat(result).containsExactlyInAnyOrderElementsOf(expectedRow);
    }

    @Test
    public void testBucketCompactionScore() throws Exception {
        write(table, GenericRow.of(5, 1, 3));
        write(table, GenericRow.of(7, 1, 3));
        write(table, GenericRow.of(8, 1, 3));
        write(table, GenericRow.of(9, 1, 3));
        write(table, GenericRow.of(10, 1, 3));
        List<InternalRow> expectedRow = new ArrayList<>();
        expectedRow.add(GenericRow.of(0, 0L, 0L));
        expectedRow.add(GenericRow.of(1, 3L, 0L));

        List<InternalRow> result = read(bucketsTable, new int[][] {{0}, {4}, {5}});
        assertThat(result).containsExactlyInAnyOrderElementsOf(expectedRow);
    }
}
