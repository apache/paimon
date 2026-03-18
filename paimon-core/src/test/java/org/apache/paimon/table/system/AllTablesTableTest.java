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

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.ReadonlyTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.paimon.catalog.Catalog.SYSTEM_DATABASE_NAME;
import static org.apache.paimon.table.system.AllTablesTable.ALL_TABLES;
import static org.apache.paimon.table.system.AllTablesTable.TABLE_TYPE;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link AllTablesTable}. */
public class AllTablesTableTest extends TableTestBase {

    private AllTablesTable allTablesTable;

    @BeforeEach
    public void before() throws Exception {
        Identifier identifier = identifier("T");
        Schema schema =
                Schema.newBuilder()
                        .column("pk", DataTypes.INT())
                        .column("f1", DataTypes.INT())
                        .column("f2", DataTypes.INT())
                        .primaryKey("pk")
                        .partitionKeys("f1")
                        .build();
        catalog.createTable(identifier, schema, true);
        allTablesTable =
                (AllTablesTable) catalog.getTable(new Identifier(SYSTEM_DATABASE_NAME, ALL_TABLES));
    }

    @Test
    public void testAllTablesTable() throws Exception {
        List<String> result =
                read(allTablesTable).stream().map(Objects::toString).collect(Collectors.toList());
        result = result.stream().filter(r -> !r.contains("path")).collect(Collectors.toList());
        assertThat(result)
                .containsOnly(
                        "+I(default,T,table,true,true,null,null,null,null,null,null,null,null,null)");
    }

    @Test
    void testAllTablesTableWithOwnerField() throws Exception {
        ReadonlyTable table = allTablesTable;

        RowType readType =
                new RowType(
                        java.util.Arrays.asList(
                                TABLE_TYPE.getField(0), // database_name
                                TABLE_TYPE.getField(1), // table_name
                                TABLE_TYPE.getField(5))); // owner (field ID 5)

        InnerTableScan scan = table.newScan();
        InnerTableRead read = table.newRead().withReadType(readType);

        List<InternalRow> rows = new java.util.ArrayList<>();
        try (RecordReader<InternalRow> reader = read.createReader(scan.plan())) {
            reader.forEachRemaining(rows::add);
        }

        assertThat(rows).isNotEmpty();
        for (InternalRow row : rows) {
            assertThat(row.getFieldCount()).isEqualTo(3);
            assertThat(row.isNullAt(2) || row.getString(2) != null).isTrue();
        }
    }
}
