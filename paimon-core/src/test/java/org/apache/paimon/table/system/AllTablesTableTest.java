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
import org.apache.paimon.rest.responses.AuditRESTResponse;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.ReadonlyTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.TableSnapshot;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;

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
    void testAllTablesTableWithNonNumericAuditOption() throws Exception {
        // created-at/updated-at are ordinary table options: a non-numeric value must
        // surface as NULL instead of failing the whole ALL_TABLES listing
        Identifier identifier = identifier("T2");
        Schema schema =
                Schema.newBuilder()
                        .column("pk", DataTypes.INT())
                        .primaryKey("pk")
                        .option(AuditRESTResponse.FIELD_CREATED_AT, "not-a-number")
                        .build();
        catalog.createTable(identifier, schema, true);

        // the listing is computed when the table object is fetched, so build it from the
        // tables that exist now (the fetched handle is cached with the old listing)
        List<Pair<Table, TableSnapshot>> tables = new java.util.ArrayList<>();
        for (String tn : catalog.listTables(database)) {
            if (tn.equals("T2")) {
                tables.add(Pair.of(catalog.getTable(identifier(tn)), null));
            }
        }
        AllTablesTable listing = AllTablesTable.fromTables(tables);
        List<String> result =
                read(listing).stream().map(Objects::toString).collect(Collectors.toList());
        assertThat(result).anyMatch(r -> r.contains("T2") && r.contains("null,null"));
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
