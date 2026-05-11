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
import org.apache.paimon.data.GenericRow;
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

import static org.apache.paimon.catalog.Catalog.SYSTEM_DATABASE_NAME;
import static org.apache.paimon.table.system.AllPartitionsTable.ALL_PARTITIONS;
import static org.apache.paimon.table.system.AllPartitionsTable.TABLE_TYPE;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link AllPartitionsTable}. */
public class AllPartitionsTableTest extends TableTestBase {

    private AllPartitionsTable allPartitionsTable;

    @BeforeEach
    public void before() throws Exception {
        Identifier identifier = identifier("T");
        Schema schema =
                Schema.newBuilder()
                        .column("f0", DataTypes.INT())
                        .column("f1", DataTypes.INT())
                        .column("f2", DataTypes.INT())
                        .partitionKeys("f1")
                        .build();
        catalog.createTable(identifier, schema, true);

        write(getTable(identifier), GenericRow.of(1, 1, 1));
        allPartitionsTable =
                (AllPartitionsTable)
                        catalog.getTable(new Identifier(SYSTEM_DATABASE_NAME, ALL_PARTITIONS));
    }

    @Test
    public void testAllPartitionsTable() throws Exception {
        List<InternalRow> rows = read(allPartitionsTable);

        assertThat(rows.size()).isEqualTo(1);
        InternalRow row = rows.get(0);
        assertThat(row.getFieldCount()).isEqualTo(8);
        assertThat(row.getString(0).toString()).isEqualTo("default"); // database_name
        assertThat(row.getString(1).toString()).isEqualTo("T"); // table_name
        assertThat(row.getString(2).toString()).isEqualTo("f1=1"); // partition_name
        assertThat(row.getLong(3)).isEqualTo(1L); // record_count
        assertThat(row.getLong(4)).isGreaterThan(0L); // file_size_in_bytes
        assertThat(row.getLong(5)).isEqualTo(1L); // file_count
        assertThat(row.getLong(6))
                .isLessThanOrEqualTo(System.currentTimeMillis()); // last_file_creation_time
        assertThat(row.getBoolean(7)).isFalse(); // done
    }

    @Test
    void testAllPartitionsTableWithProjection() throws Exception {
        ReadonlyTable table = allPartitionsTable;

        RowType readType =
                new RowType(
                        java.util.Arrays.asList(
                                TABLE_TYPE.getField(0), // database_name
                                TABLE_TYPE.getField(1), // table_name
                                TABLE_TYPE.getField(5))); // file_count (field ID 5)

        InnerTableScan scan = table.newScan();
        InnerTableRead read = table.newRead().withReadType(readType);

        List<InternalRow> rows = new java.util.ArrayList<>();
        try (RecordReader<InternalRow> reader = read.createReader(scan.plan())) {
            reader.forEachRemaining(rows::add);
        }

        assertThat(rows).isNotEmpty();
        for (InternalRow row : rows) {
            assertThat(row.getFieldCount()).isEqualTo(3);
            if (!row.isNullAt(2)) {
                row.getLong(2);
            }
        }
    }
}
