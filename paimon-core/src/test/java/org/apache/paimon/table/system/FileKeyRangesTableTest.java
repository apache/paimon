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
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.predicate.In;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.paimon.catalog.Identifier.SYSTEM_TABLE_SPLITTER;
import static org.apache.paimon.io.DataFileTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FileKeyRangesTable}. */
public class FileKeyRangesTableTest extends TableTestBase {

    private static final String TABLE_NAME = "MyTable";

    private FileStoreTable table;
    private FileKeyRangesTable fileKeyRangesTable;

    @BeforeEach
    public void before() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("pk", DataTypes.INT())
                        .column("pt", DataTypes.INT())
                        .column("col", DataTypes.INT())
                        .partitionKeys("pt")
                        .primaryKey("pk", "pt")
                        .option("bucket", "2")
                        .build();
        catalog.createTable(identifier(TABLE_NAME), schema, false);
        table = (FileStoreTable) catalog.getTable(identifier(TABLE_NAME));

        Identifier fileKeyRangesId =
                identifier(TABLE_NAME + SYSTEM_TABLE_SPLITTER + FileKeyRangesTable.FILE_KEY_RANGES);
        fileKeyRangesTable = (FileKeyRangesTable) catalog.getTable(fileKeyRangesId);

        // snapshot 1: write two partitions
        write(table, GenericRow.of(1, 1, 10), GenericRow.of(1, 2, 20));
        // snapshot 2: write more rows
        write(table, GenericRow.of(2, 1, 30), GenericRow.of(2, 2, 40));
    }

    @Test
    public void testReadBasic() throws Exception {
        List<String> rows = readPartBucketLevel(null);
        assertThat(rows).isNotEmpty();

        // verify that file_path, min_key and max_key fields are readable
        ReadBuilder rb = fileKeyRangesTable.newReadBuilder();
        rb.newRead()
                .createReader(rb.newScan().plan())
                .forEachRemaining(
                        row -> {
                            // file_path (index 2) should be non-null
                            assertThat(row.getString(2)).isNotNull();
                            // min_key (index 8) and max_key (index 9) should be non-null for
                            // primary key tables
                            assertThat(row.getString(8)).isNotNull();
                            assertThat(row.getString(9)).isNotNull();
                        });
    }

    @Test
    public void testPartitionFilter() throws Exception {
        PredicateBuilder builder = new PredicateBuilder(FileKeyRangesTable.TABLE_TYPE);
        List<String> rows = readPartBucketLevel(builder.equal(0, BinaryString.fromString("{1}")));
        assertThat(rows).isNotEmpty();
        for (String row : rows) {
            assertThat(row).startsWith("{1}-");
        }
    }

    @Test
    public void testBucketFilter() throws Exception {
        PredicateBuilder builder = new PredicateBuilder(FileKeyRangesTable.TABLE_TYPE);
        List<String> rows = readPartBucketLevel(builder.equal(1, 0));
        assertThat(rows).isNotEmpty();
        for (String row : rows) {
            String[] parts = row.split("-");
            assertThat(parts[1]).isEqualTo("0");
        }
    }

    @Test
    public void testLevelFilter() throws Exception {
        // compact to produce level-5 files
        compact(table, row(1), 0);
        compact(table, row(2), 0);

        PredicateBuilder builder = new PredicateBuilder(FileKeyRangesTable.TABLE_TYPE);
        List<String> rows = readPartBucketLevel(builder.equal(5, 5));
        assertThat(rows).isNotEmpty();
        for (String row : rows) {
            String[] parts = row.split("-");
            assertThat(parts[2]).isEqualTo("5");
        }
    }

    @Test
    public void testFirstRowId() throws Exception {
        // first_row_id (index 10) is nullable BigInt; for a primary-key table without
        // first_row_id configured it will be null - just verify the field is accessible
        ReadBuilder rb = fileKeyRangesTable.newReadBuilder();
        rb.newRead()
                .createReader(rb.newScan().plan())
                .forEachRemaining(
                        row -> {
                            // accessing index 10 should not throw even if null
                            if (!row.isNullAt(10)) {
                                assertThat(row.getLong(10)).isGreaterThanOrEqualTo(0L);
                            }
                        });
    }

    @Test
    public void testSystemTableName() throws Exception {
        Identifier id =
                identifier(TABLE_NAME + SYSTEM_TABLE_SPLITTER + FileKeyRangesTable.FILE_KEY_RANGES);
        FileKeyRangesTable t = (FileKeyRangesTable) catalog.getTable(id);
        assertThat(t.rowType().getFieldCount()).isEqualTo(11);
    }

    @Test
    public void testPartitionInFilter() throws Exception {
        DataField partitionField = FileKeyRangesTable.TABLE_TYPE.getFields().get(0);
        Predicate inPredicate =
                new LeafPredicate(
                        In.INSTANCE,
                        partitionField.type(),
                        0,
                        partitionField.name(),
                        Arrays.asList(
                                BinaryString.fromString("{1}"), BinaryString.fromString("{2}")));
        List<String> rows = readPartBucketLevel(inPredicate);
        assertThat(rows).isNotEmpty();
        boolean hasPt1 = false;
        boolean hasPt2 = false;
        for (String row : rows) {
            if (row.startsWith("{1}-")) {
                hasPt1 = true;
            }
            if (row.startsWith("{2}-")) {
                hasPt2 = true;
            }
        }
        assertThat(hasPt1).isTrue();
        assertThat(hasPt2).isTrue();
    }

    private List<String> readPartBucketLevel(Predicate predicate) throws IOException {
        ReadBuilder rb = fileKeyRangesTable.newReadBuilder();
        if (predicate != null) {
            rb = rb.withFilter(predicate);
        }
        List<String> rows = new ArrayList<>();
        rb.newRead()
                .createReader(rb.newScan().plan())
                .forEachRemaining(
                        row ->
                                rows.add(
                                        row.getString(0)
                                                + "-"
                                                + row.getInt(1)
                                                + "-"
                                                + row.getInt(5)));
        return rows;
    }
}
