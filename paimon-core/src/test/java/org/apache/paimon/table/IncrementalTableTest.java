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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.Pair;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.paimon.CoreOptions.INCREMENTAL_BETWEEN;
import static org.apache.paimon.data.BinaryString.fromString;
import static org.apache.paimon.io.DataFileTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link CoreOptions#INCREMENTAL_BETWEEN}. */
public class IncrementalTableTest extends TableTestBase {

    @Test
    public void testPrimaryKeyTable() throws Exception {
        Identifier identifier = identifier("T");
        Schema schema =
                Schema.newBuilder()
                        .column("pt", DataTypes.INT())
                        .column("pk", DataTypes.INT())
                        .column("col1", DataTypes.INT())
                        .partitionKeys("pt")
                        .primaryKey("pk", "pt")
                        .option("bucket", "1")
                        .build();
        catalog.createTable(identifier, schema, true);
        Table table = catalog.getTable(identifier);

        // snapshot 1: append
        write(
                table,
                GenericRow.of(1, 1, 1),
                GenericRow.of(1, 2, 1),
                GenericRow.of(1, 3, 1),
                GenericRow.of(2, 1, 1));

        // snapshot 2: append
        write(
                table,
                GenericRow.of(1, 1, 2),
                GenericRow.of(1, 2, 2),
                GenericRow.of(1, 4, 1),
                GenericRow.of(2, 1, 2));

        // snapshot 3: compact
        compact(table, row(1), 0);

        // snapshot 4: append
        write(
                table,
                GenericRow.of(1, 1, 3),
                GenericRow.of(1, 2, 3),
                GenericRow.of(2, 1, 3),
                GenericRow.of(2, 2, 1));

        // snapshot 5: append
        write(table, GenericRow.of(1, 1, 4), GenericRow.of(1, 2, 4), GenericRow.of(2, 1, 4));

        // snapshot 6: append
        write(table, GenericRow.of(1, 1, 5), GenericRow.of(1, 2, 5), GenericRow.of(2, 1, 5));

        List<InternalRow> result = read(table, Pair.of(INCREMENTAL_BETWEEN, "2,5"));
        assertThat(result)
                .containsExactlyInAnyOrder(
                        GenericRow.of(1, 1, 4),
                        GenericRow.of(1, 2, 4),
                        GenericRow.of(2, 1, 4),
                        GenericRow.of(2, 2, 1));
    }

    @Test
    public void testAppendTable() throws Exception {
        Identifier identifier = identifier("T");
        Schema schema =
                Schema.newBuilder()
                        .column("pt", DataTypes.INT())
                        .column("pk", DataTypes.INT())
                        .column("col1", DataTypes.INT())
                        .partitionKeys("pt")
                        .build();
        catalog.createTable(identifier, schema, true);
        Table table = catalog.getTable(identifier);

        // snapshot 1: append
        write(
                table,
                GenericRow.of(1, 1, 1),
                GenericRow.of(1, 2, 1),
                GenericRow.of(1, 3, 1),
                GenericRow.of(2, 1, 1));

        // snapshot 2: append
        write(
                table,
                GenericRow.of(1, 1, 2),
                GenericRow.of(1, 2, 2),
                GenericRow.of(1, 4, 1),
                GenericRow.of(2, 1, 2));

        // snapshot 3: append
        write(
                table,
                GenericRow.of(1, 1, 3),
                GenericRow.of(1, 2, 3),
                GenericRow.of(2, 1, 3),
                GenericRow.of(2, 2, 1));

        // snapshot 4: append
        write(table, GenericRow.of(1, 1, 4), GenericRow.of(1, 2, 4), GenericRow.of(2, 1, 4));

        // snapshot 5: append
        write(table, GenericRow.of(1, 1, 5), GenericRow.of(1, 2, 5), GenericRow.of(2, 1, 5));

        List<InternalRow> result = read(table, Pair.of(INCREMENTAL_BETWEEN, "2,4"));
        assertThat(result)
                .containsExactlyInAnyOrder(
                        GenericRow.of(1, 1, 3),
                        GenericRow.of(1, 2, 3),
                        GenericRow.of(2, 1, 3),
                        GenericRow.of(2, 2, 1),
                        GenericRow.of(1, 1, 4),
                        GenericRow.of(1, 2, 4),
                        GenericRow.of(2, 1, 4));
    }

    @Test
    public void testAuditLog() throws Exception {
        Identifier identifier = identifier("T");
        Schema schema =
                Schema.newBuilder()
                        .column("pt", DataTypes.INT())
                        .column("pk", DataTypes.INT())
                        .column("col1", DataTypes.INT())
                        .partitionKeys("pt")
                        .primaryKey("pk", "pt")
                        .option("bucket", "1")
                        .build();
        catalog.createTable(identifier, schema, true);
        Table table = catalog.getTable(identifier);

        // snapshot 1: append
        write(
                table,
                GenericRow.of(1, 1, 1),
                GenericRow.of(1, 2, 1),
                GenericRow.of(1, 3, 1),
                GenericRow.of(2, 1, 1));

        // snapshot 2: append + and -
        write(
                table,
                GenericRow.ofKind(RowKind.DELETE, 1, 1, 1),
                GenericRow.ofKind(RowKind.DELETE, 1, 2, 1),
                GenericRow.of(1, 4, 1),
                GenericRow.of(2, 1, 2));

        // snapshot 3: append + and -
        write(
                table,
                GenericRow.ofKind(RowKind.DELETE, 1, 3, 1),
                GenericRow.of(1, 1, 2),
                GenericRow.of(2, 1, 3),
                GenericRow.of(2, 2, 1));

        Table auditLog = catalog.getTable(identifier("T$audit_log"));
        List<InternalRow> result = read(auditLog, Pair.of(INCREMENTAL_BETWEEN, "1,3"));
        assertThat(result)
                .containsExactlyInAnyOrder(
                        GenericRow.of(fromString("+I"), 2, 1, 3),
                        GenericRow.of(fromString("+I"), 2, 2, 1),
                        GenericRow.of(fromString("+I"), 1, 1, 2),
                        GenericRow.of(fromString("+I"), 1, 4, 1),
                        GenericRow.of(fromString("-D"), 1, 2, 1),
                        GenericRow.of(fromString("-D"), 1, 3, 1));
    }

    @Test
    public void testTagIncremental() throws Exception {
        Identifier identifier = identifier("T");
        Schema schema =
                Schema.newBuilder()
                        .column("pt", DataTypes.INT())
                        .column("pk", DataTypes.INT())
                        .column("col1", DataTypes.INT())
                        .partitionKeys("pt")
                        .primaryKey("pk", "pt")
                        .option("bucket", "1")
                        .build();
        catalog.createTable(identifier, schema, true);
        Table table = catalog.getTable(identifier);
        Table auditLog = catalog.getTable(identifier("T$audit_log"));

        // snapshot 1: append
        write(
                table,
                GenericRow.of(1, 1, 1),
                GenericRow.of(1, 2, 1),
                GenericRow.of(1, 3, 1),
                GenericRow.of(1, 4, 1),
                GenericRow.of(1, 5, 1),
                GenericRow.of(2, 1, 1));

        // snapshot 2: append
        write(
                table,
                // DELETE
                GenericRow.ofKind(RowKind.DELETE, 1, 1, 1),
                // UPDATE
                GenericRow.of(1, 2, 2),
                // NEW
                GenericRow.of(1, 6, 1));

        // snapshot 3: compact
        compact(table, row(1), 0);

        table.createTag("TAG1", 1);
        table.createTag("TAG2", 2);
        table.createTag("TAG3", 3);

        // read tag1 tag2
        List<InternalRow> result = read(table, Pair.of(INCREMENTAL_BETWEEN, "TAG1,TAG2"));
        assertThat(result)
                .containsExactlyInAnyOrder(GenericRow.of(1, 2, 2), GenericRow.of(1, 6, 1));
        result = read(auditLog, Pair.of(INCREMENTAL_BETWEEN, "TAG1,TAG2"));
        assertThat(result)
                .containsExactlyInAnyOrder(
                        GenericRow.of(fromString("-D"), 1, 1, 1),
                        GenericRow.of(fromString("+I"), 1, 2, 2),
                        GenericRow.of(fromString("+I"), 1, 6, 1));

        // read tag1 tag3
        result = read(table, Pair.of(INCREMENTAL_BETWEEN, "TAG1,TAG3"));
        assertThat(result)
                .containsExactlyInAnyOrder(GenericRow.of(1, 2, 2), GenericRow.of(1, 6, 1));

        // read tag1 tag3 auditLog
        result = read(auditLog, Pair.of(INCREMENTAL_BETWEEN, "TAG1,TAG3"));
        assertThat(result)
                .containsExactlyInAnyOrder(
                        GenericRow.of(fromString("-D"), 1, 1, 1),
                        GenericRow.of(fromString("+I"), 1, 2, 2),
                        GenericRow.of(fromString("+I"), 1, 6, 1));

        // read tag1 tag3 projection
        result = read(table, new int[][] {{1}}, Pair.of(INCREMENTAL_BETWEEN, "TAG1,TAG3"));
        assertThat(result).containsExactlyInAnyOrder(GenericRow.of(2), GenericRow.of(6));

        assertThatThrownBy(() -> read(table, Pair.of(INCREMENTAL_BETWEEN, "TAG2,TAG1")))
                .hasMessageContaining(
                        "Tag end TAG1 with snapshot id 1 should be larger than tag start TAG2 with snapshot id 2");
    }

    @Test
    public void testAppendTableTag() throws Exception {
        Identifier identifier = identifier("T");
        Schema schema =
                Schema.newBuilder()
                        .column("pt", DataTypes.INT())
                        .column("pk", DataTypes.INT())
                        .column("col1", DataTypes.INT())
                        .partitionKeys("pt")
                        .build();
        catalog.createTable(identifier, schema, true);
        Table table = catalog.getTable(identifier);

        write(table, GenericRow.of(1, 1, 1));
        write(table, GenericRow.of(1, 1, 2));

        table.createTag("TAG1", 1);
        table.createTag("TAG2", 2);

        assertThat(read(table, Pair.of(INCREMENTAL_BETWEEN, "TAG1,TAG2")))
                .containsExactlyInAnyOrder(GenericRow.of(1, 1, 2));
    }
}
