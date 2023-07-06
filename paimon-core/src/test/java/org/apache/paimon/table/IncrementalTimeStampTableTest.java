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
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.paimon.CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP;
import static org.apache.paimon.io.DataFileTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link CoreOptions#INCREMENTAL_BETWEEN_TIMESTAMP}. */
public class IncrementalTimeStampTableTest extends TableTestBase {

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
                        .build();
        catalog.createTable(identifier, schema, true);
        Table table = catalog.getTable(identifier);
        Path tablePath = new Path(String.format("%s/%s.db/%s", warehouse, database, "T"));
        SnapshotManager snapshotManager = new SnapshotManager(LocalFileIO.create(), tablePath);

        Long timestampEarliest = System.currentTimeMillis();
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
        Long timestampSnapshot2 = snapshotManager.snapshot(2).timeMillis();

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
        Long timestampSnapshot4 = snapshotManager.snapshot(5).timeMillis();

        // snapshot 6: append
        write(table, GenericRow.of(1, 1, 5), GenericRow.of(1, 2, 5), GenericRow.of(2, 1, 5));

        List<InternalRow> result1 =
                read(
                        table,
                        Pair.of(
                                INCREMENTAL_BETWEEN_TIMESTAMP,
                                String.format("%s,%s", timestampEarliest - 1, timestampEarliest)));
        assertThat(result1).isEmpty();

        List<InternalRow> result2 =
                read(
                        table,
                        Pair.of(
                                INCREMENTAL_BETWEEN_TIMESTAMP,
                                String.format("%s,%s", timestampEarliest, timestampSnapshot2)));
        assertThat(result2)
                .containsExactlyInAnyOrder(
                        GenericRow.of(1, 1, 2),
                        GenericRow.of(1, 2, 2),
                        GenericRow.of(1, 3, 1),
                        GenericRow.of(1, 4, 1),
                        GenericRow.of(2, 1, 2));

        List<InternalRow> result3 =
                read(
                        table,
                        Pair.of(
                                INCREMENTAL_BETWEEN_TIMESTAMP,
                                String.format("%s,%s", timestampSnapshot2, timestampSnapshot4)));
        assertThat(result3)
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
        Path tablePath = new Path(String.format("%s/%s.db/%s", warehouse, database, "T"));
        SnapshotManager snapshotManager = new SnapshotManager(LocalFileIO.create(), tablePath);
        Long timestampEarliest = System.currentTimeMillis();
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
        Long timestampSnapshot2 = snapshotManager.snapshot(2).timeMillis();

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

        Long timestampSnapshot4 = snapshotManager.snapshot(4).timeMillis();

        List<InternalRow> result1 =
                read(
                        table,
                        Pair.of(
                                INCREMENTAL_BETWEEN_TIMESTAMP,
                                String.format("%s,%s", timestampEarliest - 1, timestampEarliest)));
        assertThat(result1).isEmpty();

        List<InternalRow> result2 =
                read(
                        table,
                        Pair.of(
                                INCREMENTAL_BETWEEN_TIMESTAMP,
                                String.format("%s,%s", timestampEarliest, timestampSnapshot2)));
        assertThat(result2)
                .containsExactlyInAnyOrder(
                        GenericRow.of(1, 1, 1),
                        GenericRow.of(1, 1, 2),
                        GenericRow.of(1, 2, 1),
                        GenericRow.of(1, 2, 2),
                        GenericRow.of(1, 3, 1),
                        GenericRow.of(1, 4, 1),
                        GenericRow.of(2, 1, 1),
                        GenericRow.of(2, 1, 2));

        List<InternalRow> result3 =
                read(
                        table,
                        Pair.of(
                                INCREMENTAL_BETWEEN_TIMESTAMP,
                                String.format("%s,%s", timestampSnapshot2, timestampSnapshot4)));
        assertThat(result3)
                .containsExactlyInAnyOrder(
                        GenericRow.of(1, 1, 3),
                        GenericRow.of(1, 2, 3),
                        GenericRow.of(2, 1, 3),
                        GenericRow.of(2, 2, 1),
                        GenericRow.of(1, 1, 4),
                        GenericRow.of(1, 2, 4),
                        GenericRow.of(2, 1, 4));
    }
}
