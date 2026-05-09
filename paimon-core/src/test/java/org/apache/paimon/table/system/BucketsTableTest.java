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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.Projection;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.paimon.catalog.Identifier.SYSTEM_TABLE_SPLITTER;
import static org.apache.paimon.table.system.BucketsTable.BUCKETS;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link BucketsTable}. */
public class BucketsTableTest extends TableTestBase {

    private Table bucketsTable;

    @BeforeEach
    public void before() throws Exception {
        String tableName = "MyTable";
        FileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(String.format("%s/%s.db/%s", warehouse, database, tableName));
        Schema schema =
                Schema.newBuilder()
                        .column("pk", DataTypes.INT())
                        .column("pt", DataTypes.INT())
                        .column("col1", DataTypes.INT())
                        .partitionKeys("pt")
                        .primaryKey("pk", "pt")
                        .option("bucket", "2")
                        .build();
        TableSchema tableSchema =
                SchemaUtils.forceCommit(new SchemaManager(fileIO, tablePath), schema);
        FileStoreTable table =
                FileStoreTableFactory.create(LocalFileIO.create(), tablePath, tableSchema);

        bucketsTable = catalog.getTable(identifier(tableName + SYSTEM_TABLE_SPLITTER + BUCKETS));

        write(table, GenericRow.of(1, 1, 1), GenericRow.of(1, 2, 5));
        write(table, GenericRow.of(1, 1, 3), GenericRow.of(1, 2, 4));
    }

    @Test
    public void testBucketsTable() throws Exception {
        assertThat(read(bucketsTable, new int[] {0, 1, 2, 4}))
                .containsExactlyInAnyOrder(
                        GenericRow.of(BinaryString.fromString("{1}"), 0, 2L, 2L),
                        GenericRow.of(BinaryString.fromString("{2}"), 0, 2L, 2L));
    }

    @Test
    public void testBucketsTableWithPartitionFilter() throws Exception {
        PredicateBuilder builder = new PredicateBuilder(BucketsTable.TABLE_TYPE);

        Predicate filter = builder.equal(0, BinaryString.fromString("{1}"));
        assertThat(readWithFilter(bucketsTable, filter, new int[] {0, 1, 2, 4}))
                .containsExactlyInAnyOrder(
                        GenericRow.of(BinaryString.fromString("{1}"), 0, 2L, 2L));

        filter = builder.equal(0, BinaryString.fromString("{2}"));
        assertThat(readWithFilter(bucketsTable, filter, new int[] {0, 1, 2, 4}))
                .containsExactlyInAnyOrder(
                        GenericRow.of(BinaryString.fromString("{2}"), 0, 2L, 2L));

        filter =
                builder.in(
                        0,
                        Arrays.asList(
                                BinaryString.fromString("{1}"), BinaryString.fromString("{2}")));
        assertThat(readWithFilter(bucketsTable, filter, new int[] {0, 1, 2, 4}))
                .containsExactlyInAnyOrder(
                        GenericRow.of(BinaryString.fromString("{1}"), 0, 2L, 2L),
                        GenericRow.of(BinaryString.fromString("{2}"), 0, 2L, 2L));

        filter = builder.equal(0, BinaryString.fromString("{999}"));
        assertThat(readWithFilter(bucketsTable, filter, new int[] {0, 1, 2, 4})).isEmpty();
    }

    @Test
    public void testBucketsTableWithBucketFilter() throws Exception {
        PredicateBuilder builder = new PredicateBuilder(BucketsTable.TABLE_TYPE);

        Predicate filter = builder.equal(1, 0);
        assertThat(readWithFilter(bucketsTable, filter, new int[] {0, 1, 2, 4}))
                .containsExactlyInAnyOrder(
                        GenericRow.of(BinaryString.fromString("{1}"), 0, 2L, 2L),
                        GenericRow.of(BinaryString.fromString("{2}"), 0, 2L, 2L));

        filter = builder.equal(1, 1);
        assertThat(readWithFilter(bucketsTable, filter, new int[] {0, 1, 2, 4})).isEmpty();
    }

    @Test
    public void testBucketsTableWithCombinedFilter() throws Exception {
        PredicateBuilder builder = new PredicateBuilder(BucketsTable.TABLE_TYPE);

        Predicate filter =
                PredicateBuilder.and(
                        builder.equal(0, BinaryString.fromString("{1}")), builder.equal(1, 0));
        assertThat(readWithFilter(bucketsTable, filter, new int[] {0, 1, 2, 4}))
                .containsExactlyInAnyOrder(
                        GenericRow.of(BinaryString.fromString("{1}"), 0, 2L, 2L));

        filter =
                PredicateBuilder.and(
                        builder.equal(0, BinaryString.fromString("{1}")), builder.equal(1, 1));
        assertThat(readWithFilter(bucketsTable, filter, new int[] {0, 1, 2, 4})).isEmpty();
    }

    @Test
    public void testBucketsTableUnsupportedPredicateFallsBackToFullScan() throws Exception {
        PredicateBuilder builder = new PredicateBuilder(BucketsTable.TABLE_TYPE);

        // isNotNull cannot be pushed down as partition filter — must fall back to full scan
        Predicate filter = builder.isNotNull(0);
        assertThat(readWithFilter(bucketsTable, filter, new int[] {0, 1, 2, 4})).hasSize(2);
    }

    private List<InternalRow> readWithFilter(Table table, Predicate filter, int[] projection)
            throws Exception {
        ReadBuilder readBuilder = table.newReadBuilder().withFilter(filter);
        if (projection != null) {
            readBuilder.withProjection(projection);
        }
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());
        InternalRowSerializer serializer =
                new InternalRowSerializer(
                        projection == null
                                ? table.rowType()
                                : Projection.of(projection).project(table.rowType()));
        List<InternalRow> rows = new ArrayList<>();
        reader.forEachRemaining(row -> rows.add(serializer.copy(row)));
        return rows;
    }
}
