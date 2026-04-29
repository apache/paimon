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
import org.apache.paimon.catalog.Identifier;
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
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.catalog.Identifier.SYSTEM_TABLE_SPLITTER;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link PartitionsTable}. */
public class PartitionsTableTest extends TableTestBase {

    private static final String tableName = "MyTable";

    protected FileStoreTable table;

    protected PartitionsTable partitionsTable;

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
                identifier(tableName + SYSTEM_TABLE_SPLITTER + PartitionsTable.PARTITIONS);
        partitionsTable = (PartitionsTable) catalog.getTable(filesTableId);

        // snapshot 1: append
        write(table, GenericRow.of(1, 1, 1), GenericRow.of(1, 3, 5));

        write(table, GenericRow.of(1, 1, 3), GenericRow.of(1, 2, 4));
    }

    @Test
    public void testPartitionRecordCount() throws Exception {
        List<InternalRow> expectedRow = new ArrayList<>();
        expectedRow.add(GenericRow.of(BinaryString.fromString("pt=1"), 2L));
        expectedRow.add(GenericRow.of(BinaryString.fromString("pt=2"), 1L));
        expectedRow.add(GenericRow.of(BinaryString.fromString("pt=3"), 1L));

        // Only read partition and record count, record size may not stable.
        List<InternalRow> result = read(partitionsTable, new int[] {0, 1});
        assertThat(result).containsExactlyInAnyOrderElementsOf(expectedRow);
    }

    @Test
    public void testPartitionTimeTravel() throws Exception {
        List<InternalRow> expectedRow = new ArrayList<>();
        expectedRow.add(GenericRow.of(BinaryString.fromString("pt=1"), 1L));
        expectedRow.add(GenericRow.of(BinaryString.fromString("pt=3"), 1L));

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
        expectedRow.add(GenericRow.of(BinaryString.fromString("pt=1"), 4L, 3L));
        expectedRow.add(GenericRow.of(BinaryString.fromString("pt=2"), 1L, 1L));
        expectedRow.add(GenericRow.of(BinaryString.fromString("pt=3"), 1L, 1L));

        List<InternalRow> result = read(partitionsTable, new int[] {0, 1, 3});
        assertThat(result).containsExactlyInAnyOrderElementsOf(expectedRow);
    }

    @Test
    void testPartitionAuditFieldsNull() throws Exception {
        List<InternalRow> result = read(partitionsTable, new int[] {0, 5, 6, 7, 8});
        assertThat(result).hasSize(3);

        for (InternalRow row : result) {
            assertThat(row.isNullAt(1)).isTrue(); // created_at
            assertThat(row.isNullAt(2)).isTrue(); //  created_by
            assertThat(row.isNullAt(3)).isTrue(); // updated_by
            assertThat(row.isNullAt(4)).isTrue(); // options
        }
    }

    @Test
    void testPartitionWithLegacyPartitionName() throws Exception {
        String testTableName = "TestLegacyTable";
        Schema testSchema =
                Schema.newBuilder()
                        .column("pk", DataTypes.INT())
                        .column("pt", DataTypes.INT())
                        .column("col1", DataTypes.INT())
                        .partitionKeys("pt")
                        .primaryKey("pk", "pt")
                        .option(CoreOptions.CHANGELOG_PRODUCER.key(), "input")
                        .option("bucket", "1")
                        .option(CoreOptions.PARTITION_GENERATE_LEGACY_NAME.key(), "false")
                        .build();

        Identifier testTableId = identifier(testTableName);
        catalog.createTable(testTableId, testSchema, true);
        FileStoreTable testTable = (FileStoreTable) catalog.getTable(testTableId);

        write(testTable, GenericRow.of(1, 10, 1), GenericRow.of(2, 20, 2));

        Identifier testPartitionsTableId =
                identifier(testTableName + SYSTEM_TABLE_SPLITTER + PartitionsTable.PARTITIONS);
        PartitionsTable testPartitionsTable =
                (PartitionsTable) catalog.getTable(testPartitionsTableId);

        List<InternalRow> expectedRow = new ArrayList<>();
        expectedRow.add(GenericRow.of(BinaryString.fromString("pt=10"), 1L));
        expectedRow.add(GenericRow.of(BinaryString.fromString("pt=20"), 1L));

        List<InternalRow> result = read(testPartitionsTable, new int[] {0, 1});
        assertThat(result).containsExactlyInAnyOrderElementsOf(expectedRow);
    }

    @Test
    public void testPartitionPredicateFilterEqual() throws Exception {
        PredicateBuilder builder = new PredicateBuilder(PartitionsTable.TABLE_TYPE);

        // Equal filter: partition = 'pt=1'
        Predicate filter = builder.equal(0, BinaryString.fromString("pt=1"));
        assertThat(readWithFilter(partitionsTable, filter, new int[] {0, 1}))
                .containsExactlyInAnyOrder(GenericRow.of(BinaryString.fromString("pt=1"), 2L));

        // Equal filter: partition = 'pt=2'
        filter = builder.equal(0, BinaryString.fromString("pt=2"));
        assertThat(readWithFilter(partitionsTable, filter, new int[] {0, 1}))
                .containsExactlyInAnyOrder(GenericRow.of(BinaryString.fromString("pt=2"), 1L));
    }

    @Test
    public void testPartitionPredicateFilterIn() throws Exception {
        PredicateBuilder builder = new PredicateBuilder(PartitionsTable.TABLE_TYPE);

        Predicate filter =
                builder.in(
                        0,
                        Arrays.asList(
                                BinaryString.fromString("pt=1"), BinaryString.fromString("pt=3")));
        assertThat(readWithFilter(partitionsTable, filter, new int[] {0, 1}))
                .containsExactlyInAnyOrder(
                        GenericRow.of(BinaryString.fromString("pt=1"), 2L),
                        GenericRow.of(BinaryString.fromString("pt=3"), 1L));
    }

    @Test
    public void testPartitionPredicateFilterNoMatch() throws Exception {
        PredicateBuilder builder = new PredicateBuilder(PartitionsTable.TABLE_TYPE);

        Predicate filter = builder.equal(0, BinaryString.fromString("pt=999"));
        assertThat(readWithFilter(partitionsTable, filter, new int[] {0, 1})).isEmpty();
    }

    @Test
    public void testPartitionPredicateFilterNonPartitionColumn() throws Exception {
        PredicateBuilder builder = new PredicateBuilder(PartitionsTable.TABLE_TYPE);

        // Filter on record_count column — should be safely ignored, return all partitions
        Predicate filter = builder.greaterThan(1, 0L);
        assertThat(readWithFilter(partitionsTable, filter, new int[] {0, 1})).hasSize(3);
    }

    @Test
    public void testPartitionPredicateFilterMultiColumnKeys() throws Exception {
        String testTableName = "MultiPartTable";
        Schema schema =
                Schema.newBuilder()
                        .column("pk", DataTypes.INT())
                        .column("dt", DataTypes.INT())
                        .column("region", DataTypes.INT())
                        .column("col1", DataTypes.INT())
                        .partitionKeys("dt", "region")
                        .primaryKey("pk", "dt", "region")
                        .option(CoreOptions.CHANGELOG_PRODUCER.key(), "input")
                        .option("bucket", "1")
                        .build();
        Identifier multiTableId = identifier(testTableName);
        catalog.createTable(multiTableId, schema, true);
        FileStoreTable multiTable = (FileStoreTable) catalog.getTable(multiTableId);

        Identifier multiPartitionsTableId =
                identifier(testTableName + SYSTEM_TABLE_SPLITTER + PartitionsTable.PARTITIONS);
        PartitionsTable multiPartitionsTable =
                (PartitionsTable) catalog.getTable(multiPartitionsTableId);

        write(multiTable, GenericRow.of(1, 20260410, 1, 100));
        write(multiTable, GenericRow.of(2, 20260410, 2, 200));
        write(multiTable, GenericRow.of(3, 20260411, 1, 300));

        PredicateBuilder builder = new PredicateBuilder(PartitionsTable.TABLE_TYPE);

        // Equal filter on multi-column partition
        Predicate filter = builder.equal(0, BinaryString.fromString("dt=20260410/region=1"));
        assertThat(readWithFilter(multiPartitionsTable, filter, new int[] {0, 1}))
                .containsExactlyInAnyOrder(
                        GenericRow.of(BinaryString.fromString("dt=20260410/region=1"), 1L));

        // IN filter on multi-column partition
        filter =
                builder.in(
                        0,
                        Arrays.asList(
                                BinaryString.fromString("dt=20260410/region=1"),
                                BinaryString.fromString("dt=20260411/region=1")));
        assertThat(readWithFilter(multiPartitionsTable, filter, new int[] {0, 1}))
                .containsExactlyInAnyOrder(
                        GenericRow.of(BinaryString.fromString("dt=20260410/region=1"), 1L),
                        GenericRow.of(BinaryString.fromString("dt=20260411/region=1"), 1L));
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
