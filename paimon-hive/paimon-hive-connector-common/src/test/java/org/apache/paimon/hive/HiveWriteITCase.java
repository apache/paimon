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

package org.apache.paimon.hive;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.hive.mapred.PaimonOutputFormat;
import org.apache.paimon.hive.objectinspector.PaimonObjectInspectorFactory;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.StringUtils;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.paimon.hive.FileStoreTestUtils.DATABASE_NAME;
import static org.apache.paimon.hive.FileStoreTestUtils.TABLE_NAME;
import static org.apache.paimon.hive.RandomGenericRowDataGenerator.randomBigDecimal;
import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for {@link PaimonStorageHandler} and {@link PaimonOutputFormat}. */
public class HiveWriteITCase extends HiveTestBase {

    private static String engine;

    private long commitIdentifier;

    @BeforeClass
    public static void beforeClass() {
        // only run with mr
        engine = "mr";
    }

    @Before
    public void before() {
        hiveShell.execute("SET hive.execution.engine=" + engine);
        commitIdentifier = 0;
    }

    private String createChangelogExternalTable(
            RowType rowType,
            List<String> partitionKeys,
            List<String> primaryKeys,
            List<InternalRow> data)
            throws Exception {

        return createChangelogExternalTable(rowType, partitionKeys, primaryKeys, data, "");
    }

    private String createChangelogExternalTable(
            RowType rowType,
            List<String> partitionKeys,
            List<String> primaryKeys,
            List<InternalRow> data,
            String tableName)
            throws Exception {
        String path = folder.newFolder().toURI().toString();
        String tableNameNotNull =
                StringUtils.isNullOrWhitespaceOnly(tableName) ? TABLE_NAME : tableName;
        String tablePath = String.format("%s/test_db.db/%s", path, tableNameNotNull);
        Options conf = new Options();
        conf.set(CatalogOptions.WAREHOUSE, path);
        conf.set(CoreOptions.BUCKET, 2);
        conf.set(CoreOptions.FILE_FORMAT, CoreOptions.FILE_FORMAT_AVRO);
        Identifier identifier = Identifier.create(DATABASE_NAME, tableNameNotNull);
        Table table =
                FileStoreTestUtils.createFileStoreTable(
                        conf, rowType, partitionKeys, primaryKeys, identifier);

        return writeData(table, tablePath, data);
    }

    private String createAppendOnlyExternalTable(
            RowType rowType, List<String> partitionKeys, List<InternalRow> data) throws Exception {
        return createAppendOnlyExternalTable(rowType, partitionKeys, data, "");
    }

    private String createAppendOnlyExternalTable(
            RowType rowType, List<String> partitionKeys, List<InternalRow> data, String tableName)
            throws Exception {
        String path = folder.newFolder().toURI().toString();
        String tableNameNotNull =
                StringUtils.isNullOrWhitespaceOnly(tableName) ? TABLE_NAME : tableName;
        String tablePath = String.format("%s/test_db.db/%s", path, tableNameNotNull);
        Options conf = new Options();
        conf.set(CatalogOptions.WAREHOUSE, path);
        conf.set(CoreOptions.BUCKET, 2);
        conf.set(CoreOptions.FILE_FORMAT, CoreOptions.FILE_FORMAT_AVRO);
        Identifier identifier = Identifier.create(DATABASE_NAME, tableNameNotNull);
        Table table =
                FileStoreTestUtils.createFileStoreTable(
                        conf, rowType, partitionKeys, Collections.emptyList(), identifier);

        return writeData(table, tablePath, data);
    }

    private String writeData(Table table, String path, List<InternalRow> data) throws Exception {
        StreamWriteBuilder streamWriteBuilder = table.newStreamWriteBuilder();
        StreamTableWrite write = streamWriteBuilder.newWrite();
        StreamTableCommit commit = streamWriteBuilder.newCommit();
        for (InternalRow rowData : data) {
            write.write(rowData);
            if (ThreadLocalRandom.current().nextInt(5) == 0) {
                commit.commit(commitIdentifier, write.prepareCommit(false, commitIdentifier));
                commitIdentifier++;
            }
        }
        commit.commit(commitIdentifier, write.prepareCommit(true, commitIdentifier));
        commitIdentifier++;
        write.close();
        commit.close();

        String tableName = "test_table_" + (UUID.randomUUID().toString().substring(0, 4));
        hiveShell.execute(
                String.join(
                        "\n",
                        Arrays.asList(
                                "CREATE EXTERNAL TABLE " + tableName + " ",
                                "STORED BY '" + PaimonStorageHandler.class.getName() + "'",
                                "LOCATION '" + path + "'")));
        return tableName;
    }

    @Test
    public void testInsert() throws Exception {
        List<InternalRow> emptyData = Collections.emptyList();

        String outputTableName =
                createAppendOnlyExternalTable(
                        RowType.of(
                                new DataType[] {
                                    DataTypes.INT(),
                                    DataTypes.INT(),
                                    DataTypes.BIGINT(),
                                    DataTypes.STRING()
                                },
                                new String[] {"pt", "a", "b", "c"}),
                        Collections.singletonList("pt"),
                        emptyData,
                        "hive_test_table_output");

        hiveShell.execute(
                "insert into " + outputTableName + " values (1,2,3,'Hello'),(4,5,6,'Fine')");
        List<String> select = hiveShell.executeQuery("select * from " + outputTableName);
        assertThat(select).containsExactly("1\t2\t3\tHello", "4\t5\t6\tFine");
    }

    @Test
    public void testWriteOnlyWithChangeLogTableOption() throws Exception {

        String innerName = "hive_test_table_output";

        String path = folder.newFolder().toURI().toString();
        String tablePath = String.format("%s/test_db.db/%s", path, innerName);
        Options conf = new Options();
        conf.set(CatalogOptions.WAREHOUSE, path);
        conf.set(CoreOptions.BUCKET, 1);
        conf.set(CoreOptions.FILE_FORMAT, CoreOptions.FILE_FORMAT_AVRO);
        Identifier identifier = Identifier.create(DATABASE_NAME, innerName);
        Table table =
                FileStoreTestUtils.createFileStoreTable(
                        conf,
                        RowType.of(
                                new DataType[] {
                                    DataTypes.INT(),
                                    DataTypes.INT(),
                                    DataTypes.INT(),
                                    DataTypes.STRING(),
                                },
                                new String[] {"pt", "a", "b", "c"}),
                        Collections.singletonList("pt"),
                        Arrays.asList("a", "pt"),
                        identifier);
        String tableName = "test_table_" + (UUID.randomUUID().toString().substring(0, 4));
        hiveShell.execute(
                String.join(
                        "\n",
                        Arrays.asList(
                                "CREATE EXTERNAL TABLE " + tableName + " ",
                                "STORED BY '" + PaimonStorageHandler.class.getName() + "'",
                                "LOCATION '" + tablePath + "'")));
        for (int i = 0; i < 5; i++) {
            hiveShell.execute(
                    "insert into " + tableName + " values (1,2,3,'Hello'),(4,5,6,'Fine')");
        }
        TableScan scan = table.newReadBuilder().newStreamScan();
        DataSplit split = (DataSplit) scan.plan().splits().get(0);
        // no compact snapshot
        assertThat(split.snapshotId()).isEqualTo(5L);
    }

    @Test
    public void testWriteOnlyWithAppendOnlyTableOption() throws Exception {

        String innerName = "hive_test_table_output";
        int maxCompact = 3;
        String path = folder.newFolder().toURI().toString();
        String tablePath = String.format("%s/test_db.db/%s", path, innerName);
        Options conf = new Options();
        conf.set(CatalogOptions.WAREHOUSE, path);
        conf.set(CoreOptions.BUCKET, 1);
        conf.set(CoreOptions.FILE_FORMAT, CoreOptions.FILE_FORMAT_AVRO);
        conf.set(CoreOptions.COMPACTION_MAX_FILE_NUM, maxCompact);
        Identifier identifier = Identifier.create(DATABASE_NAME, innerName);
        Table table =
                FileStoreTestUtils.createFileStoreTable(
                        conf,
                        RowType.of(
                                new DataType[] {
                                    DataTypes.INT(),
                                    DataTypes.INT(),
                                    DataTypes.INT(),
                                    DataTypes.STRING(),
                                },
                                new String[] {"pt", "a", "b", "c"}),
                        Collections.singletonList("pt"),
                        Collections.emptyList(),
                        identifier);
        String tableName = "test_table_" + (UUID.randomUUID().toString().substring(0, 4));
        hiveShell.execute(
                String.join(
                        "\n",
                        Arrays.asList(
                                "CREATE EXTERNAL TABLE " + tableName + " ",
                                "STORED BY '" + PaimonStorageHandler.class.getName() + "'",
                                "LOCATION '" + tablePath + "'")));
        for (int i = 0; i < maxCompact; i++) {
            hiveShell.execute(
                    "insert into " + tableName + " values (1,2,3,'Hello'),(4,5,6,'Fine')");
        }
        TableScan scan = table.newReadBuilder().newStreamScan();
        DataSplit split = (DataSplit) scan.plan().splits().get(0);
        // no compact snapshot
        assertThat(split.snapshotId()).isEqualTo(maxCompact);
    }

    @Test
    public void testInsertFromSelectWithPartitionWithPk() throws Exception {

        List<InternalRow> data =
                Arrays.asList(
                        GenericRow.of(1, 10, 100L, BinaryString.fromString("Hi")),
                        GenericRow.of(2, 10, 200L, BinaryString.fromString("Hello")),
                        GenericRow.of(1, 20, 300L, BinaryString.fromString("World")),
                        GenericRow.of(1, 10, 100L, BinaryString.fromString("Hi Again")),
                        GenericRow.ofKind(
                                RowKind.DELETE, 1, 20, 300L, BinaryString.fromString("World")),
                        GenericRow.of(2, 20, 100L, null),
                        GenericRow.of(1, 30, 200L, BinaryString.fromString("Store")));
        List<InternalRow> emptyData = Collections.emptyList();
        String tableName =
                createChangelogExternalTable(
                        RowType.of(
                                new DataType[] {
                                    DataTypes.INT(),
                                    DataTypes.INT(),
                                    DataTypes.BIGINT(),
                                    DataTypes.STRING()
                                },
                                new String[] {"pt", "a", "b", "c"}),
                        Collections.singletonList("pt"),
                        Arrays.asList("pt", "a"),
                        data);

        String outputTableName =
                createAppendOnlyExternalTable(
                        RowType.of(
                                new DataType[] {
                                    DataTypes.INT(),
                                    DataTypes.INT(),
                                    DataTypes.BIGINT(),
                                    DataTypes.STRING()
                                },
                                new String[] {"pt", "a", "b", "c"}),
                        Collections.singletonList("pt"),
                        emptyData,
                        "hive_test_table_output");

        hiveShell.execute("insert into " + outputTableName + " SELECT * FROM " + tableName);
        List<Object[]> select = hiveShell.executeStatement("select * from " + outputTableName);
        List<Object[]> expect = hiveShell.executeStatement("select * from " + tableName);
        assertThat(select.toArray()).containsExactlyInAnyOrder(expect.toArray());
    }

    @Test
    public void testInsertFromSelectNoPartitionWithPk() throws Exception {

        List<InternalRow> data =
                Arrays.asList(
                        GenericRow.of(
                                1,
                                10L,
                                BinaryString.fromString("Hi"),
                                Decimal.fromBigDecimal(randomBigDecimal(5, 3), 5, 3)),
                        GenericRow.of(
                                1,
                                20L,
                                BinaryString.fromString("Hello"),
                                Decimal.fromBigDecimal(randomBigDecimal(5, 3), 5, 3)),
                        GenericRow.of(
                                2,
                                30L,
                                BinaryString.fromString("World"),
                                Decimal.fromBigDecimal(randomBigDecimal(5, 3), 5, 3)),
                        GenericRow.of(
                                1,
                                10L,
                                BinaryString.fromString("Hi Again"),
                                Decimal.fromBigDecimal(randomBigDecimal(5, 3), 5, 3)),
                        GenericRow.ofKind(
                                RowKind.DELETE,
                                2,
                                30L,
                                BinaryString.fromString("World"),
                                Decimal.fromBigDecimal(randomBigDecimal(5, 3), 5, 3)),
                        GenericRow.of(
                                2, 40L, null, Decimal.fromBigDecimal(randomBigDecimal(5, 3), 5, 3)),
                        GenericRow.of(
                                3,
                                50L,
                                BinaryString.fromString("Store"),
                                Decimal.fromBigDecimal(randomBigDecimal(5, 3), 5, 3)));

        List<InternalRow> emptyData = Collections.emptyList();
        String tableName =
                createChangelogExternalTable(
                        RowType.of(
                                new DataType[] {
                                    DataTypes.INT(),
                                    DataTypes.BIGINT(),
                                    DataTypes.STRING(),
                                    DataTypes.DECIMAL(5, 3)
                                },
                                new String[] {"a", "b", "c", "d"}),
                        Collections.emptyList(),
                        Arrays.asList("a", "b"),
                        data);

        String outputTableName =
                createAppendOnlyExternalTable(
                        RowType.of(
                                new DataType[] {
                                    DataTypes.INT(),
                                    DataTypes.BIGINT(),
                                    DataTypes.STRING(),
                                    DataTypes.DECIMAL(5, 3)
                                },
                                new String[] {"a", "b", "c", "d"}),
                        Collections.emptyList(),
                        emptyData,
                        "hive_test_table_output");

        hiveShell.execute("insert into " + outputTableName + " SELECT * FROM " + tableName);
        List<Object[]> select = hiveShell.executeStatement("select * from " + outputTableName);
        List<Object[]> expect = hiveShell.executeStatement("select * from " + tableName);
        assertThat(select.toArray()).containsExactlyInAnyOrder(expect.toArray());
    }

    @Test
    public void testInsertFromSelectWhereWithPartitionWithPk() throws Exception {

        List<InternalRow> data =
                Arrays.asList(
                        GenericRow.of(1, 10, 100L, BinaryString.fromString("Hi")),
                        GenericRow.of(2, 10, 200L, BinaryString.fromString("Hello")),
                        GenericRow.of(1, 20, 300L, BinaryString.fromString("World")),
                        GenericRow.of(1, 10, 100L, BinaryString.fromString("Hi Again")),
                        GenericRow.ofKind(
                                RowKind.DELETE, 1, 20, 300L, BinaryString.fromString("World")),
                        GenericRow.of(2, 20, 100L, null),
                        GenericRow.of(1, 30, 200L, BinaryString.fromString("Store")));
        List<InternalRow> emptyData = Collections.emptyList();
        String tableName =
                createChangelogExternalTable(
                        RowType.of(
                                new DataType[] {
                                    DataTypes.INT(),
                                    DataTypes.INT(),
                                    DataTypes.BIGINT(),
                                    DataTypes.STRING()
                                },
                                new String[] {"pt", "a", "b", "c"}),
                        Collections.singletonList("pt"),
                        Arrays.asList("pt", "a"),
                        data);

        String outputTableName =
                createAppendOnlyExternalTable(
                        RowType.of(
                                new DataType[] {
                                    DataTypes.INT(),
                                    DataTypes.INT(),
                                    DataTypes.BIGINT(),
                                    DataTypes.STRING()
                                },
                                new String[] {"pt", "a", "b", "c"}),
                        Collections.singletonList("pt"),
                        emptyData,
                        "hive_test_table_output");

        hiveShell.execute(
                "insert into " + outputTableName + " SELECT * FROM " + tableName + " where a > 10");
        List<Object[]> select = hiveShell.executeStatement("select * from " + outputTableName);
        List<Object[]> expect =
                hiveShell.executeStatement("select * from " + tableName + " where a > 10");
        assertThat(select.toArray()).containsExactlyInAnyOrder(expect.toArray());
    }

    @Test
    public void testInsertFromSelectOrderWithPartitionWithPk() throws Exception {

        List<InternalRow> data =
                Arrays.asList(
                        GenericRow.of(1, 10, 100L, BinaryString.fromString("Hi")),
                        GenericRow.of(2, 10, 200L, BinaryString.fromString("Hello")),
                        GenericRow.of(1, 20, 300L, BinaryString.fromString("World")),
                        GenericRow.of(1, 10, 100L, BinaryString.fromString("Hi Again")),
                        GenericRow.ofKind(
                                RowKind.DELETE, 1, 20, 300L, BinaryString.fromString("World")),
                        GenericRow.of(2, 20, 100L, null),
                        GenericRow.of(1, 30, 200L, BinaryString.fromString("Store")));
        List<InternalRow> emptyData = Collections.emptyList();
        String tableName =
                createChangelogExternalTable(
                        RowType.of(
                                new DataType[] {
                                    DataTypes.INT(),
                                    DataTypes.INT(),
                                    DataTypes.BIGINT(),
                                    DataTypes.STRING()
                                },
                                new String[] {"pt", "a", "b", "c"}),
                        Collections.singletonList("pt"),
                        Arrays.asList("pt", "a"),
                        data);

        String outputTableName =
                createChangelogExternalTable(
                        RowType.of(
                                new DataType[] {
                                    DataTypes.INT(),
                                    DataTypes.INT(),
                                    DataTypes.BIGINT(),
                                    DataTypes.STRING()
                                },
                                new String[] {"pt", "a", "b", "c"}),
                        Collections.singletonList("pt"),
                        Arrays.asList("pt", "b"),
                        emptyData,
                        "hive_test_table_output");

        hiveShell.execute(
                "insert into "
                        + outputTableName
                        + " SELECT * FROM "
                        + tableName
                        + " order by b desc");
        List<Object[]> select = hiveShell.executeStatement("select * from " + outputTableName);
        List<Object[]> expect =
                hiveShell.executeStatement("select * from " + tableName + " order by b desc");
        assertThat(select.toArray()).containsExactlyInAnyOrder(expect.toArray());
    }

    @Test
    public void testInsertFromJoiningWithPartitionWithPk() throws Exception {

        List<InternalRow> leftData =
                Arrays.asList(
                        GenericRow.of(1, 10, 100L, BinaryString.fromString("Hi")),
                        GenericRow.of(2, 10, 200L, BinaryString.fromString("Hello")),
                        GenericRow.of(1, 20, 300L, BinaryString.fromString("World")),
                        GenericRow.of(1, 10, 100L, BinaryString.fromString("Hi Again")));
        List<InternalRow> rightData =
                Arrays.asList(
                        GenericRow.of(1, 10, 1L, BinaryString.fromString("HZY")),
                        GenericRow.of(2, 10, 2L, BinaryString.fromString("LN")),
                        GenericRow.of(1, 20, 3L, BinaryString.fromString("GOOD")),
                        GenericRow.of(1, 10, 4L, BinaryString.fromString("")));
        List<InternalRow> emptyData = Collections.emptyList();
        String leftTable =
                createAppendOnlyExternalTable(
                        RowType.of(
                                new DataType[] {
                                    DataTypes.INT(),
                                    DataTypes.INT(),
                                    DataTypes.BIGINT(),
                                    DataTypes.STRING()
                                },
                                new String[] {"pt", "a", "b", "c"}),
                        Collections.singletonList("pt"),
                        leftData);
        String rightTable =
                createAppendOnlyExternalTable(
                        RowType.of(
                                new DataType[] {
                                    DataTypes.INT(),
                                    DataTypes.INT(),
                                    DataTypes.BIGINT(),
                                    DataTypes.STRING()
                                },
                                new String[] {"pt", "a", "b", "c"}),
                        Collections.singletonList("pt"),
                        rightData);

        String outputTableName =
                createAppendOnlyExternalTable(
                        RowType.of(
                                new DataType[] {
                                    DataTypes.INT(),
                                    DataTypes.INT(),
                                    DataTypes.BIGINT(),
                                    DataTypes.STRING()
                                },
                                new String[] {"pt", "a", "b", "c"}),
                        Collections.singletonList("pt"),
                        emptyData,
                        "hive_test_table_output");

        hiveShell.execute(
                "insert into "
                        + outputTableName
                        + " SELECT r.pt as pt,l.a as a,l.b as b ,r.c as c FROM "
                        + leftTable
                        + " l left join "
                        + rightTable
                        + " r on l.a = r.a");
        List<Object[]> select = hiveShell.executeStatement("select * from " + outputTableName);
        List<Object[]> expect =
                hiveShell.executeStatement(
                        " SELECT r.pt as pt,l.a as a,l.b as b ,r.c as c FROM "
                                + leftTable
                                + " l left join "
                                + rightTable
                                + " r on l.a = r.a");
        assertThat(select.toArray()).containsExactlyInAnyOrder(expect.toArray());
    }

    @Test
    public void testInsertAllSupportedTypes() throws Exception {

        String root = folder.newFolder().toString();
        String tablePath = String.format("%s/test_db.db/hive_test_table", root);
        Options conf = new Options();
        conf.set(CatalogOptions.WAREHOUSE, root);
        conf.set(CoreOptions.FILE_FORMAT, CoreOptions.FILE_FORMAT_AVRO);
        conf.set("bucket", "1");
        Table table =
                FileStoreTestUtils.createFileStoreTable(
                        conf,
                        RandomGenericRowDataGenerator.ROW_TYPE,
                        Collections.emptyList(),
                        Collections.singletonList("f_int"));

        ThreadLocalRandom random = ThreadLocalRandom.current();
        List<GenericRow> input = new ArrayList<>();
        for (int i = random.nextInt(50); i > 0; i--) {
            while (true) {
                // pk must not be null
                GenericRow rowData = RandomGenericRowDataGenerator.generate();
                if (!rowData.isNullAt(3)) {
                    input.add(rowData);
                    break;
                }
            }
        }

        StreamWriteBuilder streamWriteBuilder = table.newStreamWriteBuilder();
        StreamTableWrite write = streamWriteBuilder.newWrite();
        StreamTableCommit commit = streamWriteBuilder.newCommit();
        for (GenericRow rowData : input) {
            write.write(rowData);
        }
        commit.commit(0, write.prepareCommit(true, 0));
        write.close();
        commit.close();

        hiveShell.execute(
                String.join(
                        "\n",
                        Arrays.asList(
                                "CREATE EXTERNAL TABLE test_table",
                                "STORED BY '" + PaimonStorageHandler.class.getName() + "'",
                                "LOCATION '" + tablePath + "'")));

        List<InternalRow> emptyData = Collections.emptyList();
        String outputTableName =
                createChangelogExternalTable(
                        RandomGenericRowDataGenerator.ROW_TYPE,
                        Collections.emptyList(),
                        Collections.singletonList("f_int"),
                        emptyData,
                        "hive_test_table_output");
        hiveShell.execute("insert into " + outputTableName + " SELECT * FROM test_table");
        List<Object[]> actual = hiveShell.executeStatement("select * from " + outputTableName);
        Map<Integer, GenericRow> expected = new HashMap<>();
        for (GenericRow rowData : input) {
            int key = rowData.getInt(3);
            expected.put(key, rowData);
        }
        // do not use assertThat(select.toArray()).containsExactlyInAnyOrder(expect.toArray());
        // because containsExactlyInAnyOrder cannot compare map objects correctly
        for (Object[] actualRow : actual) {
            int key = (int) actualRow[3];
            assertThat(expected.containsKey(key)).isTrue();
            GenericRow expectedRow = expected.get(key);
            assertThat(actualRow.length).isEqualTo(expectedRow.getFieldCount());
            for (int i = 0; i < actualRow.length; i++) {
                if (expectedRow.isNullAt(i)) {
                    assertThat(actualRow[i]).isNull();
                    continue;
                }
                ObjectInspector oi =
                        PaimonObjectInspectorFactory.create(
                                RandomGenericRowDataGenerator.LOGICAL_TYPES.get(i));
                switch (oi.getCategory()) {
                    case PRIMITIVE:
                        AbstractPrimitiveJavaObjectInspector primitiveOi =
                                (AbstractPrimitiveJavaObjectInspector) oi;
                        Object expectedObject =
                                primitiveOi.getPrimitiveJavaObject(expectedRow.getField(i));
                        if (expectedObject instanceof byte[]) {
                            assertThat((byte[]) actualRow[i])
                                    .containsExactly((byte[]) expectedObject);
                        } else if (expectedObject instanceof HiveDecimal) {
                            // HiveDecimal will remove trailing zeros
                            // so we have to compare it from the original DecimalData
                            assertThat(actualRow[i]).isEqualTo(expectedRow.getField(i).toString());
                        } else {
                            assertThat(String.valueOf(actualRow[i]))
                                    .isEqualTo(String.valueOf(expectedObject));
                        }
                        break;
                    case LIST:
                        ListObjectInspector listOi = (ListObjectInspector) oi;
                        assertThat(actualRow[i])
                                .isEqualTo(
                                        String.valueOf(listOi.getList(expectedRow.getField(i)))
                                                .replace(" ", ""));
                        break;
                    case MAP:
                        MapObjectInspector mapOi = (MapObjectInspector) oi;
                        Map<String, String> expectedMap = new HashMap<>();
                        mapOi.getMap(expectedRow.getField(i))
                                .forEach(
                                        (k, v) -> expectedMap.put(k.toString(), String.valueOf(v)));
                        String actualString = actualRow[i].toString();
                        actualString = actualString.substring(1, actualString.length() - 1);
                        for (String kv : actualString.split(",")) {
                            if (kv.trim().isEmpty()) {
                                continue;
                            }
                            String[] split = kv.split(":");
                            String k = split[0].substring(1, split[0].length() - 1);
                            assertThat(split[1]).isEqualTo(expectedMap.get(k));
                            expectedMap.remove(k);
                        }
                        break;
                    default:
                        throw new UnsupportedOperationException();
                }
            }
            expected.remove(key);
        }
        assertThat(expected).isEmpty();
    }

    @Test
    public void testInsertArrayOfPrimitiveType() throws Exception {

        List<InternalRow> data =
                Arrays.asList(
                        GenericRow.of(
                                1,
                                new GenericArray(
                                        Collections.singletonList(
                                                        BinaryString.fromString("xiaoyang"))
                                                .toArray()),
                                new GenericArray(
                                        Collections.singletonList(BinaryString.fromString("hi"))
                                                .toArray()),
                                new GenericArray(
                                        Collections.singletonList(
                                                        Decimal.fromBigDecimal(
                                                                randomBigDecimal(5, 3), 5, 3))
                                                .toArray())),
                        GenericRow.of(
                                1,
                                new GenericArray(
                                        Collections.singletonList(BinaryString.fromString("hzy"))
                                                .toArray()),
                                new GenericArray(
                                        Collections.singletonList(BinaryString.fromString("hello"))
                                                .toArray()),
                                new GenericArray(
                                        Collections.singletonList(
                                                        Decimal.fromBigDecimal(
                                                                randomBigDecimal(5, 3), 5, 3))
                                                .toArray())));

        List<InternalRow> emptyData = Collections.emptyList();
        String tableName =
                createChangelogExternalTable(
                        RowType.of(
                                new DataType[] {
                                    DataTypes.INT(),
                                    DataTypes.ARRAY(DataTypes.CHAR(20)),
                                    DataTypes.ARRAY(DataTypes.VARCHAR(100)),
                                    DataTypes.ARRAY(DataTypes.DECIMAL(5, 3))
                                },
                                new String[] {"a", "b", "c", "d"}),
                        Collections.emptyList(),
                        Arrays.asList("a"),
                        data);

        String outputTableName =
                createAppendOnlyExternalTable(
                        RowType.of(
                                new DataType[] {
                                    DataTypes.INT(),
                                    DataTypes.ARRAY(DataTypes.CHAR(20)),
                                    DataTypes.ARRAY(DataTypes.VARCHAR(100)),
                                    DataTypes.ARRAY(DataTypes.DECIMAL(5, 3))
                                },
                                new String[] {"a", "b", "c", "d"}),
                        Collections.emptyList(),
                        emptyData,
                        "hive_test_table_output");

        hiveShell.execute("insert into " + outputTableName + " SELECT * FROM " + tableName);
        List<Object[]> select = hiveShell.executeStatement("select * from " + outputTableName);
        List<Object[]> expect = hiveShell.executeStatement("select * from " + tableName);
        assertThat(select.toArray()).containsExactlyInAnyOrder(expect.toArray());
    }

    @Test
    public void testInsertArrayOfArrayType() throws Exception {

        List<InternalRow> data =
                Arrays.asList(
                        GenericRow.of(
                                1,
                                new GenericArray(
                                        Collections.singletonList(
                                                        new GenericArray(
                                                                Collections.singletonList(
                                                                                BinaryString
                                                                                        .fromString(
                                                                                                "xiaoyang"))
                                                                        .toArray()))
                                                .toArray()),
                                new GenericArray(
                                        Collections.singletonList(
                                                        new GenericArray(
                                                                Collections.singletonList(
                                                                                new GenericArray(
                                                                                        Collections
                                                                                                .singletonList(
                                                                                                        BinaryString
                                                                                                                .fromString(
                                                                                                                        "xiaoyang"))
                                                                                                .toArray()))
                                                                        .toArray()))
                                                .toArray())));

        List<InternalRow> emptyData = Collections.emptyList();
        String tableName =
                createChangelogExternalTable(
                        RowType.of(
                                new DataType[] {
                                    DataTypes.INT(),
                                    DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.STRING())),
                                    DataTypes.ARRAY(
                                            DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.STRING())))
                                },
                                new String[] {"a", "b", "c"}),
                        Collections.emptyList(),
                        Arrays.asList("a"),
                        data);

        String outputTableName =
                createAppendOnlyExternalTable(
                        RowType.of(
                                new DataType[] {
                                    DataTypes.INT(),
                                    DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.STRING())),
                                    DataTypes.ARRAY(
                                            DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.STRING())))
                                },
                                new String[] {"a", "b", "c"}),
                        Collections.emptyList(),
                        emptyData,
                        "hive_test_table_output");

        hiveShell.execute("insert into " + outputTableName + " SELECT * FROM " + tableName);
        List<Object[]> select = hiveShell.executeStatement("select * from " + outputTableName);
        List<Object[]> expect = hiveShell.executeStatement("select * from " + tableName);
        assertThat(select.toArray()).containsExactlyInAnyOrder(expect.toArray());
    }

    @Test
    public void testInsertArrayOfMapType() throws Exception {

        List<InternalRow> data =
                Arrays.asList(
                        GenericRow.of(
                                1,
                                new GenericArray(
                                        Collections.singletonList(
                                                        new GenericMap(
                                                                Collections.singletonMap(
                                                                        BinaryString.fromString(
                                                                                "xiaoyang"),
                                                                        BinaryString.fromString(
                                                                                "xiaolan"))))
                                                .toArray())));

        List<InternalRow> emptyData = Collections.emptyList();
        String tableName =
                createChangelogExternalTable(
                        RowType.of(
                                new DataType[] {
                                    DataTypes.INT(),
                                    DataTypes.ARRAY(
                                            DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
                                },
                                new String[] {"a", "b"}),
                        Collections.emptyList(),
                        Arrays.asList("a"),
                        data);

        String outputTableName =
                createAppendOnlyExternalTable(
                        RowType.of(
                                new DataType[] {
                                    DataTypes.INT(),
                                    DataTypes.ARRAY(
                                            DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
                                },
                                new String[] {"a", "b"}),
                        Collections.emptyList(),
                        emptyData,
                        "hive_test_table_output");

        hiveShell.execute("insert into " + outputTableName + " SELECT * FROM " + tableName);
        List<Object[]> select = hiveShell.executeStatement("select * from " + outputTableName);
        List<Object[]> expect = hiveShell.executeStatement("select * from " + tableName);
        assertThat(select.toArray()).containsExactlyInAnyOrder(expect.toArray());
    }

    @Test
    public void testInsertArrayOfRowType() throws Exception {

        List<InternalRow> data =
                Arrays.asList(
                        GenericRow.of(
                                1,
                                new GenericArray(
                                        Collections.singletonList(
                                                        GenericRow.of(
                                                                GenericRow.of(
                                                                        BinaryString.fromString(
                                                                                "xiaoyang")),
                                                                BinaryString.fromString("xiaolan")))
                                                .toArray())));

        List<InternalRow> emptyData = Collections.emptyList();
        String tableName =
                createChangelogExternalTable(
                        RowType.of(
                                new DataType[] {
                                    DataTypes.INT(),
                                    DataTypes.ARRAY(
                                            DataTypes.ROW(
                                                    new DataField(
                                                            2,
                                                            "b1",
                                                            DataTypes.ROW(
                                                                    new DataField(
                                                                            4,
                                                                            "b1_1",
                                                                            DataTypes.STRING()))),
                                                    new DataField(3, "b2", DataTypes.STRING())))
                                },
                                new String[] {"a", "b"}),
                        Collections.emptyList(),
                        Arrays.asList("a"),
                        data);

        String outputTableName =
                createAppendOnlyExternalTable(
                        RowType.of(
                                new DataType[] {
                                    DataTypes.INT(),
                                    DataTypes.ARRAY(
                                            DataTypes.ROW(
                                                    new DataField(
                                                            2,
                                                            "b1",
                                                            DataTypes.ROW(
                                                                    new DataField(
                                                                            4,
                                                                            "b1_1",
                                                                            DataTypes.STRING()))),
                                                    new DataField(3, "b2", DataTypes.STRING())))
                                },
                                new String[] {"a", "b"}),
                        Collections.emptyList(),
                        emptyData,
                        "hive_test_table_output");

        hiveShell.execute("insert into " + outputTableName + " SELECT * FROM " + tableName);
        List<Object[]> select = hiveShell.executeStatement("select * from " + outputTableName);
        List<Object[]> expect = hiveShell.executeStatement("select * from " + tableName);
        assertThat(select.toArray()).containsExactlyInAnyOrder(expect.toArray());
    }

    @Test
    public void testInsertMapOfPrimitiveType() throws Exception {

        List<InternalRow> data =
                Arrays.asList(
                        GenericRow.of(
                                1,
                                new GenericMap(
                                        Collections.singletonMap(
                                                BinaryString.fromString("xiaoyang"),
                                                BinaryString.fromString("xiaolan")))));

        List<InternalRow> emptyData = Collections.emptyList();
        String tableName =
                createChangelogExternalTable(
                        RowType.of(
                                new DataType[] {
                                    DataTypes.INT(),
                                    DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())
                                },
                                new String[] {"a", "b"}),
                        Collections.emptyList(),
                        Arrays.asList("a"),
                        data);

        String outputTableName =
                createAppendOnlyExternalTable(
                        RowType.of(
                                new DataType[] {
                                    DataTypes.INT(),
                                    DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())
                                },
                                new String[] {"a", "b"}),
                        Collections.emptyList(),
                        emptyData,
                        "hive_test_table_output");

        hiveShell.execute("insert into " + outputTableName + " SELECT * FROM " + tableName);
        List<Object[]> select = hiveShell.executeStatement("select * from " + outputTableName);
        List<Object[]> expect = hiveShell.executeStatement("select * from " + tableName);
        assertThat(select.toArray()).containsExactlyInAnyOrder(expect.toArray());
    }

    @Test
    public void testInsertMapOfArrayType() throws Exception {

        List<InternalRow> data =
                Arrays.asList(
                        GenericRow.of(
                                1,
                                new GenericMap(
                                        Collections.singletonMap(
                                                BinaryString.fromString("xiaoyang"),
                                                new GenericArray(
                                                        Collections.singletonList(
                                                                        BinaryString.fromString(
                                                                                "xiaoyang"))
                                                                .toArray())))));

        List<InternalRow> emptyData = Collections.emptyList();
        String tableName =
                createChangelogExternalTable(
                        RowType.of(
                                new DataType[] {
                                    DataTypes.INT(),
                                    DataTypes.MAP(
                                            DataTypes.STRING(), DataTypes.ARRAY(DataTypes.STRING()))
                                },
                                new String[] {"a", "b"}),
                        Collections.emptyList(),
                        Arrays.asList("a"),
                        data);

        String outputTableName =
                createAppendOnlyExternalTable(
                        RowType.of(
                                new DataType[] {
                                    DataTypes.INT(),
                                    DataTypes.MAP(
                                            DataTypes.STRING(), DataTypes.ARRAY(DataTypes.STRING()))
                                },
                                new String[] {"a", "b"}),
                        Collections.emptyList(),
                        emptyData,
                        "hive_test_table_output");

        hiveShell.execute("insert into " + outputTableName + " SELECT * FROM " + tableName);
        List<Object[]> select = hiveShell.executeStatement("select * from " + outputTableName);
        List<Object[]> expect = hiveShell.executeStatement("select * from " + tableName);
        assertThat(select.toArray()).containsExactlyInAnyOrder(expect.toArray());
    }

    @Test
    public void testInsertMapOfMapType() throws Exception {

        List<InternalRow> data =
                Arrays.asList(
                        GenericRow.of(
                                1,
                                new GenericMap(
                                        Collections.singletonMap(
                                                BinaryString.fromString("xiaolan"),
                                                new GenericMap(
                                                        Collections.singletonMap(
                                                                BinaryString.fromString("xiaoyang"),
                                                                BinaryString.fromString(
                                                                        "xiaolan")))))));

        List<InternalRow> emptyData = Collections.emptyList();
        String tableName =
                createChangelogExternalTable(
                        RowType.of(
                                new DataType[] {
                                    DataTypes.INT(),
                                    DataTypes.MAP(
                                            DataTypes.STRING(),
                                            DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
                                },
                                new String[] {"a", "b"}),
                        Collections.emptyList(),
                        Arrays.asList("a"),
                        data);

        String outputTableName =
                createAppendOnlyExternalTable(
                        RowType.of(
                                new DataType[] {
                                    DataTypes.INT(),
                                    DataTypes.MAP(
                                            DataTypes.STRING(),
                                            DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
                                },
                                new String[] {"a", "b"}),
                        Collections.emptyList(),
                        emptyData,
                        "hive_test_table_output");

        hiveShell.execute("insert into " + outputTableName + " SELECT * FROM " + tableName);
        List<Object[]> select = hiveShell.executeStatement("select * from " + outputTableName);
        List<Object[]> expect = hiveShell.executeStatement("select * from " + tableName);
        assertThat(select.toArray()).containsExactlyInAnyOrder(expect.toArray());
    }

    @Test
    public void testInsertMapOfRowType() throws Exception {

        List<InternalRow> data =
                Arrays.asList(
                        GenericRow.of(
                                1,
                                new GenericMap(
                                        Collections.singletonMap(
                                                BinaryString.fromString("xiaolan"),
                                                GenericRow.of(
                                                        GenericRow.of(
                                                                BinaryString.fromString(
                                                                        "xiaoyang")),
                                                        BinaryString.fromString("xiaolan"))))));

        List<InternalRow> emptyData = Collections.emptyList();
        String tableName =
                createChangelogExternalTable(
                        RowType.of(
                                new DataType[] {
                                    DataTypes.INT(),
                                    DataTypes.MAP(
                                            DataTypes.STRING(),
                                            DataTypes.ROW(
                                                    new DataField(
                                                            5,
                                                            "c1",
                                                            DataTypes.ROW(
                                                                    new DataField(
                                                                            6,
                                                                            "c1_1",
                                                                            DataTypes.STRING()))),
                                                    new DataField(7, "c2", DataTypes.STRING())))
                                },
                                new String[] {"a", "b"}),
                        Collections.emptyList(),
                        Arrays.asList("a"),
                        data);

        String outputTableName =
                createAppendOnlyExternalTable(
                        RowType.of(
                                new DataType[] {
                                    DataTypes.INT(),
                                    DataTypes.MAP(
                                            DataTypes.STRING(),
                                            DataTypes.ROW(
                                                    new DataField(
                                                            5,
                                                            "c1",
                                                            DataTypes.ROW(
                                                                    new DataField(
                                                                            6,
                                                                            "c1_1",
                                                                            DataTypes.STRING()))),
                                                    new DataField(7, "c2", DataTypes.STRING())))
                                },
                                new String[] {"a", "b"}),
                        Collections.emptyList(),
                        emptyData,
                        "hive_test_table_output");

        hiveShell.execute("insert into " + outputTableName + " SELECT * FROM " + tableName);
        List<Object[]> select = hiveShell.executeStatement("select * from " + outputTableName);
        List<Object[]> expect = hiveShell.executeStatement("select * from " + tableName);
        assertThat(select.toArray()).containsExactlyInAnyOrder(expect.toArray());
    }
}
