/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store.hive;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connectors.hive.FlinkEmbeddedHiveRunner;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowDataUtil;
import org.apache.flink.table.store.FileStoreTestHelper;
import org.apache.flink.table.store.file.FileStoreOptions;
import org.apache.flink.table.store.file.ValueKind;
import org.apache.flink.table.store.file.mergetree.compact.DeduplicateMergeFunction;
import org.apache.flink.table.store.file.mergetree.compact.ValueCountMergeFunction;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * IT cases for {@link TableStoreHiveStorageHandler} and {@link
 * org.apache.flink.table.store.mapred.TableStoreInputFormat}.
 */
@RunWith(FlinkEmbeddedHiveRunner.class)
public class TableStoreHiveStorageHandlerITCase {

    @Rule public TemporaryFolder folder = new TemporaryFolder();

    @HiveSQL(files = {})
    private static HiveShell hiveShell;

    @Before
    public void before() {
        hiveShell.execute("CREATE DATABASE IF NOT EXISTS test_db");
        hiveShell.execute("USE test_db");
    }

    @After
    public void after() {
        hiveShell.execute("DROP DATABASE IF EXISTS test_db CASCADE");
    }

    @Test
    public void testReadExternalTableWithPk() throws Exception {
        String root = folder.getRoot().getPath();
        Configuration conf = new Configuration();
        conf.setString(FileStoreOptions.PATH, root);
        conf.setInteger(FileStoreOptions.BUCKET, 2);
        conf.setString(FileStoreOptions.FILE_FORMAT, "avro");
        FileStoreTestHelper helper =
                new FileStoreTestHelper(
                        ObjectIdentifier.of("test_catalog", "test_db", "test_table"),
                        conf,
                        RowType.of(),
                        RowType.of(
                                new LogicalType[] {
                                    DataTypes.INT().getLogicalType(),
                                    DataTypes.BIGINT().getLogicalType()
                                },
                                new String[] {"_KEY_a", "_KEY_b"}),
                        RowType.of(
                                new LogicalType[] {
                                    DataTypes.INT().getLogicalType(),
                                    DataTypes.BIGINT().getLogicalType(),
                                    DataTypes.STRING().getLogicalType()
                                },
                                new String[] {"a", "b", "c"}),
                        new DeduplicateMergeFunction(),
                        (k, v) -> BinaryRowDataUtil.EMPTY_ROW,
                        k -> k.getInt(0) % 2);

        helper.write(
                ValueKind.ADD,
                GenericRowData.of(1, 10L),
                GenericRowData.of(1, 10L, StringData.fromString("Hi")));
        helper.write(
                ValueKind.ADD,
                GenericRowData.of(1, 20L),
                GenericRowData.of(1, 20L, StringData.fromString("Hello")));
        helper.write(
                ValueKind.ADD,
                GenericRowData.of(2, 30L),
                GenericRowData.of(2, 30L, StringData.fromString("World")));
        helper.write(
                ValueKind.ADD,
                GenericRowData.of(1, 10L),
                GenericRowData.of(1, 10L, StringData.fromString("Hi Again")));
        helper.write(
                ValueKind.DELETE,
                GenericRowData.of(2, 30L),
                GenericRowData.of(2, 30L, StringData.fromString("World")));
        helper.write(
                ValueKind.ADD,
                GenericRowData.of(2, 40L),
                GenericRowData.of(2, 40L, StringData.fromString("Test")));
        helper.finishWrite();

        hiveShell.execute(
                String.join(
                        "\n",
                        Arrays.asList(
                                "CREATE EXTERNAL TABLE test_table (",
                                "  a INT,",
                                "  b BIGINT,",
                                "  c STRING",
                                ")",
                                "STORED BY '" + TableStoreHiveStorageHandler.class.getName() + "'",
                                "LOCATION '"
                                        + root
                                        + "/test_catalog.catalog/test_db.db/test_table'",
                                "TBLPROPERTIES (",
                                "  'table-store.catalog' = 'test_catalog',",
                                "  'table-store.primary-keys' = 'a,b',",
                                "  'table-store.bucket' = '2',",
                                "  'table-store.file.format' = 'avro'",
                                ")")));
        List<String> actual = hiveShell.executeQuery("SELECT b, a, c FROM test_table ORDER BY b");
        List<String> expected = Arrays.asList("10\t1\tHi Again", "20\t1\tHello", "40\t2\tTest");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testReadExternalTableWithoutPk() throws Exception {
        String root = folder.getRoot().getPath();
        Configuration conf = new Configuration();
        conf.setString(FileStoreOptions.PATH, root);
        conf.setInteger(FileStoreOptions.BUCKET, 2);
        conf.setString(FileStoreOptions.FILE_FORMAT, "avro");
        FileStoreTestHelper helper =
                new FileStoreTestHelper(
                        ObjectIdentifier.of("test_catalog", "test_db", "test_table"),
                        conf,
                        RowType.of(),
                        RowType.of(
                                new LogicalType[] {
                                    DataTypes.INT().getLogicalType(),
                                    DataTypes.BIGINT().getLogicalType(),
                                    DataTypes.STRING().getLogicalType()
                                },
                                new String[] {"a", "b", "c"}),
                        RowType.of(
                                new LogicalType[] {DataTypes.BIGINT().getLogicalType()},
                                new String[] {"_VALUE_COUNT"}),
                        new ValueCountMergeFunction(),
                        (k, v) -> BinaryRowDataUtil.EMPTY_ROW,
                        k -> k.getInt(0) % 2);

        helper.write(
                ValueKind.ADD,
                GenericRowData.of(1, 10L, StringData.fromString("Hi")),
                GenericRowData.of(1L));
        helper.write(
                ValueKind.ADD,
                GenericRowData.of(1, 20L, StringData.fromString("Hello")),
                GenericRowData.of(1L));
        helper.write(
                ValueKind.ADD,
                GenericRowData.of(2, 30L, StringData.fromString("World")),
                GenericRowData.of(1L));
        helper.write(
                ValueKind.ADD,
                GenericRowData.of(1, 10L, StringData.fromString("Hi")),
                GenericRowData.of(1L));
        helper.write(
                ValueKind.DELETE,
                GenericRowData.of(2, 30L, StringData.fromString("World")),
                GenericRowData.of(1L));
        helper.write(
                ValueKind.ADD,
                GenericRowData.of(2, 40L, StringData.fromString("Test")),
                GenericRowData.of(1L));
        helper.finishWrite();

        hiveShell.execute(
                String.join(
                        "\n",
                        Arrays.asList(
                                "CREATE EXTERNAL TABLE test_table (",
                                "  a INT,",
                                "  b BIGINT,",
                                "  c STRING",
                                ")",
                                "STORED BY '" + TableStoreHiveStorageHandler.class.getName() + "'",
                                "LOCATION '"
                                        + root
                                        + "/test_catalog.catalog/test_db.db/test_table'",
                                "TBLPROPERTIES (",
                                "  'table-store.catalog' = 'test_catalog',",
                                "  'table-store.bucket' = '2',",
                                "  'table-store.file.format' = 'avro'",
                                ")")));
        List<String> actual = hiveShell.executeQuery("SELECT b, a, c FROM test_table ORDER BY b");
        List<String> expected =
                Arrays.asList("10\t1\tHi", "10\t1\tHi", "20\t1\tHello", "40\t2\tTest");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testReadAllSupportedTypes() throws Exception {
        String root = folder.newFolder().toString();
        Configuration conf = new Configuration();
        conf.setString(FileStoreOptions.PATH, root);
        conf.setString(FileStoreOptions.FILE_FORMAT, "avro");
        FileStoreTestHelper helper =
                new FileStoreTestHelper(
                        ObjectIdentifier.of("test_catalog", "test_db", "test_table"),
                        conf,
                        RowType.of(),
                        RowType.of(
                                new LogicalType[] {DataTypes.INT().getLogicalType()},
                                new String[] {"_KEY_f_int"}),
                        RowType.of(
                                RandomGenericRowDataGenerator.TYPE_INFOS.stream()
                                        .map(
                                                t ->
                                                        HiveTypeUtils.typeInfoToDataType(t)
                                                                .getLogicalType())
                                        .toArray(LogicalType[]::new),
                                RandomGenericRowDataGenerator.FIELD_NAMES.toArray(new String[0])),
                        new DeduplicateMergeFunction(),
                        (k, v) -> BinaryRowDataUtil.EMPTY_ROW,
                        k -> 0);

        ThreadLocalRandom random = ThreadLocalRandom.current();
        List<GenericRowData> input = new ArrayList<>();
        for (int i = random.nextInt(10); i > 0; i--) {
            while (true) {
                // pk must not be null
                GenericRowData rowData = RandomGenericRowDataGenerator.generate();
                if (!rowData.isNullAt(3)) {
                    input.add(rowData);
                    break;
                }
            }
        }
        for (GenericRowData rowData : input) {
            helper.write(ValueKind.ADD, GenericRowData.of(rowData.getInt(3)), rowData);
        }
        helper.finishWrite();

        StringBuilder ddl = new StringBuilder();
        for (int i = 0; i < RandomGenericRowDataGenerator.FIELD_NAMES.size(); i++) {
            if (i != 0) {
                ddl.append(",\n");
            }
            ddl.append("  ")
                    .append(RandomGenericRowDataGenerator.FIELD_NAMES.get(i))
                    .append(" ")
                    .append(RandomGenericRowDataGenerator.TYPE_NAMES.get(i))
                    .append(" COMMENT '")
                    .append(RandomGenericRowDataGenerator.FIELD_COMMENTS.get(i))
                    .append("'");
        }
        hiveShell.execute(
                String.join(
                        "\n",
                        Arrays.asList(
                                "CREATE EXTERNAL TABLE test_table (",
                                ddl.toString(),
                                ")",
                                "STORED BY '" + TableStoreHiveStorageHandler.class.getName() + "'",
                                "LOCATION '"
                                        + root
                                        + "/test_catalog.catalog/test_db.db/test_table'",
                                "TBLPROPERTIES (",
                                "  'table-store.catalog' = 'test_catalog',",
                                "  'table-store.primary-keys' = 'f_int',",
                                "  'table-store.file.format' = 'avro'",
                                ")")));
        List<Object[]> actual =
                hiveShell.executeStatement("SELECT * FROM test_table WHERE f_int > 0");

        Map<Integer, GenericRowData> expected = new HashMap<>();
        for (GenericRowData rowData : input) {
            int key = rowData.getInt(3);
            if (key > 0) {
                expected.put(key, rowData);
            }
        }
        for (Object[] actualRow : actual) {
            int key = (int) actualRow[3];
            Assert.assertTrue(expected.containsKey(key));
            GenericRowData expectedRow = expected.get(key);
            Assert.assertEquals(expectedRow.getArity(), actualRow.length);
            for (int i = 0; i < actualRow.length; i++) {
                if (expectedRow.isNullAt(i)) {
                    Assert.assertNull(actualRow[i]);
                    continue;
                }
                ObjectInspector oi =
                        HiveTypeUtils.getObjectInspector(
                                RandomGenericRowDataGenerator.TYPE_INFOS.get(i));
                switch (oi.getCategory()) {
                    case PRIMITIVE:
                        AbstractPrimitiveJavaObjectInspector primitiveOi =
                                (AbstractPrimitiveJavaObjectInspector) oi;
                        Object expectedObject =
                                primitiveOi.getPrimitiveJavaObject(expectedRow.getField(i));
                        if (expectedObject instanceof byte[]) {
                            Assert.assertArrayEquals(
                                    (byte[]) expectedObject, (byte[]) actualRow[i]);
                        } else if (expectedObject instanceof HiveDecimal) {
                            // HiveDecimal will remove trailing zeros
                            // so we have to compare it from the original DecimalData
                            Assert.assertEquals(expectedRow.getField(i).toString(), actualRow[i]);
                        } else {
                            Assert.assertEquals(
                                    String.valueOf(expectedObject), String.valueOf(actualRow[i]));
                        }
                        break;
                    case LIST:
                        ListObjectInspector listOi = (ListObjectInspector) oi;
                        Assert.assertEquals(
                                String.valueOf(listOi.getList(expectedRow.getField(i)))
                                        .replace(" ", ""),
                                actualRow[i]);
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
                            Assert.assertEquals(expectedMap.get(k), split[1]);
                            expectedMap.remove(k);
                        }
                        break;
                    default:
                        throw new UnsupportedOperationException();
                }
            }
            expected.remove(key);
        }
        Assert.assertTrue(expected.isEmpty());
    }
}
