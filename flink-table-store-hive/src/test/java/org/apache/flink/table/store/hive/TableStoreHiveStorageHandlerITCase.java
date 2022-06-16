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
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.store.FileStoreTestUtils;
import org.apache.flink.table.store.file.FileStoreOptions;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.sink.TableWrite;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

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

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
        String path = folder.newFolder().toURI().toString();
        Configuration conf = new Configuration();
        conf.setString(FileStoreOptions.PATH, path);
        conf.setInteger(FileStoreOptions.BUCKET, 2);
        conf.setString(FileStoreOptions.FILE_FORMAT, "avro");
        FileStoreTable table =
                FileStoreTestUtils.createFileStoreTable(
                        conf,
                        RowType.of(
                                new LogicalType[] {
                                    DataTypes.INT().getLogicalType(),
                                    DataTypes.BIGINT().getLogicalType(),
                                    DataTypes.STRING().getLogicalType()
                                },
                                new String[] {"a", "b", "c"}),
                        Collections.emptyList(),
                        Arrays.asList("a", "b"));

        TableWrite write = table.newWrite();
        write.write(GenericRowData.of(1, 10L, StringData.fromString("Hi")));
        write.write(GenericRowData.of(1, 20L, StringData.fromString("Hello")));
        write.write(GenericRowData.of(2, 30L, StringData.fromString("World")));
        write.write(GenericRowData.of(1, 10L, StringData.fromString("Hi Again")));
        write.write(GenericRowData.ofKind(RowKind.DELETE, 2, 30L, StringData.fromString("World")));
        write.write(GenericRowData.of(2, 40L, StringData.fromString("Test")));
        table.newCommit().commit("0", write.prepareCommit());
        write.close();

        hiveShell.execute(
                String.join(
                        "\n",
                        Arrays.asList(
                                "CREATE EXTERNAL TABLE test_table",
                                "STORED BY '" + TableStoreHiveStorageHandler.class.getName() + "'",
                                "LOCATION '" + path + "'")));
        List<String> actual = hiveShell.executeQuery("SELECT b, a, c FROM test_table ORDER BY b");
        List<String> expected = Arrays.asList("10\t1\tHi Again", "20\t1\tHello", "40\t2\tTest");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testReadExternalTableWithoutPk() throws Exception {
        String path = folder.newFolder().toURI().toString();
        Configuration conf = new Configuration();
        conf.setString(FileStoreOptions.PATH, path);
        conf.setInteger(FileStoreOptions.BUCKET, 2);
        conf.setString(FileStoreOptions.FILE_FORMAT, "avro");
        FileStoreTable table =
                FileStoreTestUtils.createFileStoreTable(
                        conf,
                        RowType.of(
                                new LogicalType[] {
                                    DataTypes.INT().getLogicalType(),
                                    DataTypes.BIGINT().getLogicalType(),
                                    DataTypes.STRING().getLogicalType()
                                },
                                new String[] {"a", "b", "c"}),
                        Collections.emptyList(),
                        Collections.emptyList());

        TableWrite write = table.newWrite();
        write.write(GenericRowData.of(1, 10L, StringData.fromString("Hi")));
        write.write(GenericRowData.of(1, 20L, StringData.fromString("Hello")));
        write.write(GenericRowData.of(2, 30L, StringData.fromString("World")));
        write.write(GenericRowData.of(1, 10L, StringData.fromString("Hi")));
        write.write(GenericRowData.ofKind(RowKind.DELETE, 2, 30L, StringData.fromString("World")));
        write.write(GenericRowData.of(2, 40L, StringData.fromString("Test")));
        table.newCommit().commit("0", write.prepareCommit());
        write.close();

        hiveShell.execute(
                String.join(
                        "\n",
                        Arrays.asList(
                                "CREATE EXTERNAL TABLE test_table",
                                "STORED BY '" + TableStoreHiveStorageHandler.class.getName() + "'",
                                "LOCATION '" + path + "'")));
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
        FileStoreTable table =
                FileStoreTestUtils.createFileStoreTable(
                        conf,
                        RowType.of(
                                RandomGenericRowDataGenerator.LOGICAL_TYPES.toArray(
                                        new LogicalType[0]),
                                RandomGenericRowDataGenerator.FIELD_NAMES.toArray(new String[0])),
                        Collections.emptyList(),
                        Collections.singletonList("f_int"));

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

        TableWrite write = table.newWrite();
        for (GenericRowData rowData : input) {
            write.write(rowData);
        }
        table.newCommit().commit("0", write.prepareCommit());
        write.close();

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
                                "LOCATION '" + root + "'")));
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
                                RandomGenericRowDataGenerator.LOGICAL_TYPES.get(i));
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

    @Test
    public void testPredicatePushDown() throws Exception {
        String path = folder.newFolder().toURI().toString();
        Configuration conf = new Configuration();
        conf.setString(FileStoreOptions.PATH, path);
        conf.setString(FileStoreOptions.FILE_FORMAT, "avro");
        FileStoreTable table =
                FileStoreTestUtils.createFileStoreTable(
                        conf,
                        RowType.of(
                                new LogicalType[] {DataTypes.INT().getLogicalType()},
                                new String[] {"a"}),
                        Collections.emptyList(),
                        Collections.emptyList());

        // TODO add NaN related tests after FLINK-27627 and FLINK-27628 are fixed

        TableWrite write = table.newWrite();
        write.write(GenericRowData.of(1));
        table.newCommit().commit("0", write.prepareCommit());
        write.write(GenericRowData.of((Object) null));
        table.newCommit().commit("1", write.prepareCommit());
        write.write(GenericRowData.of(2));
        write.write(GenericRowData.of(3));
        write.write(GenericRowData.of((Object) null));
        table.newCommit().commit("2", write.prepareCommit());
        write.write(GenericRowData.of(4));
        write.write(GenericRowData.of(5));
        write.write(GenericRowData.of(6));
        table.newCommit().commit("3", write.prepareCommit());
        write.close();

        hiveShell.execute(
                String.join(
                        "\n",
                        Arrays.asList(
                                "CREATE EXTERNAL TABLE test_table",
                                "STORED BY '" + TableStoreHiveStorageHandler.class.getName() + "'",
                                "LOCATION '" + path + "'")));
        Assert.assertEquals(
                Arrays.asList("1", "5"),
                hiveShell.executeQuery("SELECT * FROM test_table WHERE a = 1 OR a = 5"));
        Assert.assertEquals(
                Arrays.asList("2", "3", "6"),
                hiveShell.executeQuery(
                        "SELECT * FROM test_table WHERE a <> 1 AND a <> 4 AND a <> 5"));
        Assert.assertEquals(
                Arrays.asList("2", "3", "6"),
                hiveShell.executeQuery(
                        "SELECT * FROM test_table WHERE NOT (a = 1 OR a = 5) AND NOT a = 4"));
        Assert.assertEquals(
                Arrays.asList("1", "2", "3"),
                hiveShell.executeQuery("SELECT * FROM test_table WHERE a < 4"));
        Assert.assertEquals(
                Arrays.asList("1", "2", "3"),
                hiveShell.executeQuery("SELECT * FROM test_table WHERE a <= 3"));
        Assert.assertEquals(
                Arrays.asList("4", "5", "6"),
                hiveShell.executeQuery("SELECT * FROM test_table WHERE a > 3"));
        Assert.assertEquals(
                Arrays.asList("4", "5", "6"),
                hiveShell.executeQuery("SELECT * FROM test_table WHERE a >= 4"));
        Assert.assertEquals(
                Arrays.asList("1", "3"),
                hiveShell.executeQuery("SELECT * FROM test_table WHERE a IN (0, 1, 3, 7)"));
        Assert.assertEquals(
                Arrays.asList("4", "6"),
                hiveShell.executeQuery(
                        "SELECT * FROM test_table WHERE a NOT IN (0, 1, 3, 2, 5, 7)"));
        Assert.assertEquals(
                Arrays.asList("2", "3"),
                hiveShell.executeQuery("SELECT * FROM test_table WHERE a BETWEEN 2 AND 3"));
        Assert.assertEquals(
                Arrays.asList("1", "5", "6"),
                hiveShell.executeQuery("SELECT * FROM test_table WHERE a NOT BETWEEN 2 AND 4"));
        Assert.assertEquals(
                Arrays.asList("NULL", "NULL"),
                hiveShell.executeQuery("SELECT * FROM test_table WHERE a IS NULL"));
        Assert.assertEquals(
                Arrays.asList("1", "2", "3", "4", "5", "6"),
                hiveShell.executeQuery("SELECT * FROM test_table WHERE a IS NOT NULL"));
    }

    @Test
    public void testDateAndTimestamp() throws Exception {
        String path = folder.newFolder().toURI().toString();
        Configuration conf = new Configuration();
        conf.setString(FileStoreOptions.PATH, path);
        conf.setString(FileStoreOptions.FILE_FORMAT, "avro");
        FileStoreTable table =
                FileStoreTestUtils.createFileStoreTable(
                        conf,
                        RowType.of(
                                new LogicalType[] {
                                    DataTypes.DATE().getLogicalType(),
                                    DataTypes.TIMESTAMP(3).getLogicalType()
                                },
                                new String[] {"dt", "ts"}),
                        Collections.emptyList(),
                        Collections.emptyList());

        TableWrite write = table.newWrite();
        write.write(
                GenericRowData.of(
                        375, /* 1971-01-11 */
                        TimestampData.fromLocalDateTime(
                                LocalDateTime.of(2022, 5, 17, 17, 29, 20))));
        table.newCommit().commit("0", write.prepareCommit());
        write.write(GenericRowData.of(null, null));
        table.newCommit().commit("1", write.prepareCommit());
        write.write(GenericRowData.of(376 /* 1971-01-12 */, null));
        write.write(
                GenericRowData.of(
                        null,
                        TimestampData.fromLocalDateTime(LocalDateTime.of(2022, 6, 18, 8, 30, 0))));
        table.newCommit().commit("2", write.prepareCommit());
        write.close();

        hiveShell.execute(
                String.join(
                        "\n",
                        Arrays.asList(
                                "CREATE EXTERNAL TABLE test_table",
                                "STORED BY '" + TableStoreHiveStorageHandler.class.getName() + "'",
                                "LOCATION '" + path + "'")));
        Assert.assertEquals(
                Collections.singletonList("1971-01-11\t2022-05-17 17:29:20.0"),
                hiveShell.executeQuery("SELECT * FROM test_table WHERE dt = '1971-01-11'"));
        Assert.assertEquals(
                Collections.singletonList("1971-01-11\t2022-05-17 17:29:20.0"),
                hiveShell.executeQuery(
                        "SELECT * FROM test_table WHERE ts = '2022-05-17 17:29:20'"));
        Assert.assertEquals(
                Collections.singletonList("1971-01-12\tNULL"),
                hiveShell.executeQuery("SELECT * FROM test_table WHERE dt = '1971-01-12'"));
        Assert.assertEquals(
                Collections.singletonList("NULL\t2022-06-18 08:30:00.0"),
                hiveShell.executeQuery(
                        "SELECT * FROM test_table WHERE ts = '2022-06-18 08:30:00'"));
    }
}
