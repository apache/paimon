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
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.hive.mapred.PaimonInputFormat;
import org.apache.paimon.hive.objectinspector.PaimonObjectInspectorFactory;
import org.apache.paimon.hive.runner.PaimonEmbeddedHiveRunner;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDateTime;
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
import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for {@link PaimonStorageHandler} and {@link PaimonInputFormat}. */
@RunWith(PaimonEmbeddedHiveRunner.class)
public class PaimonStorageHandlerITCase {

    @ClassRule public static TemporaryFolder folder = new TemporaryFolder();

    @HiveSQL(files = {})
    private static HiveShell hiveShell;

    private static String engine;

    private String warehouse;
    private String tablePath;
    private Identifier identifier;
    private String externalTable;
    private long commitIdentifier;

    @BeforeClass
    public static void beforeClass() {
        // TODO Currently FlinkEmbeddedHiveRunner can only be used for one test class,
        //  so we have to select engine randomly. Write our own Hive tester in the future.
        // engine = ThreadLocalRandom.current().nextBoolean() ? "mr" : "tez";
        engine = "mr";
    }

    @Before
    public void before() throws IOException {
        if ("mr".equals(engine)) {
            hiveShell.execute("SET hive.execution.engine=mr");
        } else if ("tez".equals(engine)) {
            hiveShell.execute("SET hive.execution.engine=tez");
            hiveShell.execute("SET tez.local.mode=true");
            hiveShell.execute("SET hive.jar.directory=" + folder.getRoot().getAbsolutePath());
            hiveShell.execute("SET tez.staging-dir=" + folder.getRoot().getAbsolutePath());
            // JVM will crash if we do not set this and include paimon-flink-common as dependency
            // not sure why
            // in real use case there won't be any Flink dependency in Hive's classpath, so it's OK
            hiveShell.execute("SET hive.tez.exec.inplace.progress=false");
        } else {
            throw new UnsupportedOperationException("Unsupported engine " + engine);
        }

        hiveShell.execute("CREATE DATABASE IF NOT EXISTS test_db");
        hiveShell.execute("USE test_db");

        warehouse = folder.newFolder().toURI().toString();
        tablePath = String.format("%s/test_db.db/%s", warehouse, TABLE_NAME);
        identifier = Identifier.create(DATABASE_NAME, TABLE_NAME);
        externalTable = "test_table_" + UUID.randomUUID().toString().substring(0, 4);
        commitIdentifier = 0;
    }

    @After
    public void after() {
        hiveShell.execute("DROP DATABASE IF EXISTS test_db CASCADE");
    }

    @Test
    public void testReadExternalTableNoPartitionWithPk() throws Exception {
        List<InternalRow> data =
                Arrays.asList(
                        GenericRow.of(1, 10L, BinaryString.fromString("Hi"), 100L),
                        GenericRow.of(1, 20L, BinaryString.fromString("Hello"), 200L),
                        GenericRow.of(2, 30L, BinaryString.fromString("World"), 300L),
                        GenericRow.of(1, 10L, BinaryString.fromString("Hi Again"), 1000L),
                        GenericRow.ofKind(
                                RowKind.DELETE, 2, 30L, BinaryString.fromString("World"), 300L),
                        GenericRow.of(2, 40L, null, 400L),
                        GenericRow.of(3, 50L, BinaryString.fromString("Store"), 200L));

        Options conf = getBasicConf();
        conf.set(CoreOptions.FILE_FORMAT, CoreOptions.FILE_FORMAT_AVRO);
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT(),
                            DataTypes.BIGINT(),
                            DataTypes.STRING(),
                            DataTypes.BIGINT()
                        },
                        new String[] {"a", "b", "c", "d"});
        conf.set(CoreOptions.BUCKET, 1);
        Table table =
                FileStoreTestUtils.createFileStoreTable(
                        conf,
                        rowType,
                        Collections.emptyList(),
                        Arrays.asList("a", "b"),
                        identifier);

        createExternalTable();
        writeData(table, data);

        List<String> actual =
                hiveShell.executeQuery("SELECT * FROM " + externalTable + " ORDER BY b");
        List<String> expected =
                Arrays.asList(
                        "1\t10\tHi Again\t1000",
                        "1\t20\tHello\t200",
                        "2\t40\tNULL\t400",
                        "3\t50\tStore\t200");
        assertThat(actual).isEqualTo(expected);

        actual = hiveShell.executeQuery("SELECT c, b FROM " + externalTable + " ORDER BY b");
        expected = Arrays.asList("Hi Again\t10", "Hello\t20", "NULL\t40", "Store\t50");
        assertThat(actual).isEqualTo(expected);

        actual =
                hiveShell.executeQuery(
                        "SELECT * FROM " + externalTable + " WHERE d > 200 ORDER BY b");
        expected = Arrays.asList("1\t10\tHi Again\t1000", "2\t40\tNULL\t400");
        assertThat(actual).isEqualTo(expected);

        actual =
                hiveShell.executeQuery(
                        "SELECT a, sum(d) FROM " + externalTable + " GROUP BY a ORDER BY a");
        expected = Arrays.asList("1\t1200", "2\t400", "3\t200");
        assertThat(actual).isEqualTo(expected);

        actual =
                hiveShell.executeQuery(
                        "SELECT d, sum(b) FROM " + externalTable + " GROUP BY d ORDER BY d");
        expected = Arrays.asList("200\t70", "400\t40", "1000\t10");
        assertThat(actual).isEqualTo(expected);

        actual =
                hiveShell.executeQuery(
                        "SELECT T1.a, T1.b, T1.d + T2.d FROM "
                                + externalTable
                                + " T1 INNER JOIN "
                                + externalTable
                                + " T2 ON T1.a = T2.a AND T1.b = T2.b ORDER BY T1.a, T1.b");
        expected = Arrays.asList("1\t10\t2000", "1\t20\t400", "2\t40\t800", "3\t50\t400");
        assertThat(actual).isEqualTo(expected);

        actual =
                hiveShell.executeQuery(
                        "SELECT T1.a, T1.b, T2.b, T1.d + T2.d FROM "
                                + externalTable
                                + " T1 INNER JOIN "
                                + externalTable
                                + " T2 ON T1.a = T2.a ORDER BY T1.a, T1.b, T2.b");
        expected =
                Arrays.asList(
                        "1\t10\t10\t2000",
                        "1\t10\t20\t1200",
                        "1\t20\t10\t1200",
                        "1\t20\t20\t400",
                        "2\t40\t40\t800",
                        "3\t50\t50\t400");
        assertThat(actual).isEqualTo(expected);

        long snapshotId = ((FileStoreTable) table).snapshotManager().latestSnapshot().id();

        // write new data
        data =
                Collections.singletonList(
                        GenericRow.of(1, 10L, BinaryString.fromString("Hi Time Travel"), 10000L));
        writeData(table, data);

        // validate new data
        actual = hiveShell.executeQuery("SELECT * FROM " + externalTable + " ORDER BY b");
        expected =
                Arrays.asList(
                        "1\t10\tHi Time Travel\t10000",
                        "1\t20\tHello\t200",
                        "2\t40\tNULL\t400",
                        "3\t50\tStore\t200");
        assertThat(actual).isEqualTo(expected);

        // test time travel
        hiveShell.execute("SET paimon.scan.snapshot-id=" + snapshotId);
        actual = hiveShell.executeQuery("SELECT * FROM " + externalTable + " ORDER BY b");
        expected =
                Arrays.asList(
                        "1\t10\tHi Again\t1000",
                        "1\t20\tHello\t200",
                        "2\t40\tNULL\t400",
                        "3\t50\tStore\t200");
        assertThat(actual).isEqualTo(expected);
        hiveShell.execute("SET paimon.scan.snapshot-id=null");
    }

    @Test
    public void testReadExternalTableWithPartitionWithPk() throws Exception {
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

        Options conf = getBasicConf();
        conf.set(CoreOptions.FILE_FORMAT, CoreOptions.FILE_FORMAT_AVRO);
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT(), DataTypes.INT(), DataTypes.BIGINT(), DataTypes.STRING()
                        },
                        new String[] {"pt", "a", "b", "c"});
        conf.set(CoreOptions.BUCKET, 1);
        Table table =
                FileStoreTestUtils.createFileStoreTable(
                        conf,
                        rowType,
                        Collections.singletonList("pt"),
                        Arrays.asList("pt", "a"),
                        identifier);

        createExternalTable();
        writeData(table, data);

        List<String> actual =
                hiveShell.executeQuery("SELECT * FROM " + externalTable + " ORDER BY pt, a");
        List<String> expected =
                Arrays.asList(
                        "1\t10\t100\tHi Again",
                        "1\t30\t200\tStore",
                        "2\t10\t200\tHello",
                        "2\t20\t100\tNULL");
        assertThat(actual).isEqualTo(expected);

        actual = hiveShell.executeQuery("SELECT c, a FROM " + externalTable + " ORDER BY c, a");
        expected = Arrays.asList("NULL\t20", "Hello\t10", "Hi Again\t10", "Store\t30");
        assertThat(actual).isEqualTo(expected);

        actual =
                hiveShell.executeQuery(
                        "SELECT * FROM " + externalTable + " WHERE b > 100 ORDER BY pt, a");
        expected = Arrays.asList("1\t30\t200\tStore", "2\t10\t200\tHello");
        assertThat(actual).isEqualTo(expected);

        actual =
                hiveShell.executeQuery(
                        "SELECT pt, sum(b), max(c) FROM "
                                + externalTable
                                + " GROUP BY pt ORDER BY pt");
        expected = Arrays.asList("1\t300\tStore", "2\t300\tHello");
        assertThat(actual).isEqualTo(expected);

        actual =
                hiveShell.executeQuery(
                        "SELECT a, sum(b), max(c) FROM "
                                + externalTable
                                + " GROUP BY a ORDER BY a");
        expected = Arrays.asList("10\t300\tHi Again", "20\t100\tNULL", "30\t200\tStore");
        assertThat(actual).isEqualTo(expected);

        actual =
                hiveShell.executeQuery(
                        "SELECT b, sum(a), max(c) FROM "
                                + externalTable
                                + " GROUP BY b ORDER BY b");
        expected = Arrays.asList("100\t30\tHi Again", "200\t40\tStore");
        assertThat(actual).isEqualTo(expected);

        actual =
                hiveShell.executeQuery(
                        "SELECT a, b FROM (SELECT T1.a AS a, T1.b + T2.b AS b FROM "
                                + externalTable
                                + " T1 JOIN "
                                + externalTable
                                + " T2 ON T1.a = T2.a) T3 ORDER BY a, b");
        expected = Arrays.asList("10\t200", "10\t300", "10\t300", "10\t400", "20\t200", "30\t400");
        assertThat(actual).isEqualTo(expected);

        actual =
                hiveShell.executeQuery(
                        "SELECT b, a FROM (SELECT T1.b AS b, T1.a + T2.a AS a FROM "
                                + externalTable
                                + " T1 JOIN "
                                + externalTable
                                + " T2 ON T1.b = T2.b) T3 ORDER BY b, a");
        expected =
                Arrays.asList(
                        "100\t20", "100\t30", "100\t30", "100\t40", "200\t20", "200\t40", "200\t40",
                        "200\t60");
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testReadExternalTableNoPartitionAppendOnly() throws Exception {
        List<InternalRow> data =
                Arrays.asList(
                        GenericRow.of(1, 10L, BinaryString.fromString("Hi"), 100L),
                        GenericRow.of(1, 20L, BinaryString.fromString("Hello"), 200L),
                        GenericRow.of(2, 30L, BinaryString.fromString("World"), 300L),
                        GenericRow.of(1, 10L, BinaryString.fromString("Hi Again"), 1000L),
                        GenericRow.of(2, 40L, null, 400L),
                        GenericRow.of(3, 50L, BinaryString.fromString("Store"), 200L));

        Options conf = getBasicConf();
        conf.set(CoreOptions.FILE_FORMAT, CoreOptions.FILE_FORMAT_AVRO);
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT(),
                            DataTypes.BIGINT(),
                            DataTypes.STRING(),
                            DataTypes.BIGINT()
                        },
                        new String[] {"a", "b", "c", "d"});
        Table table =
                FileStoreTestUtils.createFileStoreTable(
                        conf,
                        rowType,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        identifier);

        createExternalTable();
        writeData(table, data);

        List<String> actual =
                hiveShell.executeQuery("SELECT * FROM " + externalTable + " ORDER BY a, b, c");
        List<String> expected =
                Arrays.asList(
                        "1\t10\tHi\t100",
                        "1\t10\tHi Again\t1000",
                        "1\t20\tHello\t200",
                        "2\t30\tWorld\t300",
                        "2\t40\tNULL\t400",
                        "3\t50\tStore\t200");
        assertThat(actual).isEqualTo(expected);

        actual = hiveShell.executeQuery("SELECT c, b FROM " + externalTable + " ORDER BY c");
        expected =
                Arrays.asList(
                        "NULL\t40",
                        "Hello\t20",
                        "Hi\t10",
                        "Hi Again\t10",
                        "Store\t50",
                        "World\t30");
        assertThat(actual).isEqualTo(expected);

        actual =
                hiveShell.executeQuery(
                        "SELECT * FROM " + externalTable + " WHERE d < 300 ORDER BY b, d");
        expected = Arrays.asList("1\t10\tHi\t100", "1\t20\tHello\t200", "3\t50\tStore\t200");
        assertThat(actual).isEqualTo(expected);

        actual =
                hiveShell.executeQuery(
                        "SELECT a, sum(d) FROM " + externalTable + " GROUP BY a ORDER BY a");
        expected = Arrays.asList("1\t1300", "2\t700", "3\t200");
        assertThat(actual).isEqualTo(expected);

        actual =
                hiveShell.executeQuery(
                        "SELECT T1.a, T1.b, T2.b FROM "
                                + externalTable
                                + " T1 JOIN "
                                + externalTable
                                + " T2 ON T1.a = T2.a WHERE T1.a > 1 ORDER BY T1.a, T1.b, T2.b");
        expected = Arrays.asList("2\t30\t30", "2\t30\t40", "2\t40\t30", "2\t40\t40", "3\t50\t50");
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testReadExternalTableWithPartitionAppendOnly() throws Exception {
        List<InternalRow> data =
                Arrays.asList(
                        GenericRow.of(1, 10, 100L, BinaryString.fromString("Hi")),
                        GenericRow.of(2, 10, 200L, BinaryString.fromString("Hello")),
                        GenericRow.of(1, 20, 300L, BinaryString.fromString("World")),
                        GenericRow.of(1, 10, 100L, BinaryString.fromString("Hi Again")),
                        GenericRow.of(2, 20, 400L, null),
                        GenericRow.of(1, 30, 500L, BinaryString.fromString("Store")));

        Options conf = getBasicConf();
        conf.set(CoreOptions.FILE_FORMAT, CoreOptions.FILE_FORMAT_AVRO);
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT(), DataTypes.INT(), DataTypes.BIGINT(), DataTypes.STRING()
                        },
                        new String[] {"pt", "a", "b", "c"});
        Table table =
                FileStoreTestUtils.createFileStoreTable(
                        conf,
                        rowType,
                        Collections.singletonList("pt"),
                        Collections.emptyList(),
                        identifier);

        createExternalTable();
        writeData(table, data);

        List<String> actual =
                hiveShell.executeQuery("SELECT * FROM " + externalTable + " ORDER BY pt, a, c");
        List<String> expected =
                Arrays.asList(
                        "1\t10\t100\tHi",
                        "1\t10\t100\tHi Again",
                        "1\t20\t300\tWorld",
                        "1\t30\t500\tStore",
                        "2\t10\t200\tHello",
                        "2\t20\t400\tNULL");
        assertThat(actual).isEqualTo(expected);

        actual = hiveShell.executeQuery("SELECT c, b FROM " + externalTable + " ORDER BY c");
        expected =
                Arrays.asList(
                        "NULL\t400",
                        "Hello\t200",
                        "Hi\t100",
                        "Hi Again\t100",
                        "Store\t500",
                        "World\t300");
        assertThat(actual).isEqualTo(expected);

        actual =
                hiveShell.executeQuery(
                        "SELECT * FROM " + externalTable + " WHERE b < 400 ORDER BY b, c");
        expected =
                Arrays.asList(
                        "1\t10\t100\tHi",
                        "1\t10\t100\tHi Again",
                        "2\t10\t200\tHello",
                        "1\t20\t300\tWorld");
        assertThat(actual).isEqualTo(expected);

        actual =
                hiveShell.executeQuery(
                        "SELECT pt, max(a), min(c) FROM "
                                + externalTable
                                + " GROUP BY pt ORDER BY pt");
        expected = Arrays.asList("1\t30\tHi", "2\t20\tHello");
        assertThat(actual).isEqualTo(expected);

        actual =
                hiveShell.executeQuery(
                        "SELECT a, sum(b), min(c) FROM "
                                + externalTable
                                + " GROUP BY a ORDER BY a");
        expected = Arrays.asList("10\t400\tHello", "20\t700\tWorld", "30\t500\tStore");
        assertThat(actual).isEqualTo(expected);

        actual =
                hiveShell.executeQuery(
                        "SELECT T1.a, T1.b, T2.b FROM "
                                + externalTable
                                + " T1 JOIN "
                                + externalTable
                                + " T2 ON T1.a = T2.a WHERE T1.a > 10 ORDER BY T1.a, T1.b, T2.b");
        expected =
                Arrays.asList(
                        "20\t300\t300",
                        "20\t300\t400",
                        "20\t400\t300",
                        "20\t400\t400",
                        "30\t500\t500");
        assertThat(actual).isEqualTo(expected);
    }

    private void writeData(Table table, List<InternalRow> data) throws Exception {
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
    }

    @Test
    public void testReadAllSupportedTypes() throws Exception {
        Options conf = getBasicConf();
        conf.set(CoreOptions.FILE_FORMAT, CoreOptions.FILE_FORMAT_AVRO);
        conf.set(CoreOptions.BUCKET, 1);
        Table table =
                FileStoreTestUtils.createFileStoreTable(
                        conf,
                        RandomGenericRowDataGenerator.ROW_TYPE,
                        Collections.emptyList(),
                        Collections.singletonList("f_int"));

        ThreadLocalRandom random = ThreadLocalRandom.current();
        List<GenericRow> input = new ArrayList<>();
        for (int i = random.nextInt(10); i > 0; i--) {
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

        createExternalTable();
        List<Object[]> actual =
                hiveShell.executeStatement(
                        "SELECT * FROM `" + externalTable + "`  WHERE f_int > 0");

        Map<Integer, GenericRow> expected = new HashMap<>();
        for (GenericRow rowData : input) {
            int key = rowData.getInt(3);
            if (key > 0) {
                expected.put(key, rowData);
            }
        }
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
                            // HiveDecimal will remove trailing zeros,
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
    public void testPredicatePushDown() throws Exception {
        Options conf = getBasicConf();
        conf.set(CoreOptions.FILE_FORMAT, CoreOptions.FILE_FORMAT_AVRO);
        Table table =
                FileStoreTestUtils.createFileStoreTable(
                        conf,
                        RowType.of(new DataType[] {DataTypes.INT()}, new String[] {"a"}),
                        Collections.emptyList(),
                        Collections.emptyList());

        // TODO add NaN related tests

        StreamWriteBuilder streamWriteBuilder = table.newStreamWriteBuilder();
        StreamTableWrite write = streamWriteBuilder.newWrite();
        StreamTableCommit commit = streamWriteBuilder.newCommit();
        write.write(GenericRow.of(1));
        commit.commit(0, write.prepareCommit(true, 0));
        write.write(GenericRow.of((Object) null));
        commit.commit(1, write.prepareCommit(true, 1));
        write.write(GenericRow.of(2));
        write.write(GenericRow.of(3));
        write.write(GenericRow.of((Object) null));
        commit.commit(2, write.prepareCommit(true, 2));
        write.write(GenericRow.of(4));
        write.write(GenericRow.of(5));
        write.write(GenericRow.of(6));
        commit.commit(3, write.prepareCommit(true, 3));
        write.close();
        commit.close();
        hiveShell.execute(
                String.join(
                        "\n",
                        Arrays.asList(
                                "CREATE EXTERNAL TABLE test_table",
                                "STORED BY '" + PaimonStorageHandler.class.getName() + "'",
                                "LOCATION '" + tablePath + "'")));
        assertThat(hiveShell.executeQuery("SELECT * FROM test_table WHERE a = 1 OR a = 5"))
                .containsExactly("1", "5");
        assertThat(
                        hiveShell.executeQuery(
                                "SELECT * FROM test_table WHERE a <> 1 AND a <> 4 AND a <> 5"))
                .containsExactly("2", "3", "6");
        assertThat(
                        hiveShell.executeQuery(
                                "SELECT * FROM test_table WHERE NOT (a = 1 OR a = 5) AND NOT a = 4"))
                .containsExactly("2", "3", "6");
        assertThat(hiveShell.executeQuery("SELECT * FROM test_table WHERE a < 4"))
                .containsExactly("1", "2", "3");
        assertThat(hiveShell.executeQuery("SELECT * FROM test_table WHERE a <= 3"))
                .containsExactly("1", "2", "3");
        assertThat(hiveShell.executeQuery("SELECT * FROM test_table WHERE a > 3"))
                .containsExactly("4", "5", "6");
        assertThat(hiveShell.executeQuery("SELECT * FROM test_table WHERE a >= 4"))
                .containsExactly("4", "5", "6");
        assertThat(hiveShell.executeQuery("SELECT * FROM test_table WHERE a IN (0, 1, 3, 7)"))
                .containsExactly("1", "3");
        assertThat(hiveShell.executeQuery("SELECT * FROM test_table WHERE a IN (0, NULL, 3, 7)"))
                .containsExactly("3");
        assertThat(
                        hiveShell.executeQuery(
                                "SELECT * FROM test_table WHERE a NOT IN (0, 1, 3, 2, 5, 7)"))
                .containsExactly("4", "6");
        assertThat(
                        hiveShell.executeQuery(
                                "SELECT * FROM test_table WHERE a NOT IN (0, 1, NULL, 2, 5, 7)"))
                .isEmpty();
        assertThat(hiveShell.executeQuery("SELECT * FROM test_table WHERE a BETWEEN 2 AND 3"))
                .containsExactly("2", "3");
        assertThat(hiveShell.executeQuery("SELECT * FROM test_table WHERE a NOT BETWEEN 2 AND 4"))
                .containsExactly("1", "5", "6");
        assertThat(hiveShell.executeQuery("SELECT * FROM test_table WHERE a IS NULL"))
                .containsExactly("NULL", "NULL");
        assertThat(hiveShell.executeQuery("SELECT * FROM test_table WHERE a IS NOT NULL"))
                .containsExactly("1", "2", "3", "4", "5", "6");
    }

    @Test
    public void testDateAndTimestamp() throws Exception {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        Options conf = getBasicConf();

        String fileFormatType =
                random.nextBoolean()
                        ? CoreOptions.FILE_FORMAT_ORC
                        : CoreOptions.FILE_FORMAT_PARQUET;
        conf.set(CoreOptions.FILE_FORMAT, fileFormatType);

        int precision = random.nextInt(10);

        Table table =
                FileStoreTestUtils.createFileStoreTable(
                        conf,
                        RowType.of(
                                new DataType[] {DataTypes.DATE(), DataTypes.TIMESTAMP(precision)},
                                new String[] {"dt", "ts"}),
                        Collections.emptyList(),
                        Collections.emptyList());

        StreamWriteBuilder streamWriteBuilder = table.newStreamWriteBuilder();
        StreamTableWrite write = streamWriteBuilder.newWrite();
        StreamTableCommit commit = streamWriteBuilder.newCommit();
        write.write(
                GenericRow.of(
                        375, /* 1971-01-11 */
                        Timestamp.fromLocalDateTime(
                                LocalDateTime.of(2022, 5, 17, 17, 29, 20, 100_000_000))));
        commit.commit(0, write.prepareCommit(true, 0));
        write.write(GenericRow.of(null, null));
        commit.commit(1, write.prepareCommit(true, 1));
        write.write(GenericRow.of(376 /* 1971-01-12 */, null));
        write.write(
                GenericRow.of(
                        null,
                        Timestamp.fromLocalDateTime(
                                // to test different precisions
                                LocalDateTime.of(2022, 6, 18, 8, 30, 0, 123_456_789))));
        commit.commit(2, write.prepareCommit(true, 2));
        write.close();
        commit.close();

        createExternalTable();

        assertThat(
                        hiveShell.executeQuery(
                                "SELECT * FROM `" + externalTable + "` WHERE dt = '1971-01-11'"))
                .containsExactly("1971-01-11\t2022-05-17 17:29:20.1");
        assertThat(
                        hiveShell.executeQuery(
                                String.format(
                                        // do not test '.123456789' because the filter pushdown will
                                        // cause wrong result
                                        "SELECT * FROM `%s` WHERE ts = '2022-05-17 17:29:20.1'",
                                        externalTable)))
                .containsExactly("1971-01-11\t2022-05-17 17:29:20.1");

        assertThat(
                        hiveShell.executeQuery(
                                "SELECT * FROM `" + externalTable + "` WHERE dt = '1971-01-12'"))
                .containsExactly("1971-01-12\tNULL");

        // validate '2022-06-18 08:30:00.123456789'
        // the original precision is maintained, but the file format will affect the result
        // parquet stores timestamp with three forms
        String fraction;
        if (fileFormatType.equals(CoreOptions.FILE_FORMAT_ORC)) {
            fraction = ".123456789";
        } else {
            if (precision <= 3) {
                fraction = ".123";
            } else if (precision <= 6) {
                fraction = ".123456";
            } else {
                fraction = ".123456789";
            }
        }
        assertThat(
                        hiveShell.executeQuery(
                                "SELECT * FROM `"
                                        + externalTable
                                        + "` WHERE dt IS NULL and ts IS NOT NULL"))
                .containsExactly("NULL\t2022-06-18 08:30:00" + fraction);
    }

    @Test
    public void testTime() throws Exception {
        Options options = getBasicConf();
        options.set(CoreOptions.BUCKET, 1);
        Table table =
                FileStoreTestUtils.createFileStoreTable(
                        options,
                        RowType.of(
                                new DataType[] {
                                    DataTypes.INT().notNull(), DataTypes.TIME(), DataTypes.TIME(2)
                                },
                                new String[] {"pk", "time0", "time2"}),
                        Collections.emptyList(),
                        Collections.singletonList("pk"));

        StreamWriteBuilder streamWriteBuilder = table.newStreamWriteBuilder();
        StreamTableWrite write = streamWriteBuilder.newWrite();
        StreamTableCommit commit = streamWriteBuilder.newCommit();

        write.write(GenericRow.of(1, 0, 0)); // 00:00
        commit.commit(0, write.prepareCommit(true, 0));

        write.write(GenericRow.of(2, 86_399_999, 86_399_999)); // 23:59:59.999999
        commit.commit(1, write.prepareCommit(true, 1));

        write.write(GenericRow.of(3, 45_001_000, 45_001_000)); // 12:30:01
        commit.commit(2, write.prepareCommit(true, 2));

        write.write(GenericRow.of(4, null, null));
        commit.commit(4, write.prepareCommit(true, 3));

        write.close();
        commit.close();

        createExternalTable();

        // the TIME column will be converted to STRING column
        assertThat(hiveShell.executeQuery("SHOW CREATE TABLE " + externalTable))
                .contains(
                        "  `time0` string COMMENT 'from deserializer', ",
                        "  `time2` string COMMENT 'from deserializer')");

        assertThat(hiveShell.executeQuery("SELECT * FROM " + externalTable))
                .containsExactlyInAnyOrder(
                        "1\t00:00\t00:00",
                        "2\t23:59:59.999\t23:59:59.999",
                        "3\t12:30:01\t12:30:01",
                        "4\tNULL\tNULL");

        assertThat(
                        hiveShell.executeQuery(
                                "SELECT * FROM " + externalTable + " WHERE time0 = '12:30:01'"))
                .containsExactlyInAnyOrder("3\t12:30:01\t12:30:01");
    }

    @Test
    public void testMapKey() throws Exception {
        Options conf = getBasicConf();
        conf.set(
                CoreOptions.FILE_FORMAT,
                ThreadLocalRandom.current().nextBoolean()
                        ? CoreOptions.FILE_FORMAT_ORC
                        : CoreOptions.FILE_FORMAT_PARQUET);
        Table table =
                FileStoreTestUtils.createFileStoreTable(
                        conf,
                        RowType.of(
                                new DataType[] {
                                    DataTypes.MAP(DataTypes.DATE(), DataTypes.STRING()),
                                    DataTypes.MAP(DataTypes.TIMESTAMP(3), DataTypes.STRING()),
                                    DataTypes.MAP(DataTypes.TIMESTAMP(5), DataTypes.STRING()),
                                    DataTypes.MAP(DataTypes.DECIMAL(2, 1), DataTypes.STRING()),
                                    DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()),
                                    DataTypes.MAP(DataTypes.VARCHAR(10), DataTypes.STRING())
                                },
                                new String[] {
                                    "date_key",
                                    "timestamp3_key",
                                    "timestamp5_key",
                                    "decimal_key",
                                    "string_key",
                                    "varchar_key"
                                }),
                        Collections.emptyList(),
                        Collections.emptyList());

        StreamWriteBuilder streamWriteBuilder = table.newStreamWriteBuilder();
        StreamTableWrite write = streamWriteBuilder.newWrite();
        StreamTableCommit commit = streamWriteBuilder.newCommit();

        Map<Integer, BinaryString> dateMap =
                Collections.singletonMap(375, BinaryString.fromString("Date 1971-01-11"));
        Map<Timestamp, BinaryString> timestamp3Map =
                Collections.singletonMap(
                        Timestamp.fromLocalDateTime(
                                LocalDateTime.of(2023, 7, 18, 12, 29, 59, 123_000_000)),
                        BinaryString.fromString("Test timestamp(3)"));
        Map<Timestamp, BinaryString> timestamp5Map =
                Collections.singletonMap(
                        Timestamp.fromLocalDateTime(
                                LocalDateTime.of(2023, 7, 18, 12, 29, 59, 123_450_000)),
                        BinaryString.fromString("Test timestamp(5)"));
        Map<Decimal, BinaryString> decimalMap =
                Collections.singletonMap(
                        Decimal.fromBigDecimal(new BigDecimal("1.2"), 2, 1),
                        BinaryString.fromString("一点二"));
        Map<BinaryString, BinaryString> stringMap =
                Collections.singletonMap(
                        BinaryString.fromString("Engine"), BinaryString.fromString("Hive"));
        Map<BinaryString, BinaryString> varcharMap =
                Collections.singletonMap(
                        BinaryString.fromString("Name"), BinaryString.fromString("Paimon"));

        write.write(
                GenericRow.of(
                        new GenericMap(dateMap),
                        new GenericMap(timestamp3Map),
                        new GenericMap(timestamp5Map),
                        new GenericMap(decimalMap),
                        new GenericMap(stringMap),
                        new GenericMap(varcharMap)));
        commit.commit(0, write.prepareCommit(true, 0));
        write.close();
        commit.close();

        createExternalTable();

        assertThat(
                        hiveShell.executeQuery(
                                "SELECT "
                                        + "date_key[CAST('1971-01-11' AS DATE)],"
                                        + "timestamp3_key[CAST('2023-7-18 12:29:59.123' AS TIMESTAMP)],"
                                        + "timestamp5_key[CAST('2023-7-18 12:29:59.12345' AS TIMESTAMP)],"
                                        + "decimal_key[1.2],"
                                        + "string_key['Engine'],"
                                        + "varchar_key['Name']"
                                        + " FROM `"
                                        + externalTable
                                        + "`"))
                .containsExactly(
                        "Date 1971-01-11\tTest timestamp(3)\tTest timestamp(5)\t一点二\tHive\tPaimon");
    }

    private Options getBasicConf() {
        Options conf = new Options();
        conf.set(CatalogOptions.WAREHOUSE, warehouse);
        return conf;
    }

    private void createExternalTable() {
        hiveShell.execute(
                String.join(
                        "\n",
                        Arrays.asList(
                                "CREATE EXTERNAL TABLE " + externalTable + " ",
                                "STORED BY '" + PaimonStorageHandler.class.getName() + "'",
                                "LOCATION '" + tablePath + "'")));
    }
}
