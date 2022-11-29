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

package org.apache.flink.table.store.spark;

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.store.file.schema.ArrayDataType;
import org.apache.flink.table.store.file.schema.AtomicDataType;
import org.apache.flink.table.store.file.schema.DataField;
import org.apache.flink.table.store.file.schema.DataType;
import org.apache.flink.table.store.file.schema.RowDataType;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.table.FileStoreTableFactory;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;

import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for spark reader. */
public class SparkReadITCase {

    private static File warehouse = null;

    private static SparkSession spark = null;

    private static Path warehousePath = null;

    private static Path tablePath1;

    private static Path tablePath2;

    @BeforeAll
    public static void startMetastoreAndSpark() throws Exception {
        warehouse = Files.createTempFile("warehouse", null).toFile();
        assertThat(warehouse.delete()).isTrue();
        warehousePath = new Path("file:" + warehouse);
        spark = SparkSession.builder().master("local[2]").getOrCreate();
        spark.conf().set("spark.sql.catalog.tablestore", SparkCatalog.class.getName());
        spark.conf().set("spark.sql.catalog.tablestore.warehouse", warehousePath.toString());

        // flink sink
        tablePath1 = new Path(warehousePath, "default.db/t1");
        SimpleTableTestHelper testHelper1 = new SimpleTableTestHelper(tablePath1, rowType1());
        testHelper1.write(GenericRowData.of(1, 2L, StringData.fromString("1")));
        testHelper1.write(GenericRowData.of(3, 4L, StringData.fromString("2")));
        testHelper1.write(GenericRowData.of(5, 6L, StringData.fromString("3")));
        testHelper1.write(GenericRowData.ofKind(RowKind.DELETE, 3, 4L, StringData.fromString("2")));
        testHelper1.commit();

        // a int not null
        // b array<varchar> not null
        // c row<row<double, array<boolean> not null> not null, bigint> not null
        tablePath2 = new Path(warehousePath, "default.db/t2");
        SimpleTableTestHelper testHelper2 = new SimpleTableTestHelper(tablePath2, rowType2());
        testHelper2.write(
                GenericRowData.of(
                        1,
                        new GenericArrayData(
                                new StringData[] {
                                    StringData.fromString("AAA"), StringData.fromString("BBB")
                                }),
                        GenericRowData.of(
                                GenericRowData.of(1.0d, new GenericArrayData(new Boolean[] {null})),
                                1L)));
        testHelper2.write(
                GenericRowData.of(
                        2,
                        new GenericArrayData(
                                new StringData[] {
                                    StringData.fromString("CCC"), StringData.fromString("DDD")
                                }),
                        GenericRowData.of(
                                GenericRowData.of(null, new GenericArrayData(new Boolean[] {true})),
                                null)));
        testHelper2.commit();

        testHelper2.write(
                GenericRowData.of(
                        3,
                        new GenericArrayData(new StringData[] {null, null}),
                        GenericRowData.of(
                                GenericRowData.of(
                                        2.0d, new GenericArrayData(new boolean[] {true, false})),
                                2L)));

        testHelper2.write(
                GenericRowData.of(
                        4,
                        new GenericArrayData(new StringData[] {null, StringData.fromString("EEE")}),
                        GenericRowData.of(
                                GenericRowData.of(
                                        3.0d,
                                        new GenericArrayData(new Boolean[] {true, false, true})),
                                3L)));
        testHelper2.commit();
    }

    private static SimpleTableTestHelper createTestHelper(Path tablePath) throws Exception {
        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new RowType.RowField("a", new IntType(false)),
                                new RowType.RowField("b", new BigIntType()),
                                new RowType.RowField("c", new VarCharType())));
        return new SimpleTableTestHelper(tablePath, rowType);
    }

    private static SimpleTableTestHelper createTestHelperWithoutDDL(Path tablePath)
            throws Exception {
        return new SimpleTableTestHelper(tablePath);
    }

    private static RowType rowType1() {
        return new RowType(
                Arrays.asList(
                        new RowType.RowField("a", new IntType(false)),
                        new RowType.RowField("b", new BigIntType()),
                        new RowType.RowField("c", new VarCharType())));
    }

    private static RowType rowType2() {
        return new RowType(
                Arrays.asList(
                        new RowType.RowField("a", new IntType(false), "comment about a"),
                        new RowType.RowField("b", new ArrayType(false, new VarCharType())),
                        new RowType.RowField(
                                "c",
                                new RowType(
                                        false,
                                        Arrays.asList(
                                                new RowType.RowField(
                                                        "c1",
                                                        new RowType(
                                                                false,
                                                                Arrays.asList(
                                                                        new RowType.RowField(
                                                                                "c11",
                                                                                new DoubleType()),
                                                                        new RowType.RowField(
                                                                                "c12",
                                                                                new ArrayType(
                                                                                        false,
                                                                                        new BooleanType()))))),
                                                new RowType.RowField(
                                                        "c2",
                                                        new BigIntType(),
                                                        "comment about c2"))),
                                "comment about c")));
    }

    @AfterAll
    public static void stopMetastoreAndSpark() throws IOException {
        if (warehouse != null && warehouse.exists()) {
            FileUtils.deleteDirectory(warehouse);
        }
        if (spark != null) {
            spark.stop();
            spark = null;
        }
    }

    @Test
    public void testNormal() {
        innerTestSimpleType(spark.read().format("tablestore").load(tablePath1.toString()));

        innerTestNestedType(spark.read().format("tablestore").load(tablePath2.toString()));
    }

    @Test
    public void testFilterPushDown() {
        innerTestSimpleTypeFilterPushDown(
                spark.read().format("tablestore").load(tablePath1.toString()));

        innerTestNestedTypeFilterPushDown(
                spark.read().format("tablestore").load(tablePath2.toString()));
    }

    @Test
    public void testCatalogNormal() {
        innerTestSimpleType(spark.table("tablestore.default.t1"));
        innerTestNestedType(spark.table("tablestore.default.t2"));
    }

    @Test
    public void testMetadataTable() {
        List<Row> rows =
                spark.table("tablestore.default.`t1$snapshots`")
                        .select("snapshot_id", "schema_id", "commit_user", "commit_kind")
                        .collectAsList();
        assertThat(rows.toString()).isEqualTo("[[1,0,user,APPEND]]");
    }

    @Test
    public void testCatalogFilterPushDown() {
        innerTestSimpleTypeFilterPushDown(spark.table("tablestore.default.t1"));

        innerTestNestedTypeFilterPushDown(spark.table("tablestore.default.t2"));
    }

    @Test
    public void testSetAndRemoveOption() {
        spark.sql("ALTER TABLE tablestore.default.t1 SET TBLPROPERTIES('xyc' 'unknown1')");

        Map<String, String> options = schema1().options();
        assertThat(options).containsEntry("xyc", "unknown1");

        spark.sql("ALTER TABLE tablestore.default.t1 UNSET TBLPROPERTIES('xyc')");

        options = schema1().options();
        assertThat(options).doesNotContainKey("xyc");

        assertThatThrownBy(
                        () ->
                                spark.sql(
                                        "ALTER TABLE tablestore.default.t1 SET TBLPROPERTIES('primary-key' = 'a')"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Alter primary key is not supported");
    }

    @Test
    public void testAddColumn() throws Exception {
        Path tablePath = new Path(warehousePath, "default.db/testAddColumn");
        SimpleTableTestHelper testHelper1 = createTestHelper(tablePath);
        testHelper1.write(GenericRowData.of(1, 2L, StringData.fromString("1")));
        testHelper1.write(GenericRowData.of(5, 6L, StringData.fromString("3")));
        testHelper1.commit();

        spark.sql("ALTER TABLE tablestore.default.testAddColumn ADD COLUMN d STRING");

        Dataset<Row> table = spark.table("tablestore.default.testAddColumn");
        List<Row> results = table.collectAsList();
        assertThat(results.toString()).isEqualTo("[[1,2,1,null], [5,6,3,null]]");

        results = table.select("a", "c").collectAsList();
        assertThat(results.toString()).isEqualTo("[[1,1], [5,3]]");

        results = table.groupBy().sum("b").collectAsList();
        assertThat(results.toString()).isEqualTo("[[8]]");
    }

    @Test
    public void testRenameColumn() throws Exception {
        Path tablePath = new Path(warehousePath, "default.db/testRenameColumn");
        SimpleTableTestHelper testHelper1 = createTestHelper(tablePath);
        testHelper1.write(GenericRowData.of(1, 2L, StringData.fromString("1")));
        testHelper1.write(GenericRowData.of(5, 6L, StringData.fromString("3")));
        testHelper1.commit();

        List<Row> beforeRename =
                spark.sql("SHOW CREATE TABLE tablestore.default.testRenameColumn").collectAsList();
        assertThat(beforeRename.toString())
                .isEqualTo(
                        "[[CREATE TABLE testRenameColumn (\n"
                                + "  `a` INT NOT NULL,\n"
                                + "  `b` BIGINT,\n"
                                + "  `c` STRING)\n"
                                + "]]");
        Dataset<Row> table1 = spark.table("tablestore.default.testRenameColumn");
        List<Row> results = table1.select("a", "c").collectAsList();
        assertThat(results.toString()).isEqualTo("[[1,1], [5,3]]");

        // Rename "a" to "aa"
        spark.sql("ALTER TABLE tablestore.default.testRenameColumn RENAME COLUMN a to aa");
        List<Row> afterRename =
                spark.sql("SHOW CREATE TABLE tablestore.default.testRenameColumn").collectAsList();
        assertThat(afterRename.toString())
                .isEqualTo(
                        "[[CREATE TABLE testRenameColumn (\n"
                                + "  `aa` INT NOT NULL,\n"
                                + "  `b` BIGINT,\n"
                                + "  `c` STRING)\n"
                                + "]]");
        Dataset<Row> table2 = spark.table("tablestore.default.testRenameColumn");
        results = table2.select("aa", "c").collectAsList();
        assertThat(results.toString()).isEqualTo("[[1,1], [5,3]]");
        assertThatThrownBy(() -> table2.select("a", "c").collectAsList())
                .isInstanceOf(AnalysisException.class)
                .hasMessage(
                        String.format(
                                "cannot resolve '%s' given input columns: "
                                        + "[tablestore.default.testRenameColumn.aa, "
                                        + "tablestore.default.testRenameColumn.b, "
                                        + "tablestore.default.testRenameColumn.c];\n"
                                        + "'Project ['%s, c#32]\n"
                                        + "+- SubqueryAlias tablestore.default.testRenameColumn\n"
                                        + "   +- RelationV2[aa#30, b#31L, c#32] testRenameColumn\n",
                                "a", "a"));
    }

    @Test
    public void testRenamePartitionKey() {
        spark.sql("USE tablestore");
        spark.sql(
                "CREATE TABLE default.testRenamePartitionKey (\n"
                        + "a BIGINT,\n"
                        + "b STRING) USING tablestore\n"
                        + "COMMENT 'table comment'\n"
                        + "PARTITIONED BY (a)\n"
                        + "TBLPROPERTIES ('foo' = 'bar')");

        List<Row> beforeRename =
                spark.sql("SHOW CREATE TABLE tablestore.default.testRenamePartitionKey")
                        .collectAsList();
        assertThat(beforeRename.toString())
                .isEqualTo(
                        "[[CREATE TABLE testRenamePartitionKey (\n"
                                + "  `a` BIGINT,\n"
                                + "  `b` STRING)\n"
                                + "PARTITIONED BY (a)\n"
                                + "]]");

        assertThatThrownBy(
                        () ->
                                spark.sql(
                                        "ALTER TABLE tablestore.default.testRenamePartitionKey RENAME COLUMN a to aa"))
                .isInstanceOf(RuntimeException.class)
                .hasMessage(
                        String.format(
                                "java.lang.UnsupportedOperationException: Cannot rename partition key[%s]",
                                "a"));
    }

    @Test
    public void testDropSingleColumn() throws Exception {
        Path tablePath = new Path(warehousePath, "default.db/testDropSingleColumn");
        SimpleTableTestHelper testHelper = createTestHelper(tablePath);
        testHelper.write(GenericRowData.of(1, 2L, StringData.fromString("1")));
        testHelper.write(GenericRowData.of(5, 6L, StringData.fromString("3")));
        testHelper.commit();

        List<Row> beforeDrop =
                spark.sql("SHOW CREATE TABLE tablestore.default.testDropSingleColumn")
                        .collectAsList();
        assertThat(beforeDrop.toString())
                .isEqualTo(
                        "[[CREATE TABLE testDropSingleColumn (\n"
                                + "  `a` INT NOT NULL,\n"
                                + "  `b` BIGINT,\n"
                                + "  `c` STRING)\n"
                                + "]]");

        spark.sql("ALTER TABLE tablestore.default.testDropSingleColumn DROP COLUMN a");

        List<Row> afterDrop =
                spark.sql("SHOW CREATE TABLE tablestore.default.testDropSingleColumn")
                        .collectAsList();
        assertThat(afterDrop.toString())
                .isEqualTo(
                        "[[CREATE TABLE testDropSingleColumn (\n"
                                + "  `b` BIGINT,\n"
                                + "  `c` STRING)\n"
                                + "]]");

        Dataset<Row> table = spark.table("tablestore.default.testDropSingleColumn");
        List<Row> results = table.collectAsList();
        assertThat(results.toString()).isEqualTo("[[2,1], [6,3]]");
    }

    @Test
    public void testDropColumns() throws Exception {
        Path tablePath = new Path(warehousePath, "default.db/testDropColumns");
        createTestHelper(tablePath);

        List<Row> beforeRename =
                spark.sql("SHOW CREATE TABLE tablestore.default.testDropColumns").collectAsList();
        assertThat(beforeRename.toString())
                .isEqualTo(
                        "[[CREATE TABLE testDropColumns (\n"
                                + "  `a` INT NOT NULL,\n"
                                + "  `b` BIGINT,\n"
                                + "  `c` STRING)\n"
                                + "]]");

        spark.sql("ALTER TABLE tablestore.default.testDropColumns DROP COLUMNS a, b");

        List<Row> afterRename =
                spark.sql("SHOW CREATE TABLE tablestore.default.testDropColumns").collectAsList();
        assertThat(afterRename.toString())
                .isEqualTo("[[CREATE TABLE testDropColumns (\n" + "  `c` STRING)\n" + "]]");
    }

    @Test
    public void testDropPartitionKey() {
        spark.sql("USE tablestore");
        spark.sql(
                "CREATE TABLE default.testDropPartitionKey (\n"
                        + "a BIGINT,\n"
                        + "b STRING) USING tablestore\n"
                        + "COMMENT 'table comment'\n"
                        + "PARTITIONED BY (a)\n"
                        + "TBLPROPERTIES ('foo' = 'bar')");

        List<Row> beforeRename =
                spark.sql("SHOW CREATE TABLE tablestore.default.testDropPartitionKey")
                        .collectAsList();
        assertThat(beforeRename.toString())
                .isEqualTo(
                        "[[CREATE TABLE testDropPartitionKey (\n"
                                + "  `a` BIGINT,\n"
                                + "  `b` STRING)\n"
                                + "PARTITIONED BY (a)\n"
                                + "]]");

        assertThatThrownBy(
                        () ->
                                spark.sql(
                                        "ALTER TABLE tablestore.default.testDropPartitionKey DROP COLUMN a"))
                .isInstanceOf(RuntimeException.class)
                .hasMessage(
                        String.format(
                                "java.lang.UnsupportedOperationException: Cannot drop partition key[%s]",
                                "a"));
    }

    @Test
    public void testDropPrimaryKey() {
        spark.sql("USE tablestore");
        spark.sql(
                "CREATE TABLE default.testDropPrimaryKey (\n"
                        + "a BIGINT,\n"
                        + "b STRING) USING tablestore\n"
                        + "COMMENT 'table comment'\n"
                        + "PARTITIONED BY (a)\n"
                        + "TBLPROPERTIES ('primary-key' = 'a, b')");

        List<Row> beforeRename =
                spark.sql("SHOW CREATE TABLE tablestore.default.testDropPrimaryKey")
                        .collectAsList();
        assertThat(beforeRename.toString())
                .isEqualTo(
                        "[[CREATE TABLE testDropPrimaryKey (\n"
                                + "  `a` BIGINT NOT NULL,\n"
                                + "  `b` STRING NOT NULL)\n"
                                + "PARTITIONED BY (a)\n"
                                + "]]");

        assertThatThrownBy(
                        () ->
                                spark.sql(
                                        "ALTER TABLE tablestore.default.testDropPrimaryKey DROP COLUMN b"))
                .isInstanceOf(RuntimeException.class)
                .hasMessage(
                        String.format(
                                "java.lang.UnsupportedOperationException: Cannot drop primary key[%s]",
                                "b"));
    }

    /**
     * In fact, the table store does not currently support alter column type. In this case, changing
     * "a" type from int to bigint can run successfully because the underlying orc supports directly
     * reading int to bigint. At present, we read int value from orc into {@link RowData} according
     * to the underlying data schema, and then read long from {@link RowData} will cause failure.
     * TODO: This case needs to be ignored first and will be completely fixed in
     * https://issues.apache.org/jira/browse/FLINK-27845
     */
    @Disabled
    @Test
    public void testAlterColumnType() throws Exception {
        Path tablePath = new Path(warehousePath, "default.db/testAlterColumnType");
        SimpleTableTestHelper testHelper1 = createTestHelper(tablePath);
        testHelper1.write(GenericRowData.of(1, 2L, StringData.fromString("1")));
        testHelper1.write(GenericRowData.of(5, 6L, StringData.fromString("3")));
        testHelper1.commit();

        spark.sql("ALTER TABLE tablestore.default.testAlterColumnType ALTER COLUMN a TYPE BIGINT");
        innerTestSimpleType(spark.table("tablestore.default.testAlterColumnType"));
    }

    @Test
    public void testAlterTableColumnNullability() {
        assertThat(fieldIsNullable(getField(schema2(), 0))).isFalse();
        assertThat(fieldIsNullable(getField(schema2(), 1))).isFalse();
        assertThat(fieldIsNullable(getField(schema2(), 2))).isFalse();
        assertThat(fieldIsNullable(getNestedField(getField(schema2(), 2), 0))).isFalse();
        assertThat(fieldIsNullable(getNestedField(getField(schema2(), 2), 1))).isTrue();
        assertThat(fieldIsNullable(getNestedField(getNestedField(getField(schema2(), 2), 0), 0)))
                .isTrue();
        assertThat(fieldIsNullable(getNestedField(getNestedField(getField(schema2(), 2), 0), 1)))
                .isFalse();

        // note: for Spark, it is illegal to change nullable column to non-nullable
        spark.sql("ALTER TABLE tablestore.default.t2 ALTER COLUMN a DROP NOT NULL");
        assertThat(fieldIsNullable(getField(schema2(), 0))).isTrue();

        spark.sql("ALTER TABLE tablestore.default.t2 ALTER COLUMN b DROP NOT NULL");
        assertThat(fieldIsNullable(getField(schema2(), 1))).isTrue();

        spark.sql("ALTER TABLE tablestore.default.t2 ALTER COLUMN c DROP NOT NULL");
        assertThat(fieldIsNullable(getField(schema2(), 2))).isTrue();

        spark.sql("ALTER TABLE tablestore.default.t2 ALTER COLUMN c.c1 DROP NOT NULL");
        assertThat(fieldIsNullable(getNestedField(getField(schema2(), 2), 0))).isTrue();

        spark.sql("ALTER TABLE tablestore.default.t2 ALTER COLUMN c.c1.c12 DROP NOT NULL");
        assertThat(fieldIsNullable(getNestedField(getNestedField(getField(schema2(), 2), 0), 1)))
                .isTrue();
    }

    @Test
    public void testDefaultNamespace() {
        spark.sql("USE tablestore");
        assertThat(spark.sql("SHOW CURRENT NAMESPACE").collectAsList().toString())
                .isEqualTo("[[tablestore,default]]");
    }

    @Test
    public void testAlterPrimaryKeyNullability() {
        spark.sql("USE tablestore");
        spark.sql(
                "CREATE TABLE default.testAlterPkNullability (\n"
                        + "a BIGINT,\n"
                        + "b STRING) USING tablestore\n"
                        + "COMMENT 'table comment'\n"
                        + "TBLPROPERTIES ('primary-key' = 'a')");
        assertThatThrownBy(
                        () ->
                                spark.sql(
                                        "ALTER TABLE default.testAlterPkNullability ALTER COLUMN a DROP NOT NULL"))
                .getRootCause()
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Cannot change nullability of primary key");
    }

    @Test
    public void testAlterTableColumnComment() {
        assertThat(getField(schema1(), 0).description()).isNull();

        spark.sql("ALTER TABLE tablestore.default.t1 ALTER COLUMN a COMMENT 'a new comment'");
        assertThat(getField(schema1(), 0).description()).isEqualTo("a new comment");

        spark.sql("ALTER TABLE tablestore.default.t1 ALTER COLUMN a COMMENT 'yet another comment'");
        assertThat(getField(schema1(), 0).description()).isEqualTo("yet another comment");

        assertThat(getField(schema2(), 2).description()).isEqualTo("comment about c");
        assertThat(getNestedField(getField(schema2(), 2), 0).description()).isNull();
        assertThat(getNestedField(getField(schema2(), 2), 1).description())
                .isEqualTo("comment about c2");
        assertThat(getNestedField(getNestedField(getField(schema2(), 2), 0), 0).description())
                .isNull();
        assertThat(getNestedField(getNestedField(getField(schema2(), 2), 0), 1).description())
                .isNull();

        spark.sql(
                "ALTER TABLE tablestore.default.t2 ALTER COLUMN c COMMENT 'yet another comment about c'");
        spark.sql("ALTER TABLE tablestore.default.t2 ALTER COLUMN c.c1 COMMENT 'a nested type'");
        spark.sql("ALTER TABLE tablestore.default.t2 ALTER COLUMN c.c2 COMMENT 'a bigint type'");
        spark.sql(
                "ALTER TABLE tablestore.default.t2 ALTER COLUMN c.c1.c11 COMMENT 'a double type'");
        spark.sql(
                "ALTER TABLE tablestore.default.t2 ALTER COLUMN c.c1.c12 COMMENT 'a boolean array'");

        assertThat(getField(schema2(), 2).description()).isEqualTo("yet another comment about c");
        assertThat(getNestedField(getField(schema2(), 2), 0).description())
                .isEqualTo("a nested type");
        assertThat(getNestedField(getField(schema2(), 2), 1).description())
                .isEqualTo("a bigint type");
        assertThat(getNestedField(getNestedField(getField(schema2(), 2), 0), 0).description())
                .isEqualTo("a double type");
        assertThat(getNestedField(getNestedField(getField(schema2(), 2), 0), 1).description())
                .isEqualTo("a boolean array");
    }

    @Test
    public void testCreateTableWithNullablePk() {
        spark.sql("USE tablestore");
        spark.sql(
                "CREATE TABLE default.PkTable (\n"
                        + "a BIGINT,\n"
                        + "b STRING) USING tablestore\n"
                        + "COMMENT 'table comment'\n"
                        + "TBLPROPERTIES ('primary-key' = 'a')");
        Path tablePath = new Path(warehousePath, "default.db/PkTable");
        TableSchema schema = FileStoreTableFactory.create(tablePath).schema();
        assertThat(schema.logicalRowType().getTypeAt(0).isNullable()).isFalse();
    }

    @Test
    public void testDescribeTable() {
        spark.sql("USE tablestore");
        spark.sql(
                "CREATE TABLE default.PartitionedTable (\n"
                        + "a BIGINT,\n"
                        + "b STRING) USING tablestore\n"
                        + "COMMENT 'table comment'\n"
                        + "PARTITIONED BY (a)\n"
                        + "TBLPROPERTIES ('foo' = 'bar')");
        assertThat(spark.sql("DESCRIBE default.PartitionedTable").collectAsList().toString())
                .isEqualTo("[[a,bigint,], [b,string,], [,,], [# Partitioning,,], [Part 0,a,]]");
    }

    @Test
    public void testCreateTableWithInvalidPk() {
        spark.sql("USE tablestore");
        assertThatThrownBy(
                        () ->
                                spark.sql(
                                        "CREATE TABLE default.PartitionedPkTable (\n"
                                                + "a BIGINT,\n"
                                                + "b STRING,\n"
                                                + "c DOUBLE) USING tablestore\n"
                                                + "COMMENT 'table comment'\n"
                                                + "PARTITIONED BY (b)"
                                                + "TBLPROPERTIES ('primary-key' = 'a')"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "Primary key constraint [a] should include all partition fields [b]");
    }

    @Test
    public void testCreateTableWithNonexistentPk() {
        spark.sql("USE tablestore");
        assertThatThrownBy(
                        () ->
                                spark.sql(
                                        "CREATE TABLE default.PartitionedPkTable (\n"
                                                + "a BIGINT,\n"
                                                + "b STRING,\n"
                                                + "c DOUBLE) USING tablestore\n"
                                                + "COMMENT 'table comment'\n"
                                                + "PARTITIONED BY (b)"
                                                + "TBLPROPERTIES ('primary-key' = 'd')"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "Table column [a, b, c] should include all primary key constraint [d]");
    }

    @Test
    public void testCreateTableWithNonexistentPartition() {
        spark.sql("USE tablestore");
        assertThatThrownBy(
                        () ->
                                spark.sql(
                                        "CREATE TABLE default.PartitionedPkTable (\n"
                                                + "a BIGINT,\n"
                                                + "b STRING,\n"
                                                + "c DOUBLE) USING tablestore\n"
                                                + "COMMENT 'table comment'\n"
                                                + "PARTITIONED BY (d)"
                                                + "TBLPROPERTIES ('primary-key' = 'a')"))
                .isInstanceOf(AnalysisException.class)
                .hasMessageContaining("Couldn't find column d");
    }

    @Test
    public void testCreateAndDropTable() throws Exception {
        innerTest("MyTable1", true, true, false);
        innerTest("MyTable2", true, false, false);
        innerTest("MyTable3", false, false, false);
        innerTest("MyTable4", false, false, true);
        innerTest("MyTable5", false, true, false);
        innerTest("MyTable6", false, true, true);
    }

    /**
     * Test for schema evolution as followed:
     *
     * <ul>
     *   <li>1. Create table with fields ["a", "b", "c"], insert 2 records
     *   <li>2. Rename "a->aa", "c"->"a", "b"->"c", insert 2 records
     *   <li>3. Drop fields "aa", "c", insert 2 records
     *   <li>4. Add new fields "d", "c", "b", insert 2 records
     * </ul>
     *
     * <p>Verify records in table above.
     */
    @Test
    public void testSchemaEvolution() throws Exception {
        // Create table with fields [a, b, c] and insert 2 records
        Path tablePath = new Path(warehousePath, "default.db/testSchemaEvolution");
        SimpleTableTestHelper testHelper1 = createTestHelper(tablePath);
        testHelper1.write(GenericRowData.of(1, 2L, StringData.fromString("3")));
        testHelper1.write(GenericRowData.of(4, 5L, StringData.fromString("6")));
        testHelper1.commit();
        assertThat(spark.table("tablestore.default.testSchemaEvolution").collectAsList().toString())
                .isEqualTo("[[1,2,3], [4,5,6]]");
        assertThat(
                        spark.table("tablestore.default.testSchemaEvolution")
                                .select("a", "b", "c")
                                .collectAsList()
                                .toString())
                .isEqualTo("[[1,2,3], [4,5,6]]");
        assertThat(
                        spark.table("tablestore.default.testSchemaEvolution")
                                .filter("a>1")
                                .collectAsList()
                                .toString())
                .isEqualTo("[[4,5,6]]");

        // Rename "a->aa", "c"->"a", "b"->"c" and the fields are [aa, c, a], insert 2 records
        spark.sql("ALTER TABLE tablestore.default.testSchemaEvolution RENAME COLUMN a to aa");
        spark.sql("ALTER TABLE tablestore.default.testSchemaEvolution RENAME COLUMN c to a");
        spark.sql("ALTER TABLE tablestore.default.testSchemaEvolution RENAME COLUMN b to c");
        SimpleTableTestHelper testHelper2 = createTestHelperWithoutDDL(tablePath);
        testHelper2.write(GenericRowData.of(7, 8L, StringData.fromString("9")));
        testHelper2.write(GenericRowData.of(10, 11L, StringData.fromString("12")));
        testHelper2.commit();
        assertThat(spark.table("tablestore.default.testSchemaEvolution").collectAsList().toString())
                .isEqualTo("[[1,2,3], [4,5,6], [7,8,9], [10,11,12]]");
        assertThat(
                        spark.table("tablestore.default.testSchemaEvolution")
                                .select("aa", "a", "c")
                                .collectAsList()
                                .toString())
                .isEqualTo("[[1,3,2], [4,6,5], [7,9,8], [10,12,11]]");
        assertThat(
                        spark.table("tablestore.default.testSchemaEvolution")
                                .select("aa", "a", "c")
                                .filter("aa>4")
                                .collectAsList()
                                .toString())
                .isEqualTo("[[7,9,8], [10,12,11]]");

        // Drop fields "aa", "c" and the fields are [a], insert 2 records
        spark.sql("ALTER TABLE tablestore.default.testSchemaEvolution DROP COLUMNS aa, c");
        SimpleTableTestHelper testHelper3 = createTestHelperWithoutDDL(tablePath);
        testHelper3.write(GenericRowData.of(StringData.fromString("13")));
        testHelper3.write(GenericRowData.of(StringData.fromString("14")));
        testHelper3.commit();
        assertThat(spark.table("tablestore.default.testSchemaEvolution").collectAsList().toString())
                .isEqualTo("[[3], [6], [9], [12], [13], [14]]");
        assertThat(
                        spark.table("tablestore.default.testSchemaEvolution")
                                .select("a")
                                .filter("a>10")
                                .collectAsList()
                                .toString())
                .isEqualTo("[[12], [13], [14]]");

        // Add new fields "d", "c", "b" and the fields are [a, d, c, b], insert 2 records
        spark.sql(
                "ALTER TABLE tablestore.default.testSchemaEvolution ADD COLUMNS (d INT, c INT, b INT)");
        SimpleTableTestHelper testHelper4 = createTestHelperWithoutDDL(tablePath);
        testHelper4.write(GenericRowData.of(StringData.fromString("15"), 16, 17, 18));
        testHelper4.write(GenericRowData.of(StringData.fromString("19"), 20, 21, 22));
        testHelper4.commit();
        assertThat(spark.table("tablestore.default.testSchemaEvolution").collectAsList().toString())
                .isEqualTo(
                        "[[3,null,null,null], "
                                + "[6,null,null,null], "
                                + "[9,null,null,null], "
                                + "[12,null,null,null], "
                                + "[13,null,null,null], "
                                + "[14,null,null,null], "
                                + "[15,16,17,18], "
                                + "[19,20,21,22]]");
        assertThat(
                        spark.table("tablestore.default.testSchemaEvolution")
                                .filter("a>10")
                                .collectAsList()
                                .toString())
                .isEqualTo(
                        "[[12,null,null,null], "
                                + "[13,null,null,null], "
                                + "[14,null,null,null], "
                                + "[15,16,17,18], "
                                + "[19,20,21,22]]");
        assertThat(
                        spark.table("tablestore.default.testSchemaEvolution")
                                .select("a", "b", "c", "d")
                                .filter("a>10")
                                .collectAsList()
                                .toString())
                .isEqualTo(
                        "[[12,null,null,null], "
                                + "[13,null,null,null], "
                                + "[14,null,null,null], "
                                + "[15,18,17,16], "
                                + "[19,22,21,20]]");
        assertThat(
                        spark.table("tablestore.default.testSchemaEvolution")
                                .select("a", "b", "c", "d")
                                .filter("a>10 and b is not null")
                                .collectAsList()
                                .toString())
                .isEqualTo("[[15,18,17,16], [19,22,21,20]]");
    }

    private void innerTest(String tableName, boolean hasPk, boolean partitioned, boolean appendOnly)
            throws Exception {
        spark.sql("USE tablestore");
        String ddlTemplate =
                "CREATE TABLE default.%s (\n"
                        + "order_id BIGINT NOT NULL comment 'order_id',\n"
                        + "buyer_id BIGINT NOT NULL COMMENT 'buyer_id',\n"
                        + "coupon_info ARRAY<STRING> NOT NULL COMMENT 'coupon_info',\n"
                        + "order_amount DOUBLE NOT NULL COMMENT 'order_amount',\n"
                        + "dt STRING NOT NULL COMMENT 'dt',\n"
                        + "hh STRING NOT NULL COMMENT 'hh') USING tablestore\n"
                        + "COMMENT 'table comment'\n"
                        + "%s\n"
                        + "TBLPROPERTIES (%s)";
        Map<String, String> tableProperties = new HashMap<>();
        tableProperties.put("foo", "bar");
        List<String> columns =
                Arrays.asList("order_id", "buyer_id", "coupon_info", "order_amount", "dt", "hh");
        List<DataType> types =
                Arrays.asList(
                        new AtomicDataType(new BigIntType(false)),
                        new AtomicDataType(new BigIntType(false)),
                        new ArrayDataType(
                                false,
                                new AtomicDataType(new VarCharType(true, VarCharType.MAX_LENGTH))),
                        new AtomicDataType(new DoubleType(false)),
                        new AtomicDataType(new VarCharType(false, VarCharType.MAX_LENGTH)),
                        new AtomicDataType(new VarCharType(false, VarCharType.MAX_LENGTH)));
        List<DataField> fields =
                IntStream.range(0, columns.size())
                        .boxed()
                        .map(i -> new DataField(i, columns.get(i), types.get(i), columns.get(i)))
                        .collect(Collectors.toList());
        String partitionStr = "";
        if (hasPk) {
            tableProperties.put("primary-key", partitioned ? "order_id,dt,hh" : "order_id");
        }
        if (appendOnly) {
            tableProperties.put("write-mode", "append-only");
        }
        if (partitioned) {
            partitionStr = "PARTITIONED BY (dt, hh)";
        }

        String ddl =
                String.format(
                        ddlTemplate,
                        tableName,
                        partitionStr,
                        tableProperties.entrySet().stream()
                                .map(
                                        entry ->
                                                String.format(
                                                        "'%s' = '%s'",
                                                        entry.getKey(), entry.getValue()))
                                .collect(Collectors.joining(", ")));

        spark.sql(ddl);
        assertThatThrownBy(() -> spark.sql(ddl))
                .isInstanceOf(TableAlreadyExistsException.class)
                .hasMessageContaining(String.format("Table default.%s already exists", tableName));
        assertThatThrownBy(() -> spark.sql(ddl.replace("default", "foo")))
                .isInstanceOf(NoSuchNamespaceException.class)
                .hasMessageContaining("Namespace 'foo' not found");

        assertThatThrownBy(
                        () ->
                                spark.sql(
                                        String.format(
                                                "ALTER TABLE default.%s UNSET TBLPROPERTIES('primary-key')",
                                                tableName)))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Alter primary key is not supported");
        assertThatThrownBy(
                        () ->
                                spark.sql(
                                        String.format(
                                                "ALTER TABLE default.%s SET TBLPROPERTIES('write-mode' = 'append-only')",
                                                tableName)))
                .getRootCause()
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Change 'write-mode' is not supported yet");

        Path tablePath = new Path(warehousePath, String.format("default.db/%s", tableName));
        TableSchema schema = FileStoreTableFactory.create(tablePath).schema();
        assertThat(schema.fields()).containsExactlyElementsOf(fields);
        assertThat(schema.options()).containsEntry("foo", "bar");
        assertThat(schema.options()).doesNotContainKey("primary-key");

        if (hasPk) {
            if (partitioned) {
                assertThat(schema.primaryKeys()).containsExactly("order_id", "dt", "hh");
            } else {
                assertThat(schema.primaryKeys()).containsExactly("order_id");
            }
            assertThat(schema.trimmedPrimaryKeys()).containsOnly("order_id");
        } else {
            assertThat(schema.primaryKeys()).isEmpty();
        }

        if (partitioned) {
            assertThat(schema.partitionKeys()).containsExactly("dt", "hh");
        } else {
            assertThat(schema.partitionKeys()).isEmpty();
        }

        if (appendOnly) {
            assertThat(schema.options()).containsEntry("write-mode", "append-only");
        } else {
            assertThat(schema.options()).doesNotContainEntry("write-mode", "append-only");
        }

        assertThat(schema.comment()).isEqualTo("table comment");

        SimpleTableTestHelper testHelper =
                new SimpleTableTestHelper(
                        tablePath,
                        schema.logicalRowType(),
                        partitioned ? Arrays.asList("dt", "hh") : Collections.emptyList(),
                        hasPk
                                ? partitioned
                                        ? Arrays.asList("order_id", "dt", "hh")
                                        : Collections.singletonList("order_id")
                                : Collections.emptyList());
        testHelper.write(
                GenericRowData.of(
                        1L,
                        10L,
                        new GenericArrayData(
                                new BinaryStringData[] {
                                    BinaryStringData.fromString("loyalty_discount"),
                                    BinaryStringData.fromString("shipping_discount")
                                }),
                        199.0d,
                        BinaryStringData.fromString("2022-07-20"),
                        BinaryStringData.fromString("12")));
        testHelper.commit();

        Dataset<Row> dataset = spark.read().format("tablestore").load(tablePath.toString());
        assertThat(dataset.select("order_id", "buyer_id", "dt").collectAsList().toString())
                .isEqualTo("[[1,10,2022-07-20]]");
        assertThat(dataset.select("coupon_info").collectAsList().toString())
                .isEqualTo("[[WrappedArray(loyalty_discount, shipping_discount)]]");

        // test drop table
        assertThat(
                        spark.sql(
                                        String.format(
                                                "SHOW TABLES IN tablestore.default LIKE '%s'",
                                                tableName))
                                .select("namespace", "tableName")
                                .collectAsList()
                                .toString())
                .isEqualTo(String.format("[[default,%s]]", tableName));

        spark.sql(String.format("DROP TABLE tablestore.default.%s", tableName));

        assertThat(
                        spark.sql(
                                        String.format(
                                                "SHOW TABLES IN tablestore.default LIKE '%s'",
                                                tableName))
                                .select("namespace", "tableName")
                                .collectAsList()
                                .toString())
                .isEqualTo("[]");

        assertThat(new File(tablePath.toUri())).doesNotExist();
    }

    @Test
    public void testCreateAndDropNamespace() {
        // create namespace
        spark.sql("USE tablestore");
        spark.sql("CREATE NAMESPACE bar");

        assertThatThrownBy(() -> spark.sql("CREATE NAMESPACE bar"))
                .isInstanceOf(NamespaceAlreadyExistsException.class)
                .hasMessageContaining("Namespace 'bar' already exists");

        assertThat(
                        spark.sql("SHOW NAMESPACES").collectAsList().stream()
                                .map(row -> row.getString(0))
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("bar", "default");

        Path nsPath = new Path(warehousePath, "bar.db");
        assertThat(new File(nsPath.toUri())).exists();

        // drop namespace
        spark.sql("DROP NAMESPACE bar");
        assertThat(spark.sql("SHOW NAMESPACES").collectAsList().toString())
                .isEqualTo("[[default]]");
        assertThat(new File(nsPath.toUri())).doesNotExist();
    }

    private TableSchema schema1() {
        return FileStoreTableFactory.create(tablePath1).schema();
    }

    private TableSchema schema2() {
        return FileStoreTableFactory.create(tablePath2).schema();
    }

    private void innerTestSimpleType(Dataset<Row> dataset) {
        List<Row> results = dataset.collectAsList();
        assertThat(results.toString()).isEqualTo("[[1,2,1], [5,6,3]]");

        results = dataset.select("a", "c").collectAsList();
        assertThat(results.toString()).isEqualTo("[[1,1], [5,3]]");

        results = dataset.groupBy().sum("b").collectAsList();
        assertThat(results.toString()).isEqualTo("[[8]]");
    }

    private void innerTestNestedType(Dataset<Row> dataset) {
        List<Row> results = dataset.collectAsList();
        assertThat(results.toString())
                .isEqualTo(
                        "[[1,WrappedArray(AAA, BBB),[[1.0,WrappedArray(null)],1]], "
                                + "[2,WrappedArray(CCC, DDD),[[null,WrappedArray(true)],null]], "
                                + "[3,WrappedArray(null, null),[[2.0,WrappedArray(true, false)],2]], "
                                + "[4,WrappedArray(null, EEE),[[3.0,WrappedArray(true, false, true)],3]]]");

        results = dataset.select("a").collectAsList();
        assertThat(results.toString()).isEqualTo("[[1], [2], [3], [4]]");

        results = dataset.select("c.c1").collectAsList();
        assertThat(results.toString())
                .isEqualTo(
                        "[[[1.0,WrappedArray(null)]], [[null,WrappedArray(true)]], "
                                + "[[2.0,WrappedArray(true, false)]], "
                                + "[[3.0,WrappedArray(true, false, true)]]]");

        results = dataset.select("c.c2").collectAsList();
        assertThat(results.toString()).isEqualTo("[[1], [null], [2], [3]]");

        results = dataset.select("c.c1.c11").collectAsList();
        assertThat(results.toString()).isEqualTo("[[1.0], [null], [2.0], [3.0]]");

        results = dataset.select("c.c1.c12").collectAsList();
        assertThat(results.toString())
                .isEqualTo(
                        "[[WrappedArray(null)], "
                                + "[WrappedArray(true)], "
                                + "[WrappedArray(true, false)], "
                                + "[WrappedArray(true, false, true)]]");
    }

    private void innerTestSimpleTypeFilterPushDown(Dataset<Row> dataset) {
        List<Row> results = dataset.filter("a < 4").select("a", "c").collectAsList();
        assertThat(results.toString()).isEqualTo("[[1,1]]");

        results = dataset.filter("b = 4").select("a", "c").collectAsList();
        assertThat(results.toString()).isEqualTo("[]");
    }

    private void innerTestNestedTypeFilterPushDown(Dataset<Row> dataset) {
        List<Row> results = dataset.filter("a < 4").select("a").collectAsList();
        assertThat(results.toString()).isEqualTo("[[1], [2], [3]]");

        results = dataset.filter("array_contains(b, 'AAA')").select("b").collectAsList();
        assertThat(results.toString()).isEqualTo("[[WrappedArray(AAA, BBB)]]");

        results = dataset.filter("c.c1.c11 is null").select("a", "c").collectAsList();
        assertThat(results.toString()).isEqualTo("[[2,[[null,WrappedArray(true)],null]]]");

        results = dataset.filter("c.c1.c11 = 1.0").select("a", "c.c1").collectAsList();
        assertThat(results.toString()).isEqualTo("[[1,[1.0,WrappedArray(null)]]]");

        results = dataset.filter("c.c2 is null").select("a", "c").collectAsList();
        assertThat(results.toString()).isEqualTo("[[2,[[null,WrappedArray(true)],null]]]");

        results =
                dataset.filter("array_contains(c.c1.c12, false)")
                        .select("a", "c.c1.c12", "c.c2")
                        .collectAsList();
        assertThat(results.toString())
                .isEqualTo(
                        "[[3,WrappedArray(true, false),2], [4,WrappedArray(true, false, true),3]]");
    }

    private boolean fieldIsNullable(DataField field) {
        return field.type().logicalType().isNullable();
    }

    private DataField getField(TableSchema schema, int index) {
        return schema.fields().get(index);
    }

    private DataField getNestedField(DataField field, int index) {
        if (field.type() instanceof RowDataType) {
            RowDataType rowDataType = (RowDataType) field.type();
            return rowDataType.fields().get(index);
        }
        throw new IllegalArgumentException();
    }
}
