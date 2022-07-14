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
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.store.file.schema.DataField;
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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for spark reader. */
public class SparkReadITCase {

    private static File warehouse = null;

    private static SparkSession spark = null;

    private static Path warehousePath = null;

    private static Path tablePath1;

    private static Path tablePath2;

    @BeforeAll
    public static void startMetastoreAndSpark() throws Exception {
        warehouse = File.createTempFile("warehouse", null);
        assertThat(warehouse.delete()).isTrue();
        warehousePath = new Path("file:" + warehouse);
        spark = SparkSession.builder().master("local[2]").getOrCreate();
        spark.conf().set("spark.sql.catalog.table_store", SparkCatalog.class.getName());
        spark.conf().set("spark.sql.catalog.table_store.warehouse", warehousePath.toString());

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
        innerTestSimpleType(
                spark.read().format("tablestore").option("path", tablePath1.toString()).load());

        innerTestNestedType(
                spark.read().format("tablestore").option("path", tablePath2.toString()).load());
    }

    @Test
    public void testFilterPushDown() {
        innerTestSimpleTypeFilterPushDown(
                spark.read().format("tablestore").option("path", tablePath1.toString()).load());

        innerTestNestedTypeFilterPushDown(
                spark.read().format("tablestore").option("path", tablePath2.toString()).load());
    }

    @Test
    public void testCatalogNormal() {
        innerTestSimpleType(spark.table("table_store.default.t1"));

        innerTestNestedType(spark.table("table_store.default.t2"));
    }

    @Test
    public void testCatalogFilterPushDown() {
        innerTestSimpleTypeFilterPushDown(spark.table("table_store.default.t1"));

        innerTestNestedTypeFilterPushDown(spark.table("table_store.default.t2"));
    }

    @Test
    public void testSetAndRemoveOption() {
        spark.sql("ALTER TABLE table_store.default.t1 SET TBLPROPERTIES('xyc' 'unknown1')");

        Map<String, String> options = schema1().options();
        assertThat(options).containsEntry("xyc", "unknown1");

        spark.sql("ALTER TABLE table_store.default.t1 UNSET TBLPROPERTIES('xyc')");

        options = schema1().options();
        assertThat(options).doesNotContainKey("xyc");
    }

    @Test
    public void testAddColumn() throws Exception {
        Path tablePath = new Path(warehousePath, "default.db/testAddColumn");
        SimpleTableTestHelper testHelper1 = createTestHelper(tablePath);
        testHelper1.write(GenericRowData.of(1, 2L, StringData.fromString("1")));
        testHelper1.write(GenericRowData.of(5, 6L, StringData.fromString("3")));
        testHelper1.commit();

        spark.sql("ALTER TABLE table_store.default.testAddColumn ADD COLUMN d STRING");

        Dataset<Row> table = spark.table("table_store.default.testAddColumn");
        List<Row> results = table.collectAsList();
        assertThat(results.toString()).isEqualTo("[[1,2,1,null], [5,6,3,null]]");

        results = table.select("a", "c").collectAsList();
        assertThat(results.toString()).isEqualTo("[[1,1], [5,3]]");

        results = table.groupBy().sum("b").collectAsList();
        assertThat(results.toString()).isEqualTo("[[8]]");
    }

    @Test
    public void testAlterColumnType() throws Exception {
        Path tablePath = new Path(warehousePath, "default.db/testAlterColumnType");
        SimpleTableTestHelper testHelper1 = createTestHelper(tablePath);
        testHelper1.write(GenericRowData.of(1, 2L, StringData.fromString("1")));
        testHelper1.write(GenericRowData.of(5, 6L, StringData.fromString("3")));
        testHelper1.commit();

        spark.sql("ALTER TABLE table_store.default.testAlterColumnType ALTER COLUMN a TYPE BIGINT");
        innerTestSimpleType(spark.table("table_store.default.testAlterColumnType"));
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
        spark.sql("ALTER TABLE table_store.default.t2 ALTER COLUMN a DROP NOT NULL");
        assertThat(fieldIsNullable(getField(schema2(), 0))).isTrue();

        spark.sql("ALTER TABLE table_store.default.t2 ALTER COLUMN b DROP NOT NULL");
        assertThat(fieldIsNullable(getField(schema2(), 1))).isTrue();

        spark.sql("ALTER TABLE table_store.default.t2 ALTER COLUMN c DROP NOT NULL");
        assertThat(fieldIsNullable(getField(schema2(), 2))).isTrue();

        spark.sql("ALTER TABLE table_store.default.t2 ALTER COLUMN c.c1 DROP NOT NULL");
        assertThat(fieldIsNullable(getNestedField(getField(schema2(), 2), 0))).isTrue();

        spark.sql("ALTER TABLE table_store.default.t2 ALTER COLUMN c.c1.c12 DROP NOT NULL");
        assertThat(fieldIsNullable(getNestedField(getNestedField(getField(schema2(), 2), 0), 1)))
                .isTrue();
    }

    @Test
    public void testAlterTableColumnComment() {
        assertThat(getField(schema1(), 0).description()).isNull();

        spark.sql("ALTER TABLE table_store.default.t1 ALTER COLUMN a COMMENT 'a new comment'");
        assertThat(getField(schema1(), 0).description()).isEqualTo("a new comment");

        spark.sql(
                "ALTER TABLE table_store.default.t1 ALTER COLUMN a COMMENT 'yet another comment'");
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
                "ALTER TABLE table_store.default.t2 ALTER COLUMN c COMMENT 'yet another comment about c'");
        spark.sql("ALTER TABLE table_store.default.t2 ALTER COLUMN c.c1 COMMENT 'a nested type'");
        spark.sql("ALTER TABLE table_store.default.t2 ALTER COLUMN c.c2 COMMENT 'a bigint type'");
        spark.sql(
                "ALTER TABLE table_store.default.t2 ALTER COLUMN c.c1.c11 COMMENT 'a double type'");
        spark.sql(
                "ALTER TABLE table_store.default.t2 ALTER COLUMN c.c1.c12 COMMENT 'a boolean array'");

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
