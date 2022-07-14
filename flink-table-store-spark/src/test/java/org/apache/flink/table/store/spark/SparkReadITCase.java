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
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.table.FileStoreTableFactory;
import org.apache.flink.table.types.logical.BigIntType;
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
import java.util.UUID;

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
        SimpleTableTestHelper testHelper1 = createTestHelper(tablePath1);
        testHelper1.write(GenericRowData.of(1, 2L, StringData.fromString("1")));
        testHelper1.write(GenericRowData.of(3, 4L, StringData.fromString("2")));
        testHelper1.write(GenericRowData.of(5, 6L, StringData.fromString("3")));
        testHelper1.write(GenericRowData.ofKind(RowKind.DELETE, 3, 4L, StringData.fromString("2")));
        testHelper1.commit();

        tablePath2 = new Path(warehousePath, "default.db/t2");
        SimpleTableTestHelper testHelper2 = createTestHelper(tablePath2);
        testHelper2.write(GenericRowData.of(1, 2L, StringData.fromString("1")));
        testHelper2.write(GenericRowData.of(3, 4L, StringData.fromString("2")));
        testHelper2.commit();
        testHelper2.write(GenericRowData.of(5, 6L, StringData.fromString("3")));
        testHelper2.write(GenericRowData.of(7, 8L, StringData.fromString("4")));
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
        innerTestNormal(
                spark.read().format("tablestore").option("path", tablePath1.toString()).load());
    }

    @Test
    public void testFilterPushDown() {
        innerTestFilterPushDown(
                spark.read().format("tablestore").option("path", tablePath2.toString()).load());
    }

    @Test
    public void testCatalogNormal() {
        innerTestNormal(spark.table("table_store.default.t1"));
    }

    @Test
    public void testCatalogFilterPushDown() {
        innerTestFilterPushDown(spark.table("table_store.default.t2"));
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
        Path tablePath = new Path(warehousePath, "default.db/" + UUID.randomUUID());
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
        Path tablePath = new Path(warehousePath, "default.db/testAddColumn");
        SimpleTableTestHelper testHelper1 = createTestHelper(tablePath);
        testHelper1.write(GenericRowData.of(1, 2L, StringData.fromString("1")));
        testHelper1.write(GenericRowData.of(5, 6L, StringData.fromString("3")));
        testHelper1.commit();

        spark.sql("ALTER TABLE table_store.default.testAddColumn ALTER COLUMN a TYPE BIGINT");
        innerTestNormal(spark.table("table_store.default.testAddColumn"));
    }

    @Test
    public void testAlterTableColumnNullability() {
        assertThat(schema1().fields().get(0).type().logicalType().isNullable()).isFalse();

        // note: for Spark, it is illegal to change nullable column to non-nullable
        spark.sql("ALTER TABLE table_store.default.t1 ALTER COLUMN a DROP NOT NULL");
        assertThat(schema1().fields().get(0).type().logicalType().isNullable()).isTrue();
    }

    @Test
    public void testAlterTableColumnComment() {
        assertThat(schema1().fields().get(0).description()).isNull();

        spark.sql("ALTER TABLE table_store.default.t1 ALTER COLUMN a COMMENT 'a new comment'");
        assertThat(schema1().fields().get(0).description()).isEqualTo("a new comment");

        spark.sql(
                "ALTER TABLE table_store.default.t1 ALTER COLUMN a COMMENT 'yet another comment'");
        assertThat(schema1().fields().get(0).description()).isEqualTo("yet another comment");
    }

    private TableSchema schema1() {
        return FileStoreTableFactory.create(tablePath1).schema();
    }

    private void innerTestNormal(Dataset<Row> dataset) {
        List<Row> results = dataset.collectAsList();
        assertThat(results.toString()).isEqualTo("[[1,2,1], [5,6,3]]");

        results = dataset.select("a", "c").collectAsList();
        assertThat(results.toString()).isEqualTo("[[1,1], [5,3]]");

        results = dataset.groupBy().sum("b").collectAsList();
        assertThat(results.toString()).isEqualTo("[[8]]");
    }

    private void innerTestFilterPushDown(Dataset<Row> dataset) {
        List<Row> results = dataset.filter("a < 4").select("a", "c").collectAsList();
        assertThat(results.toString()).isEqualTo("[[1,1], [3,2]]");
    }
}
