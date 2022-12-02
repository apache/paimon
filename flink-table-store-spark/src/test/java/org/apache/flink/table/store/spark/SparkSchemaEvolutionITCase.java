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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for schema evolution in spark. */
public class SparkSchemaEvolutionITCase extends SparkReadTestBase {

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
                .hasMessageContaining("cannot resolve '%s' given input columns", "a");
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
                                "java.lang.UnsupportedOperationException: Cannot drop/rename partition key[%s]",
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
                                "java.lang.UnsupportedOperationException: Cannot drop/rename partition key[%s]",
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
                                "java.lang.UnsupportedOperationException: Cannot drop/rename primary key[%s]",
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
}
