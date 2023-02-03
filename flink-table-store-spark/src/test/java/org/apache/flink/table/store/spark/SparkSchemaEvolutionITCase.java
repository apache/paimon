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

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for schema evolution in spark. */
public class SparkSchemaEvolutionITCase extends SparkReadTestBase {

    @Test
    public void testSetAndRemoveOption() {
        spark.sql("ALTER TABLE tablestore.default.t1 SET TBLPROPERTIES('xyc' 'unknown1')");

        Map<String, String> options =
                rowsToMap(
                        spark.sql("SELECT * FROM tablestore.default.`t1$options`").collectAsList());
        assertThat(options).containsEntry("xyc", "unknown1");

        spark.sql("ALTER TABLE tablestore.default.t1 UNSET TBLPROPERTIES('xyc')");

        options =
                rowsToMap(
                        spark.sql("SELECT * FROM tablestore.default.`t1$options`").collectAsList());
        assertThat(options).doesNotContainKey("xyc");

        assertThatThrownBy(
                        () ->
                                spark.sql(
                                        "ALTER TABLE tablestore.default.t1 SET TBLPROPERTIES('primary-key' = 'a')"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Alter primary key is not supported");
    }

    private Map<String, String> rowsToMap(List<Row> rows) {
        Map<String, String> map = new HashMap<>();
        rows.forEach(r -> map.put(r.getString(0), r.getString(1)));

        return map;
    }

    @Test
    public void testAddColumn() {
        createTable("testAddColumn");
        writeTable("testAddColumn", "(1, 2L, '1')", "(5, 6L, '3')");

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
    public void testAddNotNullColumn() {
        createTable("testAddNotNullColumn");

        List<Row> beforeAdd =
                spark.sql("SHOW CREATE TABLE tablestore.default.testAddNotNullColumn")
                        .collectAsList();
        assertThat(beforeAdd.toString())
                .isEqualTo(
                        "[[CREATE TABLE testAddNotNullColumn (\n"
                                + "  `a` INT NOT NULL,\n"
                                + "  `b` BIGINT,\n"
                                + "  `c` STRING)\n"
                                + buildTableProperties("default.db/testAddNotNullColumn")
                                + "]]");

        assertThatThrownBy(
                        () ->
                                spark.sql(
                                        "ALTER TABLE tablestore.default.testAddNotNullColumn ADD COLUMNS (d INT NOT NULL)"))
                .isInstanceOf(RuntimeException.class)
                .hasMessage(
                        "java.lang.IllegalArgumentException: ADD COLUMN cannot specify NOT NULL.");
    }

    @Test
    public void testRenameColumn() {
        createTable("testRenameColumn");
        writeTable("testRenameColumn", "(1, 2L, '1')", "(5, 6L, '3')");

        List<Row> beforeRename =
                spark.sql("SHOW CREATE TABLE tablestore.default.testRenameColumn").collectAsList();
        assertThat(beforeRename.toString())
                .isEqualTo(
                        "[[CREATE TABLE testRenameColumn (\n"
                                + "  `a` INT NOT NULL,\n"
                                + "  `b` BIGINT,\n"
                                + "  `c` STRING)\n"
                                + buildTableProperties("default.db/testRenameColumn")
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
                                + buildTableProperties("default.db/testRenameColumn")
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
                                + "USING tablestore\n"
                                + "PARTITIONED BY (a)\n"
                                + "COMMENT 'table comment'\n"
                                + "TBLPROPERTIES(\n"
                                + "  'foo' = 'bar',\n"
                                + String.format(
                                        "  'path' = '%s/%s')\n",
                                        warehousePath, "default.db/testRenamePartitionKey")
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
    public void testDropSingleColumn() {
        createTable("testDropSingleColumn");
        writeTable("testDropSingleColumn", "(1, 2L, '1')", "(5, 6L, '3')");

        List<Row> beforeDrop =
                spark.sql("SHOW CREATE TABLE tablestore.default.testDropSingleColumn")
                        .collectAsList();
        assertThat(beforeDrop.toString())
                .isEqualTo(
                        "[[CREATE TABLE testDropSingleColumn (\n"
                                + "  `a` INT NOT NULL,\n"
                                + "  `b` BIGINT,\n"
                                + "  `c` STRING)\n"
                                + buildTableProperties("default.db/testDropSingleColumn")
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
                                + buildTableProperties("default.db/testDropSingleColumn")
                                + "]]");

        Dataset<Row> table = spark.table("tablestore.default.testDropSingleColumn");
        List<Row> results = table.collectAsList();
        assertThat(results.toString()).isEqualTo("[[2,1], [6,3]]");
    }

    @Test
    public void testDropColumns() {
        createTable("testDropColumns");

        List<Row> beforeRename =
                spark.sql("SHOW CREATE TABLE tablestore.default.testDropColumns").collectAsList();
        assertThat(beforeRename.toString())
                .isEqualTo(
                        "[[CREATE TABLE testDropColumns (\n"
                                + "  `a` INT NOT NULL,\n"
                                + "  `b` BIGINT,\n"
                                + "  `c` STRING)\n"
                                + buildTableProperties("default.db/testDropColumns")
                                + "]]");

        spark.sql("ALTER TABLE tablestore.default.testDropColumns DROP COLUMNS a, b");

        List<Row> afterRename =
                spark.sql("SHOW CREATE TABLE tablestore.default.testDropColumns").collectAsList();
        assertThat(afterRename.toString())
                .isEqualTo(
                        "[[CREATE TABLE testDropColumns (\n"
                                + "  `c` STRING)\n"
                                + buildTableProperties("default.db/testDropColumns")
                                + "]]");
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
                                + "USING tablestore\n"
                                + "PARTITIONED BY (a)\n"
                                + "COMMENT 'table comment'\n"
                                + "TBLPROPERTIES(\n"
                                + "  'foo' = 'bar',\n"
                                + String.format(
                                        "  'path' = '%s/%s')\n",
                                        warehousePath, "default.db/testDropPartitionKey")
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
                                + "USING tablestore\n"
                                + "PARTITIONED BY (a)\n"
                                + "COMMENT 'table comment'\n"
                                + "TBLPROPERTIES(\n"
                                + String.format(
                                        "  'path' = '%s/%s')\n",
                                        warehousePath, "default.db/testDropPrimaryKey")
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

    @Test
    public void testAlterColumnType() {
        createTable("testAlterColumnType");
        writeTable("testAlterColumnType", "(1, 2L, '1')", "(5, 6L, '3')");

        spark.sql("ALTER TABLE tablestore.default.testAlterColumnType ALTER COLUMN b TYPE DOUBLE");
        assertThat(
                        spark.table("tablestore.default.testAlterColumnType").collectAsList()
                                .stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("[1,2.0,1]", "[5,6.0,3]");

        spark.sql("ALTER TABLE tablestore.default.testAlterColumnType DROP COLUMNS a");
        assertThat(
                        spark.table("tablestore.default.testAlterColumnType").collectAsList()
                                .stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("[2.0,1]", "[6.0,3]");
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
        createTable("testAlterTableColumnComment");
        Row row =
                spark.sql("SHOW CREATE TABLE tablestore.default.`testAlterTableColumnComment`")
                        .collectAsList()
                        .get(0);
        System.out.println(row);
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
     *   <li>1. Create table with fields [(1, a, int), (2, b, bigint), (3, c, string), (4, d, int),
     *       (5, e, int), (6, f, int)], insert 2 records
     *   <li>2. Rename (a, int)->(aa, bigint), c->a, b->c, (d, int)->(b, bigint), (f, int)->(ff,
     *       float), the fields are [(1, aa, bigint), (2, c, bigint), (3, a, string), (4, b,
     *       bigint), (5, e, int), (6, ff, float)] and insert 2 records
     *   <li>3. Drop fields aa, c, e, the fields are [(3, a, string), (4, b, bigint), (6, ff,
     *       float)] and insert 2 records
     *   <li>4. Add new fields d, c, e, the fields are [(3, a, string), (4, b, bigint), (6, ff,
     *       float), (7, d, int), (8, c, int), (9, e, int)] insert 2 records
     * </ul>
     *
     * <p>Verify records in table above.
     */
    @Test
    public void testSchemaEvolution() {
        // Create table with fields [a, b, c] and insert 2 records
        spark.sql(
                "CREATE TABLE tablestore.default.testSchemaEvolution(\n"
                        + "a INT NOT NULL, \n"
                        + "b BIGINT NOT NULL, \n"
                        + "c VARCHAR(10), \n"
                        + "d INT NOT NULL, \n"
                        + "e INT NOT NULL, \n"
                        + "f INT NOT NULL) \n"
                        + "TBLPROPERTIES ('file.format'='avro')");
        writeTable("testSchemaEvolution", "(1, 2L, '3', 4, 5, 6)", "(7, 8L, '9', 10, 11, 12)");
        assertThat(
                        spark.table("tablestore.default.testSchemaEvolution").collectAsList()
                                .stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("[1,2,3,4,5,6]", "[7,8,9,10,11,12]");
        assertThat(
                        spark.table("tablestore.default.testSchemaEvolution").select("a", "c", "e")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("[1,3,5]", "[7,9,11]");
        assertThat(
                        spark.table("tablestore.default.testSchemaEvolution").filter("a>1")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("[7,8,9,10,11,12]");

        // Rename (a, int)->(aa, bigint), c->a, b->c, (d, int)->(b, bigint), (f, int)->(ff, float),
        // the fields are [(1, aa, bigint), (2, c, bigint), (3, a, string), (4, b, bigint), (5, e,
        // int), (6, ff, float)] and insert 2 records
        spark.sql("ALTER TABLE tablestore.default.testSchemaEvolution RENAME COLUMN a to aa");
        spark.sql("ALTER TABLE tablestore.default.testSchemaEvolution ALTER COLUMN aa TYPE bigint");
        spark.sql("ALTER TABLE tablestore.default.testSchemaEvolution RENAME COLUMN c to a");
        spark.sql("ALTER TABLE tablestore.default.testSchemaEvolution RENAME COLUMN b to c");
        spark.sql("ALTER TABLE tablestore.default.testSchemaEvolution RENAME COLUMN d to b");
        spark.sql("ALTER TABLE tablestore.default.testSchemaEvolution ALTER COLUMN b TYPE bigint");
        spark.sql("ALTER TABLE tablestore.default.testSchemaEvolution RENAME COLUMN f to ff");
        spark.sql("ALTER TABLE tablestore.default.testSchemaEvolution ALTER COLUMN ff TYPE float");
        writeTable(
                "testSchemaEvolution",
                "(13L, 14L, '15', 16L, 17, 18.0F)",
                "(19L, 20L, '21', 22L, 23, 24.0F)");
        assertThat(
                        spark.table("tablestore.default.testSchemaEvolution").collectAsList()
                                .stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[1,2,3,4,5,6.0]",
                        "[7,8,9,10,11,12.0]",
                        "[13,14,15,16,17,18.0]",
                        "[19,20,21,22,23,24.0]");
        assertThat(
                        spark.table("tablestore.default.testSchemaEvolution")
                                .select("aa", "b", "ff").collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[1,4,6.0]", "[7,10,12.0]", "[13,16,18.0]", "[19,22,24.0]");
        assertThat(
                        spark.table("tablestore.default.testSchemaEvolution")
                                .select("aa", "a", "ff").filter("b>10L").collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("[13,15,18.0]", "[19,21,24.0]");

        // Drop fields aa, c, e, the fields are [(3, a, string), (4, b, bigint), (6, ff, float)] and
        // insert 2 records
        spark.sql("ALTER TABLE tablestore.default.testSchemaEvolution DROP COLUMNS aa, c, e");
        writeTable("testSchemaEvolution", "('25', 26L, 27.0F)", "('28', 29L, 30.0)");

        assertThat(
                        spark.table("tablestore.default.testSchemaEvolution").collectAsList()
                                .stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[3,4,6.0]",
                        "[9,10,12.0]",
                        "[15,16,18.0]",
                        "[21,22,24.0]",
                        "[25,26,27.0]",
                        "[28,29,30.0]");
        assertThat(
                        spark.table("tablestore.default.testSchemaEvolution").select("a", "ff")
                                .filter("b>10L").collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("[15,18.0]", "[21,24.0]", "[25,27.0]", "[28,30.0]");

        // Add new fields d, c, e, the fields are [(3, a, string), (4, b, bigint), (6, ff, float),
        // (7, d, int), (8, c, int), (9, e, int)] insert 2 records
        spark.sql(
                "ALTER TABLE tablestore.default.testSchemaEvolution ADD COLUMNS (d INT, c INT, e INT)");
        writeTable(
                "testSchemaEvolution",
                "('31', 32L, 33.0F, 34, 35, 36)",
                "('37', 38L, 39.0F, 40, 41, 42)");

        assertThat(
                        spark.table("tablestore.default.testSchemaEvolution").collectAsList()
                                .stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[3,4,6.0,null,null,null]",
                        "[9,10,12.0,null,null,null]",
                        "[15,16,18.0,null,null,null]",
                        "[21,22,24.0,null,null,null]",
                        "[25,26,27.0,null,null,null]",
                        "[28,29,30.0,null,null,null]",
                        "[31,32,33.0,34,35,36]",
                        "[37,38,39.0,40,41,42]");
        assertThat(
                        spark.table("tablestore.default.testSchemaEvolution").filter("b>10")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[15,16,18.0,null,null,null]",
                        "[21,22,24.0,null,null,null]",
                        "[25,26,27.0,null,null,null]",
                        "[28,29,30.0,null,null,null]",
                        "[31,32,33.0,34,35,36]",
                        "[37,38,39.0,40,41,42]");
        assertThat(
                        spark.table("tablestore.default.testSchemaEvolution")
                                .select("e", "a", "ff", "d", "b").filter("b>10L").collectAsList()
                                .stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[null,15,18.0,null,16]",
                        "[null,21,24.0,null,22]",
                        "[null,25,27.0,null,26]",
                        "[null,28,30.0,null,29]",
                        "[36,31,33.0,34,32]",
                        "[42,37,39.0,40,38]");
        assertThat(
                        spark.table("tablestore.default.testSchemaEvolution")
                                .select("e", "a", "ff", "d", "b").filter("b>10 and e is not null")
                                .collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("[36,31,33.0,34,32]", "[42,37,39.0,40,38]");
    }

    @Test
    public void testFilesTable() {
        // Create table with fields [a, b, c] and insert 2 records
        createTable("testFilesTable");
        writeTable("testFilesTable", "(1, 2L, '3')", "(4, 5L, '6')");
        assertThat(
                        getFieldStatsList(
                                spark.sql("SELECT * FROM tablestore.default.`testFilesTable$files`")
                                        .collectAsList()))
                .containsExactlyInAnyOrder("{a=0, b=0, c=0},{a=1, b=2, c=3},{a=4, b=5, c=6}");

        // Add new fields d, e, f and the fields are [a, b, c, d, e, f], insert 2 records
        spark.sql(
                "ALTER TABLE tablestore.default.testFilesTable ADD COLUMNS (d INT, e INT, f INT)");
        writeTable("testFilesTable", "(7, 8L, '9', 10, 11, 12)", "(13, 14L, '15', 16, 17, 18)");
        assertThat(
                        getFieldStatsList(
                                spark.sql("SELECT * FROM tablestore.default.`testFilesTable$files`")
                                        .collectAsList()))
                .containsExactlyInAnyOrder(
                        "{a=0, b=0, c=0, d=2, e=2, f=2},{a=1, b=2, c=3, d=null, e=null, f=null},{a=4, b=5, c=6, d=null, e=null, f=null}",
                        "{a=0, b=0, c=0, d=0, e=0, f=0},{a=7, b=8, c=15, d=10, e=11, f=12},{a=13, b=14, c=9, d=16, e=17, f=18}");

        // Drop fields c, e and the fields are [a, b, d, f], insert 2 records
        spark.sql("ALTER TABLE tablestore.default.testFilesTable DROP COLUMNS c, e");
        writeTable("testFilesTable", "(19, 20L, 21, 22)", "(23, 24L, 25, 26)");
        assertThat(
                        getFieldStatsList(
                                spark.sql("SELECT * FROM tablestore.default.`testFilesTable$files`")
                                        .collectAsList()))
                .containsExactlyInAnyOrder(
                        "{a=0, b=0, d=2, f=2},{a=1, b=2, d=null, f=null},{a=4, b=5, d=null, f=null}",
                        "{a=0, b=0, d=0, f=0},{a=7, b=8, d=10, f=12},{a=13, b=14, d=16, f=18}",
                        "{a=0, b=0, d=0, f=0},{a=19, b=20, d=21, f=22},{a=23, b=24, d=25, f=26}");
    }

    private List<String> getFieldStatsList(List<Row> fieldStatsRows) {
        return fieldStatsRows.stream()
                .map(
                        v ->
                                StringUtils.join(
                                        new Object[] {
                                            v.getString(11), v.getString(12), v.getString(13)
                                        },
                                        ","))
                .collect(Collectors.toList());
    }

    private String buildTableProperties(String tablePath) {
        return String.format(
                "TBLPROPERTIES(\n" + "  'file.format' = 'avro',\n" + "  'path' = '%s/%s')\n",
                warehousePath, tablePath);
    }
}
