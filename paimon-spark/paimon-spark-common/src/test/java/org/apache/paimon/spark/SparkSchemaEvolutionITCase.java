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

package org.apache.paimon.spark;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.testutils.assertj.PaimonAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * ITCase for schema evolution in spark.
 *
 * <p>NOTICE: since we make all fields nullable when convert paimon schema to spark schema, we
 * remove "NOT NULL" identifier for some fields even though these fields are supposed to be
 * non-nullable.
 */
public class SparkSchemaEvolutionITCase extends SparkReadTestBase {

    @Test
    public void testSetAndRemoveOption() {
        spark.sql("ALTER TABLE t1 SET TBLPROPERTIES('xyc' 'unknown1')");

        Map<String, String> options =
                rowsToMap(spark.sql("SELECT * FROM `t1$options`").collectAsList());
        assertThat(options).containsEntry("xyc", "unknown1");

        spark.sql("ALTER TABLE t1 UNSET TBLPROPERTIES('xyc')");

        options = rowsToMap(spark.sql("SELECT * FROM `t1$options`").collectAsList());
        assertThat(options).doesNotContainKey("xyc");

        assertThatThrownBy(() -> spark.sql("ALTER TABLE t1 SET TBLPROPERTIES('primary-key' = 'a')"))
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

        List<Row> beforeAdd = spark.sql("SHOW CREATE TABLE testAddColumn").collectAsList();
        assertThat(beforeAdd.toString()).contains(defaultShowCreateString("testAddColumn"));

        spark.sql("ALTER TABLE testAddColumn ADD COLUMN d STRING");

        List<Row> afterAdd = spark.sql("SHOW CREATE TABLE testAddColumn").collectAsList();
        assertThat(afterAdd.toString())
                .contains(
                        showCreateString(
                                "testAddColumn",
                                "a INT NOT NULL",
                                "b BIGINT",
                                "c STRING",
                                "d STRING"));

        assertThat(spark.table("testAddColumn").collectAsList().toString())
                .isEqualTo("[[1,2,1,null], [5,6,3,null]]");
    }

    @Test
    public void testAddNotNullColumn() {
        createTable("testAddNotNullColumn");

        List<Row> beforeAdd = spark.sql("SHOW CREATE TABLE testAddNotNullColumn").collectAsList();
        assertThat(beforeAdd.toString()).contains(defaultShowCreateString("testAddNotNullColumn"));

        assertThatThrownBy(
                        () ->
                                spark.sql(
                                        "ALTER TABLE testAddNotNullColumn ADD COLUMNS (d INT NOT NULL)"))
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "Column d cannot specify NOT NULL in the default.testAddNotNullColumn table."));
    }

    @Test
    public void testAddColumnPosition() {
        createTable("testAddColumnPositionFirst");
        spark.sql("ALTER TABLE testAddColumnPositionFirst ADD COLUMN d INT FIRST");
        List<Row> result =
                spark.sql("SHOW CREATE TABLE testAddColumnPositionFirst").collectAsList();
        assertThat(result.toString())
                .contains(
                        showCreateString(
                                "testAddColumnPositionFirst",
                                "d INT",
                                "a INT NOT NULL",
                                "b BIGINT",
                                "c STRING"));

        createTable("testAddColumnPositionAfter");
        spark.sql("ALTER TABLE testAddColumnPositionAfter ADD COLUMN d INT AFTER b");
        result = spark.sql("SHOW CREATE TABLE testAddColumnPositionAfter").collectAsList();
        assertThat(result.toString())
                .contains(
                        showCreateString(
                                "testAddColumnPositionAfter",
                                "a INT NOT NULL",
                                "b BIGINT",
                                "d INT",
                                "c STRING"));
    }

    @Test
    public void testRenameTable() {
        // TODO: add test case for hive catalog table
        assertThatThrownBy(() -> spark.sql("ALTER TABLE t3 RENAME TO t4"))
                .isInstanceOf(AnalysisException.class)
                .hasMessageContaining("The table or view `t3` cannot be found");

        assertThatThrownBy(() -> spark.sql("ALTER TABLE t1 RENAME TO t2"))
                .isInstanceOf(AnalysisException.class)
                .hasMessageContaining(
                        "Cannot create table or view default.t2 because it already exists");

        spark.sql("ALTER TABLE t1 RENAME TO t3");
        List<Row> tables = spark.sql("SHOW TABLES").collectAsList();
        assertThat(tables.stream().map(Row::toString))
                .containsExactlyInAnyOrder("[default,t2,false]", "[default,t3,false]");

        List<Row> afterRename = spark.sql("SHOW CREATE TABLE t3").collectAsList();
        assertThat(afterRename.toString()).contains(defaultShowCreateString("t3"));

        List<Row> data = spark.sql("SELECT * FROM t3").collectAsList();
        assertThat(data.toString()).isEqualTo("[[1,2,1], [5,6,3]]");
    }

    @Test
    public void testRenameColumn() {
        createTable("testRenameColumn");
        writeTable("testRenameColumn", "(1, 2L, '1')", "(5, 6L, '3')");

        List<Row> beforeRename = spark.sql("SHOW CREATE TABLE testRenameColumn").collectAsList();
        assertThat(beforeRename.toString()).contains(defaultShowCreateString("testRenameColumn"));
        List<Row> results = spark.table("testRenameColumn").select("a", "c").collectAsList();
        assertThat(results.toString()).isEqualTo("[[1,1], [5,3]]");

        // Rename "b" to "bb"
        spark.sql("ALTER TABLE testRenameColumn RENAME COLUMN b to bb");
        List<Row> afterRename = spark.sql("SHOW CREATE TABLE testRenameColumn").collectAsList();
        assertThat(afterRename.toString())
                .contains(
                        showCreateString(
                                "testRenameColumn", "a INT NOT NULL", "bb BIGINT", "c STRING"));
        Dataset<Row> table = spark.table("testRenameColumn");
        results = table.select("bb", "c").collectAsList();
        assertThat(results.toString()).isEqualTo("[[2,1], [6,3]]");
        assertThatThrownBy(() -> table.select("b", "c"))
                .isInstanceOf(AnalysisException.class)
                .hasMessageContaining(
                        "A column or function parameter with name `b` cannot be resolved. Did you mean one of the following?");
    }

    @Test
    public void testRenamePartitionKey() {
        spark.sql(
                "CREATE TABLE testRenamePartitionKey (\n"
                        + "a BIGINT,\n"
                        + "b STRING)\n"
                        + "PARTITIONED BY (a)\n");
        List<Row> beforeRename =
                spark.sql("SHOW CREATE TABLE testRenamePartitionKey").collectAsList();
        assertThat(beforeRename.toString())
                .contains(showCreateString("testRenamePartitionKey", "a BIGINT", "b STRING"));

        assertThatThrownBy(
                        () -> spark.sql("ALTER TABLE testRenamePartitionKey RENAME COLUMN a to aa"))
                .satisfies(
                        anyCauseMatches(
                                UnsupportedOperationException.class,
                                "Cannot rename partition column: [a]"));
    }

    @Test
    public void testDropSingleColumn() {
        createTable("testDropSingleColumn");
        writeTable("testDropSingleColumn", "(1, 2L, '1')", "(5, 6L, '3')");

        List<Row> beforeDrop = spark.sql("SHOW CREATE TABLE testDropSingleColumn").collectAsList();
        assertThat(beforeDrop.toString()).contains(defaultShowCreateString("testDropSingleColumn"));

        spark.sql("ALTER TABLE testDropSingleColumn DROP COLUMN b");

        List<Row> afterDrop = spark.sql("SHOW CREATE TABLE testDropSingleColumn").collectAsList();
        assertThat(afterDrop.toString())
                .contains(showCreateString("testDropSingleColumn", "a INT NOT NULL", "c STRING"));

        List<Row> results = spark.table("testDropSingleColumn").collectAsList();
        assertThat(results.toString()).isEqualTo("[[1,1], [5,3]]");
    }

    @Test
    public void testDropColumns() {
        createTable("testDropColumns");

        List<Row> beforeDrop = spark.sql("SHOW CREATE TABLE testDropColumns").collectAsList();
        assertThat(beforeDrop.toString()).contains(defaultShowCreateString("testDropColumns"));

        spark.sql("ALTER TABLE testDropColumns DROP COLUMNS b,c");

        List<Row> afterDrop = spark.sql("SHOW CREATE TABLE testDropColumns").collectAsList();
        assertThat(afterDrop.toString())
                .contains(showCreateString("testDropColumns", "a INT NOT NULL"));
    }

    @Test
    public void testDropPartitionKey() {
        spark.sql(
                "CREATE TABLE testDropPartitionKey (\n"
                        + "a BIGINT,\n"
                        + "b STRING) \n"
                        + "PARTITIONED BY (a)");

        List<Row> beforeDrop = spark.sql("SHOW CREATE TABLE testDropPartitionKey").collectAsList();
        assertThat(beforeDrop.toString())
                .contains(showCreateString("testDropPartitionKey", "a BIGINT", "b STRING"));

        assertThatThrownBy(() -> spark.sql("ALTER TABLE testDropPartitionKey DROP COLUMN a"))
                .satisfies(
                        anyCauseMatches(
                                UnsupportedOperationException.class,
                                "Cannot drop partition key or primary key: [a]"));
    }

    @Test
    public void testDropPrimaryKey() {
        spark.sql(
                "CREATE TABLE testDropPrimaryKey (\n"
                        + "a BIGINT,\n"
                        + "b STRING)\n"
                        + "PARTITIONED BY (a)\n"
                        + "TBLPROPERTIES ('primary-key' = 'a, b')");

        List<Row> beforeDrop = spark.sql("SHOW CREATE TABLE testDropPrimaryKey").collectAsList();
        assertThat(beforeDrop.toString())
                .contains(
                        showCreateString(
                                "testDropPrimaryKey", "a BIGINT NOT NULL", "b STRING NOT NULL"));

        assertThatThrownBy(() -> spark.sql("ALTER TABLE testDropPrimaryKey DROP COLUMN b"))
                .satisfies(
                        anyCauseMatches(
                                UnsupportedOperationException.class,
                                "Cannot drop partition key or primary key: [b]"));
    }

    @Test
    public void testRenamePrimaryKey() {
        spark.sql(
                "CREATE TABLE test_rename_primary_key_table (\n"
                        + "a BIGINT NOT NULL,\n"
                        + "b STRING)\n"
                        + "TBLPROPERTIES ('primary-key' = 'a')");

        spark.sql("INSERT INTO test_rename_primary_key_table VALUES(1, 'aaa'), (2, 'bbb')");

        spark.sql("ALTER TABLE test_rename_primary_key_table RENAME COLUMN a to a_");

        List<Row> result =
                spark.sql("SHOW CREATE TABLE test_rename_primary_key_table").collectAsList();
        assertThat(result.toString())
                .contains(
                        showCreateString(
                                "test_rename_primary_key_table", "a_ BIGINT NOT NULL", "b STRING"))
                .contains("'primary-key' = 'a_'");

        List<String> actual =
                spark.sql("SELECT * FROM test_rename_primary_key_table").collectAsList().stream()
                        .map(Row::toString)
                        .collect(Collectors.toList());

        assertThat(actual).containsExactlyInAnyOrder("[1,aaa]", "[2,bbb]");

        spark.sql("INSERT INTO test_rename_primary_key_table VALUES(1, 'AAA'), (2, 'BBB')");

        actual =
                spark.sql("SELECT * FROM test_rename_primary_key_table").collectAsList().stream()
                        .map(Row::toString)
                        .collect(Collectors.toList());
        assertThat(actual).containsExactlyInAnyOrder("[1,AAA]", "[2,BBB]");
    }

    @Test
    public void testRenameBucketKey() {
        spark.sql(
                "CREATE TABLE test_rename_bucket_key_table (\n"
                        + "a BIGINT NOT NULL,\n"
                        + "b STRING)\n"
                        + "TBLPROPERTIES ('bucket-key' = 'a,b', 'bucket'='16')");

        spark.sql("INSERT INTO test_rename_bucket_key_table VALUES(1, 'aaa'), (2, 'bbb')");

        spark.sql("ALTER TABLE test_rename_bucket_key_table RENAME COLUMN b to b_");

        List<Row> result =
                spark.sql("SHOW CREATE TABLE test_rename_bucket_key_table").collectAsList();
        assertThat(result.toString())
                .contains(
                        showCreateString(
                                "test_rename_bucket_key_table", "a BIGINT NOT NULL", "b_ STRING"))
                .contains("'bucket-key' = 'a,b_'");

        List<String> actual =
                spark.sql("SELECT * FROM test_rename_bucket_key_table where b_ = 'bbb'")
                        .collectAsList().stream()
                        .map(Row::toString)
                        .collect(Collectors.toList());

        assertThat(actual).containsExactlyInAnyOrder("[2,bbb]");
    }

    @Test
    public void testUpdateColumnPosition() {
        // move first
        createTable("tableFirst");
        spark.sql("ALTER TABLE tableFirst ALTER COLUMN b FIRST");
        List<Row> result = spark.sql("SHOW CREATE TABLE tableFirst").collectAsList();
        assertThat(result.toString())
                .contains(showCreateString("tableFirst", "b BIGINT", "a INT NOT NULL", "c STRING"));

        // move after
        createTable("tableAfter");
        spark.sql("ALTER TABLE tableAfter ALTER COLUMN c AFTER a");
        result = spark.sql("SHOW CREATE TABLE tableAfter").collectAsList();
        assertThat(result.toString())
                .contains(showCreateString("tableAfter", "a INT NOT NULL", "c STRING", "b BIGINT"));

        spark.sql("CREATE TABLE tableAfter1 (a INT, b BIGINT, c STRING, d DOUBLE)");
        spark.sql("ALTER TABLE tableAfter1 ALTER COLUMN b AFTER c");
        result = spark.sql("SHOW CREATE TABLE tableAfter1").collectAsList();
        assertThat(result.toString())
                .contains(
                        showCreateString(
                                "tableAfter1", "a INT", "c STRING", "b BIGINT", "d DOUBLE"));

        //  move self to first test
        createTable("tableFirstSelf");
        assertThatThrownBy(() -> spark.sql("ALTER TABLE tableFirstSelf ALTER COLUMN a FIRST"))
                .satisfies(
                        anyCauseMatches(
                                UnsupportedOperationException.class,
                                "Cannot move itself for column a"));

        //  move self to after test
        createTable("tableAfterSelf");
        assertThatThrownBy(() -> spark.sql("ALTER TABLE tableAfterSelf ALTER COLUMN b AFTER b"))
                .satisfies(
                        anyCauseMatches(
                                UnsupportedOperationException.class,
                                "Cannot move itself for column b"));

        // missing column
        createTable("tableMissing");
        assertThatThrownBy(() -> spark.sql("ALTER TABLE tableMissing ALTER COLUMN d FIRST"))
                .hasMessageContaining("Missing field d in table paimon.default.tableMissing");

        createTable("tableMissingAfter");
        assertThatThrownBy(() -> spark.sql("ALTER TABLE tableMissingAfter ALTER COLUMN a AFTER d"))
                .hasMessageContaining("Missing field d in table paimon.default.tableMissingAfter");
    }

    @Test
    public void testAlterColumnType() {
        createTableWithNonNullColumn("testAlterColumnType");
        writeTable("testAlterColumnType", "(1, 2L, '1')", "(5, 6L, '3')");

        assertThatThrownBy(
                        () -> {
                            writeTable("testAlterColumnType", "(1, null, 'a')");
                        })
                .hasMessageContaining("Cannot write null to non-null column(b)");

        List<Row> beforeAlter = spark.sql("SHOW CREATE TABLE testAlterColumnType").collectAsList();
        assertThat(beforeAlter.toString())
                .contains(defaultShowCreateStringWithNonNullColumn("testAlterColumnType"));

        spark.sql("ALTER TABLE testAlterColumnType ALTER COLUMN b TYPE DOUBLE");
        assertThat(spark.table("testAlterColumnType").collectAsList().toString())
                .isEqualTo("[[1,2.0,1], [5,6.0,3]]");

        List<Row> afterAlter = spark.sql("SHOW CREATE TABLE testAlterColumnType").collectAsList();
        assertThat(afterAlter.toString())
                .contains(
                        showCreateString(
                                "testAlterColumnType",
                                "a INT NOT NULL",
                                "b DOUBLE NOT NULL",
                                "c STRING"));
        assertThatThrownBy(
                        () -> {
                            writeTable("testAlterColumnType", "(1, null, 'a')");
                        })
                .hasMessageContaining("Cannot write null to non-null column(b)");
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
        spark.sql("ALTER TABLE t2 ALTER COLUMN a DROP NOT NULL");
        assertThat(fieldIsNullable(getField(schema2(), 0))).isTrue();

        spark.sql("ALTER TABLE t2 ALTER COLUMN b DROP NOT NULL");
        assertThat(fieldIsNullable(getField(schema2(), 1))).isTrue();

        spark.sql("ALTER TABLE t2 ALTER COLUMN c DROP NOT NULL");
        assertThat(fieldIsNullable(getField(schema2(), 2))).isTrue();

        spark.sql("ALTER TABLE t2 ALTER COLUMN c.c1 DROP NOT NULL");
        assertThat(fieldIsNullable(getNestedField(getField(schema2(), 2), 0))).isTrue();

        spark.sql("ALTER TABLE t2 ALTER COLUMN c.c1.c12 DROP NOT NULL");
        assertThat(fieldIsNullable(getNestedField(getNestedField(getField(schema2(), 2), 0), 1)))
                .isTrue();
    }

    @Test
    public void testAlterPrimaryKeyNullability() {
        spark.sql(
                "CREATE TABLE testAlterPkNullability (\n"
                        + "a BIGINT,\n"
                        + "b STRING)\n"
                        + "TBLPROPERTIES ('primary-key' = 'a')");
        assertThatThrownBy(
                        () ->
                                spark.sql(
                                        "ALTER TABLE testAlterPkNullability ALTER COLUMN a DROP NOT NULL"))
                .satisfies(
                        anyCauseMatches(
                                UnsupportedOperationException.class,
                                "Cannot change nullability of primary key"));
    }

    @Test
    public void testAlterTableColumnComment() {
        createTable("testAlterTableColumnComment");
        assertThat(getField(schema1(), 0).description()).isNull();

        spark.sql("ALTER TABLE t1 ALTER COLUMN a COMMENT 'a new comment'");
        assertThat(getField(schema1(), 0).description()).isEqualTo("a new comment");

        spark.sql("ALTER TABLE t1 ALTER COLUMN a COMMENT 'yet another comment'");
        assertThat(getField(schema1(), 0).description()).isEqualTo("yet another comment");

        assertThat(getField(schema2(), 2).description()).isEqualTo("comment about c");
        assertThat(getNestedField(getField(schema2(), 2), 0).description()).isNull();
        assertThat(getNestedField(getField(schema2(), 2), 1).description())
                .isEqualTo("comment about c2");
        assertThat(getNestedField(getNestedField(getField(schema2(), 2), 0), 0).description())
                .isNull();
        assertThat(getNestedField(getNestedField(getField(schema2(), 2), 0), 1).description())
                .isNull();

        spark.sql("ALTER TABLE t2 ALTER COLUMN c COMMENT 'yet another comment about c'");
        spark.sql("ALTER TABLE t2 ALTER COLUMN c.c1 COMMENT 'a nested type'");
        spark.sql("ALTER TABLE t2 ALTER COLUMN c.c2 COMMENT 'a bigint type'");
        spark.sql("ALTER TABLE t2 ALTER COLUMN c.c1.c11 COMMENT 'a double type'");
        spark.sql("ALTER TABLE t2 ALTER COLUMN c.c1.c12 COMMENT 'a boolean array'");

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
                "CREATE TABLE testSchemaEvolution(\n"
                        + "a INT, \n"
                        + "b BIGINT, \n"
                        + "c VARCHAR(10), \n"
                        + "d INT, \n"
                        + "e INT, \n"
                        + "f INT) \n"
                        + "TBLPROPERTIES ('file.format'='avro')");
        writeTable("testSchemaEvolution", "(1, 2L, '3', 4, 5, 6)", "(7, 8L, '9', 10, 11, 12)");

        Dataset<Row> table = spark.table("testSchemaEvolution");
        assertThat(table.collectAsList().stream().map(Row::toString).collect(Collectors.toList()))
                .containsExactlyInAnyOrder("[1,2,3,4,5,6]", "[7,8,9,10,11,12]");
        assertThat(
                        table.select("a", "c", "e").collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("[1,3,5]", "[7,9,11]");
        assertThat(
                        table.filter("a>1").collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("[7,8,9,10,11,12]");

        // Rename (a, int)->(aa, bigint), c->a, b->c, (d, int)->(b, bigint), (f, int)->(ff, float),
        // the fields are [(1, aa, bigint), (2, c, bigint), (3, a, string), (4, b, bigint), (5, e,
        // int), (6, ff, float)] and insert 2 records
        spark.sql("ALTER TABLE testSchemaEvolution RENAME COLUMN a to aa");
        spark.sql("ALTER TABLE testSchemaEvolution ALTER COLUMN aa TYPE bigint");
        spark.sql("ALTER TABLE testSchemaEvolution RENAME COLUMN c to a");
        spark.sql("ALTER TABLE testSchemaEvolution RENAME COLUMN b to c");
        spark.sql("ALTER TABLE testSchemaEvolution RENAME COLUMN d to b");
        spark.sql("ALTER TABLE testSchemaEvolution ALTER COLUMN b TYPE bigint");
        spark.sql("ALTER TABLE testSchemaEvolution RENAME COLUMN f to ff");
        spark.sql("ALTER TABLE testSchemaEvolution ALTER COLUMN ff TYPE float");
        writeTable(
                "testSchemaEvolution",
                "(13L, 14L, '15', 16L, 17, 18.0F)",
                "(19L, 20L, '21', 22L, 23, 24.0F)");

        table = spark.table("testSchemaEvolution");
        assertThat(table.collectAsList().stream().map(Row::toString).collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[1,2,3,4,5,6.0]",
                        "[7,8,9,10,11,12.0]",
                        "[13,14,15,16,17,18.0]",
                        "[19,20,21,22,23,24.0]");
        assertThat(
                        table.select("aa", "b", "ff").collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[1,4,6.0]", "[7,10,12.0]", "[13,16,18.0]", "[19,22,24.0]");
        assertThat(
                        table.select("aa", "a", "ff").filter("b>10L").collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("[13,15,18.0]", "[19,21,24.0]");

        // Drop fields aa, c, e, the fields are [(3, a, string), (4, b, bigint), (6, ff, float)] and
        // insert 2 records
        spark.sql("ALTER TABLE testSchemaEvolution DROP COLUMNS aa, c, e");
        writeTable("testSchemaEvolution", "('25', 26L, 27.0F)", "('28', 29L, 30.0)");

        table = spark.table("testSchemaEvolution");
        assertThat(table.collectAsList().stream().map(Row::toString).collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[3,4,6.0]",
                        "[9,10,12.0]",
                        "[15,16,18.0]",
                        "[21,22,24.0]",
                        "[25,26,27.0]",
                        "[28,29,30.0]");
        assertThat(
                        table.select("a", "ff").filter("b>10L").collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("[15,18.0]", "[21,24.0]", "[25,27.0]", "[28,30.0]");

        // Add new fields d, c, e, the fields are [(3, a, string), (4, b, bigint), (6, ff, float),
        // (7, d, int), (8, c, int), (9, e, int)] insert 2 records
        spark.sql("ALTER TABLE testSchemaEvolution ADD COLUMNS (d INT, c INT, e INT)");
        writeTable(
                "testSchemaEvolution",
                "('31', 32L, 33.0F, 34, 35, 36)",
                "('37', 38L, 39.0F, 40, 41, 42)");

        table = spark.table("testSchemaEvolution");
        assertThat(table.collectAsList().stream().map(Row::toString).collect(Collectors.toList()))
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
                        table.filter("b>10").collectAsList().stream()
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
                        table.select("e", "a", "ff", "d", "b").filter("b>10L").collectAsList()
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
                        table.select("e", "a", "ff", "d", "b").filter("b>10 and e is not null")
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
                                spark.sql("SELECT * FROM `testFilesTable$files`").collectAsList()))
                .containsExactlyInAnyOrder("{a=0, b=0, c=0},{a=1, b=2, c=3},{a=4, b=5, c=6}");

        // Add new fields d, e, f and the fields are [a, b, c, d, e, f], insert 2 records
        spark.sql("ALTER TABLE testFilesTable ADD COLUMNS (d INT, e INT, f INT)");
        writeTable("testFilesTable", "(7, 8L, '9', 10, 11, 12)", "(13, 14L, '15', 16, 17, 18)");
        assertThat(
                        getFieldStatsList(
                                spark.sql("SELECT * FROM `testFilesTable$files`").collectAsList()))
                .containsExactlyInAnyOrder(
                        "{a=0, b=0, c=0, d=2, e=2, f=2},{a=1, b=2, c=3, d=null, e=null, f=null},{a=4, b=5, c=6, d=null, e=null, f=null}",
                        "{a=0, b=0, c=0, d=0, e=0, f=0},{a=7, b=8, c=15, d=10, e=11, f=12},{a=13, b=14, c=9, d=16, e=17, f=18}");

        // Drop fields c, e and the fields are [a, b, d, f], insert 2 records
        spark.sql("ALTER TABLE testFilesTable DROP COLUMNS c, e");
        writeTable("testFilesTable", "(19, 20L, 21, 22)", "(23, 24L, 25, 26)");
        assertThat(
                        getFieldStatsList(
                                spark.sql("SELECT * FROM `testFilesTable$files`").collectAsList()))
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
                                            v.getString(10), v.getString(11), v.getString(12)
                                        },
                                        ","))
                .collect(Collectors.toList());
    }

    @ParameterizedTest()
    @ValueSource(strings = {"orc", "avro", "parquet"})
    public void testAddAndDropNestedColumn(String formatType) {
        String tableName = "testAddNestedColumnTable";
        spark.sql(
                "CREATE TABLE paimon.default."
                        + tableName
                        + " (k INT NOT NULL, v STRUCT<f1: INT, f2: STRUCT<f1: STRING, f2: INT>>) "
                        + "TBLPROPERTIES ('bucket' = '1', 'primary-key' = 'k', 'file.format' = '"
                        + formatType
                        + "')");
        spark.sql(
                "INSERT INTO paimon.default."
                        + tableName
                        + " VALUES (1, STRUCT(10, STRUCT('apple', 100))), (2, STRUCT(20, STRUCT('banana', 200)))");
        assertThat(
                        spark.sql("SELECT * FROM paimon.default." + tableName).collectAsList()
                                .stream()
                                .map(Row::toString))
                .containsExactlyInAnyOrder("[1,[10,[apple,100]]]", "[2,[20,[banana,200]]]");
        assertThat(
                        spark.sql("SELECT v.f2.f1, k FROM paimon.default." + tableName)
                                .collectAsList().stream()
                                .map(Row::toString))
                .containsExactlyInAnyOrder("[apple,1]", "[banana,2]");

        spark.sql("ALTER TABLE paimon.default." + tableName + " ADD COLUMN v.f3 STRING");
        spark.sql("ALTER TABLE paimon.default." + tableName + " ADD COLUMN v.f2.f3 BIGINT");
        spark.sql(
                "INSERT INTO paimon.default."
                        + tableName
                        + " VALUES (1, STRUCT(11, STRUCT('APPLE', 101, 1001), 'one')), (3, STRUCT(31, STRUCT('CHERRY', 301, 3001), 'three'))");
        assertThat(
                        spark.sql("SELECT * FROM paimon.default." + tableName).collectAsList()
                                .stream()
                                .map(Row::toString))
                .containsExactlyInAnyOrder(
                        "[1,[11,[APPLE,101,1001],one]]",
                        "[2,[20,[banana,200,null],null]]",
                        "[3,[31,[CHERRY,301,3001],three]]");
        assertThat(
                        spark.sql("SELECT v.f2.f2, v.f3, k FROM paimon.default." + tableName)
                                .collectAsList().stream()
                                .map(Row::toString))
                .containsExactlyInAnyOrder("[101,one,1]", "[200,null,2]", "[301,three,3]");

        spark.sql("ALTER TABLE paimon.default." + tableName + " DROP COLUMN v.f2.f1");
        spark.sql(
                "INSERT INTO paimon.default."
                        + tableName
                        + " VALUES (1, STRUCT(12, STRUCT(102, 1002), 'one')), (4, STRUCT(42, STRUCT(402, 4002), 'four'))");
        assertThat(
                        spark.sql("SELECT * FROM paimon.default." + tableName).collectAsList()
                                .stream()
                                .map(Row::toString))
                .containsExactlyInAnyOrder(
                        "[1,[12,[102,1002],one]]",
                        "[2,[20,[200,null],null]]",
                        "[3,[31,[301,3001],three]]",
                        "[4,[42,[402,4002],four]]");

        spark.sql(
                "ALTER TABLE paimon.default."
                        + tableName
                        + " ADD COLUMN v.f2.f1 DECIMAL(5, 2) AFTER f2");
        spark.sql(
                "INSERT INTO paimon.default."
                        + tableName
                        + " VALUES (1, STRUCT(13, STRUCT(103, 100.03, 1003), 'one')), (5, STRUCT(53, STRUCT(503, 500.03, 5003), 'five'))");
        assertThat(
                        spark.sql("SELECT * FROM paimon.default." + tableName).collectAsList()
                                .stream()
                                .map(Row::toString))
                .containsExactlyInAnyOrder(
                        "[1,[13,[103,100.03,1003],one]]",
                        "[2,[20,[200,null,null],null]]",
                        "[3,[31,[301,null,3001],three]]",
                        "[4,[42,[402,null,4002],four]]",
                        "[5,[53,[503,500.03,5003],five]]");
    }

    @ParameterizedTest()
    @ValueSource(strings = {"orc", "avro", "parquet"})
    public void testRenameNestedColumn(String formatType) {
        String tableName = "testRenameNestedColumnTable";
        spark.sql(
                "CREATE TABLE paimon.default."
                        + tableName
                        + " (k INT NOT NULL, v STRUCT<f1: INT, f2: STRUCT<f1: STRING, f2: INT>>) "
                        + "TBLPROPERTIES ('file.format' = '"
                        + formatType
                        + "')");
        spark.sql(
                "INSERT INTO paimon.default."
                        + tableName
                        + " VALUES (1, STRUCT(10, STRUCT('apple', 100))), (2, STRUCT(20, STRUCT('banana', 200)))");
        assertThat(
                        spark.sql("SELECT v.f2.f1, k FROM paimon.default." + tableName)
                                .collectAsList().stream()
                                .map(Row::toString))
                .containsExactlyInAnyOrder("[apple,1]", "[banana,2]");

        spark.sql("ALTER TABLE paimon.default." + tableName + " RENAME COLUMN v.f2.f1 to f100");
        assertThat(
                        spark.sql("SELECT v.f2.f100, k FROM paimon.default." + tableName)
                                .collectAsList().stream()
                                .map(Row::toString))
                .containsExactlyInAnyOrder("[apple,1]", "[banana,2]");
    }

    @ParameterizedTest()
    @ValueSource(strings = {"orc", "avro", "parquet"})
    public void testUpdateNestedColumnType(String formatType) {
        String tableName = "testRenameNestedColumnTable";
        spark.sql(
                "CREATE TABLE paimon.default."
                        + tableName
                        + " (k INT NOT NULL, v STRUCT<f1: INT, f2: STRUCT<f1: STRING, f2: INT>>) "
                        + "TBLPROPERTIES ('bucket' = '1', 'primary-key' = 'k', 'file.format' = '"
                        + formatType
                        + "')");
        spark.sql(
                "INSERT INTO paimon.default."
                        + tableName
                        + " VALUES (1, STRUCT(10, STRUCT('apple', 100))), (2, STRUCT(20, STRUCT('banana', 200)))");
        assertThat(
                        spark.sql("SELECT v.f2.f2, k FROM paimon.default." + tableName)
                                .collectAsList().stream()
                                .map(Row::toString))
                .containsExactlyInAnyOrder("[100,1]", "[200,2]");

        spark.sql("ALTER TABLE paimon.default." + tableName + " CHANGE COLUMN v.f2.f2 f2 BIGINT");
        spark.sql(
                "INSERT INTO paimon.default."
                        + tableName
                        + " VALUES (1, STRUCT(11, STRUCT('APPLE', 101))), (3, STRUCT(31, STRUCT('CHERRY', 3000000000000)))");
        assertThat(
                        spark.sql("SELECT v.f2.f2, k FROM paimon.default." + tableName)
                                .collectAsList().stream()
                                .map(Row::toString))
                .containsExactlyInAnyOrder("[101,1]", "[200,2]", "[3000000000000,3]");
    }
}
