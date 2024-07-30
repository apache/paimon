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

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.VarCharType;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for spark reader. */
public class SparkReadITCase extends SparkReadTestBase {

    @Test
    public void testNormal() {
        innerTestSimpleType(spark.table("t1"));

        innerTestNestedType(spark.table("t2"));
    }

    @Test
    public void testFilterPushDown() {
        innerTestSimpleTypeFilterPushDown(spark.table("t1"));

        innerTestNestedTypeFilterPushDown(spark.table("t2"));
    }

    @Test
    public void testCatalogNormal() {
        innerTestSimpleType(spark.table("t1"));
        innerTestNestedType(spark.table("t2"));
    }

    @Test
    public void testSnapshotsTable() {
        List<Row> rows =
                spark.table("`t1$snapshots`")
                        .select("snapshot_id", "schema_id", "commit_user", "commit_kind")
                        .collectAsList();
        String commitUser = rows.get(0).getString(2);
        String rowString = String.format("[[1,0,%s,APPEND]]", commitUser);
        assertThat(rows.toString()).isEqualTo(rowString);

        spark.sql(
                "CREATE TABLE schemasTable (\n"
                        + "a BIGINT,\n"
                        + "b STRING)\n"
                        + "TBLPROPERTIES ('primary-key' = 'a')");
        spark.sql("ALTER TABLE schemasTable ADD COLUMN c STRING");
        List<Row> schemas = spark.table("`schemasTable$schemas`").collectAsList();
        List<?> fieldsList = schemas.stream().map(row -> row.get(1)).collect(Collectors.toList());
        assertThat(fieldsList.stream().map(Object::toString).collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[{\"id\":0,\"name\":\"a\",\"type\":\"BIGINT NOT NULL\"},"
                                + "{\"id\":1,\"name\":\"b\",\"type\":\"STRING\"}]",
                        "[{\"id\":0,\"name\":\"a\",\"type\":\"BIGINT NOT NULL\"},"
                                + "{\"id\":1,\"name\":\"b\",\"type\":\"STRING\"},"
                                + "{\"id\":2,\"name\":\"c\",\"type\":\"STRING\"}]");
    }

    @Test
    public void testSnapshotsTableWithRecordCount() {
        List<Row> rows =
                spark.table("`t1$snapshots`")
                        .select(
                                "snapshot_id",
                                "total_record_count",
                                "delta_record_count",
                                "changelog_record_count")
                        .collectAsList();
        assertThat(rows.toString()).isEqualTo("[[1,3,3,0]]");
    }

    @Test
    public void testManifestsTable() {
        List<Row> rows =
                spark.table("`t1$manifests`")
                        .select("schema_id", "file_name", "file_size")
                        .collectAsList();
        Long schemaId = rows.get(0).getLong(0);
        String fileName = rows.get(0).getString(1);
        Long fileSize = rows.get(0).getLong(2);

        assertThat(schemaId).isEqualTo(0L);
        assertThat(fileName).startsWith("manifest");
        assertThat(fileSize).isGreaterThan(0L);
    }

    @Test
    public void testManifestsTableWithRecordCount() {
        List<Row> rows =
                spark.table("`t1$manifests`")
                        .select("num_added_files", "num_deleted_files")
                        .collectAsList();
        assertThat(rows.toString()).isEqualTo("[[1,0]]");
    }

    @Test
    public void testCatalogFilterPushDown() {
        innerTestSimpleTypeFilterPushDown(spark.table("t1"));

        innerTestNestedTypeFilterPushDown(spark.table("t2"));
    }

    @Test
    public void testDefaultNamespace() {
        assertThat(spark.sql("SHOW CURRENT NAMESPACE").collectAsList().toString())
                .isEqualTo("[[paimon,default]]");
    }

    @Test
    public void testCreateTable() {
        spark.sql(
                "CREATE TABLE testCreateTable(\n"
                        + "a BIGINT,\n"
                        + "b VARCHAR(10),\n"
                        + "c CHAR(10))");
        assertThat(
                        spark.sql("SELECT fields FROM `testCreateTable$schemas`")
                                .collectAsList()
                                .toString())
                .isEqualTo(
                        "[[["
                                + "{\"id\":0,\"name\":\"a\",\"type\":\"BIGINT\"},"
                                + "{\"id\":1,\"name\":\"b\",\"type\":\"VARCHAR(10)\"},"
                                + "{\"id\":2,\"name\":\"c\",\"type\":\"CHAR(10)\"}]]]");
    }

    @Test
    public void testCreateTableAs() {
        spark.sql(
                "CREATE TABLE testCreateTable(\n"
                        + "a BIGINT,\n"
                        + "b VARCHAR(10),\n"
                        + "c CHAR(10))");
        spark.sql("INSERT INTO testCreateTable VALUES(1,'a','b')");
        spark.sql("CREATE TABLE testCreateTableAs AS SELECT * FROM testCreateTable");
        List<Row> result = spark.sql("SELECT * FROM testCreateTableAs").collectAsList();

        assertThat(result.stream().map(Row::toString))
                .containsExactlyInAnyOrder("[1,a,b         ]");

        // partitioned table
        spark.sql(
                "CREATE TABLE partitionedTable (\n"
                        + "a BIGINT,\n"
                        + "b STRING,\n"
                        + "c STRING)\n"
                        + "PARTITIONED BY (a,b)");
        spark.sql("INSERT INTO partitionedTable VALUES(1,'aaa','bbb')");
        spark.sql(
                "CREATE TABLE partitionedTableAs PARTITIONED BY (a) AS SELECT * FROM partitionedTable");
        assertThat(spark.sql("SHOW CREATE TABLE partitionedTableAs").collectAsList().toString())
                .isEqualTo(
                        String.format(
                                "[[%s"
                                        + "PARTITIONED BY (a)\n"
                                        + "TBLPROPERTIES (\n"
                                        + "  'path' = '%s')\n"
                                        + "]]",
                                showCreateString(
                                        "partitionedTableAs", "a BIGINT", "b STRING", "c STRING"),
                                new Path(warehousePath, "default.db/partitionedTableAs")));
        List<Row> resultPartition = spark.sql("SELECT * FROM partitionedTableAs").collectAsList();
        assertThat(resultPartition.stream().map(Row::toString))
                .containsExactlyInAnyOrder("[1,aaa,bbb]");

        // change TBLPROPERTIES
        spark.sql(
                "CREATE TABLE testTable(\n"
                        + "a BIGINT,\n"
                        + "b VARCHAR(10),\n"
                        + "c CHAR(10))\n"
                        + " TBLPROPERTIES(\n"
                        + " 'file.format' = 'orc'\n"
                        + ")");
        spark.sql("INSERT INTO testTable VALUES(1,'a','b')");
        spark.sql(
                "CREATE TABLE testTableAs TBLPROPERTIES ('file.format' = 'parquet') AS SELECT * FROM testTable");
        assertThat(spark.sql("SHOW CREATE TABLE testTableAs").collectAsList().toString())
                .isEqualTo(
                        String.format(
                                "[[%s"
                                        + "TBLPROPERTIES (\n"
                                        + "  'file.format' = 'parquet',\n"
                                        + "  'path' = '%s')\n"
                                        + "]]",
                                showCreateString(
                                        "testTableAs", "a BIGINT", "b VARCHAR(10)", "c CHAR(10)"),
                                new Path(warehousePath, "default.db/testTableAs")));
        List<Row> resultProp = spark.sql("SELECT * FROM testTableAs").collectAsList();

        assertThat(resultProp.stream().map(Row::toString))
                .containsExactlyInAnyOrder("[1,a,b         ]");

        // primary key
        spark.sql(
                "CREATE TABLE t_pk (\n"
                        + "a BIGINT,\n"
                        + "b STRING,\n"
                        + "c STRING\n"
                        + ") TBLPROPERTIES (\n"
                        + "  'primary-key' = 'a,b'\n"
                        + ")\n"
                        + "COMMENT 'table comment'");
        spark.sql("INSERT INTO t_pk VALUES(1,'aaa','bbb')");
        spark.sql("CREATE TABLE t_pk_as TBLPROPERTIES ('primary-key' = 'a') AS SELECT * FROM t_pk");
        assertThat(spark.sql("SHOW CREATE TABLE t_pk_as").collectAsList().toString())
                .isEqualTo(
                        String.format(
                                "[[%sTBLPROPERTIES (\n  'path' = '%s',\n  'primary-key' = 'a')\n]]",
                                showCreateString(
                                        "t_pk_as", "a BIGINT NOT NULL", "b STRING", "c STRING"),
                                new Path(warehousePath, "default.db/t_pk_as")));
        List<Row> resultPk = spark.sql("SELECT * FROM t_pk_as").collectAsList();

        assertThat(resultPk.stream().map(Row::toString)).containsExactlyInAnyOrder("[1,aaa,bbb]");

        // primary key + partition
        spark.sql(
                "CREATE TABLE t_all (\n"
                        + "    user_id BIGINT,\n"
                        + "    item_id BIGINT,\n"
                        + "    behavior STRING,\n"
                        + "    dt STRING,\n"
                        + "    hh STRING\n"
                        + ") PARTITIONED BY (dt, hh) TBLPROPERTIES (\n"
                        + "    'primary-key' = 'dt,hh,user_id'\n"
                        + ")");
        spark.sql("INSERT INTO t_all VALUES(1,2,'bbb','2020-01-01','12')");
        spark.sql(
                "CREATE TABLE t_all_as PARTITIONED BY (dt) TBLPROPERTIES ('primary-key' = 'dt,hh') AS SELECT * FROM t_all");
        assertThat(spark.sql("SHOW CREATE TABLE t_all_as").collectAsList().toString())
                .isEqualTo(
                        String.format(
                                "[[%s"
                                        + "PARTITIONED BY (dt)\n"
                                        + "TBLPROPERTIES (\n"
                                        + "  'path' = '%s',\n"
                                        + "  'primary-key' = 'dt,hh')\n"
                                        + "]]",
                                showCreateString(
                                        "t_all_as",
                                        "user_id BIGINT",
                                        "item_id BIGINT",
                                        "behavior STRING",
                                        "dt STRING NOT NULL",
                                        "hh STRING NOT NULL"),
                                new Path(warehousePath, "default.db/t_all_as")));
        List<Row> resultAll = spark.sql("SELECT * FROM t_all_as").collectAsList();
        assertThat(resultAll.stream().map(Row::toString))
                .containsExactlyInAnyOrder("[1,2,bbb,2020-01-01,12]");
    }

    @Test
    public void testConflictOption() {
        assertThatThrownBy(
                        () ->
                                spark.sql(
                                        "CREATE TABLE T (a INT) TBLPROPERTIES ('changelog-producer' = 'input')"))
                .rootCause()
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(
                        "Can not set changelog-producer on table without primary keys");

        spark.sql("CREATE TABLE T (a INT)");

        assertThatThrownBy(
                        () ->
                                spark.sql(
                                        "ALTER TABLE T SET TBLPROPERTIES('changelog-producer' 'input')"))
                .rootCause()
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(
                        "Can not set changelog-producer on table without primary keys");
    }

    @Test
    public void testShowTablesSorted() {
        spark.sql("create table t3(id int, name string)");
        spark.sql("create table t4(id int, name string)");
        List<Row> tables = spark.sql("SHOW TABLES").collectAsList();
        assertThat(tables.toString())
                .isEqualTo(
                        "[[default,t1,false], [default,t2,false], [default,t3,false], [default,t4,false]]");
    }

    @Test
    public void testCreateTableWithNullablePk() {
        spark.sql(
                "CREATE TABLE PkTable (\n"
                        + "a BIGINT,\n"
                        + "b STRING)\n"
                        + "TBLPROPERTIES ('primary-key' = 'a')");
        Path tablePath = new Path(warehousePath, "default.db/PkTable");
        TableSchema schema = FileStoreTableFactory.create(LocalFileIO.create(), tablePath).schema();
        assertThat(schema.logicalRowType().getTypeAt(0).isNullable()).isFalse();
    }

    @Test
    public void testDescribeTable() {
        spark.sql(
                "CREATE TABLE PartitionedTable (\n"
                        + "a BIGINT,\n"
                        + "b STRING)\n"
                        + "PARTITIONED BY (a)\n");
        assertThat(spark.sql("DESCRIBE PartitionedTable").collectAsList().toString())
                .isEqualTo(
                        "[[a,bigint,null], [b,string,null], [# Partition Information,,], [# col_name,data_type,comment], [a,bigint,null]]");
    }

    @Test
    public void testShowCreateTable() {
        spark.sql(
                "CREATE TABLE tbl (\n"
                        + "  a INT COMMENT 'a comment',\n"
                        + "  b STRING\n"
                        + ") USING paimon\n"
                        + "PARTITIONED BY (b)\n"
                        + "COMMENT 'tbl comment'\n"
                        + "TBLPROPERTIES (\n"
                        + "  'primary-key' = 'a,b',\n"
                        + "  'k1' = 'v1'\n"
                        + ")");

        assertThat(spark.sql("SHOW CREATE TABLE tbl").collectAsList().toString())
                .isEqualTo(
                        String.format(
                                "[[%s"
                                        + "PARTITIONED BY (b)\n"
                                        + "COMMENT 'tbl comment'\n"
                                        + "TBLPROPERTIES (\n"
                                        + "  'k1' = 'v1',\n"
                                        + "  'path' = '%s',\n"
                                        + "  'primary-key' = 'a,b')\n]]",
                                showCreateString(
                                        "tbl",
                                        "a INT NOT NULL COMMENT 'a comment'",
                                        "b STRING NOT NULL"),
                                new Path(warehousePath, "default.db/tbl")));
    }

    @Test
    public void testShowTableProperties() {
        spark.sql(
                "CREATE TABLE tbl (\n"
                        + "  a INT)\n"
                        + "TBLPROPERTIES (\n"
                        + "  'k1' = 'v1',\n"
                        + "  'k2' = 'v2'\n"
                        + ")");

        assertThat(
                        spark.sql("SHOW TBLPROPERTIES tbl").collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .contains("[k1,v1]", "[k2,v2]");
    }

    @Test
    public void testCreateTableWithNonexistentPk() {
        spark.sql("USE paimon");
        assertThatThrownBy(
                        () ->
                                spark.sql(
                                        "CREATE TABLE default.PartitionedPkTable (\n"
                                                + "a BIGINT,\n"
                                                + "b STRING,\n"
                                                + "c DOUBLE) USING paimon\n"
                                                + "COMMENT 'table comment'\n"
                                                + "PARTITIONED BY (b)\n"
                                                + "TBLPROPERTIES ('primary-key' = 'd')"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "Table column [a, b, c] should include all primary key constraint [d]");
    }

    @Test
    public void testCreateTableWithNonexistentPartition() {
        assertThatThrownBy(
                        () ->
                                spark.sql(
                                        "CREATE TABLE PartitionedPkTable (\n"
                                                + "a BIGINT,\n"
                                                + "b STRING,\n"
                                                + "c DOUBLE)\n"
                                                + "PARTITIONED BY (d)\n"
                                                + "TBLPROPERTIES ('primary-key' = 'a')"))
                .isInstanceOf(AnalysisException.class)
                .hasMessageContaining("Couldn't find column d");
    }

    @Test
    public void testCreateAndDropTable() {
        innerTest("MyTable1", true, true);
        innerTest("MyTable2", true, false);
        innerTest("MyTable3", false, false);
        innerTest("MyTable4", false, false);
        innerTest("MyTable5", false, true);
        innerTest("MyTable6", false, true);
    }

    private void innerTest(String tableName, boolean hasPk, boolean partitioned) {
        String ddlTemplate =
                "CREATE TABLE default.%s (\n"
                        + "order_id BIGINT NOT NULL comment 'order_id',\n"
                        + "buyer_id BIGINT NOT NULL COMMENT 'buyer_id',\n"
                        + "coupon_info ARRAY<STRING> NOT NULL COMMENT 'coupon_info',\n"
                        + "order_amount DOUBLE NOT NULL COMMENT 'order_amount',\n"
                        + "dt STRING NOT NULL COMMENT 'dt',\n"
                        + "hh STRING NOT NULL COMMENT 'hh')\n"
                        + "COMMENT 'table comment'\n"
                        + "%s\n"
                        + "TBLPROPERTIES (%s)";
        Map<String, String> tableProperties = new HashMap<>();
        tableProperties.put("foo", "bar");
        tableProperties.put("file.format", "avro");
        List<String> columns =
                Arrays.asList("order_id", "buyer_id", "coupon_info", "order_amount", "dt", "hh");
        List<DataType> types =
                Arrays.asList(
                        new BigIntType(false),
                        new BigIntType(false),
                        new ArrayType(false, VarCharType.STRING_TYPE),
                        new DoubleType(false),
                        VarCharType.stringType(false),
                        VarCharType.stringType(false));
        List<DataField> fields =
                IntStream.range(0, columns.size())
                        .boxed()
                        .map(i -> new DataField(i, columns.get(i), types.get(i), columns.get(i)))
                        .collect(Collectors.toList());
        String partitionStr = "";
        if (hasPk) {
            tableProperties.put("primary-key", partitioned ? "order_id,dt,hh" : "order_id");
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
                .hasMessageContaining(
                        String.format(
                                "Cannot create table or view `default`.`%s` because it already "
                                        + "exists",
                                tableName));
        assertThatThrownBy(() -> spark.sql(ddl.replace("default", "foo")))
                .isInstanceOf(NoSuchNamespaceException.class)
                .hasMessageContaining("The schema `foo` cannot be found");

        assertThatThrownBy(
                        () ->
                                spark.sql(
                                        String.format(
                                                "ALTER TABLE %s UNSET TBLPROPERTIES('primary-key')",
                                                tableName)))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Alter primary key is not supported");

        Path tablePath = new Path(warehousePath, String.format("default.db/%s", tableName));
        TableSchema schema = FileStoreTableFactory.create(LocalFileIO.create(), tablePath).schema();
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

        assertThat(schema.comment()).isEqualTo("table comment");

        writeTable(
                tableName,
                "(1L, 10L, array('loyalty_discount', 'shipping_discount'), 199.0d, '2022-07-20', '12')");

        Dataset<Row> dataset = spark.read().format("paimon").load(tablePath.toString());
        assertThat(dataset.select("order_id", "buyer_id", "dt").collectAsList().toString())
                .isEqualTo("[[1,10,2022-07-20]]");
        assertThat(dataset.select("coupon_info").collectAsList().toString())
                .isEqualTo("[[WrappedArray(loyalty_discount, shipping_discount)]]");

        // test drop table
        assertThat(
                        spark.sql(
                                        String.format(
                                                "SHOW TABLES IN paimon.default LIKE '%s'",
                                                tableName))
                                .select("namespace", "tableName")
                                .collectAsList()
                                .toString())
                .isEqualTo(String.format("[[default,%s]]", tableName));

        spark.sql(String.format("DROP TABLE %s", tableName));

        assertThat(
                        spark.sql(
                                        String.format(
                                                "SHOW TABLES IN paimon.default LIKE '%s'",
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
        spark.sql("CREATE NAMESPACE bar");

        assertThatThrownBy(() -> spark.sql("CREATE NAMESPACE bar"))
                .isInstanceOf(NamespaceAlreadyExistsException.class)
                .hasMessageContaining("Cannot create schema `bar` because it already exists");

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

    @Test
    public void testCreateNestedField() {
        spark.sql(
                "CREATE TABLE nested_table ( a INT, b STRUCT<b1: STRUCT<b11: INT, b12 INT>, b2 BIGINT>)");
        assertThat(spark.sql("SHOW CREATE TABLE nested_table").collectAsList().toString())
                .contains(
                        showCreateString(
                                "nested_table",
                                "a INT",
                                "b STRUCT<b1: STRUCT<b11: INT, b12: INT>, b2: BIGINT>"));
    }
}
