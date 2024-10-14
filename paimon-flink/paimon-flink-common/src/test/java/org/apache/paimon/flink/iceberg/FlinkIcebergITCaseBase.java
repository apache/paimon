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

package org.apache.paimon.flink.iceberg;

import org.apache.paimon.flink.util.AbstractTestBase;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for Paimon Iceberg compatibility. */
public abstract class FlinkIcebergITCaseBase extends AbstractTestBase {

    @ParameterizedTest
    @ValueSource(strings = {"orc", "parquet", "avro"})
    public void testPrimaryKeyTable(String format) throws Exception {
        String warehouse = getTempDirPath();
        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().parallelism(2).build();
        tEnv.executeSql(
                "CREATE CATALOG paimon WITH (\n"
                        + "  'type' = 'paimon',\n"
                        + "  'warehouse' = '"
                        + warehouse
                        + "'\n"
                        + ")");
        tEnv.executeSql(
                "CREATE TABLE paimon.`default`.T (\n"
                        + "  pt INT,\n"
                        + "  k INT,\n"
                        + "  v1 INT,\n"
                        + "  v2 STRING,\n"
                        + "  PRIMARY KEY (pt, k) NOT ENFORCED\n"
                        + ") PARTITIONED BY (pt) WITH (\n"
                        + "  'metadata.iceberg.storage' = 'hadoop-catalog',\n"
                        // make sure all changes are visible in iceberg metadata
                        + "  'full-compaction.delta-commits' = '1',\n"
                        + "  'file.format' = '"
                        + format
                        + "'\n"
                        + ")");
        tEnv.executeSql(
                        "INSERT INTO paimon.`default`.T VALUES "
                                + "(1, 10, 100, 'apple'), "
                                + "(1, 11, 110, 'banana'), "
                                + "(2, 20, 200, 'cat'), "
                                + "(2, 21, 210, 'dog')")
                .await();

        tEnv.executeSql(
                "CREATE TABLE T (\n"
                        + "  pt INT,\n"
                        + "  k INT,\n"
                        + "  v1 INT,\n"
                        + "  v2 STRING\n"
                        + ") PARTITIONED BY (pt) WITH (\n"
                        + "  'connector' = 'iceberg',\n"
                        + "  'catalog-type' = 'hadoop',\n"
                        + "  'catalog-name' = 'test',\n"
                        + "  'catalog-database' = 'default',\n"
                        + "  'warehouse' = '"
                        + warehouse
                        + "/iceberg'\n"
                        + ")");
        assertThat(collect(tEnv.executeSql("SELECT v1, k, v2, pt FROM T ORDER BY pt, k")))
                .containsExactly(
                        Row.of(100, 10, "apple", 1),
                        Row.of(110, 11, "banana", 1),
                        Row.of(200, 20, "cat", 2),
                        Row.of(210, 21, "dog", 2));

        tEnv.executeSql(
                        "INSERT INTO paimon.`default`.T VALUES "
                                + "(1, 10, 101, 'red'), "
                                + "(1, 12, 121, 'green'), "
                                + "(2, 20, 201, 'blue'), "
                                + "(2, 22, 221, 'yellow')")
                .await();
        assertThat(collect(tEnv.executeSql("SELECT v1, k, v2, pt FROM T ORDER BY pt, k")))
                .containsExactly(
                        Row.of(101, 10, "red", 1),
                        Row.of(110, 11, "banana", 1),
                        Row.of(121, 12, "green", 1),
                        Row.of(201, 20, "blue", 2),
                        Row.of(210, 21, "dog", 2),
                        Row.of(221, 22, "yellow", 2));
    }

    @ParameterizedTest
    @ValueSource(strings = {"orc", "parquet", "avro"})
    public void testAppendOnlyTable(String format) throws Exception {
        String warehouse = getTempDirPath();
        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().parallelism(2).build();
        tEnv.executeSql(
                "CREATE CATALOG paimon WITH (\n"
                        + "  'type' = 'paimon',\n"
                        + "  'warehouse' = '"
                        + warehouse
                        + "'\n"
                        + ")");
        tEnv.executeSql(
                "CREATE TABLE paimon.`default`.cities (\n"
                        + "  country STRING,\n"
                        + "  name STRING\n"
                        + ") WITH (\n"
                        + "  'metadata.iceberg.storage' = 'hadoop-catalog',\n"
                        + "  'file.format' = '"
                        + format
                        + "'\n"
                        + ")");
        tEnv.executeSql(
                        "INSERT INTO paimon.`default`.cities VALUES "
                                + "('usa', 'new york'), "
                                + "('germany', 'berlin'), "
                                + "('usa', 'chicago'), "
                                + "('germany', 'hamburg')")
                .await();

        tEnv.executeSql(
                "CREATE TABLE cities (\n"
                        + "  country STRING,\n"
                        + "  name STRING\n"
                        + ") WITH (\n"
                        + "  'connector' = 'iceberg',\n"
                        + "  'catalog-type' = 'hadoop',\n"
                        + "  'catalog-name' = 'test',\n"
                        + "  'catalog-database' = 'default',\n"
                        + "  'warehouse' = '"
                        + warehouse
                        + "/iceberg'\n"
                        + ")");
        assertThat(collect(tEnv.executeSql("SELECT name, country FROM cities")))
                .containsExactlyInAnyOrder(
                        Row.of("new york", "usa"),
                        Row.of("chicago", "usa"),
                        Row.of("berlin", "germany"),
                        Row.of("hamburg", "germany"));

        tEnv.executeSql(
                        "INSERT INTO paimon.`default`.cities VALUES "
                                + "('usa', 'houston'), "
                                + "('germany', 'munich')")
                .await();
        assertThat(collect(tEnv.executeSql("SELECT name FROM cities WHERE country = 'germany'")))
                .containsExactlyInAnyOrder(Row.of("berlin"), Row.of("hamburg"), Row.of("munich"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"orc", "parquet", "avro"})
    public void testFilterAllTypes(String format) throws Exception {
        String warehouse = getTempDirPath();
        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().parallelism(2).build();
        tEnv.executeSql(
                "CREATE CATALOG paimon WITH (\n"
                        + "  'type' = 'paimon',\n"
                        + "  'warehouse' = '"
                        + warehouse
                        + "'\n"
                        + ")");
        tEnv.executeSql(
                "CREATE TABLE paimon.`default`.T (\n"
                        + "  pt INT,\n"
                        + "  id INT,"
                        + "  v_int INT,\n"
                        + "  v_boolean BOOLEAN,\n"
                        + "  v_bigint BIGINT,\n"
                        + "  v_float FLOAT,\n"
                        + "  v_double DOUBLE,\n"
                        + "  v_decimal DECIMAL(8, 3),\n"
                        + "  v_varchar STRING,\n"
                        + "  v_varbinary VARBINARY(20),\n"
                        + "  v_date DATE,\n"
                        // it seems that Iceberg Flink connector has some bug when filtering a
                        // timestamp_ltz, so we don't test it here
                        + "  v_timestamp TIMESTAMP(6)\n"
                        + ") PARTITIONED BY (pt) WITH (\n"
                        + "  'metadata.iceberg.storage' = 'hadoop-catalog',\n"
                        + "  'file.format' = '"
                        + format
                        + "'\n"
                        + ")");
        tEnv.executeSql(
                        "INSERT INTO paimon.`default`.T VALUES "
                                + "(1, 1, 1, true, 10, CAST(100.0 AS FLOAT), 1000.0, 123.456, 'cat', CAST('B_cat' AS VARBINARY(20)), DATE '2024-10-10', TIMESTAMP '2024-10-10 11:22:33.123456'), "
                                + "(2, 2, 2, false, 20, CAST(200.0 AS FLOAT), 2000.0, 234.567, 'dog', CAST('B_dog' AS VARBINARY(20)), DATE '2024-10-20', TIMESTAMP '2024-10-20 11:22:33.123456'), "
                                + "(3, 3, CAST(NULL AS INT), CAST(NULL AS BOOLEAN), CAST(NULL AS BIGINT), CAST(NULL AS FLOAT), CAST(NULL AS DOUBLE), CAST(NULL AS DECIMAL(8, 3)), CAST(NULL AS STRING), CAST(NULL AS VARBINARY(20)), CAST(NULL AS DATE), CAST(NULL AS TIMESTAMP(6)))")
                .await();

        tEnv.executeSql(
                "CREATE TABLE T (\n"
                        + "  pt INT,\n"
                        + "  id INT,"
                        + "  v_int INT,\n"
                        + "  v_boolean BOOLEAN,\n"
                        + "  v_bigint BIGINT,\n"
                        + "  v_float FLOAT,\n"
                        + "  v_double DOUBLE,\n"
                        + "  v_decimal DECIMAL(8, 3),\n"
                        + "  v_varchar STRING,\n"
                        + "  v_varbinary VARBINARY(20),\n"
                        + "  v_date DATE,\n"
                        + "  v_timestamp TIMESTAMP(6)\n"
                        + ") PARTITIONED BY (pt) WITH (\n"
                        + "  'connector' = 'iceberg',\n"
                        + "  'catalog-type' = 'hadoop',\n"
                        + "  'catalog-name' = 'test',\n"
                        + "  'catalog-database' = 'default',\n"
                        + "  'warehouse' = '"
                        + warehouse
                        + "/iceberg'\n"
                        + ")");
        assertThat(collect(tEnv.executeSql("SELECT id FROM T where pt = 1")))
                .containsExactly(Row.of(1));
        assertThat(collect(tEnv.executeSql("SELECT id FROM T where v_int = 1")))
                .containsExactly(Row.of(1));
        assertThat(collect(tEnv.executeSql("SELECT id FROM T where v_boolean = true")))
                .containsExactly(Row.of(1));
        assertThat(collect(tEnv.executeSql("SELECT id FROM T where v_bigint = 10")))
                .containsExactly(Row.of(1));
        assertThat(collect(tEnv.executeSql("SELECT id FROM T where v_float = 100.0")))
                .containsExactly(Row.of(1));
        assertThat(collect(tEnv.executeSql("SELECT id FROM T where v_double = 1000.0")))
                .containsExactly(Row.of(1));
        assertThat(collect(tEnv.executeSql("SELECT id FROM T where v_decimal = 123.456")))
                .containsExactly(Row.of(1));
        assertThat(collect(tEnv.executeSql("SELECT id FROM T where v_varchar = 'cat'")))
                .containsExactly(Row.of(1));
        assertThat(collect(tEnv.executeSql("SELECT id FROM T where v_date = '2024-10-10'")))
                .containsExactly(Row.of(1));
        assertThat(
                        collect(
                                tEnv.executeSql(
                                        "SELECT id FROM T where v_timestamp = TIMESTAMP '2024-10-10 11:22:33.123456'")))
                .containsExactly(Row.of(1));
        assertThat(collect(tEnv.executeSql("SELECT id FROM T where v_int IS NULL")))
                .containsExactly(Row.of(3));
        assertThat(collect(tEnv.executeSql("SELECT id FROM T where v_boolean IS NULL")))
                .containsExactly(Row.of(3));
        assertThat(collect(tEnv.executeSql("SELECT id FROM T where v_bigint IS NULL")))
                .containsExactly(Row.of(3));
        assertThat(collect(tEnv.executeSql("SELECT id FROM T where v_float IS NULL")))
                .containsExactly(Row.of(3));
        assertThat(collect(tEnv.executeSql("SELECT id FROM T where v_double IS NULL")))
                .containsExactly(Row.of(3));
        assertThat(collect(tEnv.executeSql("SELECT id FROM T where v_decimal IS NULL")))
                .containsExactly(Row.of(3));
        assertThat(collect(tEnv.executeSql("SELECT id FROM T where v_varchar IS NULL")))
                .containsExactly(Row.of(3));
        assertThat(collect(tEnv.executeSql("SELECT id FROM T where v_varbinary IS NULL")))
                .containsExactly(Row.of(3));
        assertThat(collect(tEnv.executeSql("SELECT id FROM T where v_date IS NULL")))
                .containsExactly(Row.of(3));
        assertThat(collect(tEnv.executeSql("SELECT id FROM T where v_timestamp IS NULL")))
                .containsExactly(Row.of(3));
    }

    @ParameterizedTest
    @ValueSource(strings = {"orc", "parquet"})
    public void testFilterTimestampLtz(String format) throws Exception {
        String warehouse = getTempDirPath();
        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().parallelism(2).build();
        tEnv.executeSql(
                "CREATE CATALOG paimon WITH (\n"
                        + "  'type' = 'paimon',\n"
                        + "  'warehouse' = '"
                        + warehouse
                        + "'\n"
                        + ")");
        tEnv.executeSql(
                "CREATE TABLE paimon.`default`.T (\n"
                        + "  id INT,"
                        + "  v_timestampltz TIMESTAMP_LTZ(6)\n"
                        + ") WITH (\n"
                        + "  'metadata.iceberg.storage' = 'hadoop-catalog',\n"
                        + "  'file.format' = '"
                        + format
                        + "'\n"
                        + ")");
        tEnv.executeSql(
                        "INSERT INTO paimon.`default`.T VALUES "
                                + "(1, CAST(TO_TIMESTAMP_LTZ(1100000000321, 3) AS TIMESTAMP_LTZ(6))), "
                                + "(2, CAST(TO_TIMESTAMP_LTZ(1200000000321, 3) AS TIMESTAMP_LTZ(6))), "
                                + "(3, CAST(NULL AS TIMESTAMP_LTZ(6)))")
                .await();

        HadoopCatalog icebergCatalog =
                new HadoopCatalog(new Configuration(), warehouse + "/iceberg");
        TableIdentifier icebergIdentifier = TableIdentifier.of("default", "T");
        org.apache.iceberg.Table icebergTable = icebergCatalog.loadTable(icebergIdentifier);

        CloseableIterable<Record> result =
                IcebergGenerics.read(icebergTable)
                        .where(Expressions.equal("v_timestampltz", 1100000000321000L))
                        .build();
        List<Object> actual = new ArrayList<>();
        for (Record record : result) {
            actual.add(record.get(0));
        }
        result.close();
        assertThat(actual).containsExactly(1);

        result =
                IcebergGenerics.read(icebergTable)
                        .where(Expressions.isNull("v_timestampltz"))
                        .build();
        actual = new ArrayList<>();
        for (Record record : result) {
            actual.add(record.get(0));
        }
        result.close();
        assertThat(actual).containsExactly(3);
    }

    @ParameterizedTest
    @ValueSource(strings = {"orc", "parquet", "avro"})
    public void testDropAndRecreateTable(String format) throws Exception {
        String warehouse = getTempDirPath();
        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().parallelism(2).build();
        tEnv.executeSql(
                "CREATE CATALOG paimon WITH (\n"
                        + "  'type' = 'paimon',\n"
                        + "  'warehouse' = '"
                        + warehouse
                        + "'\n"
                        + ")");
        String createTableDdl =
                "CREATE TABLE paimon.`default`.cities (\n"
                        + "  country STRING,\n"
                        + "  name STRING\n"
                        + ") WITH (\n"
                        + "  'metadata.iceberg.storage' = 'hadoop-catalog',\n"
                        + "  'file.format' = '"
                        + format
                        + "'\n"
                        + ")";
        tEnv.executeSql(createTableDdl);
        tEnv.executeSql(
                        "INSERT INTO paimon.`default`.cities VALUES "
                                + "('usa', 'new york'), "
                                + "('germany', 'berlin')")
                .await();

        tEnv.executeSql(
                "CREATE TABLE cities (\n"
                        + "  country STRING,\n"
                        + "  name STRING\n"
                        + ") WITH (\n"
                        + "  'connector' = 'iceberg',\n"
                        + "  'catalog-type' = 'hadoop',\n"
                        + "  'catalog-name' = 'test',\n"
                        + "  'catalog-database' = 'default',\n"
                        + "  'warehouse' = '"
                        + warehouse
                        + "/iceberg'\n"
                        + ")");
        assertThat(collect(tEnv.executeSql("SELECT name, country FROM cities")))
                .containsExactlyInAnyOrder(Row.of("new york", "usa"), Row.of("berlin", "germany"));

        tEnv.executeSql(
                        "INSERT INTO paimon.`default`.cities VALUES "
                                + "('usa', 'chicago'), "
                                + "('germany', 'hamburg')")
                .await();
        assertThat(collect(tEnv.executeSql("SELECT name, country FROM cities")))
                .containsExactlyInAnyOrder(
                        Row.of("new york", "usa"),
                        Row.of("chicago", "usa"),
                        Row.of("berlin", "germany"),
                        Row.of("hamburg", "germany"));

        tEnv.executeSql("DROP TABLE paimon.`default`.cities");
        tEnv.executeSql(createTableDdl);
        tEnv.executeSql(
                        "INSERT INTO paimon.`default`.cities VALUES "
                                + "('usa', 'houston'), "
                                + "('germany', 'munich')")
                .await();
        assertThat(collect(tEnv.executeSql("SELECT name, country FROM cities")))
                .containsExactlyInAnyOrder(Row.of("houston", "usa"), Row.of("munich", "germany"));

        tEnv.executeSql(
                        "INSERT INTO paimon.`default`.cities VALUES "
                                + "('usa', 'san francisco'), "
                                + "('germany', 'cologne')")
                .await();
        assertThat(collect(tEnv.executeSql("SELECT name FROM cities WHERE country = 'germany'")))
                .containsExactlyInAnyOrder(Row.of("munich"), Row.of("cologne"));
    }

    private List<Row> collect(TableResult result) throws Exception {
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> it = result.collect()) {
            while (it.hasNext()) {
                rows.add(it.next());
            }
        }
        return rows;
    }
}
