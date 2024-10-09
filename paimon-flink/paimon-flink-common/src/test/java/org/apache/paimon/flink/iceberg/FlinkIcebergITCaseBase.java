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
                        + "  'metadata.iceberg.storage' = 'HADOOP_CATALOG',\n"
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
                        + "  'metadata.iceberg.storage' = 'HADOOP_CATALOG',\n"
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
