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

package org.apache.paimon.flink;

import org.apache.paimon.flink.util.AbstractTestBase;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;

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
public class FlinkIcebergITCase extends AbstractTestBase {

    @ParameterizedTest
    @ValueSource(strings = {"avro"})
    public void testDeletionVector(String format) throws Exception {
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
                        + "  v INT,\n"
                        + "  PRIMARY KEY (pt, k) NOT ENFORCED\n"
                        + ") PARTITIONED BY (pt) WITH (\n"
                        + "  'metadata.iceberg.storage' = 'hadoop-catalog',\n"
                        + "  'metadata.iceberg.format-version' = '3',\n"
                        + "  'deletion-vectors.enabled' = 'true',\n"
                        + "  'deletion-vectors.bitmap64' = 'true',\n"
                        + "  'file.format' = '"
                        + format
                        + "'\n"
                        + ")");
        tEnv.executeSql(
                        "INSERT INTO paimon.`default`.T VALUES "
                                + "(1, 9, 90), "
                                + "(1, 10, 100), "
                                + "(1, 11, 110), "
                                + "(2, 20, 200)")
                .await();

        tEnv.executeSql(
                "CREATE CATALOG iceberg WITH (\n"
                        + "  'type' = 'iceberg',\n"
                        + "  'catalog-type' = 'hadoop',\n"
                        + "  'warehouse' = '"
                        + warehouse
                        + "/iceberg',\n"
                        + "  'cache-enabled' = 'false'\n"
                        + ")");
        assertThat(
                        collect(
                                tEnv.executeSql(
                                        "SELECT v, k, pt FROM iceberg.`default`.T ORDER BY pt, k")))
                .containsExactly(
                        Row.of(90, 9, 1),
                        Row.of(100, 10, 1),
                        Row.of(110, 11, 1),
                        Row.of(200, 20, 2));

        tEnv.executeSql(
                        "INSERT INTO paimon.`default`.T VALUES "
                                + "(1, 10, 101), "
                                + "(2, 20, 201), "
                                + "(1, 12, 121)")
                .await();

        // make sure that there are dv indexes generated
        CloseableIterator<Row> iter =
                tEnv.executeSql(
                                "SELECT * FROM paimon.`default`.`T$table_indexes` WHERE index_type='DELETION_VECTORS'")
                        .collect();
        assertThat(ImmutableList.copyOf(iter).size()).isGreaterThan(0);
        assertThat(
                        collect(
                                tEnv.executeSql(
                                        "SELECT v, k, pt FROM iceberg.`default`.T ORDER BY pt, k")))
                .containsExactly(
                        Row.of(90, 9, 1),
                        Row.of(101, 10, 1),
                        Row.of(110, 11, 1),
                        Row.of(121, 12, 1),
                        Row.of(201, 20, 2));
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
