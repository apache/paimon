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

import org.apache.paimon.flink.action.ActionITCaseBase;
import org.apache.paimon.flink.action.CloneAction;
import org.apache.paimon.hive.TestHiveMetastore;
import org.apache.paimon.utils.StringUtils;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/** Test clone Iceberg table. */
public class CloneActionForIcebergITCase extends ActionITCaseBase {

    private static final TestHiveMetastore TEST_HIVE_METASTORE = new TestHiveMetastore();

    private static final int PORT = 9090;

    @TempDir java.nio.file.Path iceTempDir;

    @BeforeAll
    public static void beforeAll() {
        TEST_HIVE_METASTORE.start(PORT);
    }

    @AfterAll
    public static void afterAll() throws Exception {
        TEST_HIVE_METASTORE.stop();
    }

    @Test
    public void testUnPartitionedTable() throws Exception {
        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().build();
        String dbName = "icbergdb" + StringUtils.randomNumericString(10);
        String tableName = "icebergtable" + StringUtils.randomNumericString(10);
        String format = randomFormat();

        sql(
                tEnv,
                "CREATE CATALOG my_iceberg WITH "
                        + "( 'type' = 'iceberg', 'catalog-type' = 'hive', 'uri' = 'thrift://localhost:%s', "
                        + "'warehouse' = '%s', 'cache-enabled' = 'false')",
                PORT,
                iceTempDir);

        sql(tEnv, "CREATE DATABASE my_iceberg.`%s`", dbName);

        sql(
                tEnv,
                "CREATE TABLE my_iceberg.`%s`.`%s` ("
                        + "  id STRING PRIMARY KEY NOT ENFORCED,"
                        + "  name STRING,"
                        + "  price INT"
                        + ") WITH ("
                        + "  'format-version'='2',"
                        + "  'write.format.default'='%s'"
                        + ")",
                dbName,
                tableName,
                format);

        List<String> insertValues = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            insertValues.add(String.format("('%s', '%s', %s)", i, "A", i));
        }

        sql(
                tEnv,
                "INSERT INTO my_iceberg.`%s`.`%s` VALUES %s",
                dbName,
                tableName,
                String.join(",", insertValues));

        List<Row> r1 = sql(tEnv, "SELECT * FROM my_iceberg.`%s`.`%s`", dbName, tableName);

        sql(tEnv, "CREATE CATALOG PAIMON WITH ('type'='paimon', 'warehouse' = '%s')", warehouse);
        tEnv.useCatalog("PAIMON");
        tEnv.executeSql("CREATE DATABASE test");

        createAction(
                        CloneAction.class,
                        "clone",
                        "--database",
                        dbName,
                        "--table",
                        tableName,
                        "--catalog_conf",
                        "metastore=hive",
                        "--catalog_conf",
                        "uri=thrift://localhost:" + PORT,
                        "--target_database",
                        "test",
                        "--target_table",
                        "test_table",
                        "--target_catalog_conf",
                        "warehouse=" + warehouse)
                .run();

        List<Row> r2 = sql(tEnv, "SELECT * FROM test.test_table");
        Assertions.assertThatList(r1).containsExactlyInAnyOrderElementsOf(r2);
    }

    @Test
    public void testPartitionedTable() throws Exception {
        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().build();
        String dbName = "icbergdb" + StringUtils.randomNumericString(10);
        String tableName = "icebergtable" + StringUtils.randomNumericString(10);
        String format = randomFormat();

        sql(
                tEnv,
                "CREATE CATALOG my_iceberg WITH "
                        + "( 'type' = 'iceberg', 'catalog-type' = 'hive', 'uri' = 'thrift://localhost:%s', "
                        + "'warehouse' = '%s', 'cache-enabled' = 'false')",
                PORT,
                iceTempDir);

        sql(tEnv, "CREATE DATABASE my_iceberg.`%s`", dbName);

        sql(
                tEnv,
                "CREATE TABLE my_iceberg.`%s`.`%s` ("
                        + "  id STRING PRIMARY KEY NOT ENFORCED,"
                        + "  name STRING,"
                        + "  price INT"
                        + ") PARTITIONED BY (price) WITH ("
                        + "  'format-version'='2',"
                        + "  'write.format.default'='%s'"
                        + ")",
                dbName,
                tableName,
                format);

        List<String> insertValues = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            insertValues.add(String.format("('%s', '%s', %s)", i, "A", i % 3));
        }
        sql(
                tEnv,
                "INSERT INTO my_iceberg.`%s`.`%s` VALUES %s",
                dbName,
                tableName,
                String.join(",", insertValues));

        List<Row> r1 =
                sql(tEnv, "SELECT * FROM my_iceberg.`%s`.`%s` WHERE price > 0", dbName, tableName);

        sql(tEnv, "CREATE CATALOG PAIMON WITH ('type'='paimon', 'warehouse' = '%s')", warehouse);
        tEnv.useCatalog("PAIMON");
        tEnv.executeSql("CREATE DATABASE test");

        createAction(
                        CloneAction.class,
                        "clone",
                        "--database",
                        dbName,
                        "--table",
                        tableName,
                        "--catalog_conf",
                        "metastore=hive",
                        "--catalog_conf",
                        "uri=thrift://localhost:" + PORT,
                        "--target_database",
                        "test",
                        "--target_table",
                        "test_table",
                        "--target_catalog_conf",
                        "warehouse=" + warehouse,
                        "--where",
                        "price > 0")
                .run();

        List<Row> r2 = sql(tEnv, "SELECT * FROM test.test_table");
        Assertions.assertThatList(r1).containsExactlyInAnyOrderElementsOf(r2);
    }

    private List<Row> sql(TableEnvironment tEnv, String query, Object... args) {
        try (CloseableIterator<Row> iter = tEnv.executeSql(String.format(query, args)).collect()) {
            return ImmutableList.copyOf(iter);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String randomFormat() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int i = random.nextInt(3);
        String[] formats = new String[] {"orc", "parquet", "avro"};
        return formats[i];
    }
}
