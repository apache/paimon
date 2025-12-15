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

package org.apache.paimon.hudi;

import org.apache.paimon.flink.action.ActionITCaseBase;
import org.apache.paimon.flink.action.CloneAction;
import org.apache.paimon.hive.TestHiveMetastore;
import org.apache.paimon.utils.StringUtils;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatList;

/** Test clone Hudi table. */
public class CloneActionForHudiITCase extends ActionITCaseBase {

    private static final TestHiveMetastore TEST_HIVE_METASTORE = new TestHiveMetastore();

    private static final int PORT = 9089;

    @BeforeAll
    public static void beforeAll() {
        TEST_HIVE_METASTORE.start(PORT);
    }

    @AfterAll
    public static void afterAll() throws Exception {
        TEST_HIVE_METASTORE.stop();
    }

    @Test
    public void testMigrateOneNonPartitionedTable() throws Exception {
        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().build();
        String dbName = "hudidb" + StringUtils.randomNumericString(10);
        String tableName = "huditable" + StringUtils.randomNumericString(10);

        sql(
                tEnv,
                "CREATE TABLE %s ("
                        + "  id STRING PRIMARY KEY NOT ENFORCED,"
                        + "  name STRING,"
                        + "  price INT"
                        + ") WITH ("
                        + "  'connector' = 'hudi',"
                        + "  'path' = '%s/%s',"
                        + "  'table.type' = 'COPY_ON_WRITE',"
                        + "  'hive_sync.enable' = 'true',"
                        + "  'hive_sync.mode' = 'hms',"
                        + "  'hive_sync.metastore.uris' = 'thrift://localhost:%s',"
                        + "  'hive_sync.db' = '%s',"
                        + "  'hive_sync.table' = '%s'"
                        + ")",
                tableName,
                System.getProperty(HiveConf.ConfVars.METASTOREWAREHOUSE.varname),
                tableName,
                PORT,
                dbName,
                tableName);

        List<String> insertValues = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            insertValues.add(String.format("('%s', '%s', %s)", i, "A", i));
        }
        sql(tEnv, "INSERT INTO %s VALUES %s", tableName, String.join(",", insertValues));

        // test pk
        insertValues.clear();
        for (int i = 0; i < 50; i++) {
            insertValues.add(String.format("('%s', '%s', %s)", i, "B", i));
        }
        sql(tEnv, "INSERT INTO %s VALUES %s", tableName, String.join(",", insertValues));
        List<Row> r1 = sql(tEnv, "SELECT * FROM %s", tableName);

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
        assertThatList(r1).containsExactlyInAnyOrderElementsOf(r2);
    }

    @Test
    public void testMigrateOnePartitionedTable() throws Exception {
        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().build();
        String dbName = "hudidb" + StringUtils.randomNumericString(10);
        String tableName = "huditable" + StringUtils.randomNumericString(10);

        sql(
                tEnv,
                "CREATE TABLE %s ("
                        + "  id STRING PRIMARY KEY NOT ENFORCED,"
                        + "  name STRING,"
                        + "  pt STRING"
                        + ") PARTITIONED BY (pt) WITH ("
                        + "  'connector' = 'hudi',"
                        + "  'path' = '%s/%s',"
                        + "  'table.type' = 'COPY_ON_WRITE',"
                        + "  'hive_sync.enable' = 'true',"
                        + "  'hive_sync.mode' = 'hms',"
                        + "  'hive_sync.metastore.uris' = 'thrift://localhost:%s',"
                        + "  'hive_sync.db' = '%s',"
                        + "  'hive_sync.table' = '%s',"
                        + "  'hive_sync.partition_fields' = 'pt',"
                        + "  'hoodie.datasource.write.hive_style_partitioning' = 'true',"
                        + "  'hive_sync.partition_extractor_class' = 'org.apache.hudi.hive.HiveStylePartitionValueExtractor'"
                        + ")",
                tableName,
                System.getProperty(HiveConf.ConfVars.METASTOREWAREHOUSE.varname),
                tableName,
                PORT,
                dbName,
                tableName);

        List<String> insertValues = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            insertValues.add(String.format("('%s', '%s', '%s')", i, "A", "2025-01-01"));
        }
        sql(tEnv, "INSERT INTO %s VALUES %s", tableName, String.join(",", insertValues));

        // test pk
        insertValues.clear();
        for (int i = 0; i < 50; i++) {
            insertValues.add(String.format("('%s', '%s', '%s')", i, "B", "2025-01-01"));
        }
        sql(tEnv, "INSERT INTO %s VALUES %s", tableName, String.join(",", insertValues));
        List<Row> r1 = sql(tEnv, "SELECT * FROM %s", tableName);

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
        assertThatList(r1).containsExactlyInAnyOrderElementsOf(r2);
    }

    private List<Row> sql(TableEnvironment tEnv, String query, Object... args) {
        try (CloseableIterator<Row> iter = tEnv.executeSql(String.format(query, args)).collect()) {
            return ImmutableList.copyOf(iter);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
