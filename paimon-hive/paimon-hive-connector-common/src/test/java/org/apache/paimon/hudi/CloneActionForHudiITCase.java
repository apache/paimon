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

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.hadoop.hive.conf.HiveConf;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/** Test clone Hudi table. */
public class CloneActionForHudiITCase extends ActionITCaseBase {

    private static final TestHiveMetastore TEST_HIVE_METASTORE = new TestHiveMetastore();

    private static final int PORT = 9089;

    @BeforeEach
    public void beforeEach() throws IOException {
        super.before();
        TEST_HIVE_METASTORE.start(PORT);
    }

    @AfterEach
    public void afterEach() throws Exception {
        super.after();
        TEST_HIVE_METASTORE.stop();
    }

    @Test
    public void testMigrateOneNonPartitionedTable() throws Exception {
        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().build();

        tEnv.executeSql(
                "CREATE TABLE hudi_table ("
                        + "  id STRING PRIMARY KEY NOT ENFORCED,"
                        + "  name STRING,"
                        + "  price INT"
                        + ") WITH ("
                        + "  'connector' = 'hudi',"
                        + String.format(
                                "'path' = '%s/%s/hudi_table',",
                                System.getProperty(HiveConf.ConfVars.METASTOREWAREHOUSE.varname),
                                UUID.randomUUID())
                        + "  'table.type' = 'COPY_ON_WRITE',"
                        + "  'hive_sync.enable' = 'true',"
                        + "  'hive_sync.mode' = 'hms',"
                        + String.format(
                                "'hive_sync.metastore.uris' = 'thrift://localhost:%s',", PORT)
                        + "  'hive_sync.db' = 'default',"
                        + "  'hive_sync.table' = 'hudi_table'"
                        + ")");

        List<String> insertValues = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            insertValues.add(String.format("('%s', '%s', %s)", i, "A", i));
        }
        tEnv.executeSql("INSERT INTO hudi_table VALUES " + String.join(",", insertValues)).await();

        // test pk
        insertValues.clear();
        for (int i = 0; i < 50; i++) {
            insertValues.add(String.format("('%s', '%s', %s)", i, "B", i));
        }
        tEnv.executeSql("INSERT INTO hudi_table VALUES " + String.join(",", insertValues)).await();
        List<Row> r1 = ImmutableList.copyOf(tEnv.executeSql("SELECT * FROM hudi_table").collect());

        tEnv.executeSql(
                "CREATE CATALOG PAIMON WITH ('type'='paimon', 'warehouse' = '" + warehouse + "')");
        tEnv.useCatalog("PAIMON");
        tEnv.executeSql("CREATE DATABASE test");

        createAction(
                        CloneAction.class,
                        "clone",
                        "--database",
                        "default",
                        "--table",
                        "hudi_table",
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

        List<Row> r2 =
                ImmutableList.copyOf(tEnv.executeSql("SELECT * FROM test.test_table").collect());
        Assertions.assertThatList(r1).containsExactlyInAnyOrderElementsOf(r2);
    }

    @Test
    public void testMigrateOnePartitionedTable() throws Exception {
        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().build();

        tEnv.executeSql(
                "CREATE TABLE hudi_table ("
                        + "  id STRING PRIMARY KEY NOT ENFORCED,"
                        + "  name STRING,"
                        + "  pt STRING"
                        + ") PARTITIONED BY (pt) WITH ("
                        + "  'connector' = 'hudi',"
                        + String.format(
                                "'path' = '%s/%s/hudi_table',",
                                System.getProperty(HiveConf.ConfVars.METASTOREWAREHOUSE.varname),
                                UUID.randomUUID())
                        + "  'table.type' = 'COPY_ON_WRITE',"
                        + "  'hive_sync.enable' = 'true',"
                        + "  'hive_sync.mode' = 'hms',"
                        + String.format(
                                "'hive_sync.metastore.uris' = 'thrift://localhost:%s',", PORT)
                        + "  'hive_sync.db' = 'default',"
                        + "  'hive_sync.table' = 'hudi_table',"
                        + "  'hive_sync.partition_fields' = 'pt',"
                        + "  'hoodie.datasource.write.hive_style_partitioning' = 'true',"
                        + "  'hive_sync.partition_extractor_class' = 'org.apache.hudi.hive.HiveStylePartitionValueExtractor'"
                        + ")");

        List<String> insertValues = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            insertValues.add(String.format("('%s', '%s', '%s')", i, "A", "2025-01-01"));
        }
        tEnv.executeSql("insert into hudi_table values " + String.join(",", insertValues)).await();

        // test pk
        insertValues.clear();
        for (int i = 0; i < 50; i++) {
            insertValues.add(String.format("('%s', '%s', '%s')", i, "B", "2025-01-01"));
        }
        tEnv.executeSql("INSERT INTO hudi_table VALUES " + String.join(",", insertValues)).await();
        List<Row> r1 = ImmutableList.copyOf(tEnv.executeSql("SELECT * FROM hudi_table").collect());

        tEnv.executeSql(
                "CREATE CATALOG PAIMON WITH ('type'='paimon', 'warehouse' = '" + warehouse + "')");
        tEnv.useCatalog("PAIMON");
        tEnv.executeSql("CREATE DATABASE test");

        createAction(
                        CloneAction.class,
                        "clone",
                        "--database",
                        "default",
                        "--table",
                        "hudi_table",
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

        List<Row> r2 =
                ImmutableList.copyOf(tEnv.executeSql("SELECT * FROM test.test_table").collect());
        Assertions.assertThatList(r1).containsExactlyInAnyOrderElementsOf(r2);
    }
}
