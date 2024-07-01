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

package org.apache.paimon.hive.procedure;

import org.apache.paimon.flink.action.ActionITCaseBase;
import org.apache.paimon.flink.action.RepairAction;
import org.apache.paimon.hive.TestHiveMetastore;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.hadoop.hive.conf.HiveConf;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Tests for {@link RepairAction}. */
public class RepairActionITCase extends ActionITCaseBase {

    private static final TestHiveMetastore TEST_HIVE_METASTORE = new TestHiveMetastore();

    private static final int PORT = 9083;

    @BeforeEach
    public void beforeEach() {
        TEST_HIVE_METASTORE.start(PORT);
    }

    @AfterEach
    public void afterEach() throws Exception {
        TEST_HIVE_METASTORE.stop();
    }

    @Test
    public void testRepairTableAction() throws Exception {
        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().build();
        tEnv.executeSql(
                "CREATE CATALOG PAIMON WITH ('type'='paimon', 'metastore' = 'hive', 'uri' = 'thrift://localhost:"
                        + PORT
                        + "' , 'warehouse' = '"
                        + System.getProperty(HiveConf.ConfVars.METASTOREWAREHOUSE.varname)
                        + "')");
        tEnv.useCatalog("PAIMON");

        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS test_db;").await();
        tEnv.executeSql("USE test_db").await();
        tEnv.executeSql(
                        "CREATE TABLE t_repair_hive (\n"
                                + "    user_id BIGINT,\n"
                                + "    behavior STRING,\n"
                                + "    dt STRING,\n"
                                + "    hh STRING,\n"
                                + "    PRIMARY KEY (dt, hh, user_id) NOT ENFORCED\n"
                                + ") PARTITIONED BY (dt, hh)"
                                + " WITH (\n"
                                + "'metastore.partitioned-table' = 'true'\n"
                                + ");")
                .await();
        tEnv.executeSql("INSERT INTO t_repair_hive VALUES(1, 'login', '2020-01-02', '09')").await();
        Map<String, String> catalogConf = new HashMap<>();
        catalogConf.put("metastore", "hive");
        catalogConf.put("uri", "thrift://localhost:" + PORT);
        RepairAction repairAction =
                new RepairAction(
                        System.getProperty(HiveConf.ConfVars.METASTOREWAREHOUSE.varname),
                        "test_db",
                        "t_repair_hive",
                        catalogConf);
        repairAction.run();

        List<Row> ret =
                ImmutableList.copyOf(tEnv.executeSql("SHOW PARTITIONS t_repair_hive").collect());
        Assertions.assertThat(ret.size() == 1);
        Assertions.assertThat(ret.get(0).toString()).isEqualTo("+I[dt=2020-01-02/hh=09]");
    }
}
