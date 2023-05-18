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

package org.apache.paimon.tests.cdc;

import org.apache.paimon.flink.action.cdc.mysql.MySqlContainer;
import org.apache.paimon.flink.action.cdc.mysql.MySqlVersion;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;

import java.sql.Connection;
import java.sql.Statement;

/** E2e test for MySql CDC with computed column. */
public class MySqlComputedColumnE2ETest extends MySqlCdcE2eTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlComputedColumnE2ETest.class);

    protected MySqlComputedColumnE2ETest() {
        super(MySqlVersion.V5_7);
    }

    @Test
    public void testSyncTable() throws Exception {
        String runActionCommand =
                String.join(
                        " ",
                        "bin/flink",
                        "run",
                        "-D",
                        "execution.checkpointing.interval=1s",
                        "--detached",
                        "lib/paimon-flink-action.jar",
                        "mysql-sync-table",
                        "--warehouse",
                        warehousePath,
                        "--database",
                        "default",
                        "--table",
                        "ts_table",
                        "--partition-keys",
                        // computed from _datetime
                        "_year",
                        "--primary-keys",
                        "pk,_year",
                        "--computed-column '_year=year(_datetime)'",
                        "--mysql-conf",
                        "hostname=mysql-1",
                        "--mysql-conf",
                        String.format("port=%d", MySqlContainer.MYSQL_PORT),
                        "--mysql-conf",
                        String.format("username='%s'", mySqlContainer.getUsername()),
                        "--mysql-conf",
                        String.format("password='%s'", mySqlContainer.getPassword()),
                        "--mysql-conf",
                        "database-name='test_computed_column'",
                        "--mysql-conf",
                        "table-name='T'",
                        "--table-conf",
                        "bucket=2");
        Container.ExecResult execResult =
                jobManager.execInContainer("su", "flink", "-c", runActionCommand);
        LOG.info(execResult.getStdout());
        LOG.info(execResult.getStderr());

        try (Connection conn = getMySqlConnection();
                Statement statement = conn.createStatement()) {
            statement.executeUpdate("USE test_computed_column");

            statement.executeUpdate("INSERT INTO T VALUES (1, '2023-05-10 12:30:20')");

            String jobId =
                    runSql(
                            "INSERT INTO result1 SELECT * FROM ts_table;",
                            catalogDdl,
                            useCatalogCmd,
                            createResultSink("result1", "pk INT, _date TIMESTAMP(0), _year INT"));
            checkResult("1, 2023-05-10T12:30:20, 2023");
            clearCurrentResults();
            cancelJob(jobId);
        }
    }

    @Disabled("Not supported")
    @Test
    public void testSyncDatabase() {}
}
