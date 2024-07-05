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

import org.apache.paimon.flink.action.cdc.mysql.MySqlVersion;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;

/** E2e test for MySql CDC type convert tinyint(1) to tinyint. */
public class MySqlTinyIntConvertE2ETest extends MySqlCdcE2eTestBase {

    protected MySqlTinyIntConvertE2ETest() {
        super(MySqlVersion.V5_7);
    }

    @Test
    public void testSyncTable() throws Exception {
        runAction(
                ACTION_SYNC_TABLE,
                null,
                "pk",
                "tinyint1-not-bool",
                ImmutableMap.of(),
                ImmutableMap.of("database-name", "test_tinyint_convert", "table-name", "'T'"),
                ImmutableMap.of("bucket", "2"));

        try (Connection conn = getMySqlConnection();
                Statement statement = conn.createStatement()) {
            statement.executeUpdate("USE test_tinyint_convert");

            statement.executeUpdate("INSERT INTO T VALUES (1, '2023-05-10 12:30:20', 21)");

            String jobId =
                    runBatchSql(
                            "INSERT INTO result1 SELECT * FROM ts_table",
                            catalogDdl,
                            useCatalogCmd,
                            createResultSink(
                                    "result1", "pk INT, _date TIMESTAMP(0), _tinyint1 TINYINT"));
            checkResult("1, 2023-05-10T12:30:20, 21");
            clearCurrentResults();
            cancelJob(jobId);
        }
    }

    @Disabled("Not supported")
    @Test
    public void testSyncDatabase() {}
}
