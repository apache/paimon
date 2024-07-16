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

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for table with branches using SQL. */
public class BranchSqlITCase extends AbstractTestBase {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testAlterTable() throws Exception {
        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().build();
        tEnv.executeSql(
                "CREATE CATALOG mycat WITH ( 'type' = 'paimon', 'warehouse' = '" + tempDir + "' )");
        tEnv.executeSql("USE CATALOG mycat");
        tEnv.executeSql(
                "CREATE TABLE t ( pt INT, k INT, v STRING, PRIMARY KEY (pt, k) NOT ENFORCED ) "
                        + "PARTITIONED BY (pt) WITH ( 'bucket' = '2' )");

        tEnv.executeSql(
                        "INSERT INTO t VALUES (1, 10, 'apple'), (1, 20, 'banana'), (2, 10, 'cat'), (2, 20, 'dog')")
                .await();
        tEnv.executeSql("CALL sys.create_branch('default.t', 'test', 1)");
        tEnv.executeSql("INSERT INTO t VALUES (1, 10, 'APPLE'), (2, 20, 'DOG'), (2, 30, 'horse')")
                .await();

        tEnv.executeSql("ALTER TABLE `t$branch_test` ADD (v2 INT)").await();
        tEnv.executeSql(
                        "INSERT INTO `t$branch_test` VALUES "
                                + "(1, 10, 'cherry', 100), (2, 20, 'bird', 200), (2, 40, 'wolf', 400)")
                .await();

        assertThat(collectResult(tEnv, "SELECT * FROM t"))
                .containsExactlyInAnyOrder(
                        "+I[1, 10, APPLE]",
                        "+I[1, 20, banana]",
                        "+I[2, 30, horse]",
                        "+I[2, 10, cat]",
                        "+I[2, 20, DOG]");
        assertThat(collectResult(tEnv, "SELECT * FROM t$branch_test"))
                .containsExactlyInAnyOrder(
                        "+I[1, 10, cherry, 100]",
                        "+I[1, 20, banana, null]",
                        "+I[2, 10, cat, null]",
                        "+I[2, 20, bird, 200]",
                        "+I[2, 40, wolf, 400]");
    }

    private List<String> collectResult(TableEnvironment tEnv, String sql) throws Exception {
        List<String> result = new ArrayList<>();
        try (CloseableIterator<Row> it = tEnv.executeSql(sql).collect()) {
            while (it.hasNext()) {
                result.add(it.next().toString());
            }
        }
        return result;
    }
}
