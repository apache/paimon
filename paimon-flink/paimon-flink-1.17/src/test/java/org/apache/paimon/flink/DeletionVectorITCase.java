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

import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.junit.jupiter.api.Test;

/** ITCase for deletion vector table. */
public class DeletionVectorITCase extends CatalogITCaseBase {

    @Test
    public void testDVTableWithUpgrade() throws Exception {
        sEnv.executeSql(
                "CREATE TABLE T ("
                        + "    order_number BIGINT,"
                        + "    order_name   VARCHAR(3),"
                        + "    price        DECIMAL(32,2),"
                        + "    buyer        ROW<first_name STRING, last_name STRING>,"
                        + "    order_time   TIMESTAMP(3),"
                        + "    dt           DATE,"
                        + "    primary key (order_number, order_name, dt) NOT ENFORCED"
                        + ") PARTITIONED BY(dt) "
                        + "WITH ("
                        + "    'bucket' = '8',"
                        + "    'target-file-size' = '500mb',"
                        + "    'manifest.target-file-size' = '32mb',"
                        + "    'sink.parallelism' = '4',"
                        + "    'snapshot.expire.execution-mode' = 'async',"
                        + "    'deletion-vectors.enabled' = 'true'"
                        + ")");

        sEnv.executeSql(
                "CREATE TEMPORARY TABLE orders_gen ("
                        + "    order_number BIGINT,"
                        + "    order_name   VARCHAR(3),"
                        + "    price        DECIMAL(32,2),"
                        + "    buyer        ROW<first_name STRING, last_name STRING>,"
                        + "    order_time   TIMESTAMP(3),"
                        + "    dt           DATE"
                        + ") WITH ("
                        + "    'connector' = 'datagen', "
                        + "    'rows-per-second' = '10000',"
                        + "    'fields.order_number.min' = '1',"
                        + "    'fields.order_number.max' = '10000000'"
                        + ");");
        sEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4);
        TableResult result = sEnv.executeSql("INSERT INTO T SELECT * FROM orders_gen");
        Thread.sleep(600000);
        result.getJobClient().get().cancel();
    }
}
