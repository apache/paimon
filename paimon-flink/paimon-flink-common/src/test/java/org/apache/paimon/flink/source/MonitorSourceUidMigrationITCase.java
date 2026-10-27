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

package org.apache.paimon.flink.source;

import org.apache.paimon.flink.CatalogITCaseBase;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import static org.apache.paimon.utils.CommonTestUtils.waitUtil;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Turning {@code source.operator-uid.cover-table-source-operators} on renames the Monitor, whose
 * state is then dropped; on the exactly-once consumer route the consumer carries the position.
 */
class MonitorSourceUidMigrationITCase extends CatalogITCaseBase {

    @Override
    protected List<String> ddl() {
        return Arrays.asList(
                "CREATE TABLE src (k INT) WITH ('source.operator-uid.suffix' = 'test-uid',"
                        + " 'consumer-id' = 'c', 'consumer.expiration-time' = '1 d')",
                "CREATE TABLE snk (k INT) WITH ('sink.operator-uid.suffix' = 'test-uid')");
    }

    @Test
    @Timeout(300)
    void testConsumerKeepsPositionWhenMonitorStateIsDropped() throws Exception {
        insert(0, 1, 2);
        JobClient client = copy("false");
        awaitRows(3);
        String savepoint =
                client.stopWithSavepoint(
                                false, getTempDirPath("savepoint"), SavepointFormatType.CANONICAL)
                        .get();

        insert(3, 4, 5);
        sEnv.getConfig().getConfiguration().setString("execution.state-recovery.path", savepoint);
        sEnv.getConfig()
                .getConfiguration()
                .setString("execution.state-recovery.ignore-unclaimed-state", "true");
        client = copy("true");
        awaitRows(6);
        client.cancel().get();

        assertThat(batchSql("SELECT k FROM snk"))
                .as("every snapshot read exactly once across the restore")
                .containsExactlyInAnyOrder(
                        Row.of(0), Row.of(1), Row.of(2), Row.of(3), Row.of(4), Row.of(5));
    }

    /** One snapshot per key. */
    private void insert(int... keys) {
        for (int k : keys) {
            batchSql("INSERT INTO src VALUES (%d)", k);
        }
    }

    private JobClient copy(String cover) throws Exception {
        return sEnv.executeSql(
                        "INSERT INTO snk SELECT k FROM src /*+ OPTIONS("
                                + "'source.operator-uid.cover-table-source-operators' = '"
                                + cover
                                + "') */")
                .getJobClient()
                .get();
    }

    private void awaitRows(int count) throws Exception {
        waitUtil(
                () -> batchSql("SELECT k FROM snk").size() >= count,
                Duration.ofMinutes(2),
                Duration.ofMillis(500),
                "the sink never reached " + count + " rows");
    }
}
