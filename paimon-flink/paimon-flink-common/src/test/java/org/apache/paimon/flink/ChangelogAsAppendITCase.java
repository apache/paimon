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

import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** SQL planner and committed append-table coverage for changelog payload materialization. */
@Timeout(120)
public class ChangelogAsAppendITCase extends CatalogITCaseBase {
    @Override
    protected int defaultParallelism() {
        return 1;
    }

    @Test
    public void testEveryRowKindSurvivesSqlAndCommittedRead() throws Exception {
        // Two equal inserts must remain separate. Deletes carry a full non-key payload.
        List<Row> input =
                Arrays.asList(
                        Row.ofKind(RowKind.INSERT, 1, 10),
                        Row.ofKind(RowKind.INSERT, 1, 10),
                        Row.ofKind(RowKind.UPDATE_BEFORE, 1, 10),
                        Row.ofKind(RowKind.UPDATE_AFTER, 1, 20),
                        Row.ofKind(RowKind.DELETE, 1, 20),
                        Row.ofKind(RowKind.DELETE, 1, 10));
        createSource(input);
        createLog();
        long started = System.currentTimeMillis();
        sEnv.executeSql(
                        "INSERT INTO event_log SELECT id, amount, CAST(NULL AS STRING), "
                                + "CAST(NULL AS BIGINT) FROM changes")
                .await();
        long finished = System.currentTimeMillis();

        // A separate batch reader sees only committed storage, not the converter's output.
        List<Row> stored = batchSql("SELECT * FROM event_log");
        assertThat(payloads(stored))
                .containsExactlyInAnyOrder(
                        Row.of(1, 10, "INSERT"), Row.of(1, 10, "INSERT"),
                        Row.of(1, 10, "UPDATE_BEFORE"), Row.of(1, 20, "UPDATE_AFTER"),
                        Row.of(1, 20, "DELETE"), Row.of(1, 10, "DELETE"));
        assertEmissionTimes(stored, started, finished);
    }

    @Test
    public void testSqlAggregateSuppliesBeforeImages() throws Exception {
        // The aggregate must deliver 10 -> 30 -> 10, including both before-images.
        createSource(
                Arrays.asList(
                        Row.ofKind(RowKind.INSERT, 1, 10),
                        Row.ofKind(RowKind.INSERT, 1, 20),
                        Row.ofKind(RowKind.DELETE, 1, 20),
                        Row.ofKind(RowKind.DELETE, 1, 10)));
        createLog();
        sEnv.getConfig().getConfiguration().setString("table.exec.mini-batch.enabled", "false");
        long started = System.currentTimeMillis();
        sEnv.executeSql(
                        "INSERT INTO event_log SELECT id, SUM(amount), CAST(NULL AS STRING), "
                                + "CAST(NULL AS BIGINT) FROM changes GROUP BY id")
                .await();
        long finished = System.currentTimeMillis();

        List<Row> stored = batchSql("SELECT * FROM event_log");
        assertThat(payloads(stored))
                .containsExactlyInAnyOrder(
                        Row.of(1, 10, "INSERT"), Row.of(1, 10, "UPDATE_BEFORE"),
                        Row.of(1, 30, "UPDATE_AFTER"), Row.of(1, 30, "UPDATE_BEFORE"),
                        Row.of(1, 10, "UPDATE_AFTER"), Row.of(1, 10, "DELETE"));
        assertEmissionTimes(stored, started, finished);
    }

    private void createSource(List<Row> input) {
        String dataId = TestValuesTableFactory.registerData(input);
        sEnv.executeSql(
                "CREATE TEMPORARY TABLE changes (id INT, amount INT) WITH ("
                        + "'connector'='values', 'bounded'='true', 'changelog-mode'='I,UB,UA,D', "
                        + "'data-id'='"
                        + dataId
                        + "')");
    }

    private void createLog() {
        batchSql(
                "CREATE TABLE event_log (id INT, amount INT, original_kind STRING, emitted_ms BIGINT) WITH ("
                        + "'bucket'='-1', 'sink.changelog-as-append'='true', "
                        + "'sink.changelog-as-append.kind-field'='original_kind', "
                        + "'sink.changelog-as-append.time-field'='emitted_ms')");
    }

    private List<Row> payloads(List<Row> rows) {
        return rows.stream()
                .map(row -> Row.of(row.getField(0), row.getField(1), row.getField(2)))
                .collect(Collectors.toList());
    }

    private void assertEmissionTimes(List<Row> rows, long started, long finished) {
        for (Row row : rows) {
            assertThat(row.getKind()).isEqualTo(RowKind.INSERT);
            assertThat((Long) row.getField(3)).isNotNull().isBetween(started, finished);
        }
    }
}
