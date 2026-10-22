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

import org.apache.paimon.utils.CommonTestUtils;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.ExecutionConfigOptions.UpsertMaterialize;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions.NonDeterministicUpdateStrategy;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests preserved retraction fields with an external upsert sink. */
public class LookupChangelogPreserveFieldITCase extends CatalogITCaseBase {

    @Override
    protected int defaultParallelism() {
        return 1;
    }

    @ParameterizedTest
    @CsvSource({"AUTO, true, false", "NONE, true, false", "AUTO, false, false", "AUTO, true, true"})
    public void testExternalUpsertSink(
            UpsertMaterialize materialize, boolean preserveField, boolean matchingPrimaryKeys)
            throws Exception {
        TestValuesTableFactory.clearAllData();
        // The test environment defaults to NONE; explicitly exercise Flink's default AUTO too.
        sEnv.getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_SINK_UPSERT_MATERIALIZE, materialize);
        sEnv.getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_NONDETERMINISTIC_UPDATE_STRATEGY,
                        NonDeterministicUpdateStrategy.IGNORE);
        sql(
                "CREATE TABLE T (id INT PRIMARY KEY NOT ENFORCED, data INT, event_ts BIGINT) "
                        + "WITH ('bucket'='1', 'merge-engine'='deduplicate', "
                        + "'changelog-producer'='lookup', 'sequence.field'='event_ts', "
                        + "'continuous.discovery-interval'='100 ms'%s)",
                preserveField ? ", 'changelog-producer.preserve-field-on-retract'='event_ts'" : "");
        sEnv.executeSql(
                "CREATE TEMPORARY TABLE event_sink (sink_key INT, event_ts BIGINT, "
                        + "PRIMARY KEY (sink_key) NOT ENFORCED) WITH "
                        + "('connector'='values', 'sink-insert-only'='false')");

        String insert =
                "INSERT INTO event_sink SELECT "
                        + (matchingPrimaryKeys ? "id" : "data")
                        + ", event_ts FROM T";
        boolean hasMaterializer = materialize == UpsertMaterialize.AUTO && !matchingPrimaryKeys;
        String plan = sEnv.explainSql(insert);
        if (hasMaterializer) {
            assertThat(plan).contains("upsertMaterialize=[true]");
        } else {
            assertThat(plan).doesNotContain("upsertMaterialize=[true]");
        }
        if (preserveField && hasMaterializer) {
            // Check that TRY_RESOLVE accepts the plan, then run it with this safeguard enabled.
            sEnv.getConfig()
                    .set(
                            OptimizerConfigOptions.TABLE_OPTIMIZER_NONDETERMINISTIC_UPDATE_STRATEGY,
                            NonDeterministicUpdateStrategy.TRY_RESOLVE);
            assertThat(sEnv.explainSql(insert)).contains("upsertMaterialize=[true]");
        }

        sql("INSERT INTO T VALUES (1, 10, 50)");
        JobClient job = sEnv.executeSql(insert).getJobClient().get();
        try {
            waitForSinkRow(Row.of(matchingPrimaryKeys ? 1 : 10, 50L));
            sql("INSERT INTO T VALUES (1, 20, 100)");
            Row updated = Row.of(matchingPrimaryKeys ? 1 : 20, 100L);
            waitForSinkRow(updated);

            // One bucket and parallelism one keep this later snapshot behind the update and its
            // retraction, so observing the marker proves the sink has processed both.
            sql("INSERT INTO T VALUES (2, 30, 150)");
            Row marker = Row.of(matchingPrimaryKeys ? 2 : 30, 150L);
            waitForSinkRow(marker);

            if (preserveField && hasMaterializer) {
                // The retraction (10, 100) cannot match the materializer's stored (10, 50).
                // TRY_RESOLVE also permits this plan: preserved fields are physical columns.
                assertThat(TestValuesTableFactory.getResults("event_sink"))
                        .containsExactlyInAnyOrder(Row.of(10, 50L), updated, marker);
                assertThat(TestValuesTableFactory.getRawResults("event_sink"))
                        .doesNotContain(Row.ofKind(RowKind.DELETE, 10, 100L));
            } else {
                assertThat(TestValuesTableFactory.getResults("event_sink"))
                        .containsExactlyInAnyOrder(updated, marker);
                if (!matchingPrimaryKeys) {
                    assertThat(TestValuesTableFactory.getRawResults("event_sink"))
                            .contains(
                                    Row.ofKind(
                                            materialize == UpsertMaterialize.NONE
                                                    ? RowKind.UPDATE_BEFORE
                                                    : RowKind.DELETE,
                                            10,
                                            preserveField ? 100L : 50L));
                }
            }
        } finally {
            job.cancel().get(30, TimeUnit.SECONDS);
            TestValuesTableFactory.clearAllData();
        }
    }

    private void waitForSinkRow(Row row) throws Exception {
        CommonTestUtils.waitUtil(
                () -> TestValuesTableFactory.getResults("event_sink").contains(row),
                Duration.ofSeconds(30),
                Duration.ofMillis(100),
                "Timed out waiting for external sink row " + row);
    }
}
