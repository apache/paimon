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

package org.apache.flink.table.store.kafka;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory.Context;
import org.apache.flink.table.store.log.LogOptions.LogChangelogMode;
import org.apache.flink.table.store.log.LogOptions.LogConsistency;
import org.apache.flink.types.RowKind;

import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.Comparator;
import java.util.List;
import java.util.UUID;

import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH;
import static org.apache.flink.table.store.kafka.KafkaLogTestUtils.SINK_CONTEXT;
import static org.apache.flink.table.store.kafka.KafkaLogTestUtils.SOURCE_CONTEXT;
import static org.apache.flink.table.store.kafka.KafkaLogTestUtils.discoverKafkaLogFactory;
import static org.apache.flink.table.store.kafka.KafkaLogTestUtils.testContext;
import static org.apache.flink.table.store.kafka.KafkaLogTestUtils.testRecord;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for {@link KafkaLogStoreFactory}. */
public class KafkaLogITCase extends KafkaTableTestBase {

    private final KafkaLogStoreFactory factory = discoverKafkaLogFactory();

    @Test
    public void testDropEmpty() {
        // Expect no exceptions to be thrown
        factory.onDropTable(testContext(getBootstrapServers(), LogChangelogMode.AUTO, true), true);
    }

    @Test
    public void testUpsertTransactionKeyed() throws Exception {
        innerTest(
                "UpsertTransactionKeyed",
                LogChangelogMode.UPSERT,
                LogConsistency.TRANSACTIONAL,
                true);
    }

    @Test
    public void testAllTransactionKeyed() throws Exception {
        innerTest("AllTransactionKeyed", LogChangelogMode.ALL, LogConsistency.TRANSACTIONAL, true);
    }

    @Test
    public void testUpsertEventualKeyed() throws Exception {
        innerTest("UpsertEventualKeyed", LogChangelogMode.UPSERT, LogConsistency.EVENTUAL, true);
    }

    @Test
    public void testAllEventualKeyed() throws Exception {
        innerTest("AllEventualKeyed", LogChangelogMode.ALL, LogConsistency.EVENTUAL, true);
    }

    @Test
    public void testAllTransactionNonKeyed() throws Exception {
        innerTest(
                "AllTransactionNonKeyed",
                LogChangelogMode.ALL,
                LogConsistency.TRANSACTIONAL,
                false);
    }

    @Test
    public void testUpsertTransactionNonKeyed() {
        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                innerTest(
                                        "UpsertTransactionNonKeyed",
                                        LogChangelogMode.UPSERT,
                                        LogConsistency.TRANSACTIONAL,
                                        false));
        assertThat(exception.getMessage())
                .isEqualTo("Can not use upsert changelog mode for non-pk table.");
    }

    @Test
    public void testUpsertEventualNonKeyed() {
        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                innerTest(
                                        "UpsertEventualNonKeyed",
                                        LogChangelogMode.UPSERT,
                                        LogConsistency.EVENTUAL,
                                        false));
        assertThat(exception.getMessage())
                .isEqualTo("Can not use EVENTUAL consistency mode for non-pk table.");
    }

    @Test
    public void testAllEventualNonKeyed() {
        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                innerTest(
                                        "AllEventualNonKeyed",
                                        LogChangelogMode.ALL,
                                        LogConsistency.EVENTUAL,
                                        false));
        assertThat(exception.getMessage())
                .isEqualTo("Can not use EVENTUAL consistency mode for non-pk table.");
    }

    private void innerTest(
            String name, LogChangelogMode changelogMode, LogConsistency consistency, boolean keyed)
            throws Exception {
        Context context =
                testContext(name, getBootstrapServers(), changelogMode, consistency, keyed);

        KafkaLogSinkProvider sinkProvider = factory.createSinkProvider(context, SINK_CONTEXT);
        KafkaLogSourceProvider sourceProvider =
                factory.createSourceProvider(context, SOURCE_CONTEXT);

        factory.onCreateTable(context, 3, true);
        try {
            // transactional need to commit
            enableCheckpoint();

            // 1.1 sink
            String uuid = UUID.randomUUID().toString();
            env.fromElements(
                            testRecord(true, 2, 1, 2, RowKind.DELETE),
                            testRecord(true, 1, 3, 4, RowKind.INSERT),
                            testRecord(true, 0, 5, 6, RowKind.INSERT),
                            testRecord(true, 0, 7, 8, RowKind.INSERT))
                    .sinkTo(new TestOffsetsLogSink<>(sinkProvider, uuid));
            env.execute();

            // 1.2 read
            List<RowData> records =
                    env.fromSource(
                                    sourceProvider.createSource(null),
                                    WatermarkStrategy.noWatermarks(),
                                    "source")
                            .executeAndCollect(4);
            records.sort(Comparator.comparingInt(o -> o.getInt(0)));

            // delete, upsert mode
            if (changelogMode == LogChangelogMode.UPSERT) {
                assertRow(records.get(0), RowKind.DELETE, 1, null);
            } else {
                assertRow(records.get(0), RowKind.DELETE, 1, 2);
            }

            // inserts
            assertRow(records.get(1), RowKind.INSERT, 3, 4);
            assertRow(records.get(2), RowKind.INSERT, 5, 6);
            assertRow(records.get(3), RowKind.INSERT, 7, 8);

            // 2.1 sink
            env.fromElements(
                            testRecord(true, 0, 9, 10, RowKind.INSERT),
                            testRecord(true, 1, 11, 12, RowKind.INSERT),
                            testRecord(true, 2, 13, 14, RowKind.INSERT))
                    .sinkTo(new TestOffsetsLogSink<>(sinkProvider, UUID.randomUUID().toString()));
            env.execute();

            // 2.2 read from offsets
            records =
                    env.fromSource(
                                    sourceProvider.createSource(
                                            TestOffsetsLogSink.drainOffsets(uuid)),
                                    WatermarkStrategy.noWatermarks(),
                                    "source")
                            .executeAndCollect(3);
            records.sort(Comparator.comparingInt(o -> o.getInt(0)));
            assertRow(records.get(0), RowKind.INSERT, 9, 10);
            assertRow(records.get(1), RowKind.INSERT, 11, 12);
            assertRow(records.get(2), RowKind.INSERT, 13, 14);
        } finally {
            factory.onDropTable(context, true);
        }
    }

    private void enableCheckpoint() {
        Configuration configuration = new Configuration();
        configuration.set(ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
        env.configure(configuration);
        env.enableCheckpointing(1000);
    }

    private void assertRow(RowData row, RowKind rowKind, Integer k, Integer v) {
        Assert.assertEquals(rowKind, row.getRowKind());
        Assert.assertEquals(k, row.isNullAt(0) ? null : row.getInt(0));
        Assert.assertEquals(v, row.isNullAt(1) ? null : row.getInt(1));
    }
}
