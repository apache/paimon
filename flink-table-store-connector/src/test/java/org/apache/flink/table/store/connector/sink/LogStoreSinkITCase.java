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

package org.apache.flink.table.store.connector.sink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.store.connector.TableStore;
import org.apache.flink.table.store.file.utils.BlockingIterator;
import org.apache.flink.table.store.kafka.KafkaLogSinkProvider;
import org.apache.flink.table.store.kafka.KafkaLogSourceProvider;
import org.apache.flink.table.store.kafka.KafkaLogStoreFactory;
import org.apache.flink.table.store.kafka.KafkaLogTestUtils;
import org.apache.flink.table.store.kafka.KafkaTableTestBase;
import org.apache.flink.table.store.log.LogOptions;
import org.apache.flink.types.Row;

import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.stream.Stream;

import static org.apache.flink.table.store.connector.FileStoreITCase.CONVERTER;
import static org.apache.flink.table.store.connector.FileStoreITCase.SOURCE_DATA;
import static org.apache.flink.table.store.connector.FileStoreITCase.TABLE_TYPE;
import static org.apache.flink.table.store.connector.FileStoreITCase.buildBatchEnv;
import static org.apache.flink.table.store.connector.FileStoreITCase.buildStreamEnv;
import static org.apache.flink.table.store.connector.FileStoreITCase.buildTableStore;
import static org.apache.flink.table.store.connector.FileStoreITCase.buildTestSource;
import static org.apache.flink.table.store.connector.FileStoreITCase.executeAndCollect;
import static org.apache.flink.table.store.kafka.KafkaLogTestUtils.SINK_CONTEXT;
import static org.apache.flink.table.store.kafka.KafkaLogTestUtils.SOURCE_CONTEXT;
import static org.apache.flink.table.store.kafka.KafkaLogTestUtils.discoverKafkaLogFactory;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for table store with kafka. */
@Ignore // TODO enable this
public class LogStoreSinkITCase extends KafkaTableTestBase {

    @Test
    public void testStreamingPartitioned() throws Exception {
        innerTest("testStreamingPartitioned", false, true, true, true);
    }

    @Test
    public void testStreamingNonPartitioned() throws Exception {
        innerTest("testStreamingNonPartitioned", false, false, true, true);
    }

    @Test
    public void testBatchPartitioned() throws Exception {
        innerTest("testBatchPartitioned", true, true, true, true);
    }

    @Test
    public void testStreamingEventual() throws Exception {
        innerTest("testStreamingEventual", false, true, false, true);
    }

    @Test
    public void testStreamingPartitionedNonKey() throws Exception {
        innerTest("testStreamingPartitionedNonKey", false, true, true, false);
    }

    @Test
    public void testBatchPartitionedNonKey() throws Exception {
        innerTest("testBatchPartitionedNonKey", true, true, true, false);
    }

    private void innerTest(
            String name, boolean isBatch, boolean partitioned, boolean transaction, boolean hasPk)
            throws Exception {
        StreamExecutionEnvironment env = isBatch ? buildBatchEnv() : buildStreamEnv();

        // in eventual mode, failure will result in duplicate data
        TableStore store = buildTableStore(isBatch || !transaction, TEMPORARY_FOLDER);
        if (partitioned) {
            if (hasPk) {
                store.withPrimaryKeys(new int[] {1, 2});
            } else {
                store.withPrimaryKeys(new int[0]);
            }
            store.withPartitions(new int[] {1});
        } else {
            store.withPartitions(new int[0]);
        }

        if (!hasPk) {
            store.withPrimaryKeys(new int[0]);
        }

        // prepare log
        DynamicTableFactory.Context context =
                KafkaLogTestUtils.testContext(
                        name,
                        getBootstrapServers(),
                        LogOptions.LogChangelogMode.AUTO,
                        transaction
                                ? LogOptions.LogConsistency.TRANSACTIONAL
                                : LogOptions.LogConsistency.EVENTUAL,
                        TABLE_TYPE,
                        hasPk ? new int[] {2} : new int[0]);

        KafkaLogStoreFactory factory = discoverKafkaLogFactory();
        KafkaLogSinkProvider sinkProvider = factory.createSinkProvider(context, SINK_CONTEXT);
        KafkaLogSourceProvider sourceProvider =
                factory.createSourceProvider(context, SOURCE_CONTEXT, null);

        factory.onCreateTable(context, 3, true);

        try {
            // write
            store.sinkBuilder()
                    .withInput(buildTestSource(env, isBatch))
                    .withLogSinkProvider(sinkProvider)
                    .build();
            env.execute();

            // read
            List<Row> results = executeAndCollect(store.sourceBuilder().build(env));

            Row[] expected;
            if (hasPk) {
                expected =
                        partitioned
                                ? new Row[] {
                                    Row.of(5, "p2", 1),
                                    Row.of(3, "p2", 5),
                                    Row.of(5, "p1", 1),
                                    Row.of(0, "p1", 2)
                                }
                                : new Row[] {
                                    Row.of(5, "p2", 1), Row.of(0, "p1", 2), Row.of(3, "p2", 5)
                                };
            } else {
                Stream<RowData> expectedStream =
                        isBatch
                                ? SOURCE_DATA.stream()
                                : Stream.concat(SOURCE_DATA.stream(), SOURCE_DATA.stream());
                expected = expectedStream.map(CONVERTER::toExternal).toArray(Row[]::new);
            }

            assertThat(results).containsExactlyInAnyOrder(expected);

            BlockingIterator<RowData, Row> iterator =
                    BlockingIterator.of(
                            store.sourceBuilder()
                                    .withContinuousMode(true)
                                    .withLogSourceProvider(sourceProvider)
                                    .build(buildStreamEnv())
                                    .executeAndCollect(),
                            CONVERTER::toExternal);
            results = iterator.collectAndClose(expected.length);
            assertThat(results).containsExactlyInAnyOrder(expected);
        } finally {
            factory.onDropTable(context, true);
        }
    }
}
