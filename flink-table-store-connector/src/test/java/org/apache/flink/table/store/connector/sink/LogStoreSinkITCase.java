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
import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.connector.source.FlinkSourceBuilder;
import org.apache.flink.table.store.file.utils.BlockingIterator;
import org.apache.flink.table.store.kafka.KafkaLogSinkProvider;
import org.apache.flink.table.store.kafka.KafkaLogSourceProvider;
import org.apache.flink.table.store.kafka.KafkaLogStoreFactory;
import org.apache.flink.table.store.kafka.KafkaLogTestUtils;
import org.apache.flink.table.store.kafka.KafkaTableTestBase;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.types.Row;

import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.stream.Stream;

import static org.apache.flink.table.store.connector.FileStoreITCase.CONVERTER;
import static org.apache.flink.table.store.connector.FileStoreITCase.IDENTIFIER;
import static org.apache.flink.table.store.connector.FileStoreITCase.SOURCE_DATA;
import static org.apache.flink.table.store.connector.FileStoreITCase.TABLE_TYPE;
import static org.apache.flink.table.store.connector.FileStoreITCase.buildBatchEnv;
import static org.apache.flink.table.store.connector.FileStoreITCase.buildFileStoreTable;
import static org.apache.flink.table.store.connector.FileStoreITCase.buildStreamEnv;
import static org.apache.flink.table.store.connector.FileStoreITCase.buildTestSource;
import static org.apache.flink.table.store.connector.FileStoreITCase.executeAndCollect;
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
        FileStoreTable table =
                buildFileStoreTable(
                        isBatch || !transaction,
                        TEMPORARY_FOLDER,
                        partitioned ? new int[] {1} : new int[0],
                        hasPk ? new int[] {2} : new int[0]);

        // prepare log
        DynamicTableFactory.Context context =
                KafkaLogTestUtils.testContext(
                        name,
                        getBootstrapServers(),
                        CoreOptions.LogChangelogMode.AUTO,
                        transaction
                                ? CoreOptions.LogConsistency.TRANSACTIONAL
                                : CoreOptions.LogConsistency.EVENTUAL,
                        TABLE_TYPE,
                        hasPk ? new int[] {2} : new int[0]);

        KafkaLogStoreFactory factory = discoverKafkaLogFactory();
        KafkaLogSinkProvider sinkProvider = factory.createSinkProvider(context, null);
        KafkaLogSourceProvider sourceProvider = factory.createSourceProvider(context, null, null);

        factory.onCreateTable(context, 3, true);

        try {
            // write
            new FlinkSinkBuilder(IDENTIFIER, table)
                    .withInput(buildTestSource(env, isBatch))
                    .withLogSinkFunction(sinkProvider.createSink())
                    .build();
            env.execute();

            // read
            List<Row> results =
                    executeAndCollect(
                            new FlinkSourceBuilder(IDENTIFIER, table).withEnv(env).build());

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
                            new FlinkSourceBuilder(IDENTIFIER, table)
                                    .withContinuousMode(true)
                                    .withLogSourceProvider(sourceProvider)
                                    .withEnv(buildStreamEnv())
                                    .build()
                                    .executeAndCollect(),
                            CONVERTER::toExternal);
            results = iterator.collectAndClose(expected.length);
            assertThat(results).containsExactlyInAnyOrder(expected);
        } finally {
            factory.onDropTable(context, true);
        }
    }
}
