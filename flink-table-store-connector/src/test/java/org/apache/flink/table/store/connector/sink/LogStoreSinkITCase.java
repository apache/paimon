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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.store.connector.FileStoreITCase;
import org.apache.flink.table.store.file.FileStore;
import org.apache.flink.table.store.kafka.KafkaLogSinkProvider;
import org.apache.flink.table.store.kafka.KafkaLogSourceProvider;
import org.apache.flink.table.store.kafka.KafkaLogStoreFactory;
import org.apache.flink.table.store.kafka.KafkaLogTestUtils;
import org.apache.flink.table.store.kafka.KafkaTableTestBase;
import org.apache.flink.table.store.log.LogOptions;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.store.connector.FileStoreITCase.buildBatchEnv;
import static org.apache.flink.table.store.connector.FileStoreITCase.buildConfiguration;
import static org.apache.flink.table.store.connector.FileStoreITCase.buildFileStore;
import static org.apache.flink.table.store.connector.FileStoreITCase.buildStreamEnv;
import static org.apache.flink.table.store.connector.FileStoreITCase.buildTestSource;
import static org.apache.flink.table.store.kafka.KafkaLogTestUtils.SINK_CONTEXT;
import static org.apache.flink.table.store.kafka.KafkaLogTestUtils.SOURCE_CONTEXT;
import static org.apache.flink.table.store.kafka.KafkaLogTestUtils.discoverKafkaLogFactory;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for table store with kafka. */
public class LogStoreSinkITCase extends KafkaTableTestBase {

    @Test
    public void testStreamingPartitioned() throws Exception {
        innerTest("testStreamingPartitioned", false, true, true);
    }

    @Test
    public void testStreamingNonPartitioned() throws Exception {
        innerTest("testStreamingNonPartitioned", false, false, true);
    }

    @Test
    public void testBatchPartitioned() throws Exception {
        innerTest("testBatchPartitioned", true, true, true);
    }

    @Test
    public void testStreamingEventual() throws Exception {
        innerTest("testStreamingEventual", false, true, false);
    }

    private void innerTest(String name, boolean isBatch, boolean partitioned, boolean transaction)
            throws Exception {
        StreamExecutionEnvironment env = isBatch ? buildBatchEnv() : buildStreamEnv();

        // in eventual mode, failure will result in duplicate data
        FileStore fileStore =
                buildFileStore(
                        buildConfiguration(isBatch || !transaction, TEMPORARY_FOLDER.newFolder()),
                        partitioned);

        // prepare log
        DynamicTableFactory.Context context =
                KafkaLogTestUtils.testContext(
                        name,
                        getBootstrapServers(),
                        LogOptions.LogChangelogMode.AUTO,
                        transaction
                                ? LogOptions.LogConsistency.TRANSACTIONAL
                                : LogOptions.LogConsistency.EVENTUAL,
                        FileStoreITCase.VALUE_TYPE,
                        new int[] {2});

        KafkaLogStoreFactory factory = discoverKafkaLogFactory();
        KafkaLogSinkProvider sinkProvider = factory.createSinkProvider(context, SINK_CONTEXT);
        KafkaLogSourceProvider sourceProvider =
                factory.createSourceProvider(context, SOURCE_CONTEXT);

        factory.onCreateTable(context, 3, true);

        try {
            // write
            DataStreamSource<RowData> finiteSource = buildTestSource(env, isBatch);
            FileStoreITCase.write(finiteSource, fileStore, partitioned, null, sinkProvider);

            // read
            List<Row> results = FileStoreITCase.read(env, fileStore);

            Row[] expected =
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
            assertThat(results).containsExactlyInAnyOrder(expected);

            results =
                    buildStreamEnv()
                            .fromSource(
                                    sourceProvider.createSource(null),
                                    WatermarkStrategy.noWatermarks(),
                                    "source")
                            .executeAndCollect(isBatch ? 6 : 12).stream()
                            .map(FileStoreITCase.CONVERTER::toExternal)
                            .collect(Collectors.toList());

            if (isBatch) {
                expected =
                        new Row[] {
                            Row.of(0, "p1", 1),
                            Row.of(0, "p1", 2),
                            Row.of(5, "p1", 1),
                            Row.of(6, "p2", 1),
                            Row.of(3, "p2", 5),
                            Row.of(5, "p2", 1)
                        };
            } else {
                // read log
                // expect origin data X 2 (FiniteTestSource)
                expected =
                        new Row[] {
                            Row.of(0, "p1", 1),
                            Row.of(0, "p1", 2),
                            Row.of(5, "p1", 1),
                            Row.of(6, "p2", 1),
                            Row.of(3, "p2", 5),
                            Row.of(5, "p2", 1),
                            Row.of(0, "p1", 1),
                            Row.of(0, "p1", 2),
                            Row.of(5, "p1", 1),
                            Row.of(6, "p2", 1),
                            Row.of(3, "p2", 5),
                            Row.of(5, "p2", 1)
                        };
            }
            assertThat(results).containsExactlyInAnyOrder(expected);
        } finally {
            factory.onDropTable(context, true);
        }
    }
}
