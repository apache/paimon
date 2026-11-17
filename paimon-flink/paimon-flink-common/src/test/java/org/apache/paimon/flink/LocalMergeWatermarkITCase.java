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

import org.apache.paimon.Snapshot;
import org.apache.paimon.flink.sink.FlinkSinkBuilder;
import org.apache.paimon.flink.source.AbstractNonCoordinatedSource;
import org.apache.paimon.flink.source.AbstractNonCoordinatedSourceReader;
import org.apache.paimon.flink.source.SimpleSourceSplit;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

/** IT cases for the watermark of a sink with local merge. */
public class LocalMergeWatermarkITCase extends CatalogITCaseBase {

    @Override
    protected List<String> ddl() {
        return Collections.singletonList(
                "CREATE TABLE T (k INT, v INT, PRIMARY KEY (k) NOT ENFORCED) WITH ("
                        + "'bucket' = '1', 'local-merge-buffer-size' = '5m', "
                        + "'commit.force-create-snapshot' = 'true')");
    }

    @Test
    public void testWatermarkAdvancesWhileRecordsAreFilteredOut() throws Exception {
        FileStoreTable table = paimonTable("T");
        StreamExecutionEnvironment env =
                streamExecutionEnvironmentBuilder()
                        .streamingMode()
                        .parallelism(1)
                        .checkpointIntervalMs(200)
                        .build();
        DataStream<RowData> source =
                env.fromSource(
                        new IncreasingWatermarkSource(),
                        WatermarkStrategy.noWatermarks(),
                        "increasing-watermark-source");
        // like a WHERE clause: only the first record reaches the sink, the watermark keeps moving
        new FlinkSinkBuilder(table).forRowData(source.filter(row -> row.getInt(0) == 0)).build();

        // the job is cancelled by AbstractTestBase#cleanupRunningJobs
        JobClient client = env.executeAsync();
        long first = waitForWatermark(client, table, Long.MIN_VALUE);
        waitForWatermark(client, table, first);
    }

    /** Waits for a snapshot whose watermark is greater than {@code previous}, fails on timeout. */
    private static long waitForWatermark(JobClient client, FileStoreTable table, long previous)
            throws Exception {
        long deadline = System.currentTimeMillis() + 60_000;
        while (System.currentTimeMillis() < deadline) {
            if (client.getJobStatus().get().isGloballyTerminalState()) {
                // surfaces the failure cause
                client.getJobExecutionResult().get();
            }
            Snapshot snapshot = table.snapshotManager().latestSnapshot();
            if (snapshot != null
                    && snapshot.watermark() != null
                    && snapshot.watermark() > previous) {
                return snapshot.watermark();
            }
            Thread.sleep(100);
        }
        throw new AssertionError(
                String.format("No snapshot watermark greater than %d within 60 s", previous));
    }

    /** Emits a record with an increasing watermark every 100 ms, forever. */
    private static class IncreasingWatermarkSource extends AbstractNonCoordinatedSource<RowData> {

        private static final long serialVersionUID = 1L;

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.CONTINUOUS_UNBOUNDED;
        }

        @Override
        public SourceReader<RowData, SimpleSourceSplit> createReader(SourceReaderContext context) {
            return new AbstractNonCoordinatedSourceReader<RowData>() {

                private int step;

                @Override
                public InputStatus pollNext(ReaderOutput<RowData> output)
                        throws InterruptedException {
                    output.collect(GenericRowData.of(step, step));
                    output.emitWatermark(new Watermark(step * 1000L));
                    step++;
                    Thread.sleep(100);
                    return InputStatus.MORE_AVAILABLE;
                }
            };
        }
    }
}
