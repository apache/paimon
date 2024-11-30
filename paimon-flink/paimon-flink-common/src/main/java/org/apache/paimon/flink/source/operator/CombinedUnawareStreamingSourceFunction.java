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

package org.apache.paimon.flink.source.operator;

import org.apache.paimon.append.MultiTableUnawareAppendCompactionTask;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.flink.compact.MultiTableScanBase;
import org.apache.paimon.flink.compact.MultiUnawareBucketTableScan;
import org.apache.paimon.flink.sink.MultiTableCompactionTaskTypeInfo;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.StreamSource;

import java.util.regex.Pattern;

import static org.apache.paimon.flink.compact.MultiTableScanBase.ScanResult.FINISHED;
import static org.apache.paimon.flink.compact.MultiTableScanBase.ScanResult.IS_EMPTY;

/**
 * It is responsible for monitoring compactor source in stream mode for the table of unaware bucket.
 */
public class CombinedUnawareStreamingSourceFunction
        extends CombinedCompactorSourceFunction<MultiTableUnawareAppendCompactionTask> {

    private final long monitorInterval;
    private MultiTableScanBase<MultiTableUnawareAppendCompactionTask> tableScan;

    public CombinedUnawareStreamingSourceFunction(
            Catalog.Loader catalogLoader,
            Pattern includingPattern,
            Pattern excludingPattern,
            Pattern databasePattern,
            long monitorInterval) {
        super(catalogLoader, includingPattern, excludingPattern, databasePattern, true);
        this.monitorInterval = monitorInterval;
    }

    /**
     * Do not annotate with <code>@override</code> here to maintain compatibility with Flink 1.18-.
     */
    public void open(OpenContext openContext) throws Exception {
        open(new Configuration());
    }

    /**
     * Do not annotate with <code>@override</code> here to maintain compatibility with Flink 2.0+.
     */
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        tableScan =
                new MultiUnawareBucketTableScan(
                        catalogLoader,
                        includingPattern,
                        excludingPattern,
                        databasePattern,
                        isStreaming,
                        isRunning);
    }

    @SuppressWarnings("BusyWait")
    @Override
    void scanTable() throws Exception {
        while (isRunning.get()) {
            MultiTableScanBase.ScanResult scanResult = tableScan.scanTable(ctx);
            if (scanResult == FINISHED) {
                return;
            }
            if (scanResult == IS_EMPTY) {
                Thread.sleep(monitorInterval);
            }
        }
    }

    public static DataStream<MultiTableUnawareAppendCompactionTask> buildSource(
            StreamExecutionEnvironment env,
            String name,
            Catalog.Loader catalogLoader,
            Pattern includingPattern,
            Pattern excludingPattern,
            Pattern databasePattern,
            long monitorInterval) {

        CombinedUnawareStreamingSourceFunction function =
                new CombinedUnawareStreamingSourceFunction(
                        catalogLoader,
                        includingPattern,
                        excludingPattern,
                        databasePattern,
                        monitorInterval);
        StreamSource<MultiTableUnawareAppendCompactionTask, CombinedUnawareStreamingSourceFunction>
                sourceOperator = new StreamSource<>(function);
        boolean isParallel = false;
        MultiTableCompactionTaskTypeInfo compactionTaskTypeInfo =
                new MultiTableCompactionTaskTypeInfo();
        return new DataStreamSource<>(
                        env,
                        compactionTaskTypeInfo,
                        sourceOperator,
                        isParallel,
                        name,
                        Boundedness.CONTINUOUS_UNBOUNDED)
                .forceNonParallel();
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (tableScan != null) {
            tableScan.close();
        }
    }
}
