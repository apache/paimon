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
import org.apache.paimon.flink.source.AbstractNonCoordinatedSourceReader;
import org.apache.paimon.flink.source.SimpleSourceSplit;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.regex.Pattern;

import static org.apache.paimon.flink.compact.MultiTableScanBase.ScanResult.FINISHED;
import static org.apache.paimon.flink.compact.MultiTableScanBase.ScanResult.IS_EMPTY;

/**
 * It is responsible for monitoring compactor source in stream mode for the table of unaware bucket.
 */
public class CombinedUnawareStreamingSource
        extends CombinedCompactorSource<MultiTableUnawareAppendCompactionTask> {

    private final long monitorInterval;

    public CombinedUnawareStreamingSource(
            Catalog.Loader catalogLoader,
            Pattern includingPattern,
            Pattern excludingPattern,
            Pattern databasePattern,
            long monitorInterval) {
        super(catalogLoader, includingPattern, excludingPattern, databasePattern, true);
        this.monitorInterval = monitorInterval;
    }

    @Override
    public SourceReader<MultiTableUnawareAppendCompactionTask, SimpleSourceSplit> createReader(
            SourceReaderContext sourceReaderContext) throws Exception {
        return new Reader();
    }

    private class Reader
            extends AbstractNonCoordinatedSourceReader<MultiTableUnawareAppendCompactionTask> {
        private MultiTableScanBase<MultiTableUnawareAppendCompactionTask> tableScan;

        @Override
        public void start() {
            super.start();
            tableScan =
                    new MultiUnawareBucketTableScan(
                            catalogLoader,
                            includingPattern,
                            excludingPattern,
                            databasePattern,
                            isStreaming);
        }

        @Override
        public InputStatus pollNext(
                ReaderOutput<MultiTableUnawareAppendCompactionTask> readerOutput) throws Exception {
            MultiTableScanBase.ScanResult scanResult = tableScan.scanTable(readerOutput);
            if (scanResult == FINISHED) {
                return InputStatus.END_OF_INPUT;
            }
            if (scanResult == IS_EMPTY) {
                Thread.sleep(monitorInterval);
            }
            return InputStatus.MORE_AVAILABLE;
        }

        @Override
        public void close() throws Exception {
            super.close();
            if (tableScan != null) {
                tableScan.close();
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

        CombinedUnawareStreamingSource source =
                new CombinedUnawareStreamingSource(
                        catalogLoader,
                        includingPattern,
                        excludingPattern,
                        databasePattern,
                        monitorInterval);
        MultiTableCompactionTaskTypeInfo compactionTaskTypeInfo =
                new MultiTableCompactionTaskTypeInfo();

        return env.fromSource(
                        source, WatermarkStrategy.noWatermarks(), name, compactionTaskTypeInfo)
                .forceNonParallel();
    }
}
