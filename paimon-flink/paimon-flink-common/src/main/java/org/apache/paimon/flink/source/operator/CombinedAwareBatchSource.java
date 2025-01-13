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

import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.flink.compact.MultiAwareBucketTableScan;
import org.apache.paimon.flink.compact.MultiTableScanBase;
import org.apache.paimon.flink.source.AbstractNonCoordinatedSourceReader;
import org.apache.paimon.flink.source.SimpleSourceSplit;
import org.apache.paimon.flink.utils.JavaTypeInfo;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.regex.Pattern;

import static org.apache.paimon.flink.compact.MultiTableScanBase.ScanResult.FINISHED;
import static org.apache.paimon.flink.compact.MultiTableScanBase.ScanResult.IS_EMPTY;

/** It is responsible for monitoring compactor source of aware bucket table in batch mode. */
public class CombinedAwareBatchSource extends CombinedCompactorSource<Tuple2<Split, String>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CombinedAwareBatchSource.class);

    public CombinedAwareBatchSource(
            CatalogLoader catalogLoader,
            Pattern includingPattern,
            Pattern excludingPattern,
            Pattern databasePattern) {
        super(catalogLoader, includingPattern, excludingPattern, databasePattern, false);
    }

    @Override
    public SourceReader<Tuple2<Split, String>, SimpleSourceSplit> createReader(
            SourceReaderContext sourceReaderContext) throws Exception {
        return new Reader();
    }

    private class Reader extends AbstractNonCoordinatedSourceReader<Tuple2<Split, String>> {
        private MultiTableScanBase<Tuple2<Split, String>> tableScan;

        @Override
        public void start() {
            super.start();
            tableScan =
                    new MultiAwareBucketTableScan(
                            catalogLoader,
                            includingPattern,
                            excludingPattern,
                            databasePattern,
                            isStreaming);
        }

        @Override
        public InputStatus pollNext(ReaderOutput<Tuple2<Split, String>> readerOutput)
                throws Exception {
            MultiTableScanBase.ScanResult scanResult = tableScan.scanTable(readerOutput);
            if (scanResult == FINISHED) {
                return InputStatus.END_OF_INPUT;
            }
            if (scanResult == IS_EMPTY) {
                // Currently, in the combined mode, there are two scan tasks for the table of two
                // different bucket type (multi bucket & unaware bucket) running concurrently.
                // There will be a situation that there is only one task compaction , therefore this
                // should not be thrown exception here.
                LOGGER.info("No file were collected for the table of aware-bucket");
            }
            return InputStatus.END_OF_INPUT;
        }

        @Override
        public void close() throws Exception {
            super.close();
            if (tableScan != null) {
                tableScan.close();
            }
        }
    }

    public static DataStream<RowData> buildSource(
            StreamExecutionEnvironment env,
            String name,
            TypeInformation<RowData> typeInfo,
            CatalogLoader catalogLoader,
            Pattern includingPattern,
            Pattern excludingPattern,
            Pattern databasePattern,
            Duration partitionIdleTime) {
        CombinedAwareBatchSource source =
                new CombinedAwareBatchSource(
                        catalogLoader, includingPattern, excludingPattern, databasePattern);
        TupleTypeInfo<Tuple2<Split, String>> tupleTypeInfo =
                new TupleTypeInfo<>(
                        new JavaTypeInfo<>(Split.class), BasicTypeInfo.STRING_TYPE_INFO);

        return env.fromSource(source, WatermarkStrategy.noWatermarks(), name, tupleTypeInfo)
                .forceNonParallel()
                .partitionCustom(
                        (key, numPartitions) -> key % numPartitions,
                        split -> ((DataSplit) split.f0).bucket())
                .transform(
                        name,
                        typeInfo,
                        new MultiTablesReadOperator(catalogLoader, false, partitionIdleTime));
    }
}
