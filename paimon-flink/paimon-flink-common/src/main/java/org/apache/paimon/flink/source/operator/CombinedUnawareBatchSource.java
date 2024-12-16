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
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.compact.MultiTableScanBase;
import org.apache.paimon.flink.compact.MultiUnawareBucketTableScan;
import org.apache.paimon.flink.sink.MultiTableCompactionTaskTypeInfo;
import org.apache.paimon.flink.source.AbstractNonCoordinatedSourceReader;
import org.apache.paimon.flink.source.SimpleSourceSplit;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.compact.MultiTableScanBase.ScanResult.FINISHED;
import static org.apache.paimon.flink.compact.MultiTableScanBase.ScanResult.IS_EMPTY;

/**
 * It is responsible for the batch compactor source of the table with unaware bucket in combined
 * mode.
 */
public class CombinedUnawareBatchSource
        extends CombinedCompactorSource<MultiTableUnawareAppendCompactionTask> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CombinedUnawareBatchSource.class);

    public CombinedUnawareBatchSource(
            Catalog.Loader catalogLoader,
            Pattern includingPattern,
            Pattern excludingPattern,
            Pattern databasePattern) {
        super(catalogLoader, includingPattern, excludingPattern, databasePattern, false);
    }

    @Override
    public SourceReader<MultiTableUnawareAppendCompactionTask, SimpleSourceSplit> createReader(
            SourceReaderContext sourceReaderContext) throws Exception {
        return new Reader();
    }

    private class Reader
            extends AbstractNonCoordinatedSourceReader<MultiTableUnawareAppendCompactionTask> {
        private transient MultiTableScanBase<MultiTableUnawareAppendCompactionTask> tableScan;

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
                // Currently, in the combined mode, there are two scan tasks for the table of two
                // different bucket type (multi bucket & unaware bucket) running concurrently.
                // There will be a situation that there is only one task compaction , therefore this
                // should not be thrown exception here.
                LOGGER.info("No file were collected for the table of unaware-bucket");
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

    public static DataStream<MultiTableUnawareAppendCompactionTask> buildSource(
            StreamExecutionEnvironment env,
            String name,
            Catalog.Loader catalogLoader,
            Pattern includingPattern,
            Pattern excludingPattern,
            Pattern databasePattern,
            @Nullable Duration partitionIdleTime) {
        CombinedUnawareBatchSource combinedUnawareBatchSource =
                new CombinedUnawareBatchSource(
                        catalogLoader, includingPattern, excludingPattern, databasePattern);
        MultiTableCompactionTaskTypeInfo compactionTaskTypeInfo =
                new MultiTableCompactionTaskTypeInfo();

        SingleOutputStreamOperator<MultiTableUnawareAppendCompactionTask> source =
                env.fromSource(
                                combinedUnawareBatchSource,
                                WatermarkStrategy.noWatermarks(),
                                name,
                                compactionTaskTypeInfo)
                        .forceNonParallel();

        if (partitionIdleTime != null) {
            source =
                    source.transform(
                            name,
                            compactionTaskTypeInfo,
                            new MultiUnawareTablesReadOperator(catalogLoader, partitionIdleTime));
        }

        return source;
    }

    private static Long getPartitionInfo(
            Identifier tableIdentifier,
            BinaryRow partition,
            Map<Identifier, Map<BinaryRow, Long>> multiTablesPartitionInfo,
            Catalog catalog) {
        Map<BinaryRow, Long> partitionInfo = multiTablesPartitionInfo.get(tableIdentifier);
        if (partitionInfo == null) {
            try {
                Table table = catalog.getTable(tableIdentifier);
                if (!(table instanceof FileStoreTable)) {
                    LOGGER.error(
                            String.format(
                                    "Only FileStoreTable supports compact action. The table type is '%s'.",
                                    table.getClass().getName()));
                }
                FileStoreTable fileStoreTable = (FileStoreTable) table;
                List<PartitionEntry> partitions =
                        fileStoreTable.newSnapshotReader().partitionEntries();
                partitionInfo =
                        partitions.stream()
                                .collect(
                                        Collectors.toMap(
                                                PartitionEntry::partition,
                                                PartitionEntry::lastFileCreationTime));
                multiTablesPartitionInfo.put(tableIdentifier, partitionInfo);
            } catch (Catalog.TableNotExistException e) {
                LOGGER.error(String.format("table: %s not found.", tableIdentifier.getFullName()));
            }
        }
        return partitionInfo.get(partition);
    }
}
