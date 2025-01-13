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

package org.apache.paimon.flink.source;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.system.CompactBucketsTable;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;

/**
 * Source builder to build a Flink {@link StaticFileStoreSource} or {@link
 * ContinuousFileStoreSource}. This is for dedicated compactor jobs.
 */
public class CompactorSourceBuilder {

    private final String tableIdentifier;
    private final FileStoreTable table;

    private boolean isContinuous = false;
    private StreamExecutionEnvironment env;
    @Nullable private Predicate partitionPredicate = null;
    @Nullable private Duration partitionIdleTime = null;

    public CompactorSourceBuilder(String tableIdentifier, FileStoreTable table) {
        this.tableIdentifier = tableIdentifier;
        this.table = table;
    }

    public CompactorSourceBuilder withContinuousMode(boolean isContinuous) {
        this.isContinuous = isContinuous;
        return this;
    }

    public CompactorSourceBuilder withEnv(StreamExecutionEnvironment env) {
        this.env = env;
        return this;
    }

    public CompactorSourceBuilder withPartitionIdleTime(@Nullable Duration partitionIdleTime) {
        this.partitionIdleTime = partitionIdleTime;
        return this;
    }

    private Source<RowData, ?, ?> buildSource(CompactBucketsTable compactBucketsTable) {

        if (isContinuous) {
            compactBucketsTable = compactBucketsTable.copy(streamingCompactOptions());
            return new ContinuousFileStoreSource(
                    compactBucketsTable.newReadBuilder().withFilter(partitionPredicate),
                    compactBucketsTable.options(),
                    null);
        } else {
            compactBucketsTable = compactBucketsTable.copy(batchCompactOptions());
            ReadBuilder readBuilder =
                    compactBucketsTable.newReadBuilder().withFilter(partitionPredicate);
            Options options = compactBucketsTable.coreOptions().toConfiguration();
            return new StaticFileStoreSource(
                    readBuilder,
                    null,
                    options.get(FlinkConnectorOptions.SCAN_SPLIT_ENUMERATOR_BATCH_SIZE),
                    options.get(FlinkConnectorOptions.SCAN_SPLIT_ENUMERATOR_ASSIGN_MODE));
        }
    }

    public DataStreamSource<RowData> build() {
        if (env == null) {
            throw new IllegalArgumentException("StreamExecutionEnvironment should not be null.");
        }

        CompactBucketsTable compactBucketsTable = new CompactBucketsTable(table, isContinuous);
        RowType produceType = compactBucketsTable.rowType();
        DataStreamSource<RowData> dataStream =
                env.fromSource(
                        buildSource(compactBucketsTable),
                        WatermarkStrategy.noWatermarks(),
                        tableIdentifier + "-compact-source",
                        InternalTypeInfo.of(LogicalTypeConversion.toLogicalType(produceType)));
        if (isContinuous) {
            Preconditions.checkArgument(
                    partitionIdleTime == null, "Streaming mode does not support partitionIdleTime");
        } else if (partitionIdleTime != null) {
            Map<BinaryRow, Long> partitionInfo = getPartitionInfo(compactBucketsTable);
            long historyMilli =
                    LocalDateTime.now()
                            .minus(partitionIdleTime)
                            .atZone(ZoneId.systemDefault())
                            .toInstant()
                            .toEpochMilli();
            SingleOutputStreamOperator<RowData> filterStream =
                    dataStream.filter(
                            rowData -> {
                                BinaryRow partition = deserializeBinaryRow(rowData.getBinary(1));
                                return partitionInfo.get(partition) <= historyMilli;
                            });
            dataStream = new DataStreamSource<>(filterStream);
        }
        Integer parallelism =
                Options.fromMap(table.options()).get(FlinkConnectorOptions.SCAN_PARALLELISM);
        if (parallelism != null) {
            dataStream.setParallelism(parallelism);
        }
        return dataStream;
    }

    private Map<String, String> streamingCompactOptions() {
        // set 'streaming-compact' and remove 'scan.bounded.watermark'
        return new HashMap<String, String>() {
            {
                put(
                        CoreOptions.STREAM_SCAN_MODE.key(),
                        CoreOptions.StreamScanMode.COMPACT_BUCKET_TABLE.getValue());
                put(CoreOptions.SCAN_BOUNDED_WATERMARK.key(), null);
            }
        };
    }

    private Map<String, String> batchCompactOptions() {
        // batch compactor source will compact all current files
        return new HashMap<String, String>() {
            {
                put(CoreOptions.SCAN_TIMESTAMP_MILLIS.key(), null);
                put(CoreOptions.SCAN_FILE_CREATION_TIME_MILLIS.key(), null);
                put(CoreOptions.SCAN_TIMESTAMP.key(), null);
                put(CoreOptions.SCAN_SNAPSHOT_ID.key(), null);
                put(CoreOptions.SCAN_MODE.key(), CoreOptions.StartupMode.LATEST_FULL.toString());
            }
        };
    }

    public CompactorSourceBuilder withPartitionPredicate(@Nullable Predicate partitionPredicate) {
        this.partitionPredicate = partitionPredicate;
        return this;
    }

    private Map<BinaryRow, Long> getPartitionInfo(CompactBucketsTable table) {
        List<PartitionEntry> partitions = table.newSnapshotReader().partitionEntries();

        return partitions.stream()
                .collect(
                        Collectors.toMap(
                                PartitionEntry::partition, PartitionEntry::lastFileCreationTime));
    }
}
