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
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.system.BucketsTable;
import org.apache.paimon.types.RowType;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.partition.PartitionPredicate.createPartitionPredicate;

/**
 * Source builder to build a Flink {@link StaticFileStoreSource} or {@link
 * ContinuousFileStoreSource}. This is for dedicated compactor jobs.
 */
public class CompactorSourceBuilder {

    private final String tableIdentifier;
    private final FileStoreTable table;

    private boolean isContinuous = false;
    private StreamExecutionEnvironment env;
    @Nullable private List<Map<String, String>> specifiedPartitions = null;

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

    public CompactorSourceBuilder withPartition(Map<String, String> partition) {
        return withPartitions(Collections.singletonList(partition));
    }

    public CompactorSourceBuilder withPartitions(List<Map<String, String>> partitions) {
        this.specifiedPartitions = partitions;
        return this;
    }

    private Source<RowData, ?, ?> buildSource(BucketsTable bucketsTable) {
        Predicate partitionPredicate = null;
        if (specifiedPartitions != null) {
            // This predicate is based on the row type of the original table, not bucket table.
            // Because TableScan in BucketsTable is the same with FileStoreTable,
            // and partition filter is done by scan.
            partitionPredicate =
                    PredicateBuilder.or(
                            specifiedPartitions.stream()
                                    .map(
                                            p ->
                                                    createPartitionPredicate(
                                                            p,
                                                            table.rowType(),
                                                            table.coreOptions()
                                                                    .partitionDefaultName()))
                                    .toArray(Predicate[]::new));
        }

        if (isContinuous) {
            bucketsTable = bucketsTable.copy(streamingCompactOptions());
            return new ContinuousFileStoreSource(
                    bucketsTable.newReadBuilder().withFilter(partitionPredicate),
                    bucketsTable.options(),
                    null);
        } else {
            bucketsTable = bucketsTable.copy(batchCompactOptions());
            ReadBuilder readBuilder = bucketsTable.newReadBuilder().withFilter(partitionPredicate);
            Options options = bucketsTable.coreOptions().toConfiguration();
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

        BucketsTable bucketsTable = new BucketsTable(table, isContinuous);
        RowType produceType = bucketsTable.rowType();
        DataStreamSource<RowData> dataStream =
                env.fromSource(
                        buildSource(bucketsTable),
                        WatermarkStrategy.noWatermarks(),
                        tableIdentifier + "-compact-source",
                        InternalTypeInfo.of(LogicalTypeConversion.toLogicalType(produceType)));
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
}
