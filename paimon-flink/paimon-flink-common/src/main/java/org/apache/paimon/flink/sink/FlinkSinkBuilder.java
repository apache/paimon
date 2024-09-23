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

package org.apache.paimon.flink.sink;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.FlinkRowWrapper;
import org.apache.paimon.flink.sink.index.GlobalDynamicBucketSink;
import org.apache.paimon.flink.sorter.TableSortInfo;
import org.apache.paimon.flink.sorter.TableSorter;
import org.apache.paimon.flink.sorter.TableSorter.OrderType;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.flink.FlinkConnectorOptions.CLUSTERING_SAMPLE_FACTOR;
import static org.apache.paimon.flink.FlinkConnectorOptions.CLUSTERING_STRATEGY;
import static org.apache.paimon.flink.FlinkConnectorOptions.MIN_CLUSTERING_SAMPLE_FACTOR;
import static org.apache.paimon.flink.sink.FlinkStreamPartitioner.partition;
import static org.apache.paimon.flink.sorter.TableSorter.OrderType.HILBERT;
import static org.apache.paimon.flink.sorter.TableSorter.OrderType.ORDER;
import static org.apache.paimon.flink.sorter.TableSorter.OrderType.ZORDER;
import static org.apache.paimon.table.BucketMode.BUCKET_UNAWARE;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/**
 * DataStream API for building Flink Sink.
 *
 * @since 0.8
 */
@Public
public class FlinkSinkBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkSinkBuilder.class);

    protected final FileStoreTable table;

    private DataStream<RowData> input;
    @Nullable protected Map<String, String> overwritePartition;
    @Nullable protected Integer parallelism;
    @Nullable private TableSortInfo tableSortInfo;

    // ============== for extension ==============

    protected boolean compactSink = false;
    @Nullable protected LogSinkFunction logSinkFunction;

    public FlinkSinkBuilder(Table table) {
        if (!(table instanceof FileStoreTable)) {
            throw new UnsupportedOperationException("Unsupported table type: " + table);
        }
        this.table = (FileStoreTable) table;
    }

    /**
     * From {@link DataStream} with {@link Row}, need to provide a {@link DataType} for builder to
     * convert those {@link Row}s to a {@link RowData} DataStream.
     */
    public FlinkSinkBuilder forRow(DataStream<Row> input, DataType rowDataType) {
        RowType rowType = (RowType) rowDataType.getLogicalType();
        DataType[] fieldDataTypes = rowDataType.getChildren().toArray(new DataType[0]);

        DataFormatConverters.RowConverter converter =
                new DataFormatConverters.RowConverter(fieldDataTypes);
        this.input =
                input.transform(
                                "Map",
                                InternalTypeInfo.of(rowType),
                                new StreamMapWithForwardingRecordAttributes<>(
                                        (MapFunction<Row, RowData>) converter::toInternal))
                        .setParallelism(input.getParallelism());
        return this;
    }

    /** From {@link DataStream} with {@link RowData}. */
    public FlinkSinkBuilder forRowData(DataStream<RowData> input) {
        this.input = input;
        return this;
    }

    /** INSERT OVERWRITE. */
    public FlinkSinkBuilder overwrite() {
        return overwrite(new HashMap<>());
    }

    /** INSERT OVERWRITE PARTITION (...). */
    public FlinkSinkBuilder overwrite(Map<String, String> overwritePartition) {
        this.overwritePartition = overwritePartition;
        return this;
    }

    /** Set sink parallelism. */
    public FlinkSinkBuilder parallelism(int parallelism) {
        this.parallelism = parallelism;
        return this;
    }

    /** Clustering the input data if possible. */
    public FlinkSinkBuilder clusteringIfPossible(
            String clusteringColumns,
            String clusteringStrategy,
            boolean sortInCluster,
            int sampleFactor) {
        // The clustering will be skipped if the clustering columns are empty or the execution
        // mode is STREAMING or the table type is illegal.
        if (clusteringColumns == null || clusteringColumns.isEmpty()) {
            return this;
        }
        checkState(input != null, "The input stream should be specified earlier.");
        if (FlinkSink.isStreaming(input) || !table.bucketMode().equals(BUCKET_UNAWARE)) {
            LOG.warn(
                    "Clustering is enabled; however, it has been skipped as "
                            + "it only supports the bucket unaware table without primary keys and "
                            + "BATCH execution mode.");
            return this;
        }
        // If the clustering is not skipped, check the clustering column names and sample
        // factor value.
        List<String> columns = Arrays.asList(clusteringColumns.split(","));
        List<String> fieldNames = table.schema().fieldNames();
        checkState(
                new HashSet<>(fieldNames).containsAll(new HashSet<>(columns)),
                String.format(
                        "Field names %s should contains all clustering column names %s.",
                        fieldNames, columns));
        checkState(
                sampleFactor >= MIN_CLUSTERING_SAMPLE_FACTOR,
                "The minimum allowed "
                        + CLUSTERING_SAMPLE_FACTOR.key()
                        + " is "
                        + MIN_CLUSTERING_SAMPLE_FACTOR
                        + ".");
        TableSortInfo.Builder sortInfoBuilder = new TableSortInfo.Builder();
        if (clusteringStrategy.equals(CLUSTERING_STRATEGY.defaultValue())) {
            if (columns.size() == 1) {
                sortInfoBuilder.setSortStrategy(ORDER);
            } else if (columns.size() < 5) {
                sortInfoBuilder.setSortStrategy(ZORDER);
            } else {
                sortInfoBuilder.setSortStrategy(HILBERT);
            }
        } else {
            sortInfoBuilder.setSortStrategy(OrderType.of(clusteringStrategy));
        }
        int upstreamParallelism = input.getParallelism();
        String sinkParallelismValue =
                table.options().get(FlinkConnectorOptions.SINK_PARALLELISM.key());
        int sinkParallelism =
                sinkParallelismValue == null
                        ? upstreamParallelism
                        : Integer.parseInt(sinkParallelismValue);
        sortInfoBuilder
                .setSortColumns(columns)
                .setSortInCluster(sortInCluster)
                .setSinkParallelism(sinkParallelism);
        int globalSampleSize = sinkParallelism * sampleFactor;
        // If the adaptive scheduler is not enabled, the local sample size is determined by the
        // division of global sample size by the upstream parallelism, which limits total
        // received data of global sample node. If the adaptive scheduler is enabled, the
        // local sample size will equal to sinkParallelism * minimum sample factor.
        int localSampleSize =
                upstreamParallelism > 0
                        ? Math.max(sampleFactor, globalSampleSize / upstreamParallelism)
                        : sinkParallelism * MIN_CLUSTERING_SAMPLE_FACTOR;
        this.tableSortInfo =
                sortInfoBuilder
                        .setRangeNumber(sinkParallelism)
                        .setGlobalSampleSize(globalSampleSize)
                        .setLocalSampleSize(localSampleSize)
                        .build();
        return this;
    }

    /** Build {@link DataStreamSink}. */
    public DataStreamSink<?> build() {
        input = trySortInput(input);
        DataStream<InternalRow> input = mapToInternalRow(this.input, table.rowType());
        if (table.coreOptions().localMergeEnabled() && table.schema().primaryKeys().size() > 0) {
            input =
                    input.forward()
                            .transform(
                                    "local merge",
                                    input.getType(),
                                    new LocalMergeOperator(table.schema()))
                            .setParallelism(input.getParallelism());
        }

        BucketMode bucketMode = table.bucketMode();
        switch (bucketMode) {
            case HASH_FIXED:
                return buildForFixedBucket(input);
            case HASH_DYNAMIC:
                return buildDynamicBucketSink(input, false);
            case CROSS_PARTITION:
                return buildDynamicBucketSink(input, true);
            case BUCKET_UNAWARE:
                return buildUnawareBucketSink(input);
            default:
                throw new UnsupportedOperationException("Unsupported bucket mode: " + bucketMode);
        }
    }

    protected DataStream<InternalRow> mapToInternalRow(
            DataStream<RowData> input, org.apache.paimon.types.RowType rowType) {
        return input.transform(
                        "Map",
                        org.apache.paimon.flink.utils.InternalTypeInfo.fromRowType(rowType),
                        new StreamMapWithForwardingRecordAttributes<>(
                                (MapFunction<RowData, InternalRow>) FlinkRowWrapper::new))
                .setParallelism(input.getParallelism());
    }

    protected DataStreamSink<?> buildDynamicBucketSink(
            DataStream<InternalRow> input, boolean globalIndex) {
        checkArgument(logSinkFunction == null, "Dynamic bucket mode can not work with log system.");
        return compactSink && !globalIndex
                // todo support global index sort compact
                ? new DynamicBucketCompactSink(table, overwritePartition).build(input, parallelism)
                : globalIndex
                        ? new GlobalDynamicBucketSink(table, overwritePartition)
                                .build(input, parallelism)
                        : new RowDynamicBucketSink(table, overwritePartition)
                                .build(input, parallelism);
    }

    protected DataStreamSink<?> buildForFixedBucket(DataStream<InternalRow> input) {
        DataStream<InternalRow> partitioned =
                partition(
                        input,
                        new RowDataChannelComputer(table.schema(), logSinkFunction != null),
                        parallelism);
        FixedBucketSink sink = new FixedBucketSink(table, overwritePartition, logSinkFunction);
        return sink.sinkFrom(partitioned);
    }

    private DataStreamSink<?> buildUnawareBucketSink(DataStream<InternalRow> input) {
        checkArgument(
                table.primaryKeys().isEmpty(),
                "Unaware bucket mode only works with append-only table for now.");
        return new RowUnawareBucketSink(table, overwritePartition, logSinkFunction, parallelism)
                .sinkFrom(input);
    }

    private DataStream<RowData> trySortInput(DataStream<RowData> input) {
        if (tableSortInfo != null) {
            TableSorter sorter =
                    TableSorter.getSorter(
                            input.getExecutionEnvironment(), input, table, tableSortInfo);
            return sorter.sort();
        }
        return input;
    }
}
