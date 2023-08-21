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
import org.apache.paimon.CoreOptions.StartupMode;
import org.apache.paimon.CoreOptions.StreamingReadMode;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.Projection;
import org.apache.paimon.flink.log.LogSourceProvider;
import org.apache.paimon.flink.source.align.AlignedContinuousFileStoreSource;
import org.apache.paimon.flink.source.operator.MonitorFunction;
import org.apache.paimon.flink.utils.TableScanUtils;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.util.Optional;

import static org.apache.paimon.CoreOptions.StreamingReadMode.FILE;
import static org.apache.paimon.flink.LogicalTypeConversion.toLogicalType;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Source builder to build a Flink {@link StaticFileStoreSource} or {@link
 * ContinuousFileStoreSource}. This is for normal read/write jobs.
 */
public class FlinkSourceBuilder {

    private final ObjectIdentifier tableIdentifier;
    private final Table table;
    private final Options conf;

    private boolean isContinuous = false;
    private StreamExecutionEnvironment env;
    @Nullable private int[][] projectedFields;
    @Nullable private Predicate predicate;
    @Nullable private LogSourceProvider logSourceProvider;
    @Nullable private Integer parallelism;
    @Nullable private Long limit;
    @Nullable private WatermarkStrategy<RowData> watermarkStrategy;

    public FlinkSourceBuilder(ObjectIdentifier tableIdentifier, Table table) {
        this.tableIdentifier = tableIdentifier;
        this.table = table;
        this.conf = Options.fromMap(table.options());
    }

    public FlinkSourceBuilder withContinuousMode(boolean isContinuous) {
        this.isContinuous = isContinuous;
        return this;
    }

    public FlinkSourceBuilder withEnv(StreamExecutionEnvironment env) {
        this.env = env;
        return this;
    }

    public FlinkSourceBuilder withProjection(int[][] projectedFields) {
        this.projectedFields = projectedFields;
        return this;
    }

    public FlinkSourceBuilder withPredicate(Predicate predicate) {
        this.predicate = predicate;
        return this;
    }

    public FlinkSourceBuilder withLimit(@Nullable Long limit) {
        this.limit = limit;
        return this;
    }

    public FlinkSourceBuilder withLogSourceProvider(LogSourceProvider logSourceProvider) {
        this.logSourceProvider = logSourceProvider;
        return this;
    }

    public FlinkSourceBuilder withParallelism(@Nullable Integer parallelism) {
        this.parallelism = parallelism;
        return this;
    }

    public FlinkSourceBuilder withWatermarkStrategy(
            @Nullable WatermarkStrategy<RowData> watermarkStrategy) {
        this.watermarkStrategy = watermarkStrategy;
        return this;
    }

    private ReadBuilder createReadBuilder() {
        return table.newReadBuilder().withProjection(projectedFields).withFilter(predicate);
    }

    private DataStream<RowData> buildStaticFileSource() {
        Options options = Options.fromMap(table.options());
        return toDataStream(
                new StaticFileStoreSource(
                        createReadBuilder(),
                        limit,
                        options.get(FlinkConnectorOptions.SCAN_SPLIT_ENUMERATOR_BATCH_SIZE),
                        options.get(FlinkConnectorOptions.SCAN_SPLIT_ENUMERATOR_ASSIGN_MODE)));
    }

    private DataStream<RowData> buildContinuousFileSource() {
        return toDataStream(
                new ContinuousFileStoreSource(
                        createReadBuilder(),
                        table.options(),
                        limit,
                        table instanceof FileStoreTable
                                ? ((FileStoreTable) table).bucketMode()
                                : BucketMode.FIXED));
    }

    private DataStream<RowData> buildAlignedContinuousFileSource() {
        assertStreamingConfigurationForAlignMode(env);
        return toDataStream(
                new AlignedContinuousFileStoreSource(
                        createReadBuilder(),
                        table.options(),
                        limit,
                        table instanceof FileStoreTable
                                ? ((FileStoreTable) table).bucketMode()
                                : BucketMode.FIXED));
    }

    private DataStream<RowData> toDataStream(Source<RowData, ?, ?> source) {
        DataStreamSource<RowData> dataStream =
                env.fromSource(
                        source,
                        watermarkStrategy == null
                                ? WatermarkStrategy.noWatermarks()
                                : watermarkStrategy,
                        tableIdentifier.asSummaryString(),
                        produceTypeInfo());
        if (parallelism != null) {
            dataStream.setParallelism(parallelism);
        }
        return dataStream;
    }

    private TypeInformation<RowData> produceTypeInfo() {
        RowType rowType = toLogicalType(table.rowType());
        LogicalType produceType =
                Optional.ofNullable(projectedFields)
                        .map(Projection::of)
                        .map(p -> p.project(rowType))
                        .orElse(rowType);
        return InternalTypeInfo.of(produceType);
    }

    public DataStream<RowData> build() {
        if (env == null) {
            throw new IllegalArgumentException("StreamExecutionEnvironment should not be null.");
        }

        if (isContinuous) {
            TableScanUtils.streamingReadingValidate(table);

            // TODO visit all options through CoreOptions
            StartupMode startupMode = CoreOptions.startupMode(conf);
            StreamingReadMode streamingReadMode = CoreOptions.streamReadType(conf);

            if (logSourceProvider != null && streamingReadMode != FILE) {
                if (startupMode != StartupMode.LATEST_FULL) {
                    return toDataStream(logSourceProvider.createSource(null));
                } else {
                    return toDataStream(
                            HybridSource.<RowData, StaticFileStoreSplitEnumerator>builder(
                                            LogHybridSourceFactory.buildHybridFirstSource(
                                                    table, projectedFields, predicate))
                                    .addSource(
                                            new LogHybridSourceFactory(logSourceProvider),
                                            Boundedness.CONTINUOUS_UNBOUNDED)
                                    .build());
                }
            } else {
                if (conf.get(FlinkConnectorOptions.SOURCE_CHECKPOINT_ALIGN_ENABLED)) {
                    return buildAlignedContinuousFileSource();
                } else if (conf.contains(CoreOptions.CONSUMER_ID)) {
                    return buildContinuousStreamOperator();
                } else {
                    return buildContinuousFileSource();
                }
            }
        } else {
            return buildStaticFileSource();
        }
    }

    private DataStream<RowData> buildContinuousStreamOperator() {
        DataStream<RowData> dataStream;
        if (limit != null) {
            throw new IllegalArgumentException(
                    "Cannot limit streaming source, please use batch execution mode.");
        }
        dataStream =
                MonitorFunction.buildSource(
                        env,
                        tableIdentifier.asSummaryString(),
                        produceTypeInfo(),
                        createReadBuilder(),
                        conf.get(CoreOptions.CONTINUOUS_DISCOVERY_INTERVAL).toMillis(),
                        watermarkStrategy == null);
        if (parallelism != null) {
            dataStream.getTransformation().setParallelism(parallelism);
        }
        if (watermarkStrategy != null) {
            dataStream = dataStream.assignTimestampsAndWatermarks(watermarkStrategy);
        }
        return dataStream;
    }

    public void assertStreamingConfigurationForAlignMode(StreamExecutionEnvironment env) {
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkArgument(
                checkpointConfig.isCheckpointingEnabled(),
                "The align mode of paimon source is only supported when checkpoint enabled. Please set "
                        + ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL.key()
                        + "larger than 0");
        checkArgument(
                checkpointConfig.getMaxConcurrentCheckpoints() == 1,
                "The align mode of paimon source supports at most one ongoing checkpoint at the same time. Please set "
                        + ExecutionCheckpointingOptions.MAX_CONCURRENT_CHECKPOINTS.key()
                        + " to 1");
        checkArgument(
                checkpointConfig.getCheckpointTimeout()
                        > conf.get(FlinkConnectorOptions.SOURCE_CHECKPOINT_ALIGN_TIMEOUT)
                                .toMillis(),
                "The align mode of paimon source requires that the timeout of checkpoint is greater than the timeout of the source's snapshot alignment. Please increase "
                        + ExecutionCheckpointingOptions.CHECKPOINTING_TIMEOUT.key()
                        + " or decrease "
                        + FlinkConnectorOptions.SOURCE_CHECKPOINT_ALIGN_TIMEOUT.key());
        checkArgument(
                !env.getCheckpointConfig().isUnalignedCheckpointsEnabled(),
                "The align mode of paimon source currently does not support unaligned checkpoints. Please set "
                        + ExecutionCheckpointingOptions.ENABLE_UNALIGNED.key()
                        + " to false.");
        checkArgument(
                env.getCheckpointConfig().getCheckpointingMode() == CheckpointingMode.EXACTLY_ONCE,
                "The align mode of paimon source currently only supports EXACTLY_ONCE checkpoint mode. Please set "
                        + ExecutionCheckpointingOptions.CHECKPOINTING_MODE.key()
                        + " to exactly-once");
    }
}
