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
import org.apache.paimon.flink.sink.FlinkSink;
import org.apache.paimon.flink.source.align.AlignedContinuousFileStoreSource;
import org.apache.paimon.flink.source.operator.MonitorFunction;
import org.apache.paimon.flink.utils.TableScanUtils;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.utils.StringUtils;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;
import static org.apache.paimon.CoreOptions.StreamingReadMode.FILE;
import static org.apache.paimon.flink.FlinkConnectorOptions.SOURCE_OPERATOR_UID_SUFFIX;
import static org.apache.paimon.flink.FlinkConnectorOptions.generateCustomUid;
import static org.apache.paimon.flink.LogicalTypeConversion.toLogicalType;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/**
 * DataStream API for building Flink Source.
 *
 * @since 0.8
 */
public class FlinkSourceBuilder {
    private static final String SOURCE_NAME = "Source";

    private final Table table;
    private final Options conf;
    private final BucketMode bucketMode;
    private String sourceName;
    private Boolean sourceBounded;
    private StreamExecutionEnvironment env;
    @Nullable private int[][] projectedFields;
    @Nullable private Predicate predicate;
    @Nullable private LogSourceProvider logSourceProvider;
    @Nullable private Integer parallelism;
    @Nullable private Long limit;
    @Nullable private WatermarkStrategy<RowData> watermarkStrategy;
    @Nullable private DynamicPartitionFilteringInfo dynamicPartitionFilteringInfo;

    public FlinkSourceBuilder(Table table) {
        this.table = table;
        this.bucketMode =
                table instanceof FileStoreTable
                        ? ((FileStoreTable) table).bucketMode()
                        : BucketMode.HASH_FIXED;
        this.sourceName = table.name();
        this.conf = Options.fromMap(table.options());
    }

    public FlinkSourceBuilder env(StreamExecutionEnvironment env) {
        this.env = env;
        if (sourceBounded == null) {
            sourceBounded = !FlinkSink.isStreaming(env);
        }
        return this;
    }

    public FlinkSourceBuilder sourceName(String name) {
        this.sourceName = name;
        return this;
    }

    public FlinkSourceBuilder sourceBounded(boolean bounded) {
        this.sourceBounded = bounded;
        return this;
    }

    public FlinkSourceBuilder projection(int[] projectedFields) {
        return projection(Projection.of(projectedFields).toNestedIndexes());
    }

    public FlinkSourceBuilder projection(int[][] projectedFields) {
        this.projectedFields = projectedFields;
        return this;
    }

    public FlinkSourceBuilder predicate(Predicate predicate) {
        this.predicate = predicate;
        return this;
    }

    public FlinkSourceBuilder limit(@Nullable Long limit) {
        this.limit = limit;
        return this;
    }

    public FlinkSourceBuilder sourceParallelism(@Nullable Integer parallelism) {
        this.parallelism = parallelism;
        return this;
    }

    public FlinkSourceBuilder watermarkStrategy(
            @Nullable WatermarkStrategy<RowData> watermarkStrategy) {
        this.watermarkStrategy = watermarkStrategy;
        return this;
    }

    public FlinkSourceBuilder dynamicPartitionFilteringFields(
            List<String> dynamicPartitionFilteringFields) {
        if (dynamicPartitionFilteringFields != null && !dynamicPartitionFilteringFields.isEmpty()) {
            checkState(
                    table instanceof FileStoreTable,
                    "Only Paimon FileStoreTable supports dynamic filtering but get %s.",
                    table.getClass().getName());

            this.dynamicPartitionFilteringInfo =
                    new DynamicPartitionFilteringInfo(
                            ((FileStoreTable) table).schema().logicalPartitionType(),
                            dynamicPartitionFilteringFields);
        }
        return this;
    }

    @Deprecated
    FlinkSourceBuilder logSourceProvider(LogSourceProvider logSourceProvider) {
        this.logSourceProvider = logSourceProvider;
        return this;
    }

    private ReadBuilder createReadBuilder() {
        ReadBuilder readBuilder =
                table.newReadBuilder().withProjection(projectedFields).withFilter(predicate);
        if (limit != null) {
            readBuilder.withLimit(limit.intValue());
        }
        return readBuilder;
    }

    private DataStream<RowData> buildStaticFileSource() {
        Options options = Options.fromMap(table.options());
        return toDataStream(
                new StaticFileStoreSource(
                        createReadBuilder(),
                        limit,
                        options.get(FlinkConnectorOptions.SCAN_SPLIT_ENUMERATOR_BATCH_SIZE),
                        options.get(FlinkConnectorOptions.SCAN_SPLIT_ENUMERATOR_ASSIGN_MODE),
                        dynamicPartitionFilteringInfo));
    }

    private DataStream<RowData> buildContinuousFileSource() {
        return toDataStream(
                new ContinuousFileStoreSource(
                        createReadBuilder(), table.options(), limit, bucketMode));
    }

    private DataStream<RowData> buildAlignedContinuousFileSource() {
        assertStreamingConfigurationForAlignMode(env);
        return toDataStream(
                new AlignedContinuousFileStoreSource(
                        createReadBuilder(), table.options(), limit, bucketMode));
    }

    private DataStream<RowData> toDataStream(Source<RowData, ?, ?> source) {
        DataStreamSource<RowData> dataStream =
                env.fromSource(
                        source,
                        watermarkStrategy == null
                                ? WatermarkStrategy.noWatermarks()
                                : watermarkStrategy,
                        sourceName,
                        produceTypeInfo());

        String uidSuffix = table.options().get(SOURCE_OPERATOR_UID_SUFFIX.key());
        if (!StringUtils.isNullOrWhitespaceOnly(uidSuffix)) {
            dataStream =
                    (DataStreamSource<RowData>)
                            dataStream.uid(generateCustomUid(SOURCE_NAME, table.name(), uidSuffix));
        }

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

    /** Build source {@link DataStream} with {@link RowData}. */
    public DataStream<Row> buildForRow() {
        DataType rowType = fromLogicalToDataType(toLogicalType(table.rowType()));
        DataType[] fieldDataTypes = rowType.getChildren().toArray(new DataType[0]);

        DataFormatConverters.RowConverter converter =
                new DataFormatConverters.RowConverter(fieldDataTypes);
        DataStream<RowData> source = build();
        return source.map((MapFunction<RowData, Row>) converter::toExternal)
                .setParallelism(source.getParallelism())
                .returns(ExternalTypeInfo.of(rowType));
    }

    /** Build source {@link DataStream} with {@link RowData}. */
    public DataStream<RowData> build() {
        if (env == null) {
            throw new IllegalArgumentException("StreamExecutionEnvironment should not be null.");
        }
        if (conf.contains(CoreOptions.CONSUMER_ID)
                && !conf.contains(CoreOptions.CONSUMER_EXPIRATION_TIME)) {
            throw new IllegalArgumentException(
                    "consumer.expiration-time should be specified when using consumer-id.");
        }

        if (sourceBounded) {
            return buildStaticFileSource();
        }
        TableScanUtils.streamingReadingValidate(table);

        // TODO visit all options through CoreOptions
        StartupMode startupMode = CoreOptions.startupMode(conf);
        StreamingReadMode streamingReadMode = CoreOptions.streamReadType(conf);

        if (logSourceProvider != null && streamingReadMode != FILE) {
            logSourceProvider.preCreateSource();
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
            } else if (conf.contains(CoreOptions.CONSUMER_ID)
                    && conf.get(CoreOptions.CONSUMER_CONSISTENCY_MODE)
                            == CoreOptions.ConsumerMode.EXACTLY_ONCE) {
                return buildContinuousStreamOperator();
            } else {
                return buildContinuousFileSource();
            }
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
                        sourceName,
                        produceTypeInfo(),
                        createReadBuilder(),
                        conf.get(CoreOptions.CONTINUOUS_DISCOVERY_INTERVAL).toMillis(),
                        watermarkStrategy == null,
                        conf.get(
                                FlinkConnectorOptions.STREAMING_READ_SHUFFLE_BUCKET_WITH_PARTITION),
                        bucketMode);
        if (parallelism != null) {
            dataStream.getTransformation().setParallelism(parallelism);
        }
        if (watermarkStrategy != null) {
            dataStream = dataStream.assignTimestampsAndWatermarks(watermarkStrategy);
        }
        return dataStream;
    }

    private void assertStreamingConfigurationForAlignMode(StreamExecutionEnvironment env) {
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
