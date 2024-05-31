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
import org.apache.paimon.flink.sink.index.GlobalDynamicBucketSink;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.transformations.LegacySourceTransformation;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.flink.sink.FlinkStreamPartitioner.partition;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * DataStream API for building Flink Sink.
 *
 * @since 0.8
 */
@Public
public class FlinkSinkBuilder {

    private final FileStoreTable table;

    private DataStream<RowData> input;
    @Nullable private Map<String, String> overwritePartition;
    @Nullable private Integer parallelism;
    private Boolean boundedInput = null;

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
                input.map((MapFunction<Row, RowData>) converter::toInternal)
                        .setParallelism(input.getParallelism())
                        .returns(InternalTypeInfo.of(rowType));
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

    /**
     * Set input bounded, if it is bounded, append table sink does not generate a topology for
     * merging small files.
     */
    public FlinkSinkBuilder inputBounded(boolean bounded) {
        this.boundedInput = bounded;
        return this;
    }

    /** Build {@link DataStreamSink}. */
    public DataStreamSink<?> build() {
        DataStream<InternalRow> input = MapToInternalRow.map(this.input, table.rowType());
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

    private DataStreamSink<?> buildDynamicBucketSink(
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

    private DataStreamSink<?> buildForFixedBucket(DataStream<InternalRow> input) {
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
        if (boundedInput == null) {
            boundedInput = !FlinkSink.isStreaming(input);

            if (!boundedInput) {
                List<Transformation<?>> transformations =
                        input.getExecutionEnvironment().getTransformations();
                for (Transformation<?> transformation : transformations) {
                    if ((transformation instanceof LegacySourceTransformation
                                    && ((LegacySourceTransformation<?>) transformation)
                                                    .getBoundedness()
                                            == Boundedness.BOUNDED)
                            || (transformation instanceof SourceTransformation
                                    && ((SourceTransformation<?, ?, ?>) transformation)
                                                    .getBoundedness()
                                            == Boundedness.BOUNDED)) {
                        boundedInput = true;
                        break;
                    }
                }
            }
        }
        return new RowUnawareBucketSink(
                        table, overwritePartition, logSinkFunction, parallelism, boundedInput)
                .sinkFrom(input);
    }
}
