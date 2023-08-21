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

import org.apache.paimon.flink.utils.JavaTypeInfo;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.EndOfScanException;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamTableScan;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.stream.Collectors;

/** this is a doc. */
public class StreamMonitorFunction extends RichSourceFunction<Tuple2<Split, String>>
        implements CheckpointedFunction, CheckpointListener {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(MonitorFunction.class);

    private final List<ReadBuilder> readBuilders;
    private final long monitorInterval;
    private final boolean emitSnapshotWatermark;

    private volatile boolean isRunning = true;

    private transient Map<StreamTableScan, String> scansMap;
    private transient SourceContext<Tuple2<Split, String>> ctx;

    private transient ListState<Long> checkpointState;
    private transient ListState<Tuple2<Long, Long>> nextSnapshotState;
    private transient TreeMap<Long, Long> nextSnapshotPerCheckpoint;

    public StreamMonitorFunction(
            List<ReadBuilder> readBuilders, long monitorInterval, boolean emitSnapshotWatermark) {
        this.readBuilders = readBuilders;
        this.monitorInterval = monitorInterval;
        this.emitSnapshotWatermark = emitSnapshotWatermark;
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        scansMap = new HashMap<>();
        for (ReadBuilder readBuilder : readBuilders) {
            scansMap.put(readBuilder.newStreamScan(), readBuilder.tableName());
        }

        this.checkpointState =
                context.getOperatorStateStore()
                        .getListState(
                                new ListStateDescriptor<>(
                                        "next-snapshot", LongSerializer.INSTANCE));

        @SuppressWarnings("unchecked")
        final Class<Tuple2<Long, Long>> typedTuple =
                (Class<Tuple2<Long, Long>>) (Class<?>) Tuple2.class;
        this.nextSnapshotState =
                context.getOperatorStateStore()
                        .getListState(
                                new ListStateDescriptor<>(
                                        "next-snapshot-per-checkpoint",
                                        new TupleSerializer<>(
                                                typedTuple,
                                                new TypeSerializer[] {
                                                    LongSerializer.INSTANCE, LongSerializer.INSTANCE
                                                })));

        this.nextSnapshotPerCheckpoint = new TreeMap<>();

        if (context.isRestored()) {
            LOG.info("Restoring state for the {}.", getClass().getSimpleName());

            List<Long> retrievedStates = new ArrayList<>();
            for (Long entry : this.checkpointState.get()) {
                retrievedStates.add(entry);
            }

            // given that the parallelism of the function is 1, we can only have 1 retrieved items.

            Preconditions.checkArgument(
                    retrievedStates.size() <= 1,
                    getClass().getSimpleName() + " retrieved invalid state.");

            if (retrievedStates.size() == 1) {
                for (StreamTableScan scan : this.scansMap.keySet()) {
                    scan.restore(retrievedStates.get(0));
                }
            }

            for (Tuple2<Long, Long> tuple2 : nextSnapshotState.get()) {
                nextSnapshotPerCheckpoint.put(tuple2.f0, tuple2.f1);
            }
        } else {
            LOG.info("No state to restore for the {}.", getClass().getSimpleName());
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
        this.checkpointState.clear();
        Long nextSnapshot = this.scansMap.keySet().iterator().next().checkpoint();
        if (nextSnapshot != null) {
            this.checkpointState.add(nextSnapshot);
            this.nextSnapshotPerCheckpoint.put(ctx.getCheckpointId(), nextSnapshot);
        }

        List<Tuple2<Long, Long>> nextSnapshots = new ArrayList<>();
        this.nextSnapshotPerCheckpoint.forEach((k, v) -> nextSnapshots.add(new Tuple2<>(k, v)));
        this.nextSnapshotState.update(nextSnapshots);

        if (LOG.isDebugEnabled()) {
            LOG.debug("{} checkpoint {}.", getClass().getSimpleName(), nextSnapshot);
        }
    }

    @SuppressWarnings("BusyWait")
    @Override
    public void run(SourceContext<Tuple2<Split, String>> ctx) throws Exception {
        this.ctx = ctx;
        while (isRunning) {
            boolean isEmpty;
            synchronized (ctx.getCheckpointLock()) {
                if (!isRunning) {
                    return;
                }
                try {
                    List<Tuple2<Split, String>> splits = new ArrayList<>();
                    for (StreamTableScan scan : scansMap.keySet()) {
                        splits.addAll(
                                scan.plan().splits().stream()
                                        .map(split -> new Tuple2<>(split, scansMap.get(scan)))
                                        .collect(Collectors.toList()));
                    }
                    isEmpty = splits.isEmpty();
                    splits.forEach(ctx::collect);

                    if (emitSnapshotWatermark) {
                        Long watermark = scansMap.keySet().iterator().next().watermark();
                        if (watermark != null) {
                            ctx.emitWatermark(new Watermark(watermark));
                        }
                    }
                } catch (EndOfScanException esf) {
                    LOG.info("Catching EndOfStreamException, the stream is finished.");
                    return;
                }
            }

            if (isEmpty) {
                Thread.sleep(monitorInterval);
            }
        }
    }

    // 对于 stream 和 batch 两种类型可以创建两种不同类型的 MonitorFunction
    // 对于 batch 类型，run()方法只需执行一次即可，所以就不需要 while(true) 的循环
    // 对于 stream 类型，run()方法中需要持续监测数据，所以需要 while(true) 的循环

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        NavigableMap<Long, Long> nextSnapshots =
                nextSnapshotPerCheckpoint.headMap(checkpointId, true);
        OptionalLong max = nextSnapshots.values().stream().mapToLong(Long::longValue).max();
        max.ifPresent(scansMap.keySet().iterator().next()::notifyCheckpointComplete);
        nextSnapshots.clear();
    }

    @Override
    public void cancel() {
        // this is to cover the case where cancel() is called before the run()
        if (ctx != null) {
            synchronized (ctx.getCheckpointLock()) {
                isRunning = false;
            }
        } else {
            isRunning = false;
        }
    }

    public static DataStream<RowData> buildSource(
            StreamExecutionEnvironment env,
            String name,
            TypeInformation<RowData> typeInfo,
            List<ReadBuilder> readBuilders,
            long monitorInterval,
            boolean emitSnapshotWatermark) {

        StreamMonitorFunction function =
                new StreamMonitorFunction(readBuilders, monitorInterval, emitSnapshotWatermark);
        StreamSource<Tuple2<Split, String>, ?> sourceOperator = new StreamSource<>(function);
        boolean isParallel = false;
        TupleTypeInfo<Tuple2<Split, String>> tupleTypeInfo =
                new TupleTypeInfo<>(
                        new JavaTypeInfo<>(Split.class), BasicTypeInfo.STRING_TYPE_INFO);
        return new DataStreamSource<>(
                        env,
                        tupleTypeInfo,
                        sourceOperator,
                        isParallel,
                        name + "-Monitor",
                        Boundedness.CONTINUOUS_UNBOUNDED)
                .forceNonParallel()
                .partitionCustom(
                        (key, numPartitions) -> key % numPartitions,
                        split -> ((DataSplit) split.f0).bucket())
                .transform(name + "-Reader", typeInfo, new ReadOperator2(readBuilders));
    }
}
