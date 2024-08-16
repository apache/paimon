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
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.sink.ChannelComputer;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.EndOfScanException;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamTableScan;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.OptionalLong;
import java.util.TreeMap;

import static org.apache.paimon.table.BucketMode.BUCKET_UNAWARE;

/**
 * This is the single (non-parallel) monitoring task, it is responsible for:
 *
 * <ol>
 *   <li>Monitoring snapshots of the Paimon table.
 *   <li>Creating the {@link Split splits} corresponding to the incremental files
 *   <li>Assigning them to downstream tasks for further processing.
 * </ol>
 *
 * <p>The splits to be read are forwarded to the downstream {@link ReadOperator} which can have
 * parallelism greater than one.
 *
 * <p>Currently, there are two features that rely on this monitor:
 *
 * <ol>
 *   <li>Consumer-id: rely on this function to do aligned snapshot consumption, and ensure that all
 *       data in a snapshot is consumed within each checkpoint.
 *   <li>Snapshot-watermark: when there is no watermark definition, the default Paimon table will
 *       pass the watermark recorded in the snapshot.
 * </ol>
 */
public class MonitorFunction extends RichSourceFunction<Split>
        implements CheckpointedFunction, CheckpointListener {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(MonitorFunction.class);

    private final ReadBuilder readBuilder;
    private final long monitorInterval;
    private final boolean emitSnapshotWatermark;

    private volatile boolean isRunning = true;

    private transient StreamTableScan scan;
    private transient SourceContext<Split> ctx;

    private transient ListState<Long> checkpointState;
    private transient ListState<Tuple2<Long, Long>> nextSnapshotState;
    private transient TreeMap<Long, Long> nextSnapshotPerCheckpoint;

    public MonitorFunction(
            ReadBuilder readBuilder, long monitorInterval, boolean emitSnapshotWatermark) {
        this.readBuilder = readBuilder;
        this.monitorInterval = monitorInterval;
        this.emitSnapshotWatermark = emitSnapshotWatermark;
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        this.scan = readBuilder.newStreamScan();

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
                this.scan.restore(retrievedStates.get(0));
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
        Long nextSnapshot = this.scan.checkpoint();
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
    public void run(SourceContext<Split> ctx) throws Exception {
        this.ctx = ctx;
        while (isRunning) {
            boolean isEmpty;
            synchronized (ctx.getCheckpointLock()) {
                if (!isRunning) {
                    return;
                }
                try {
                    List<Split> splits = scan.plan().splits();
                    isEmpty = splits.isEmpty();
                    splits.forEach(ctx::collect);

                    if (emitSnapshotWatermark) {
                        Long watermark = scan.watermark();
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

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        NavigableMap<Long, Long> nextSnapshots =
                nextSnapshotPerCheckpoint.headMap(checkpointId, true);
        OptionalLong max = nextSnapshots.values().stream().mapToLong(Long::longValue).max();
        max.ifPresent(scan::notifyCheckpointComplete);
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
            ReadBuilder readBuilder,
            long monitorInterval,
            boolean emitSnapshotWatermark,
            boolean shuffleBucketWithPartition,
            BucketMode bucketMode) {
        SingleOutputStreamOperator<Split> singleOutputStreamOperator =
                env.addSource(
                                new MonitorFunction(
                                        readBuilder, monitorInterval, emitSnapshotWatermark),
                                name + "-Monitor",
                                new JavaTypeInfo<>(Split.class))
                        .forceNonParallel();

        DataStream<Split> sourceDataStream =
                bucketMode == BUCKET_UNAWARE
                        ? shuffleUnwareBucket(singleOutputStreamOperator)
                        : shuffleNonUnwareBucket(
                                singleOutputStreamOperator, shuffleBucketWithPartition);

        return sourceDataStream.transform(
                name + "-Reader", typeInfo, new ReadOperator(readBuilder));
    }

    private static DataStream<Split> shuffleUnwareBucket(
            SingleOutputStreamOperator<Split> singleOutputStreamOperator) {
        return singleOutputStreamOperator.rebalance();
    }

    private static DataStream<Split> shuffleNonUnwareBucket(
            SingleOutputStreamOperator<Split> singleOutputStreamOperator,
            boolean shuffleBucketWithPartition) {
        return singleOutputStreamOperator.partitionCustom(
                (key, numPartitions) -> {
                    if (shuffleBucketWithPartition) {
                        return ChannelComputer.select(key.f0, key.f1, numPartitions);
                    }
                    return ChannelComputer.select(key.f1, numPartitions);
                },
                split -> {
                    DataSplit dataSplit = (DataSplit) split;
                    return Tuple2.of(dataSplit.partition(), dataSplit.bucket());
                });
    }
}
