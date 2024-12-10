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

import org.apache.paimon.flink.source.AbstractNonCoordinatedSource;
import org.apache.paimon.flink.source.AbstractNonCoordinatedSourceReader;
import org.apache.paimon.flink.source.SimpleSourceSplit;
import org.apache.paimon.flink.utils.JavaTypeInfo;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.sink.ChannelComputer;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.EndOfScanException;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamTableScan;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

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
 *   <li>Consumer-id: rely on this source to do aligned snapshot consumption, and ensure that all
 *       data in a snapshot is consumed within each checkpoint.
 *   <li>Snapshot-watermark: when there is no watermark definition, the default Paimon table will
 *       pass the watermark recorded in the snapshot.
 * </ol>
 */
public class MonitorSource extends AbstractNonCoordinatedSource<Split> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(MonitorSource.class);

    private final ReadBuilder readBuilder;
    private final long monitorInterval;
    private final boolean emitSnapshotWatermark;

    public MonitorSource(
            ReadBuilder readBuilder, long monitorInterval, boolean emitSnapshotWatermark) {
        this.readBuilder = readBuilder;
        this.monitorInterval = monitorInterval;
        this.emitSnapshotWatermark = emitSnapshotWatermark;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<Split, SimpleSourceSplit> createReader(
            SourceReaderContext sourceReaderContext) throws Exception {
        return new Reader();
    }

    private class Reader extends AbstractNonCoordinatedSourceReader<Split> {
        private final StreamTableScan scan = readBuilder.newStreamScan();
        private final SplitState<Long> checkpointState =
                new SplitState<>("next-snapshot", x -> Long.toString(x), Long::parseLong);
        private final SplitState<Tuple2<Long, Long>> nextSnapshotState =
                new SplitState<>(
                        "next-snapshot-per-checkpoint",
                        x -> x.f0 + ":" + x.f1,
                        x ->
                                Tuple2.of(
                                        Long.parseLong(x.split(":")[0]),
                                        Long.parseLong(x.split(":")[1])));
        private final TreeMap<Long, Long> nextSnapshotPerCheckpoint = new TreeMap<>();

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            NavigableMap<Long, Long> nextSnapshots =
                    nextSnapshotPerCheckpoint.headMap(checkpointId, true);
            OptionalLong max = nextSnapshots.values().stream().mapToLong(Long::longValue).max();
            max.ifPresent(scan::notifyCheckpointComplete);
            nextSnapshots.clear();
        }

        @Override
        public List<SimpleSourceSplit> snapshotState(long checkpointId) {
            this.checkpointState.clear();
            Long nextSnapshot = this.scan.checkpoint();
            if (nextSnapshot != null) {
                this.checkpointState.add(nextSnapshot);
                this.nextSnapshotPerCheckpoint.put(checkpointId, nextSnapshot);
            }

            List<Tuple2<Long, Long>> nextSnapshots = new ArrayList<>();
            this.nextSnapshotPerCheckpoint.forEach((k, v) -> nextSnapshots.add(new Tuple2<>(k, v)));
            this.nextSnapshotState.update(nextSnapshots);

            if (LOG.isDebugEnabled()) {
                LOG.debug("{} checkpoint {}.", getClass().getSimpleName(), nextSnapshot);
            }

            List<SimpleSourceSplit> results = new ArrayList<>();
            results.addAll(checkpointState.snapshotState());
            results.addAll(nextSnapshotState.snapshotState());
            return results;
        }

        @Override
        public void addSplits(List<SimpleSourceSplit> list) {
            LOG.info("Restoring state for the {}.", getClass().getSimpleName());
            checkpointState.restoreState(list);
            nextSnapshotState.restoreState(list);

            List<Long> retrievedStates = checkpointState.get();

            // given that the parallelism of the source is 1, we can only have 1 retrieved items.
            Preconditions.checkArgument(
                    retrievedStates.size() <= 1,
                    getClass().getSimpleName() + " retrieved invalid state.");

            if (retrievedStates.size() == 1) {
                this.scan.restore(retrievedStates.get(0));
            }

            for (Tuple2<Long, Long> tuple2 : nextSnapshotState.get()) {
                nextSnapshotPerCheckpoint.put(tuple2.f0, tuple2.f1);
            }
        }

        @Override
        public InputStatus pollNext(ReaderOutput<Split> readerOutput) throws Exception {
            boolean isEmpty;
            try {
                List<Split> splits = scan.plan().splits();
                isEmpty = splits.isEmpty();
                splits.forEach(readerOutput::collect);

                if (emitSnapshotWatermark) {
                    Long watermark = scan.watermark();
                    if (watermark != null) {
                        readerOutput.emitWatermark(new Watermark(watermark));
                    }
                }
            } catch (EndOfScanException esf) {
                LOG.info("Catching EndOfStreamException, the stream is finished.");
                return InputStatus.END_OF_INPUT;
            }

            if (isEmpty) {
                Thread.sleep(monitorInterval);
            }
            return InputStatus.MORE_AVAILABLE;
        }
    }

    private static class SplitState<T> {
        private final String identifier;
        private final List<T> values;
        private final Function<T, String> serializer;
        private final Function<String, T> deserializer;

        private SplitState(
                String identifier,
                Function<T, String> serializer,
                Function<String, T> deserializer) {
            this.identifier = identifier;
            this.serializer = serializer;
            this.deserializer = deserializer;
            this.values = new ArrayList<>();
        }

        private void add(T value) {
            values.add(value);
        }

        private List<T> get() {
            return new ArrayList<>(values);
        }

        private void update(List<T> values) {
            this.values.clear();
            this.values.addAll(values);
        }

        private void clear() {
            values.clear();
        }

        private List<SimpleSourceSplit> snapshotState() {
            return values.stream()
                    .map(x -> new SimpleSourceSplit(identifier + serializer.apply(x)))
                    .collect(Collectors.toList());
        }

        private void restoreState(List<SimpleSourceSplit> splits) {
            values.clear();
            splits.stream()
                    .map(SimpleSourceSplit::value)
                    .filter(x -> x.startsWith(identifier))
                    .map(x -> x.substring(identifier.length()))
                    .map(this.deserializer)
                    .forEach(values::add);
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
                env.fromSource(
                                new MonitorSource(
                                        readBuilder, monitorInterval, emitSnapshotWatermark),
                                WatermarkStrategy.noWatermarks(),
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
