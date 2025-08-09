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

package org.apache.paimon.flink.compact;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.append.AppendCompactTask;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.CommittableTypeInfo;
import org.apache.paimon.flink.sink.CompactionTaskTypeInfo;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.sink.CommitMessageImpl;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.EitherSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.types.Either;
import org.apache.flink.util.Preconditions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AppendPreCommitCompactCoordinatorOperator}. */
public class AppendPreCommitCompactCoordinatorOperatorTest {

    @Test
    public void testPrepareSnapshotWithMultipleFiles() throws Exception {
        Options options = new Options();
        options.set(CoreOptions.TARGET_FILE_SIZE, MemorySize.ofMebiBytes(8));
        AppendPreCommitCompactCoordinatorOperator operator =
                new AppendPreCommitCompactCoordinatorOperator(new CoreOptions(options));
        OneInputStreamOperatorTestHarness<
                        Committable, Either<Committable, Tuple2<Long, AppendCompactTask>>>
                testHarness = createTestHarness(operator);

        testHarness.open();
        testHarness.processElement(
                new StreamRecord<>(createCommittable(1, BinaryRow.EMPTY_ROW, 3, 5, 1, 2, 3)));
        testHarness.prepareSnapshotPreBarrier(1);
        testHarness.processElement(
                new StreamRecord<>(createCommittable(2, BinaryRow.EMPTY_ROW, 3, 2)));
        testHarness.prepareSnapshotPreBarrier(2);

        List<Object> output = new ArrayList<>(testHarness.getOutput());
        assertThat(output).hasSize(3);
        assertCompactionTask(output.get(0), 1, BinaryRow.EMPTY_ROW, 3, 5);
        assertCompactionTask(output.get(1), 1, BinaryRow.EMPTY_ROW, 1, 2, 3);
        assertCompactionTask(output.get(2), 2, BinaryRow.EMPTY_ROW, 3, 2);

        testHarness.close();
    }

    @Test
    public void testPrepareSnapshotWithSingleFile() throws Exception {
        Options options = new Options();
        options.set(CoreOptions.TARGET_FILE_SIZE, MemorySize.ofMebiBytes(8));
        AppendPreCommitCompactCoordinatorOperator operator =
                new AppendPreCommitCompactCoordinatorOperator(new CoreOptions(options));
        OneInputStreamOperatorTestHarness<
                        Committable, Either<Committable, Tuple2<Long, AppendCompactTask>>>
                testHarness = createTestHarness(operator);

        testHarness.open();
        testHarness.processElement(
                new StreamRecord<>(createCommittable(1, BinaryRow.EMPTY_ROW, 3, 5, 1)));
        testHarness.prepareSnapshotPreBarrier(1);
        testHarness.processElement(
                new StreamRecord<>(createCommittable(2, BinaryRow.EMPTY_ROW, 4)));
        testHarness.prepareSnapshotPreBarrier(2);

        List<Object> output = new ArrayList<>(testHarness.getOutput());
        assertThat(output).hasSize(3);
        assertCompactionTask(output.get(0), 1, BinaryRow.EMPTY_ROW, 3, 5);
        assertCommittable(output.get(1), 1, BinaryRow.EMPTY_ROW, 1);
        assertCommittable(output.get(2), 2, BinaryRow.EMPTY_ROW, 4);

        testHarness.close();
    }

    @Test
    public void testPrepareSnapshotWithMultiplePartitions() throws Exception {
        Options options = new Options();
        options.set(CoreOptions.TARGET_FILE_SIZE, MemorySize.ofMebiBytes(8));
        AppendPreCommitCompactCoordinatorOperator operator =
                new AppendPreCommitCompactCoordinatorOperator(new CoreOptions(options));
        OneInputStreamOperatorTestHarness<
                        Committable, Either<Committable, Tuple2<Long, AppendCompactTask>>>
                testHarness = createTestHarness(operator);

        Function<Integer, BinaryRow> binaryRow =
                i -> {
                    BinaryRow row = new BinaryRow(1);
                    BinaryRowWriter writer = new BinaryRowWriter(row);
                    writer.writeInt(0, i);
                    writer.complete();
                    return row;
                };

        testHarness.open();
        testHarness.processElement(
                new StreamRecord<>(createCommittable(1, binaryRow.apply(1), 3, 5, 1, 2, 3)));
        testHarness.processElement(
                new StreamRecord<>(createCommittable(1, binaryRow.apply(2), 3, 2, 4, 3)));
        testHarness.prepareSnapshotPreBarrier(1);
        testHarness.processElement(
                new StreamRecord<>(createCommittable(2, binaryRow.apply(2), 3, 2)));
        testHarness.prepareSnapshotPreBarrier(2);

        List<Object> output = new ArrayList<>(testHarness.getOutput());
        assertThat(output).hasSize(5);

        assertCompactionTask(output.get(0), 1, binaryRow.apply(1), 3, 5);
        assertCompactionTask(output.get(1), 1, binaryRow.apply(2), 3, 2, 4);
        assertCompactionTask(output.get(2), 1, binaryRow.apply(1), 1, 2, 3);
        assertCommittable(output.get(3), 1, binaryRow.apply(2), 3);
        assertCompactionTask(output.get(4), 2, binaryRow.apply(2), 3, 2);

        testHarness.close();
    }

    @Test
    public void testSkipLargeFiles() throws Exception {
        Options options = new Options();
        options.set(CoreOptions.TARGET_FILE_SIZE, MemorySize.ofMebiBytes(8));
        AppendPreCommitCompactCoordinatorOperator operator =
                new AppendPreCommitCompactCoordinatorOperator(new CoreOptions(options));
        OneInputStreamOperatorTestHarness<
                        Committable, Either<Committable, Tuple2<Long, AppendCompactTask>>>
                testHarness = createTestHarness(operator);

        testHarness.open();
        testHarness.processElement(
                new StreamRecord<>(createCommittable(1, BinaryRow.EMPTY_ROW, 8, 3, 5, 9)));
        testHarness.prepareSnapshotPreBarrier(1);

        List<Object> output = new ArrayList<>(testHarness.getOutput());
        assertThat(output).hasSize(2);
        assertCompactionTask(output.get(0), 1, BinaryRow.EMPTY_ROW, 3, 5);
        assertCommittable(output.get(1), 1, BinaryRow.EMPTY_ROW, 8, 9);

        testHarness.close();
    }

    @SuppressWarnings("unchecked")
    private void assertCommittable(Object o, long checkpointId, BinaryRow partition, int... mbs) {
        StreamRecord<Either<Committable, Tuple2<Long, AppendCompactTask>>> record =
                (StreamRecord<Either<Committable, Tuple2<Long, AppendCompactTask>>>) o;
        assertThat(record.getValue().isLeft()).isTrue();
        Committable committable = record.getValue().left();
        assertThat(committable.checkpointId()).isEqualTo(checkpointId);
        CommitMessageImpl message = (CommitMessageImpl) committable.wrappedCommittable();
        assertThat(message.partition()).isEqualTo(partition);
        assertThat(message.newFilesIncrement().deletedFiles()).isEmpty();
        assertThat(message.newFilesIncrement().changelogFiles()).isEmpty();
        assertThat(message.compactIncrement().isEmpty()).isTrue();
        assertThat(message.indexIncrement().isEmpty()).isTrue();
        assertThat(message.newFilesIncrement().newFiles().stream().map(DataFileMeta::fileSize))
                .hasSameElementsAs(
                        Arrays.stream(mbs)
                                .mapToObj(i -> MemorySize.ofMebiBytes(i).getBytes())
                                .collect(Collectors.toList()));
    }

    @SuppressWarnings("unchecked")
    private void assertCompactionTask(
            Object o, long checkpointId, BinaryRow partition, int... mbs) {
        StreamRecord<Either<Committable, Tuple2<Long, AppendCompactTask>>> record =
                (StreamRecord<Either<Committable, Tuple2<Long, AppendCompactTask>>>) o;
        assertThat(record.getValue().isRight()).isTrue();
        assertThat(record.getValue().right().f0).isEqualTo(checkpointId);
        AppendCompactTask task = record.getValue().right().f1;
        assertThat(task.partition()).isEqualTo(partition);
        assertThat(task.compactBefore().stream().map(DataFileMeta::fileSize))
                .hasSameElementsAs(
                        Arrays.stream(mbs)
                                .mapToObj(i -> MemorySize.ofMebiBytes(i).getBytes())
                                .collect(Collectors.toList()));
    }

    private Committable createCommittable(long checkpointId, BinaryRow partition, int... mbs) {
        CommitMessageImpl message =
                new CommitMessageImpl(
                        partition,
                        BucketMode.UNAWARE_BUCKET,
                        -1,
                        new DataIncrement(
                                Arrays.stream(mbs)
                                        .mapToObj(this::createDataFileMetaOfSize)
                                        .collect(Collectors.toList()),
                                Collections.emptyList(),
                                Collections.emptyList()),
                        CompactIncrement.emptyIncrement());
        return new Committable(checkpointId, Committable.Kind.FILE, message);
    }

    private DataFileMeta createDataFileMetaOfSize(int mb) {
        return DataFileMeta.forAppend(
                UUID.randomUUID().toString(),
                MemorySize.ofMebiBytes(mb).getBytes(),
                0,
                SimpleStats.EMPTY_STATS,
                0,
                0,
                1,
                Collections.emptyList(),
                null,
                null,
                null,
                null,
                null,
                null);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private OneInputStreamOperatorTestHarness<
                    Committable, Either<Committable, Tuple2<Long, AppendCompactTask>>>
            createTestHarness(AppendPreCommitCompactCoordinatorOperator operator) throws Exception {
        TypeSerializer serializer =
                new EitherSerializer<>(
                        new CommittableTypeInfo().createSerializer(new ExecutionConfig()),
                        new TupleTypeInfo<>(
                                        BasicTypeInfo.LONG_TYPE_INFO, new CompactionTaskTypeInfo())
                                .createSerializer(new SerializerConfigImpl()));
        OneInputStreamOperatorTestHarness harness =
                new OneInputStreamOperatorTestHarness(operator, 1, 1, 0);
        harness.getStreamConfig().setupNetworkInputs(Preconditions.checkNotNull(serializer));
        harness.getStreamConfig().serializeAllConfigs();
        harness.setup(serializer);
        return harness;
    }
}
