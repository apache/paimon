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

package org.apache.paimon.flink.compact.changelog;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.CommittableTypeInfo;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.sink.CommitMessageImpl;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.EitherSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.types.Either;
import org.apache.flink.util.Preconditions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ChangelogCompactCoordinateOperator}. */
public class ChangelogCompactCoordinateOperatorTest {

    @Test
    public void testPrepareSnapshotWithMultipleFiles() throws Exception {
        Options options = new Options();
        options.set(CoreOptions.TARGET_FILE_SIZE, MemorySize.ofMebiBytes(8));
        ChangelogCompactCoordinateOperator operator =
                new ChangelogCompactCoordinateOperator(new CoreOptions(options));
        OneInputStreamOperatorTestHarness<Committable, Either<Committable, ChangelogCompactTask>>
                testHarness = createTestHarness(operator);

        testHarness.open();
        testHarness.processElement(
                new StreamRecord<>(
                        createCommittable(
                                1,
                                BinaryRow.EMPTY_ROW,
                                0,
                                Collections.emptyList(),
                                Arrays.asList(3, 2, 5, 4))));
        testHarness.processElement(
                new StreamRecord<>(
                        createCommittable(
                                1,
                                BinaryRow.EMPTY_ROW,
                                1,
                                Collections.emptyList(),
                                Arrays.asList(3, 3, 2, 2))));
        testHarness.prepareSnapshotPreBarrier(1);
        testHarness.processElement(
                new StreamRecord<>(
                        createCommittable(
                                2,
                                BinaryRow.EMPTY_ROW,
                                0,
                                Collections.emptyList(),
                                Arrays.asList(2, 3))));
        testHarness.prepareSnapshotPreBarrier(2);

        List<Object> output = new ArrayList<>(testHarness.getOutput());
        assertThat(output).hasSize(7);

        Map<Integer, List<Integer>> expected = new HashMap<>();
        expected.put(0, Arrays.asList(3, 2, 5));
        assertCompactionTask(output.get(0), 1, BinaryRow.EMPTY_ROW, new HashMap<>(), expected);

        expected.clear();
        expected.put(0, Collections.singletonList(4));
        expected.put(1, Arrays.asList(3, 3));
        assertCompactionTask(output.get(2), 1, BinaryRow.EMPTY_ROW, new HashMap<>(), expected);

        expected.clear();
        expected.put(1, Arrays.asList(2, 2));
        assertCompactionTask(output.get(4), 1, BinaryRow.EMPTY_ROW, new HashMap<>(), expected);

        expected.clear();
        expected.put(0, Arrays.asList(2, 3));
        assertCompactionTask(output.get(6), 2, BinaryRow.EMPTY_ROW, new HashMap<>(), expected);

        testHarness.close();
    }

    @Test
    public void testPrepareSnapshotWithSingleFile() throws Exception {
        Options options = new Options();
        options.set(CoreOptions.TARGET_FILE_SIZE, MemorySize.ofMebiBytes(8));
        ChangelogCompactCoordinateOperator operator =
                new ChangelogCompactCoordinateOperator(new CoreOptions(options));
        OneInputStreamOperatorTestHarness<Committable, Either<Committable, ChangelogCompactTask>>
                testHarness = createTestHarness(operator);

        testHarness.open();
        testHarness.processElement(
                new StreamRecord<>(
                        createCommittable(
                                1,
                                BinaryRow.EMPTY_ROW,
                                0,
                                Arrays.asList(3, 5, 2),
                                Collections.emptyList())));
        testHarness.prepareSnapshotPreBarrier(1);

        List<Object> output = new ArrayList<>(testHarness.getOutput());
        assertThat(output).hasSize(3);

        Map<Integer, List<Integer>> expected = new HashMap<>();
        expected.put(0, Arrays.asList(3, 5));
        assertCompactionTask(output.get(0), 1, BinaryRow.EMPTY_ROW, expected, new HashMap<>());
        assertCommittable(
                output.get(2),
                BinaryRow.EMPTY_ROW,
                Collections.singletonList(2),
                Collections.emptyList());

        testHarness.close();
    }

    @Test
    public void testPrepareSnapshotWithMultiplePartitions() throws Exception {
        Options options = new Options();
        options.set(CoreOptions.TARGET_FILE_SIZE, MemorySize.ofMebiBytes(8));
        ChangelogCompactCoordinateOperator operator =
                new ChangelogCompactCoordinateOperator(new CoreOptions(options));
        OneInputStreamOperatorTestHarness<Committable, Either<Committable, ChangelogCompactTask>>
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
                new StreamRecord<>(
                        createCommittable(
                                1,
                                binaryRow.apply(1),
                                0,
                                Collections.emptyList(),
                                Arrays.asList(3, 2, 5, 4))));
        testHarness.processElement(
                new StreamRecord<>(
                        createCommittable(
                                1,
                                binaryRow.apply(2),
                                1,
                                Collections.emptyList(),
                                Arrays.asList(3, 3, 2, 2, 3))));
        testHarness.prepareSnapshotPreBarrier(1);
        testHarness.processElement(
                new StreamRecord<>(
                        createCommittable(
                                2,
                                binaryRow.apply(1),
                                0,
                                Collections.emptyList(),
                                Arrays.asList(2, 3))));
        testHarness.prepareSnapshotPreBarrier(2);

        List<Object> output = new ArrayList<>(testHarness.getOutput());
        assertThat(output).hasSize(8);

        Map<Integer, List<Integer>> expected = new HashMap<>();
        expected.put(0, Arrays.asList(3, 2, 5));
        assertCompactionTask(output.get(0), 1, binaryRow.apply(1), new HashMap<>(), expected);

        expected.clear();
        expected.put(1, Arrays.asList(3, 3, 2));
        assertCompactionTask(output.get(2), 1, binaryRow.apply(2), new HashMap<>(), expected);

        assertCommittable(
                output.get(4),
                binaryRow.apply(1),
                Collections.emptyList(),
                Collections.singletonList(4));

        expected.clear();
        expected.put(1, Arrays.asList(2, 3));
        assertCompactionTask(output.get(5), 1, binaryRow.apply(2), new HashMap<>(), expected);

        expected.clear();
        expected.put(0, Arrays.asList(2, 3));
        assertCompactionTask(output.get(7), 2, binaryRow.apply(1), new HashMap<>(), expected);

        testHarness.close();
    }

    @Test
    public void testSkipLargeFiles() throws Exception {
        Options options = new Options();
        options.set(CoreOptions.TARGET_FILE_SIZE, MemorySize.ofMebiBytes(8));
        ChangelogCompactCoordinateOperator operator =
                new ChangelogCompactCoordinateOperator(new CoreOptions(options));
        OneInputStreamOperatorTestHarness<Committable, Either<Committable, ChangelogCompactTask>>
                testHarness = createTestHarness(operator);

        testHarness.open();
        testHarness.processElement(
                new StreamRecord<>(
                        createCommittable(
                                1,
                                BinaryRow.EMPTY_ROW,
                                0,
                                Collections.emptyList(),
                                Arrays.asList(3, 10, 5, 9))));
        testHarness.prepareSnapshotPreBarrier(1);

        List<Object> output = new ArrayList<>(testHarness.getOutput());
        assertThat(output).hasSize(2);

        Map<Integer, List<Integer>> expected = new HashMap<>();
        expected.put(0, Arrays.asList(3, 5));
        assertCompactionTask(output.get(0), 1, BinaryRow.EMPTY_ROW, new HashMap<>(), expected);
        assertCommittable(
                output.get(1), BinaryRow.EMPTY_ROW, Collections.emptyList(), Arrays.asList(10, 9));

        testHarness.close();
    }

    @SuppressWarnings("unchecked")
    private void assertCommittable(
            Object o,
            BinaryRow partition,
            List<Integer> newFilesChangelogMbs,
            List<Integer> compactChangelogMbs) {
        StreamRecord<Either<Committable, ChangelogCompactTask>> record =
                (StreamRecord<Either<Committable, ChangelogCompactTask>>) o;
        assertThat(record.getValue().isLeft()).isTrue();
        Committable committable = record.getValue().left();

        assertThat(committable.checkpointId()).isEqualTo(1);
        CommitMessageImpl message = (CommitMessageImpl) committable.wrappedCommittable();
        assertThat(message.partition()).isEqualTo(partition);
        assertThat(message.bucket()).isEqualTo(0);

        assertSameSizes(message.newFilesIncrement().changelogFiles(), newFilesChangelogMbs);
        assertSameSizes(message.compactIncrement().changelogFiles(), compactChangelogMbs);
    }

    @SuppressWarnings("unchecked")
    private void assertCompactionTask(
            Object o,
            long checkpointId,
            BinaryRow partition,
            Map<Integer, List<Integer>> newFilesChangelogMbs,
            Map<Integer, List<Integer>> compactChangelogMbs) {
        StreamRecord<Either<Committable, ChangelogCompactTask>> record =
                (StreamRecord<Either<Committable, ChangelogCompactTask>>) o;
        assertThat(record.getValue().isRight()).isTrue();
        ChangelogCompactTask task = record.getValue().right();

        assertThat(task.checkpointId()).isEqualTo(checkpointId);
        assertThat(task.partition()).isEqualTo(partition);

        assertThat(task.newFileChangelogFiles().keySet()).isEqualTo(newFilesChangelogMbs.keySet());
        for (int bucket : task.newFileChangelogFiles().keySet()) {
            assertSameSizes(
                    task.newFileChangelogFiles().get(bucket), newFilesChangelogMbs.get(bucket));
        }
        assertThat(task.compactChangelogFiles().keySet()).isEqualTo(compactChangelogMbs.keySet());
        for (int bucket : task.compactChangelogFiles().keySet()) {
            assertSameSizes(
                    task.compactChangelogFiles().get(bucket), compactChangelogMbs.get(bucket));
        }
    }

    private void assertSameSizes(List<DataFileMeta> metas, List<Integer> mbs) {
        assertThat(metas.stream().mapToLong(DataFileMeta::fileSize).toArray())
                .containsExactlyInAnyOrder(
                        mbs.stream()
                                .mapToLong(mb -> MemorySize.ofMebiBytes(mb).getBytes())
                                .toArray());
    }

    private Committable createCommittable(
            long checkpointId,
            BinaryRow partition,
            int bucket,
            List<Integer> newFilesChangelogMbs,
            List<Integer> compactChangelogMbs) {
        CommitMessageImpl message =
                new CommitMessageImpl(
                        partition,
                        bucket,
                        2,
                        new DataIncrement(
                                Collections.emptyList(),
                                Collections.emptyList(),
                                newFilesChangelogMbs.stream()
                                        .map(this::createDataFileMetaOfSize)
                                        .collect(Collectors.toList())),
                        new CompactIncrement(
                                Collections.emptyList(),
                                Collections.emptyList(),
                                compactChangelogMbs.stream()
                                        .map(this::createDataFileMetaOfSize)
                                        .collect(Collectors.toList())));
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
                    Committable, Either<Committable, ChangelogCompactTask>>
            createTestHarness(ChangelogCompactCoordinateOperator operator) throws Exception {
        TypeSerializer serializer =
                new EitherSerializer<>(
                        new CommittableTypeInfo().createSerializer(new ExecutionConfig()),
                        new ChangelogTaskTypeInfo().createSerializer(new ExecutionConfig()));
        OneInputStreamOperatorTestHarness harness =
                new OneInputStreamOperatorTestHarness(operator, 1, 1, 0);
        harness.getStreamConfig().setupNetworkInputs(Preconditions.checkNotNull(serializer));
        harness.getStreamConfig().serializeAllConfigs();
        harness.setup(serializer);
        return harness;
    }
}
