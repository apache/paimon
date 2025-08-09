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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.CommittableTypeInfo;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.table.sink.CommitMessageImpl;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.Preconditions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ChangelogCompactSortOperator}. */
public class ChangelogCompactSortOperatorTest {

    @SuppressWarnings("unchecked")
    @Test
    public void testChangelogSorted() throws Exception {
        ChangelogCompactSortOperator operator = new ChangelogCompactSortOperator();
        OneInputStreamOperatorTestHarness<Committable, Committable> testHarness =
                createTestHarness(operator);

        Function<Integer, BinaryRow> binaryRow =
                i -> {
                    BinaryRow row = new BinaryRow(1);
                    BinaryRowWriter writer = new BinaryRowWriter(row);
                    writer.writeInt(0, i);
                    writer.complete();
                    return row;
                };

        testHarness.open();

        List<DataFileMeta> files = new ArrayList<>();
        for (int i = 0; i <= 10; i++) {
            files.add(createDataFileMeta(i, i * 100));
        }

        CommitMessageImpl onlyData =
                new CommitMessageImpl(
                        binaryRow.apply(0),
                        0,
                        2,
                        new DataIncrement(
                                Arrays.asList(files.get(2), files.get(1)),
                                Collections.emptyList(),
                                Collections.emptyList()),
                        CompactIncrement.emptyIncrement());
        testHarness.processElement(
                new StreamRecord<>(new Committable(1, Committable.Kind.FILE, onlyData)));

        CommitMessageImpl onlyChangelogBucket0 =
                new CommitMessageImpl(
                        binaryRow.apply(0),
                        0,
                        2,
                        new DataIncrement(
                                Collections.emptyList(),
                                Collections.emptyList(),
                                Arrays.asList(files.get(4), files.get(3))),
                        CompactIncrement.emptyIncrement());
        testHarness.processElement(
                new StreamRecord<>(
                        new Committable(1, Committable.Kind.FILE, onlyChangelogBucket0)));

        CommitMessageImpl onlyChangelogBucket1 =
                new CommitMessageImpl(
                        binaryRow.apply(0),
                        1,
                        2,
                        new DataIncrement(
                                Collections.emptyList(),
                                Collections.emptyList(),
                                Arrays.asList(files.get(7), files.get(8))),
                        CompactIncrement.emptyIncrement());
        testHarness.processElement(
                new StreamRecord<>(
                        new Committable(1, Committable.Kind.FILE, onlyChangelogBucket1)));

        CommitMessageImpl mixed =
                new CommitMessageImpl(
                        binaryRow.apply(0),
                        1,
                        2,
                        new DataIncrement(
                                Arrays.asList(files.get(10), files.get(9)),
                                Collections.emptyList(),
                                Arrays.asList(files.get(6), files.get(5))),
                        CompactIncrement.emptyIncrement());
        testHarness.processElement(
                new StreamRecord<>(new Committable(1, Committable.Kind.FILE, mixed)));

        testHarness.prepareSnapshotPreBarrier(1);

        List<Object> output = new ArrayList<>(testHarness.getOutput());
        assertThat(output).hasSize(4);

        List<CommitMessageImpl> actual = new ArrayList<>();
        for (Object o : output) {
            actual.add(
                    (CommitMessageImpl)
                            ((StreamRecord<Committable>) o).getValue().wrappedCommittable());
        }

        assertThat(actual.get(0)).isEqualTo(onlyData);
        assertThat(actual.get(1))
                .isEqualTo(
                        new CommitMessageImpl(
                                binaryRow.apply(0),
                                1,
                                2,
                                new DataIncrement(
                                        Arrays.asList(files.get(10), files.get(9)),
                                        Collections.emptyList(),
                                        Collections.emptyList()),
                                CompactIncrement.emptyIncrement()));
        assertThat(actual.get(2))
                .isEqualTo(
                        new CommitMessageImpl(
                                binaryRow.apply(0),
                                0,
                                2,
                                new DataIncrement(
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        Arrays.asList(files.get(3), files.get(4))),
                                CompactIncrement.emptyIncrement()));
        assertThat(actual.get(3))
                .isEqualTo(
                        new CommitMessageImpl(
                                binaryRow.apply(0),
                                1,
                                2,
                                new DataIncrement(
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        Arrays.asList(
                                                files.get(5),
                                                files.get(6),
                                                files.get(7),
                                                files.get(8))),
                                CompactIncrement.emptyIncrement()));

        testHarness.close();
    }

    private DataFileMeta createDataFileMeta(int mb, long creationMillis) {
        return new DataFileMeta(
                UUID.randomUUID().toString(),
                MemorySize.ofMebiBytes(mb).getBytes(),
                0,
                null,
                null,
                null,
                null,
                0,
                0,
                0,
                0,
                Collections.emptyList(),
                Timestamp.fromEpochMillis(creationMillis),
                null,
                null,
                null,
                null,
                null,
                null,
                null);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private OneInputStreamOperatorTestHarness<Committable, Committable> createTestHarness(
            ChangelogCompactSortOperator operator) throws Exception {
        TypeSerializer serializer =
                new CommittableTypeInfo().createSerializer(new ExecutionConfig());
        OneInputStreamOperatorTestHarness harness =
                new OneInputStreamOperatorTestHarness(operator, 1, 1, 0);
        harness.getStreamConfig().setupNetworkInputs(Preconditions.checkNotNull(serializer));
        harness.getStreamConfig().serializeAllConfigs();
        harness.setup(serializer);
        return harness;
    }
}
