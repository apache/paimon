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

package org.apache.flink.table.store.connector.sink.global;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessageTypeInfo;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.runtime.operators.sink.TestSink.StringCommittableSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.function.SerializableSupplier;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link GlobalCommitterOperator}. */
public class GlobalCommitterOperatorTest {

    @Test
    public void closeCommitter() throws Exception {
        final DefaultGlobalCommitter globalCommitter = new DefaultGlobalCommitter();
        final OneInputStreamOperatorTestHarness<CommittableMessage<String>, Void> testHarness =
                createTestHarness(globalCommitter);
        testHarness.initializeEmptyState();
        testHarness.open();
        testHarness.close();
        assertThat(globalCommitter.isClosed()).isTrue();
    }

    @Test
    public void restoredFromMergedState() throws Exception {

        final List<String> input1 = Arrays.asList("host", "drop");
        final OperatorSubtaskState operatorSubtaskState1 =
                buildSubtaskState(createTestHarness(), input1);

        final List<String> input2 = Arrays.asList("future", "evil", "how");
        final OperatorSubtaskState operatorSubtaskState2 =
                buildSubtaskState(createTestHarness(), input2);

        final DefaultGlobalCommitter globalCommitter = new DefaultGlobalCommitter();
        final OneInputStreamOperatorTestHarness<CommittableMessage<String>, Void> testHarness =
                createTestHarness(globalCommitter);

        final OperatorSubtaskState mergedOperatorSubtaskState =
                OneInputStreamOperatorTestHarness.repackageState(
                        operatorSubtaskState1, operatorSubtaskState2);

        testHarness.initializeState(
                OneInputStreamOperatorTestHarness.repartitionOperatorState(
                        mergedOperatorSubtaskState, 2, 2, 1, 0));
        testHarness.open();

        final List<String> expectedOutput = new ArrayList<>();
        expectedOutput.add(DefaultGlobalCommitter.COMBINER.apply(input1));
        expectedOutput.add(DefaultGlobalCommitter.COMBINER.apply(input2));

        testHarness.snapshot(1L, 1L);
        testHarness.notifyOfCompletedCheckpoint(1L);
        testHarness.close();

        assertThat(globalCommitter.getCommittedData())
                .containsExactlyInAnyOrder(expectedOutput.toArray(new String[0]));
    }

    @Test
    public void commitMultipleStagesTogether() throws Exception {

        final DefaultGlobalCommitter globalCommitter = new DefaultGlobalCommitter();

        final List<String> input1 = Arrays.asList("cautious", "nature");
        final List<String> input2 = Arrays.asList("count", "over");
        final List<String> input3 = Arrays.asList("lawyer", "grammar");

        final List<String> expectedOutput = new ArrayList<>();

        expectedOutput.add(DefaultGlobalCommitter.COMBINER.apply(input1));
        expectedOutput.add(DefaultGlobalCommitter.COMBINER.apply(input2));
        expectedOutput.add(DefaultGlobalCommitter.COMBINER.apply(input3));

        final OneInputStreamOperatorTestHarness<CommittableMessage<String>, Void> testHarness =
                createTestHarness(globalCommitter);
        testHarness.initializeEmptyState();
        testHarness.open();

        testHarness.processElements(committableRecords(input1));
        testHarness.snapshot(1L, 1L);
        testHarness.processElements(committableRecords(input2));
        testHarness.snapshot(2L, 2L);
        testHarness.processElements(committableRecords(input3));
        testHarness.snapshot(3L, 3L);

        testHarness.notifyOfCompletedCheckpoint(3L);

        testHarness.close();

        assertThat(globalCommitter.getCommittedData())
                .containsExactlyInAnyOrder(expectedOutput.toArray(new String[0]));
    }

    @Test
    public void filterRecoveredCommittables() throws Exception {
        final List<String> input = Arrays.asList("silent", "elder", "patience");
        final String successCommittedCommittable = DefaultGlobalCommitter.COMBINER.apply(input);

        final OperatorSubtaskState operatorSubtaskState =
                buildSubtaskState(createTestHarness(), input);
        final DefaultGlobalCommitter globalCommitter =
                new DefaultGlobalCommitter(successCommittedCommittable);

        final OneInputStreamOperatorTestHarness<CommittableMessage<String>, Void> testHarness =
                createTestHarness(globalCommitter);

        // all data from previous checkpoint are expected to be committed,
        // so we expect no data to be re-committed.
        testHarness.initializeState(operatorSubtaskState);
        testHarness.open();
        testHarness.snapshot(1L, 1L);
        testHarness.notifyOfCompletedCheckpoint(1L);
        assertThat(globalCommitter.getCommittedData()).isEmpty();
        testHarness.close();
    }

    @Test
    public void endOfInput() throws Exception {
        final DefaultGlobalCommitter globalCommitter = new DefaultGlobalCommitter();

        final OneInputStreamOperatorTestHarness<CommittableMessage<String>, Void> testHarness =
                createTestHarness(globalCommitter);
        testHarness.initializeEmptyState();
        testHarness.open();
        List<String> input = Arrays.asList("silent", "elder", "patience");
        testHarness.processElements(committableRecords(input));
        testHarness.endInput();
        testHarness.close();
        assertThat(globalCommitter.getCommittedData()).contains("elder+patience+silent");
    }

    private OneInputStreamOperatorTestHarness<CommittableMessage<String>, Void> createTestHarness()
            throws Exception {
        return createTestHarness(new DefaultGlobalCommitter());
    }

    private OneInputStreamOperatorTestHarness<CommittableMessage<String>, Void> createTestHarness(
            GlobalCommitter<String, String> globalCommitter) throws Exception {
        return new OneInputStreamOperatorTestHarness<>(
                new GlobalCommitterOperator<>(
                        () -> globalCommitter, () -> StringCommittableSerializer.INSTANCE),
                CommittableMessageTypeInfo.of(
                                (SerializableSupplier<SimpleVersionedSerializer<String>>)
                                        () -> StringCommittableSerializer.INSTANCE)
                        .createSerializer(new ExecutionConfig()));
    }

    public static OperatorSubtaskState buildSubtaskState(
            OneInputStreamOperatorTestHarness<CommittableMessage<String>, Void> testHarness,
            List<String> input)
            throws Exception {
        testHarness.initializeEmptyState();
        testHarness.open();
        testHarness.processElements(
                input.stream()
                        .map(GlobalCommitterOperatorTest::toCommittableMessage)
                        .map(StreamRecord::new)
                        .collect(Collectors.toList()));
        testHarness.prepareSnapshotPreBarrier(1L);
        OperatorSubtaskState operatorSubtaskState = testHarness.snapshot(1L, 1L);
        testHarness.close();
        return operatorSubtaskState;
    }

    private static List<StreamRecord<CommittableMessage<String>>> committableRecords(
            Collection<String> elements) {
        return elements.stream()
                .map(GlobalCommitterOperatorTest::toCommittableMessage)
                .map(StreamRecord::new)
                .collect(Collectors.toList());
    }

    private static CommittableMessage<String> toCommittableMessage(String input) {
        return new CommittableWithLineage<>(input, null, -1);
    }

    /** A {@link GlobalCommitter} that always commits global committables successfully. */
    private static class DefaultGlobalCommitter implements GlobalCommitter<String, String> {

        private static final Function<List<String>, String> COMBINER =
                strings -> {
                    // we sort here because we want to have a deterministic result during the unit
                    // test
                    Collections.sort(strings);
                    return String.join("+", strings);
                };

        private final Queue<String> committedData;

        private boolean isClosed;

        private final String committedSuccessData;

        DefaultGlobalCommitter() {
            this("");
        }

        DefaultGlobalCommitter(String committedSuccessData) {
            this.committedData = new ConcurrentLinkedQueue<>();
            this.isClosed = false;
            this.committedSuccessData = committedSuccessData;
        }

        @Override
        public List<String> filterRecoveredCommittables(List<String> globalCommittables) {
            if (committedSuccessData == null) {
                return globalCommittables;
            }
            return globalCommittables.stream()
                    .filter(s -> !s.equals(committedSuccessData))
                    .collect(Collectors.toList());
        }

        @Override
        public String combine(long checkpointId, List<String> committables) {
            return COMBINER.apply(committables);
        }

        @Override
        public void commit(List<String> committables) {
            committedData.addAll(committables);
        }

        public List<String> getCommittedData() {
            if (committedData != null) {
                return new ArrayList<>(committedData);
            } else {
                return Collections.emptyList();
            }
        }

        @Override
        public void close() {
            isClosed = true;
        }

        public boolean isClosed() {
            return isClosed;
        }
    }
}
