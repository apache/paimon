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

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.runtime.operators.sink.TestSink.StringCommittableSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import org.junit.Test;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.flink.table.store.connector.sink.global.GlobalCommitterOperatorTest.buildSubtaskState;
import static org.apache.flink.table.store.connector.sink.global.GlobalCommitterOperatorTest.committableRecords;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;

/** Test the {@link LocalCommitterOperator}. */
public class LocalCommitterOperatorTest {

    @Test
    public void supportRetry() throws Exception {
        final List<String> input = Arrays.asList("lazy", "leaf");
        final RetryOnceCommitter committer = new RetryOnceCommitter();
        final OneInputStreamOperatorTestHarness<
                        CommittableMessage<String>, CommittableMessage<String>>
                testHarness = createTestHarness(committer);

        testHarness.initializeEmptyState();
        testHarness.open();
        testHarness.processElements(committableRecords(input));
        testHarness.prepareSnapshotPreBarrier(1);
        testHarness.snapshot(1L, 1L);
        testHarness.notifyOfCompletedCheckpoint(1L);
        testHarness.snapshot(2L, 2L);
        testHarness.notifyOfCompletedCheckpoint(2L);

        testHarness.close();

        assertThat(committer.getCommittedData()).contains("lazy", "leaf");
    }

    @Test
    public void closeCommitter() throws Exception {
        final DefaultCommitter committer = new DefaultCommitter();
        final OneInputStreamOperatorTestHarness<
                        CommittableMessage<String>, CommittableMessage<String>>
                testHarness = createTestHarness(committer);
        testHarness.initializeEmptyState();
        testHarness.open();
        testHarness.close();
        assertThat(committer.isClosed()).isTrue();
    }

    @Test
    public void restoredFromMergedState() throws Exception {
        final List<String> input1 = Arrays.asList("today", "whom");
        final OperatorSubtaskState operatorSubtaskState1 =
                buildSubtaskState(createTestHarness(), input1);

        final List<String> input2 = Arrays.asList("future", "evil", "how");
        final OperatorSubtaskState operatorSubtaskState2 =
                buildSubtaskState(createTestHarness(), input2);

        final DefaultCommitter committer = new DefaultCommitter();
        final OneInputStreamOperatorTestHarness<
                        CommittableMessage<String>, CommittableMessage<String>>
                testHarness = createTestHarness(committer);

        final OperatorSubtaskState mergedOperatorSubtaskState =
                OneInputStreamOperatorTestHarness.repackageState(
                        operatorSubtaskState1, operatorSubtaskState2);

        testHarness.initializeState(
                OneInputStreamOperatorTestHarness.repartitionOperatorState(
                        mergedOperatorSubtaskState, 2, 2, 1, 0));
        testHarness.open();

        final List<String> expectedOutput = new ArrayList<>();
        expectedOutput.addAll(input1);
        expectedOutput.addAll(input2);

        testHarness.prepareSnapshotPreBarrier(1L);
        testHarness.snapshot(1L, 1L);
        testHarness.notifyOfCompletedCheckpoint(1);

        testHarness.close();

        assertThat(committer.getCommittedData())
                .containsExactlyInAnyOrder(expectedOutput.toArray(new String[0]));
    }

    @Test
    public void commitMultipleStagesTogether() throws Exception {

        final DefaultCommitter committer = new DefaultCommitter();

        final List<String> input1 = Arrays.asList("cautious", "nature");
        final List<String> input2 = Arrays.asList("count", "over");
        final List<String> input3 = Arrays.asList("lawyer", "grammar");

        final List<String> expectedOutput = new ArrayList<>();

        expectedOutput.addAll(input1);
        expectedOutput.addAll(input2);
        expectedOutput.addAll(input3);

        final OneInputStreamOperatorTestHarness<
                        CommittableMessage<String>, CommittableMessage<String>>
                testHarness = createTestHarness(committer);
        testHarness.initializeEmptyState();
        testHarness.open();

        testHarness.processElements(committableRecords(input1));
        testHarness.prepareSnapshotPreBarrier(1L);
        testHarness.snapshot(1L, 1L);
        testHarness.processElements(committableRecords(input2));
        testHarness.prepareSnapshotPreBarrier(2L);
        testHarness.snapshot(2L, 2L);
        testHarness.processElements(committableRecords(input3));
        testHarness.prepareSnapshotPreBarrier(3L);
        testHarness.snapshot(3L, 3L);

        testHarness.notifyOfCompletedCheckpoint(1);
        testHarness.notifyOfCompletedCheckpoint(3);

        testHarness.close();

        assertThat(fromRecords(testHarness.getRecordOutput())).isEqualTo(expectedOutput);

        assertThat(committer.getCommittedData()).isEqualTo(expectedOutput);
    }

    private static List<String> fromRecords(
            Collection<StreamRecord<CommittableMessage<String>>> elements) {
        return elements.stream()
                .map(StreamRecord::getValue)
                .filter(message -> message instanceof CommittableWithLineage)
                .map(message -> ((CommittableWithLineage<String>) message).getCommittable())
                .collect(Collectors.toList());
    }

    private OneInputStreamOperatorTestHarness<
                    CommittableMessage<String>, CommittableMessage<String>>
            createTestHarness() throws Exception {
        return createTestHarness(new DefaultCommitter());
    }

    private OneInputStreamOperatorTestHarness<
                    CommittableMessage<String>, CommittableMessage<String>>
            createTestHarness(Committer<String> committer) throws Exception {
        return new OneInputStreamOperatorTestHarness<>(
                new LocalCommitterOperator<>(
                        () -> committer, () -> StringCommittableSerializer.INSTANCE));
    }

    /** Base class for testing {@link Committer}. */
    private static class DefaultCommitter implements Committer<String>, Serializable {

        @Nullable protected Queue<String> committedData;

        private boolean isClosed;

        @Nullable private final Supplier<Queue<String>> queueSupplier;

        public DefaultCommitter() {
            this.committedData = new ConcurrentLinkedQueue<>();
            this.isClosed = false;
            this.queueSupplier = null;
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

        @Override
        public void commit(Collection<CommitRequest<String>> requests) {
            if (committedData == null) {
                assertNotNull(queueSupplier);
                committedData = queueSupplier.get();
            }
            committedData.addAll(
                    requests.stream()
                            .map(CommitRequest::getCommittable)
                            .collect(Collectors.toList()));
        }
    }

    /** A {@link Committer} that always re-commits the committables data it received. */
    private static class RetryOnceCommitter extends DefaultCommitter implements Committer<String> {

        private final Set<String> seen = new LinkedHashSet<>();

        @Override
        public void commit(Collection<CommitRequest<String>> requests) {
            requests.forEach(
                    c -> {
                        if (seen.remove(c.getCommittable())) {
                            checkNotNull(committedData);
                            committedData.add(c.getCommittable());
                        } else {
                            seen.add(c.getCommittable());
                            c.retryLater();
                        }
                    });
        }
    }
}
