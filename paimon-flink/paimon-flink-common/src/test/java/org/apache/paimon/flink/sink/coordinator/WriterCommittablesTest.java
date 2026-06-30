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

package org.apache.paimon.flink.sink.coordinator;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;

import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Unit tests for {@link WriterCommittables}. */
public class WriterCommittablesTest {

    private static final ListSerializer<Committable> serializer =
            CommittableEvent.createCommittableListSerializer();

    @Test
    public void testGetCommittablesPerCheckpoint() throws Exception {
        CommitMessage commitMessage = createEmptyCommitMessage();
        long checkpointId = 1L;
        Committable committable = new Committable(checkpointId, commitMessage);
        CommittableEvent event =
                CommittableEvent.create(
                        checkpointId, false, Collections.singletonList(committable), serializer);
        WriterCommittables committables = WriterCommittables.from(event, serializer);
        NavigableMap<Long, List<Committable>> resultCommittables =
                committables.getCommittablesPerCheckpoint();

        assertThat(resultCommittables.size()).isEqualTo(1);
        assertThat(resultCommittables.get(checkpointId).size()).isEqualTo(1);

        Committable resultCommittable = resultCommittables.get(checkpointId).get(0);
        assertThat(resultCommittable.checkpointId()).isEqualTo(checkpointId);
        assertThat(resultCommittable.commitMessage()).isEqualTo(commitMessage);
    }

    @Test
    public void testGetCommittablesBeforeCheckpoint() throws Exception {
        CommitMessage commitMessage = createEmptyCommitMessage();
        long checkpointId = 4L;
        Committable committable1 = new Committable(1L, commitMessage);
        Committable committable2 = new Committable(2L, commitMessage);
        Committable committable3 = new Committable(3L, commitMessage);
        CommittableEvent event =
                CommittableEvent.create(
                        checkpointId,
                        false,
                        Arrays.asList(committable1, committable3, committable2),
                        serializer);
        WriterCommittables committables = WriterCommittables.from(event, serializer);
        assertThat(committables.getCommittablesBeforeCheckpoint(-1, true).isEmpty()).isTrue();
        assertThat(committables.getCommittablesBeforeCheckpoint(1, false).isEmpty()).isTrue();
        assertThat(committables.getCommittablesBeforeCheckpoint(1, true).isEmpty()).isFalse();
        assertThat(committables.getCommittablesBeforeCheckpoint(3, false).size()).isEqualTo(2);
        assertThat(committables.getCommittablesBeforeCheckpoint(4, false).size()).isEqualTo(3);
    }

    @Test
    public void testReset() throws Exception {
        CommitMessage commitMessage = createEmptyCommitMessage();
        long checkpointId = 4L;
        Committable committable1 = new Committable(1L, commitMessage);
        Committable committable2 = new Committable(2L, commitMessage);
        Committable committable3 = new Committable(3L, commitMessage);
        CommittableEvent event =
                CommittableEvent.create(
                        checkpointId,
                        true,
                        Arrays.asList(committable1, committable3, committable2),
                        serializer);
        WriterCommittables committables = WriterCommittables.from(event, serializer);
        committables.reset();
        assertThat(committables.getMaxCheckpointId()).isLessThan(0);
        assertThat(committables.getCommittablesPerCheckpoint().isEmpty()).isTrue();
    }

    @Test
    public void testGroupByCheckpoint() {
        TreeMap<Long, List<Committable>> committablesPerCheckpoint = new TreeMap<>();
        List<Committable> committables = new ArrayList<>();
        CommitMessage commitMessage = createEmptyCommitMessage();
        committables.add(new Committable(1L, commitMessage));
        committables.add(new Committable(2L, commitMessage));
        committables.add(new Committable(3L, commitMessage));
        committables.add(new Committable(1L, commitMessage));
        WriterCommittables.groupByCheckpoint(committablesPerCheckpoint, committables);
        assertThat(committablesPerCheckpoint.size()).isEqualTo(3);
        assertThat(committablesPerCheckpoint.get(1L).size()).isEqualTo(2);
        assertThat(committablesPerCheckpoint.get(2L).size()).isEqualTo(1);
        assertThat(committablesPerCheckpoint.get(3L).size()).isEqualTo(1);
    }

    @Test
    public void testMergeWith() throws Exception {
        CommitMessage commitMessage = createEmptyCommitMessage();
        long checkpointId = 1L;
        WriterCommittables committables;
        // create original committables;
        Committable committable = new Committable(checkpointId, commitMessage);
        committables =
                WriterCommittables.from(
                        CommittableEvent.create(
                                checkpointId,
                                true,
                                Collections.singletonList(committable),
                                serializer),
                        serializer);
        // merge new checkpoint committables;
        checkpointId++;
        committables.mergeWith(
                WriterCommittables.from(
                        CommittableEvent.create(
                                checkpointId, false, Collections.emptyList(), serializer),
                        serializer));
        // verify
        NavigableMap<Long, List<Committable>> results = committables.getCommittablesPerCheckpoint();
        // checkpoint 2 is empty, it's ignored
        assertThat(results.size()).isEqualTo(1);
        assertThat(results.get(1L)).isNotNull();
        assertThat(results.get(1L).size()).isEqualTo(1);
        assertThat(results.get(2L)).isNull();
        assertThat(committables.getMaxCheckpointId()).isEqualTo(2L);
        // merge another new committables
        checkpointId++;
        Committable committable1 = new Committable(checkpointId, commitMessage);
        Committable committable2 = new Committable(checkpointId, commitMessage);
        committables.mergeWith(
                WriterCommittables.from(
                        CommittableEvent.create(
                                checkpointId,
                                false,
                                Arrays.asList(committable1, committable2),
                                serializer),
                        serializer));
        // verify
        assertThat(committables.getMaxCheckpointId()).isEqualTo(3L);
        assertThat(committables.getCommittablesPerCheckpoint().size()).isEqualTo(2);
        // verify checkpoint 1
        assertThat(committables.getCommittablesPerCheckpoint().get(1L)).isNotNull();
        assertThat(committables.getCommittablesPerCheckpoint().get(1L).size()).isEqualTo(1);
        assertThat(
                        committableEquals(
                                committables.getCommittablesPerCheckpoint().get(1L).get(0),
                                committable))
                .isTrue();
        // verify checkpoint 2
        assertThat(committables.getCommittablesPerCheckpoint().get(2L)).isNull();
        // verify checkpoint 3
        assertThat(committables.getCommittablesPerCheckpoint().get(3L)).isNotNull();
        assertThat(committables.getCommittablesPerCheckpoint().get(3L).size()).isEqualTo(2);
        assertThat(
                        committableEquals(
                                committables.getCommittablesPerCheckpoint().get(3L).get(0),
                                committable1))
                .isTrue();
        assertThat(
                        committableEquals(
                                committables.getCommittablesPerCheckpoint().get(3L).get(1),
                                committable2))
                .isTrue();
    }

    @Test
    public void testInvalidMerge() throws Exception {
        CommitMessage commitMessage = createEmptyCommitMessage();
        long checkpointId = 1024L;
        WriterCommittables committables =
                WriterCommittables.from(
                        CommittableEvent.create(
                                checkpointId,
                                true,
                                Collections.singletonList(
                                        new Committable(checkpointId, commitMessage)),
                                serializer),
                        serializer);
        // could not merge same checkpoint id
        assertThrows(IllegalStateException.class, () -> committables.mergeWith(committables));

        long oldCheckpointId = checkpointId - 1;
        WriterCommittables oldCommittables =
                WriterCommittables.from(
                        CommittableEvent.create(
                                oldCheckpointId,
                                false,
                                Collections.singletonList(
                                        new Committable(oldCheckpointId, commitMessage)),
                                serializer),
                        serializer);
        assertThrows(IllegalStateException.class, () -> committables.mergeWith(oldCommittables));
    }

    @Test
    public void testClearCommittablesBeforeCheckpoint() throws Exception {
        CommitMessage commitMessage = createEmptyCommitMessage();
        Committable committableCp1 = new Committable(1L, commitMessage);
        Committable committableCp2 = new Committable(2L, commitMessage);
        Committable committableCp3 = new Committable(3L, commitMessage);
        CommittableEvent event =
                CommittableEvent.create(
                        3L,
                        true,
                        Arrays.asList(committableCp3, committableCp1, committableCp2),
                        serializer);
        WriterCommittables committables = WriterCommittables.from(event, serializer);
        // clear checkpoint < 1, nothing happens
        committables.clearCommittablesBeforeCheckpoint(1L, false);
        assertThat(committables.getMaxCheckpointId()).isEqualTo(3L);
        assertThat(committables.getCommittablesPerCheckpoint().size()).isEqualTo(3);
        // clear checkpoint <= 1, checkpoint 2 and 3 left
        committables.clearCommittablesBeforeCheckpoint(1L, true);
        assertThat(committables.getMaxCheckpointId()).isEqualTo(3L);
        assertThat(committables.getCommittablesPerCheckpoint().size()).isEqualTo(2);
        assertThat(committables.getCommittablesPerCheckpoint().get(1L)).isNull();
        assertThat(committables.getCommittablesPerCheckpoint().get(2L)).isNotNull();
        assertThat(committables.getCommittablesPerCheckpoint().get(2L).size()).isEqualTo(1);
        assertThat(
                        committableEquals(
                                committables.getCommittablesPerCheckpoint().get(2L).get(0),
                                committableCp2))
                .isTrue();
        assertThat(committables.getCommittablesPerCheckpoint().get(3L)).isNotNull();
        assertThat(committables.getCommittablesPerCheckpoint().get(3L).size()).isEqualTo(1);
        assertThat(
                        committableEquals(
                                committables.getCommittablesPerCheckpoint().get(3L).get(0),
                                committableCp3))
                .isTrue();
        // clear all committables
        committables.clearCommittablesBeforeCheckpoint(10L, true);
        assertThat(committables.getMaxCheckpointId()).isEqualTo(-1);
        assertThat(committables.getCommittablesPerCheckpoint().isEmpty()).isTrue();
    }

    @Test
    public void testInvalidInputCommittables() {
        CommitMessage commitMessage = createEmptyCommitMessage();
        assertThrows(
                IllegalStateException.class,
                () ->
                        WriterCommittables.from(
                                CommittableEvent.create(
                                        1L,
                                        false,
                                        Collections.singletonList(
                                                // invalid checkpoint id, it's bigger than max
                                                // checkpoint params
                                                new Committable(2L, commitMessage)),
                                        serializer),
                                serializer));
    }

    private static CommitMessage createEmptyCommitMessage() {
        return new CommitMessageImpl(
                BinaryRow.EMPTY_ROW,
                0,
                1,
                new DataIncrement(
                        Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
                new CompactIncrement(
                        Collections.emptyList(), Collections.emptyList(), Collections.emptyList()));
    }

    // Committable does not implement 'equals'
    public static boolean committableEquals(Committable first, Committable second) {
        return first.checkpointId() == second.checkpointId()
                && Objects.equals(first.commitMessage(), second.commitMessage());
    }

    public static boolean committableEquals(List<Committable> first, List<Committable> second) {
        if (first.size() != second.size()) {
            return false;
        }
        for (int i = 0; i < first.size(); i++) {
            if (!committableEquals(first.get(i), second.get(i))) {
                return false;
            }
        }
        return true;
    }
}
