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
import org.apache.paimon.flink.sink.CommittableSerializer;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.CommitMessageSerializer;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializerTypeSerializerProxy;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Unit tests for {@link WriterCommittables}. */
public class WriterCommittablesTest {

    private static final TypeSerializer<CheckpointCommittables> SERIALIZER =
            new SimpleVersionedSerializerTypeSerializerProxy<>(
                    () ->
                            new CheckpointCommittablesSerializer(
                                    new CommittableSerializer(new CommitMessageSerializer())));

    @Test
    public void testGetCommittablesPerCheckpoint() throws Exception {
        CommitMessage commitMessage = createEmptyCommitMessage();
        long checkpointId = 1L;
        Committable committable = new Committable(checkpointId, commitMessage);
        CheckpointCommittables entry =
                new CheckpointCommittables(
                        checkpointId, Collections.singletonList(committable), Long.MIN_VALUE);
        CommittableEvent event = CommittableEvent.create(checkpointId, entry, SERIALIZER);
        WriterCommittables committables = WriterCommittables.from(event, SERIALIZER);
        NavigableMap<Long, CheckpointCommittables> resultCommittables =
                committables.getCommittablesPerCheckpoint();

        assertThat(resultCommittables.size()).isEqualTo(1);
        assertThat(resultCommittables.get(checkpointId).size()).isEqualTo(1);

        Committable resultCommittable = resultCommittables.get(checkpointId).committables().get(0);
        assertThat(resultCommittable.checkpointId()).isEqualTo(checkpointId);
        assertThat(resultCommittable.commitMessage()).isEqualTo(commitMessage);
    }

    @Test
    public void testWatermarkIsAttachedToEachCheckpoint() throws Exception {
        CommitMessage commitMessage = createEmptyCommitMessage();
        long watermark = 4242L;
        long checkpointId = 7L;
        Committable committable = new Committable(checkpointId, commitMessage);
        CheckpointCommittables entry =
                new CheckpointCommittables(
                        checkpointId, Collections.singletonList(committable), watermark);
        CommittableEvent event = CommittableEvent.create(checkpointId, entry, SERIALIZER);
        WriterCommittables committables = WriterCommittables.from(event, SERIALIZER);
        assertThat(committables.getCommittablesPerCheckpoint().get(checkpointId).watermark())
                .isEqualTo(watermark);
    }

    @Test
    public void testEmptyCheckpointKeepsAnEntryCarryingWatermark() throws Exception {
        // Even when a checkpoint carries no committables the buffer still records an entry so the
        // frozen watermark can be aligned with peer subtasks in the coordinator.
        long watermark = 999L;
        long checkpointId = 3L;
        CheckpointCommittables entry =
                new CheckpointCommittables(checkpointId, Collections.emptyList(), watermark);
        CommittableEvent event = CommittableEvent.create(checkpointId, entry, SERIALIZER);
        WriterCommittables committables = WriterCommittables.from(event, SERIALIZER);
        NavigableMap<Long, CheckpointCommittables> perCheckpoint =
                committables.getCommittablesPerCheckpoint();
        assertThat(perCheckpoint.size()).isEqualTo(1);
        assertThat(perCheckpoint.get(checkpointId).isEmpty()).isTrue();
        assertThat(perCheckpoint.get(checkpointId).watermark()).isEqualTo(watermark);
    }

    @Test
    public void testGetCommittablesBeforeCheckpoint() {
        CommitMessage commitMessage = createEmptyCommitMessage();
        long maxCheckpointId = 4L;
        List<CheckpointCommittables> entries =
                Arrays.asList(
                        new CheckpointCommittables(
                                1L,
                                Collections.singletonList(new Committable(1L, commitMessage)),
                                Long.MIN_VALUE),
                        new CheckpointCommittables(
                                2L,
                                Collections.singletonList(new Committable(2L, commitMessage)),
                                Long.MIN_VALUE),
                        new CheckpointCommittables(
                                3L,
                                Collections.singletonList(new Committable(3L, commitMessage)),
                                Long.MIN_VALUE));
        WriterCommittables committables = new WriterCommittables(maxCheckpointId, entries);
        assertThat(committables.getCommittablesBeforeCheckpoint(-1, true).isEmpty()).isTrue();
        assertThat(committables.getCommittablesBeforeCheckpoint(1, false).isEmpty()).isTrue();
        assertThat(committables.getCommittablesBeforeCheckpoint(1, true).isEmpty()).isFalse();
        assertThat(committables.getCommittablesBeforeCheckpoint(3, false).size()).isEqualTo(2);
        assertThat(committables.getCommittablesBeforeCheckpoint(4, false).size()).isEqualTo(3);
    }

    @Test
    public void testReset() {
        CommitMessage commitMessage = createEmptyCommitMessage();
        long maxCheckpointId = 4L;
        List<CheckpointCommittables> entries =
                Arrays.asList(
                        new CheckpointCommittables(
                                1L,
                                Collections.singletonList(new Committable(1L, commitMessage)),
                                Long.MIN_VALUE),
                        new CheckpointCommittables(
                                2L,
                                Collections.singletonList(new Committable(2L, commitMessage)),
                                Long.MIN_VALUE),
                        new CheckpointCommittables(
                                3L,
                                Collections.singletonList(new Committable(3L, commitMessage)),
                                Long.MIN_VALUE));
        WriterCommittables committables = new WriterCommittables(maxCheckpointId, entries);
        committables.reset();
        assertThat(committables.getMaxCheckpointId()).isLessThan(0);
        assertThat(committables.getCommittablesPerCheckpoint().isEmpty()).isTrue();
    }

    @Test
    public void testMergeWith() throws Exception {
        CommitMessage commitMessage = createEmptyCommitMessage();
        long checkpointId = 1L;
        Committable committable = new Committable(checkpointId, commitMessage);
        WriterCommittables committables =
                new WriterCommittables(
                        new CheckpointCommittables(
                                checkpointId,
                                Collections.singletonList(committable),
                                Long.MIN_VALUE));
        // merge an empty checkpoint — the entry survives so the frozen watermark for cp2 is still
        // visible to the coordinator alignment.
        checkpointId++;
        committables.mergeWith(
                new WriterCommittables(
                        new CheckpointCommittables(
                                checkpointId, Collections.emptyList(), Long.MIN_VALUE)));
        NavigableMap<Long, CheckpointCommittables> results =
                committables.getCommittablesPerCheckpoint();
        assertThat(results.size()).isEqualTo(2);
        assertThat(results.get(1L)).isNotNull();
        assertThat(results.get(1L).size()).isEqualTo(1);
        assertThat(results.get(2L)).isNotNull();
        assertThat(results.get(2L).isEmpty()).isTrue();
        assertThat(committables.getMaxCheckpointId()).isEqualTo(2L);
        // merge another new committables
        checkpointId++;
        Committable committable1 = new Committable(checkpointId, commitMessage);
        Committable committable2 = new Committable(checkpointId, commitMessage);
        committables.mergeWith(
                new WriterCommittables(
                        new CheckpointCommittables(
                                checkpointId,
                                Arrays.asList(committable1, committable2),
                                Long.MIN_VALUE)));
        // verify
        assertThat(committables.getMaxCheckpointId()).isEqualTo(3L);
        assertThat(committables.getCommittablesPerCheckpoint().size()).isEqualTo(3);
        // verify checkpoint 1
        assertThat(committables.getCommittablesPerCheckpoint().get(1L)).isNotNull();
        assertThat(committables.getCommittablesPerCheckpoint().get(1L).size()).isEqualTo(1);
        assertThat(
                        committableEquals(
                                committables
                                        .getCommittablesPerCheckpoint()
                                        .get(1L)
                                        .committables()
                                        .get(0),
                                committable))
                .isTrue();
        // verify checkpoint 2 — still present but empty
        assertThat(committables.getCommittablesPerCheckpoint().get(2L)).isNotNull();
        assertThat(committables.getCommittablesPerCheckpoint().get(2L).isEmpty()).isTrue();
        // verify checkpoint 3
        assertThat(committables.getCommittablesPerCheckpoint().get(3L)).isNotNull();
        assertThat(committables.getCommittablesPerCheckpoint().get(3L).size()).isEqualTo(2);
        assertThat(
                        committableEquals(
                                committables
                                        .getCommittablesPerCheckpoint()
                                        .get(3L)
                                        .committables()
                                        .get(0),
                                committable1))
                .isTrue();
        assertThat(
                        committableEquals(
                                committables
                                        .getCommittablesPerCheckpoint()
                                        .get(3L)
                                        .committables()
                                        .get(1),
                                committable2))
                .isTrue();
    }

    @Test
    public void testInvalidMerge() {
        CommitMessage commitMessage = createEmptyCommitMessage();
        long checkpointId = 1024L;
        WriterCommittables committables =
                new WriterCommittables(
                        new CheckpointCommittables(
                                checkpointId,
                                Collections.singletonList(
                                        new Committable(checkpointId, commitMessage)),
                                Long.MIN_VALUE));
        // could not merge same checkpoint id
        assertThrows(IllegalStateException.class, () -> committables.mergeWith(committables));

        long oldCheckpointId = checkpointId - 1;
        WriterCommittables oldCommittables =
                new WriterCommittables(
                        new CheckpointCommittables(
                                oldCheckpointId,
                                Collections.singletonList(
                                        new Committable(oldCheckpointId, commitMessage)),
                                Long.MIN_VALUE));
        assertThrows(IllegalStateException.class, () -> committables.mergeWith(oldCommittables));
    }

    @Test
    public void testClearCommittablesBeforeCheckpoint() {
        CommitMessage commitMessage = createEmptyCommitMessage();
        Committable committableCp1 = new Committable(1L, commitMessage);
        Committable committableCp2 = new Committable(2L, commitMessage);
        Committable committableCp3 = new Committable(3L, commitMessage);
        WriterCommittables committables =
                new WriterCommittables(
                        3L,
                        Arrays.asList(
                                new CheckpointCommittables(
                                        1L,
                                        Collections.singletonList(committableCp1),
                                        Long.MIN_VALUE),
                                new CheckpointCommittables(
                                        2L,
                                        Collections.singletonList(committableCp2),
                                        Long.MIN_VALUE),
                                new CheckpointCommittables(
                                        3L,
                                        Collections.singletonList(committableCp3),
                                        Long.MIN_VALUE)));
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
                                committables
                                        .getCommittablesPerCheckpoint()
                                        .get(2L)
                                        .committables()
                                        .get(0),
                                committableCp2))
                .isTrue();
        assertThat(committables.getCommittablesPerCheckpoint().get(3L)).isNotNull();
        assertThat(committables.getCommittablesPerCheckpoint().get(3L).size()).isEqualTo(1);
        assertThat(
                        committableEquals(
                                committables
                                        .getCommittablesPerCheckpoint()
                                        .get(3L)
                                        .committables()
                                        .get(0),
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
        // entry checkpointId (2L) exceeds declared maxCheckpointId (1L)
        assertThrows(
                IllegalStateException.class,
                () ->
                        new WriterCommittables(
                                1L,
                                Collections.singletonList(
                                        new CheckpointCommittables(
                                                2L,
                                                Collections.singletonList(
                                                        new Committable(2L, commitMessage)),
                                                Long.MIN_VALUE))));
    }

    @Test
    public void testIsIdleAtDistinguishesPresentAndAbsent() {
        WriterCommittables committables =
                new WriterCommittables(
                        new CheckpointCommittables(
                                1L, Collections.emptyList(), 100L, /* idle */ true));
        committables.mergeWith(
                new WriterCommittables(
                        new CheckpointCommittables(
                                2L, Collections.emptyList(), 200L, /* idle */ false)));

        // present + idle=true
        assertThat(committables.isIdleAt(1L)).isTrue();
        assertThat(committables.watermarkAt(1L)).isEqualTo(100L);
        // present + idle=false
        assertThat(committables.isIdleAt(2L)).isFalse();
        assertThat(committables.watermarkAt(2L)).isEqualTo(200L);
        // absent: mirrors Flink valve's "channel exists but never reported" — ACTIVE with MIN_VALUE
        assertThat(committables.isIdleAt(3L)).isFalse();
        assertThat(committables.watermarkAt(3L)).isEqualTo(Long.MIN_VALUE);
    }

    @Test
    public void testDuplicateCheckpointIdRejected() {
        CommitMessage commitMessage = createEmptyCommitMessage();
        // two entries with the same checkpointId must not silently overwrite each other
        assertThrows(
                IllegalStateException.class,
                () ->
                        new WriterCommittables(
                                2L,
                                Arrays.asList(
                                        new CheckpointCommittables(
                                                1L,
                                                Collections.singletonList(
                                                        new Committable(1L, commitMessage)),
                                                Long.MIN_VALUE),
                                        new CheckpointCommittables(
                                                1L,
                                                Collections.singletonList(
                                                        new Committable(1L, commitMessage)),
                                                Long.MIN_VALUE))));
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
