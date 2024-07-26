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

package org.apache.paimon.flink.sink;

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.utils.ThrowingConsumer;

import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests for {@link AutoTagForSavepointCommitterOperator}. */
public class AutoTagForSavepointCommitterOperatorTest extends CommitterOperatorTest {

    @Test
    public void testAutoTagForSavepoint() throws Exception {
        FileStoreTable table = createFileStoreTable();

        OneInputStreamOperatorTestHarness<Committable, Committable> testHarness =
                createRecoverableTestHarness(table);
        testHarness.open();
        StreamTableWrite write =
                table.newStreamWriteBuilder().withCommitUser(initialCommitUser).newWrite();

        long checkpointId = 1L, timestamp = 1L;
        processCommittable(testHarness, write, checkpointId, timestamp, GenericRow.of(1, 10L));

        // checkpoint is completed but not notified, so no snapshot is committed
        testHarness.snapshotWithLocalState(checkpointId, timestamp, CheckpointType.CHECKPOINT);
        assertThat(table.snapshotManager().latestSnapshotId()).isNull();

        // notify checkpoint success and no tags will be created
        testHarness.notifyOfCompletedCheckpoint(checkpointId);
        assertThat(table.snapshotManager().snapshotCount()).isEqualTo(1);
        assertThat(table.tagManager().tagCount()).isEqualTo(0);

        processCommittable(testHarness, write, ++checkpointId, ++timestamp, GenericRow.of(2, 20L));

        // trigger savepoint but not notified
        testHarness.snapshotWithLocalState(
                checkpointId, timestamp, SavepointType.savepoint(SavepointFormatType.CANONICAL));
        assertThat(table.snapshotManager().snapshotCount()).isEqualTo(1);
        assertThat(table.tagManager().tagCount()).isEqualTo(0);

        // trigger next checkpoint
        processCommittable(testHarness, write, ++checkpointId, ++timestamp, GenericRow.of(3, 20L));
        testHarness.snapshotWithLocalState(checkpointId, timestamp, CheckpointType.CHECKPOINT);
        assertThat(table.snapshotManager().snapshotCount()).isEqualTo(1);

        // notify checkpoint success and tag for savepoint-2
        testHarness.notifyOfCompletedCheckpoint(checkpointId);
        testHarness.close();
        assertThat(table.snapshotManager().snapshotCount()).isEqualTo(3);
        assertThat(table.tagManager().tagCount()).isEqualTo(1);

        Snapshot snapshot = table.snapshotManager().snapshot(2);
        assertThat(snapshot).isNotNull();
        assertThat(snapshot.id()).isEqualTo(2);
        Map<Snapshot, List<String>> tags = table.tagManager().tags();
        assertThat(tags).containsOnlyKeys(snapshot);
        assertThat(tags.get(snapshot))
                .containsOnly(AutoTagForSavepointCommitterOperator.SAVEPOINT_TAG_PREFIX + 2);
    }

    @Test
    public void testRestore() throws Exception {
        FileStoreTable table = createFileStoreTable();

        OneInputStreamOperatorTestHarness<Committable, Committable> testHarness =
                createRecoverableTestHarness(table);
        testHarness.open();
        StreamTableWrite write =
                table.newStreamWriteBuilder().withCommitUser(initialCommitUser).newWrite();

        long checkpointId = 1L, timestamp = 1L;
        processCommittable(testHarness, write, checkpointId, timestamp, GenericRow.of(1, 10L));

        // trigger savepoint but not notified
        OperatorSubtaskState subtaskState =
                testHarness
                        .snapshotWithLocalState(
                                checkpointId,
                                timestamp,
                                SavepointType.savepoint(SavepointFormatType.CANONICAL))
                        .getJobManagerOwnedState();
        assertThat(table.snapshotManager().latestSnapshot()).isNull();
        assertThat(table.tagManager().tagCount()).isEqualTo(0);
        testHarness.close();

        testHarness = createRecoverableTestHarness(table);
        try {
            // commit snapshot from state, fail intentionally
            testHarness.initializeState(subtaskState);
            testHarness.open();
            fail("Expecting intentional exception");
        } catch (Exception e) {
            assertThat(e)
                    .hasMessageContaining(
                            "This exception is intentionally thrown "
                                    + "after committing the restored checkpoints. "
                                    + "By restarting the job we hope that "
                                    + "writers can start writing based on these new commits.");
        }
        testHarness.close();

        Snapshot snapshot = table.snapshotManager().latestSnapshot();
        assertThat(snapshot).isNotNull();
        assertThat(snapshot.id()).isEqualTo(checkpointId);

        Map<Snapshot, List<String>> tags = table.tagManager().tags();
        assertThat(tags).containsOnlyKeys(snapshot);
        assertThat(tags.get(snapshot))
                .containsOnly(
                        AutoTagForSavepointCommitterOperator.SAVEPOINT_TAG_PREFIX + checkpointId);
    }

    @Test
    public void testAbortSavepointAndCleanTag() throws Exception {
        FileStoreTable table = createFileStoreTable();

        OneInputStreamOperatorTestHarness<Committable, Committable> testHarness =
                createRecoverableTestHarness(table);
        testHarness.open();
        StreamTableWrite write =
                table.newStreamWriteBuilder().withCommitUser(initialCommitUser).newWrite();

        long checkpointId = 1L, timestamp = 1L;
        processCommittable(testHarness, write, checkpointId, timestamp, GenericRow.of(1, 10L));

        // trigger savepoint but not notified
        testHarness.snapshotWithLocalState(
                checkpointId, timestamp, SavepointType.savepoint(SavepointFormatType.CANONICAL));
        assertThat(table.snapshotManager().snapshotCount()).isEqualTo(0);
        assertThat(table.tagManager().tagCount()).isEqualTo(0);

        // trigger checkpoint and notify complete
        processCommittable(testHarness, write, ++checkpointId, timestamp, GenericRow.of(1, 10L));
        testHarness.snapshotWithLocalState(checkpointId, timestamp, CheckpointType.CHECKPOINT);
        testHarness.notifyOfCompletedCheckpoint(checkpointId);
        assertThat(table.snapshotManager().snapshotCount()).isEqualTo(2);
        assertThat(table.tagManager().tagCount()).isEqualTo(1);

        // abort savepoint 1
        testHarness.getOneInputOperator().notifyCheckpointAborted(1);
        testHarness.close();

        assertThat(table.snapshotManager().snapshotCount()).isEqualTo(2);
        assertThat(table.tagManager().tagCount()).isEqualTo(0);
    }

    private void processCommittable(
            OneInputStreamOperatorTestHarness<Committable, Committable> testHarness,
            StreamTableWrite write,
            long checkpointId,
            long timestamp,
            InternalRow... rows)
            throws Exception {
        for (InternalRow row : rows) {
            write.write(row);
        }
        for (CommitMessage committable : write.prepareCommit(false, checkpointId)) {
            testHarness.processElement(
                    new Committable(checkpointId, Committable.Kind.FILE, committable), timestamp);
        }
    }

    @Override
    protected OneInputStreamOperator<Committable, Committable> createCommitterOperator(
            FileStoreTable table,
            String commitUser,
            CommittableStateManager<ManifestCommittable> committableStateManager) {
        return new AutoTagForSavepointCommitterOperator<>(
                (CommitterOperator<Committable, ManifestCommittable>)
                        super.createCommitterOperator(table, commitUser, committableStateManager),
                table::snapshotManager,
                table::tagManager,
                table::branchManager,
                () -> table.store().newTagDeletion(),
                () -> table.store().createTagCallbacks(),
                table.store().options().tagDefaultTimeRetained());
    }

    @Override
    protected OneInputStreamOperator<Committable, Committable> createCommitterOperator(
            FileStoreTable table,
            String commitUser,
            CommittableStateManager<ManifestCommittable> committableStateManager,
            ThrowingConsumer<StateInitializationContext, Exception> initializeFunction) {
        return new AutoTagForSavepointCommitterOperator<>(
                (CommitterOperator<Committable, ManifestCommittable>)
                        super.createCommitterOperator(
                                table, commitUser, committableStateManager, initializeFunction),
                table::snapshotManager,
                table::tagManager,
                table::branchManager,
                () -> table.store().newTagDeletion(),
                () -> table.store().createTagCallbacks(),
                table.store().options().tagDefaultTimeRetained());
    }
}
