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

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.VersionedSerializerWrapper;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestCommittableSerializer;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/** Tests for {@link CommitterOperator}. */
public class CommitterOperatorTest extends CommitterOperatorTestBase {

    private String initialCommitUser;

    @BeforeEach
    public void before() {
        super.before();
        initialCommitUser = UUID.randomUUID().toString();
    }

    // ------------------------------------------------------------------------
    //  Recoverable operator tests
    // ------------------------------------------------------------------------

    @Test
    public void testFailIntentionallyAfterRestore() throws Exception {
        FileStoreTable table = createFileStoreTable();

        OneInputStreamOperatorTestHarness<Committable, Committable> testHarness =
                createRecoverableTestHarness(table);
        testHarness.open();
        StreamTableWrite write =
                table.newStreamWriteBuilder().withCommitUser(initialCommitUser).newWrite();
        write.write(GenericRow.of(1, 10L));
        write.write(GenericRow.of(2, 20L));

        long timestamp = 1;
        for (CommitMessage committable : write.prepareCommit(false, 8)) {
            testHarness.processElement(
                    new Committable(8, Committable.Kind.FILE, committable), timestamp++);
        }
        // checkpoint is completed but not notified, so no snapshot is committed
        OperatorSubtaskState snapshot = testHarness.snapshot(0, timestamp++);
        assertThat(table.snapshotManager().latestSnapshotId()).isNull();

        testHarness = createRecoverableTestHarness(table);
        try {
            // commit snapshot from state, fail intentionally
            testHarness.initializeState(snapshot);
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
        assertResults(table, "1, 10", "2, 20");

        // snapshot is successfully committed, no failure is needed
        testHarness = createRecoverableTestHarness(table);
        testHarness.initializeState(snapshot);
        testHarness.open();
        assertResults(table, "1, 10", "2, 20");
    }

    @Test
    public void testCheckpointAbort() throws Exception {
        FileStoreTable table = createFileStoreTable();
        OneInputStreamOperatorTestHarness<Committable, Committable> testHarness =
                createRecoverableTestHarness(table);
        testHarness.open();

        // files from multiple checkpoint
        // but no snapshot
        long cpId = 0;
        for (int i = 0; i < 10; i++) {
            cpId++;
            StreamTableWrite write =
                    table.newStreamWriteBuilder().withCommitUser(initialCommitUser).newWrite();
            write.write(GenericRow.of(1, 10L));
            write.write(GenericRow.of(2, 20L));
            for (CommitMessage committable : write.prepareCommit(false, cpId)) {
                testHarness.processElement(
                        new Committable(cpId, Committable.Kind.FILE, committable), 1);
            }
        }

        // checkpoint is completed but not notified, so no snapshot is committed
        testHarness.notifyOfCompletedCheckpoint(cpId);

        SnapshotManager snapshotManager = new SnapshotManager(LocalFileIO.create(), tablePath);

        // should create 10 snapshots
        assertThat(snapshotManager.latestSnapshotId()).isEqualTo(cpId);
    }

    // ------------------------------------------------------------------------
    //  Lossy operator tests
    // ------------------------------------------------------------------------

    @Test
    public void testSnapshotLostWhenFailed() throws Exception {
        FileStoreTable table = createFileStoreTable();

        OneInputStreamOperatorTestHarness<Committable, Committable> testHarness =
                createLossyTestHarness(table);
        testHarness.open();

        long timestamp = 1;

        StreamWriteBuilder streamWriteBuilder =
                table.newStreamWriteBuilder().withCommitUser(initialCommitUser);
        // this checkpoint is notified, should be committed
        StreamTableWrite write = streamWriteBuilder.newWrite();
        write.write(GenericRow.of(1, 10L));
        write.write(GenericRow.of(2, 20L));
        for (CommitMessage committable : write.prepareCommit(false, 1)) {
            testHarness.processElement(
                    new Committable(1, Committable.Kind.FILE, committable), timestamp++);
        }
        testHarness.snapshot(1, timestamp++);
        testHarness.notifyOfCompletedCheckpoint(1);

        // this checkpoint is not notified, should not be committed
        write.write(GenericRow.of(3, 30L));
        write.write(GenericRow.of(4, 40L));
        for (CommitMessage committable : write.prepareCommit(false, 2)) {
            testHarness.processElement(
                    new Committable(2, Committable.Kind.FILE, committable), timestamp++);
        }
        OperatorSubtaskState snapshot = testHarness.snapshot(2, timestamp++);

        // reopen test harness
        write.close();
        testHarness.close();

        testHarness = createLossyTestHarness(table);
        testHarness.initializeState(snapshot);
        testHarness.open();

        // this checkpoint is notified, should be committed
        write = streamWriteBuilder.newWrite();
        write.write(GenericRow.of(5, 50L));
        write.write(GenericRow.of(6, 60L));
        for (CommitMessage committable : write.prepareCommit(false, 3)) {
            testHarness.processElement(
                    new Committable(3, Committable.Kind.FILE, committable), timestamp++);
        }
        testHarness.snapshot(3, timestamp++);
        testHarness.notifyOfCompletedCheckpoint(3);

        write.close();
        testHarness.close();

        assertResults(table, "1, 10", "2, 20", "5, 50", "6, 60");
    }

    @Test
    public void testRestoreCommitUser() throws Exception {

        FileStoreTable table = createFileStoreTable();
        String commitUser = UUID.randomUUID().toString();

        // 1. Generate operatorSubtaskState
        List<OperatorSubtaskState> operatorSubtaskStates = new ArrayList<>();

        long timestamp = 1;
        long checkpoint = 1;
        for (int i = 0; i < 5; i++) {
            OneInputStreamOperatorTestHarness<Committable, Committable> testHarness =
                    createLossyTestHarness(table, commitUser);

            testHarness.open();
            OperatorSubtaskState snapshot =
                    writeAndSnapshot(table, commitUser, timestamp, ++checkpoint, testHarness);
            operatorSubtaskStates.add(snapshot);
        }

        // 2. Clearing redundant union list state
        OperatorSubtaskState operatorSubtaskState =
                AbstractStreamOperatorTestHarness.repackageState(
                        operatorSubtaskStates.toArray(new OperatorSubtaskState[0]));

        OneInputStreamOperatorTestHarness<Committable, Committable> testHarness =
                createLossyTestHarness(table);
        testHarness.initializeState(operatorSubtaskState);
        OperatorSubtaskState snapshot =
                writeAndSnapshot(table, initialCommitUser, timestamp, ++checkpoint, testHarness);

        // 3. Check whether success
        List<String> actual = new ArrayList<>();

        CommitterOperator<Committable, ManifestCommittable> operator =
                new CommitterOperator<Committable, ManifestCommittable>(
                        true,
                        initialCommitUser,
                        user ->
                                new StoreCommitter(
                                        table.newStreamWriteBuilder()
                                                .withCommitUser(user)
                                                .newCommit()),
                        new NoopCommittableStateManager()) {
                    @Override
                    public void initializeState(StateInitializationContext context)
                            throws Exception {
                        ListState<String> state =
                                context.getOperatorStateStore()
                                        .getUnionListState(
                                                new ListStateDescriptor<>(
                                                        "commit_user_state", String.class));
                        state.get().forEach(actual::add);
                    }
                };

        OneInputStreamOperatorTestHarness<Committable, Committable> testHarness1 =
                createTestHarness(operator);
        testHarness1.initializeState(snapshot);

        Assertions.assertThat(actual.size()).isEqualTo(1);

        Assertions.assertThat(actual).hasSameElementsAs(Lists.newArrayList(commitUser));
    }

    private static OperatorSubtaskState writeAndSnapshot(
            FileStoreTable table,
            String commitUser,
            long timestamp,
            long checkpoint,
            OneInputStreamOperatorTestHarness<Committable, Committable> testHarness)
            throws Exception {
        StreamWriteBuilder streamWriteBuilder = table.newStreamWriteBuilder();

        StreamTableWrite write = streamWriteBuilder.withCommitUser(commitUser).newWrite();
        write.write(GenericRow.of(1, 10L));
        for (CommitMessage committable : write.prepareCommit(false, 1)) {
            testHarness.processElement(
                    new Committable(checkpoint, Committable.Kind.FILE, committable), ++timestamp);
        }
        OperatorSubtaskState snapshot = testHarness.snapshot(checkpoint, ++timestamp);
        return snapshot;
    }

    @Test
    public void testWatermarkCommit() throws Exception {
        FileStoreTable table = createFileStoreTable();

        OneInputStreamOperatorTestHarness<Committable, Committable> testHarness =
                createRecoverableTestHarness(table);
        testHarness.open();
        long timestamp = 0;
        StreamTableWrite write =
                table.newStreamWriteBuilder().withCommitUser(initialCommitUser).newWrite();

        long cpId = 1;
        write.write(GenericRow.of(1, 10L));
        testHarness.processElement(
                new Committable(
                        cpId, Committable.Kind.FILE, write.prepareCommit(true, cpId).get(0)),
                timestamp++);
        testHarness.processWatermark(new Watermark(1024));
        testHarness.snapshot(cpId, timestamp++);
        testHarness.notifyOfCompletedCheckpoint(cpId);
        assertThat(table.snapshotManager().latestSnapshot().watermark()).isEqualTo(1024L);

        cpId = 2;
        write.write(GenericRow.of(1, 20L));
        testHarness.processElement(
                new Committable(
                        cpId, Committable.Kind.FILE, write.prepareCommit(true, cpId).get(0)),
                timestamp++);
        testHarness.processWatermark(new Watermark(Long.MAX_VALUE));
        testHarness.snapshot(cpId, timestamp++);
        testHarness.notifyOfCompletedCheckpoint(cpId);
        assertThat(table.snapshotManager().latestSnapshot().watermark()).isEqualTo(1024L);
    }

    // ------------------------------------------------------------------------
    //  Test utils
    // ------------------------------------------------------------------------

    private OneInputStreamOperatorTestHarness<Committable, Committable>
            createRecoverableTestHarness(FileStoreTable table) throws Exception {
        CommitterOperator<Committable, ManifestCommittable> operator =
                new CommitterOperator<>(
                        true,
                        initialCommitUser,
                        user ->
                                new StoreCommitter(
                                        table.newStreamWriteBuilder()
                                                .withCommitUser(user)
                                                .newCommit()),
                        new RestoreAndFailCommittableStateManager<>(
                                () ->
                                        new VersionedSerializerWrapper<>(
                                                new ManifestCommittableSerializer())));
        return createTestHarness(operator);
    }

    private OneInputStreamOperatorTestHarness<Committable, Committable> createLossyTestHarness(
            FileStoreTable table) throws Exception {
        return createLossyTestHarness(table, null);
    }

    private OneInputStreamOperatorTestHarness<Committable, Committable> createLossyTestHarness(
            FileStoreTable table, String commitUser) throws Exception {
        CommitterOperator<Committable, ManifestCommittable> operator =
                new CommitterOperator<>(
                        true,
                        commitUser == null ? initialCommitUser : commitUser,
                        user ->
                                new StoreCommitter(
                                        table.newStreamWriteBuilder()
                                                .withCommitUser(user)
                                                .newCommit()),
                        new NoopCommittableStateManager());
        return createTestHarness(operator);
    }

    private OneInputStreamOperatorTestHarness<Committable, Committable> createTestHarness(
            CommitterOperator<Committable, ManifestCommittable> operator) throws Exception {
        TypeSerializer<Committable> serializer =
                new CommittableTypeInfo().createSerializer(new ExecutionConfig());
        OneInputStreamOperatorTestHarness<Committable, Committable> harness =
                new OneInputStreamOperatorTestHarness<>(operator, serializer);
        harness.setup(serializer);
        return harness;
    }
}
