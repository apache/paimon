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

package org.apache.flink.table.store.connector.sink;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.store.connector.VersionedSerializerWrapper;
import org.apache.flink.table.store.data.GenericRow;
import org.apache.flink.table.store.file.manifest.ManifestCommittableSerializer;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.fs.local.LocalFileIO;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.sink.CommitMessage;
import org.apache.flink.table.store.table.sink.StreamTableWrite;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

        StreamTableWrite write = table.newWrite(initialCommitUser);
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
            StreamTableWrite write = table.newWrite(initialCommitUser);
            write.write(GenericRow.of(1, 10L));
            write.write(GenericRow.of(2, 20L));
            for (CommitMessage committable : write.prepareCommit(false, cpId)) {
                testHarness.processElement(
                        new Committable(cpId, Committable.Kind.FILE, committable), 1);
            }
        }

        // checkpoint is completed but not notified, so no snapshot is committed
        testHarness.snapshot(cpId, 1);
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

        // this checkpoint is notified, should be committed
        StreamTableWrite write = table.newWrite(initialCommitUser);
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
        write = table.newWrite(initialCommitUser);
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

    // ------------------------------------------------------------------------
    //  Test utils
    // ------------------------------------------------------------------------

    private OneInputStreamOperatorTestHarness<Committable, Committable>
            createRecoverableTestHarness(FileStoreTable table) throws Exception {
        CommitterOperator operator =
                new CommitterOperator(
                        true,
                        initialCommitUser,
                        user -> new StoreCommitter(table.newCommit(user)),
                        new RestoreAndFailCommittableStateManager(
                                () ->
                                        new VersionedSerializerWrapper<>(
                                                new ManifestCommittableSerializer())));
        return createTestHarness(operator);
    }

    private OneInputStreamOperatorTestHarness<Committable, Committable> createLossyTestHarness(
            FileStoreTable table) throws Exception {
        CommitterOperator operator =
                new CommitterOperator(
                        true,
                        initialCommitUser,
                        user -> new StoreCommitter(table.newCommit(user)),
                        new NoopCommittableStateManager());
        return createTestHarness(operator);
    }

    private OneInputStreamOperatorTestHarness<Committable, Committable> createTestHarness(
            CommitterOperator operator) throws Exception {
        TypeSerializer<Committable> serializer =
                new CommittableTypeInfo().createSerializer(new ExecutionConfig());
        OneInputStreamOperatorTestHarness<Committable, Committable> harness =
                new OneInputStreamOperatorTestHarness<>(operator, serializer);
        harness.setup(serializer);
        return harness;
    }
}
