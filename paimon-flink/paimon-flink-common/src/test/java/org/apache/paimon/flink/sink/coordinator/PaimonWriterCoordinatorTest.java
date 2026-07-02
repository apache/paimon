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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.CommittableSerializer;
import org.apache.paimon.flink.sink.Committer;
import org.apache.paimon.flink.sink.StoreCommitter;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.util.ExceptionUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

/** Tests for {@link PaimonWriterCoordinator}. */
public class PaimonWriterCoordinatorTest {

    private static final long CK0 = 7L;
    private static final long CK1 = 8L;
    private static final long CK2 = 9L;
    private static final RowType ROW_TYPE =
            RowType.of(
                    new DataType[] {DataTypes.INT(), DataTypes.BIGINT()}, new String[] {"a", "b"});

    @TempDir public java.nio.file.Path tempDir;

    private Path tablePath;
    private OperatorID operatorId;
    private String commitUser;

    @BeforeEach
    public void before() {
        tablePath = new Path(tempDir.toString());
        operatorId = new OperatorID();
        commitUser = UUID.randomUUID().toString();
    }

    // ------------------------------------------------------------------------
    //  basic function tests
    // ------------------------------------------------------------------------

    @Test
    public void testCommitUserRestoredFromCoordinatorState() throws Exception {
        FileStoreTable table = createFileStoreTable();
        String restoredCommitUser = commitUser;

        PaimonWriterCoordinator previous = createCoordinator(table, 1);
        previous.start();
        byte[] coordinatorState = checkpointState(previous, CK1);
        previous.close();

        commitUser = UUID.randomUUID().toString();
        PaimonWriterCoordinator restored = createCoordinator(table, 1);
        restored.resetToCheckpoint(CK1, coordinatorState);
        restored.start();
        register(restored, 0);

        sendRequest(
                restored,
                recoveredFileInfoRequest(CK1, 0, restoredCommitUser, Collections.emptyList()));
        waitForCoordinator(restored);

        sendRequest(restored, fileInfoRequest(table, CK2, 0, GenericRow.of(1, 10L)));
        restored.notifyCheckpointComplete(CK2);
        waitForCoordinator(restored);
        assertThat(table.snapshotManager().latestSnapshot().commitUser())
                .isEqualTo(restoredCommitUser);
        restored.close();
    }

    @Test
    public void testFileInfoRequestAcksAfterReceive() throws Exception {
        FileStoreTable table = createFileStoreTable();
        PaimonWriterCoordinator coordinator = createCoordinator(table, 1);
        coordinator.start();
        register(coordinator, 0);

        FileInfoRequest request = fileInfoRequest(table, CK1, 0, 0, GenericRow.of(1, 10L));
        CoordinationResponse rawResponse = coordinator.handleCoordinationRequest(request).get();
        FileInfoReceivedResponse response = CoordinationResponseUtils.unwrap(rawResponse);
        assertThat(response.checkpointId()).isEqualTo(CK1);
        assertThat(response.subtaskId()).isEqualTo(0);

        coordinator.notifyCheckpointComplete(CK1);
        waitForCoordinator(coordinator);
        assertResults(table, "1, 10");

        coordinator.close();
    }

    @Test
    public void testFileInfoRequestFromStaleAttemptIsIgnored() throws Exception {
        FileStoreTable table = createFileStoreTable();
        PaimonWriterCoordinator coordinator = createCoordinator(table, 1);
        coordinator.start();
        register(coordinator, 0, 1);

        FileInfoRequest request = fileInfoRequest(table, CK1, 0, 0, GenericRow.of(1, 10L));
        CompletableFuture<CoordinationResponse> response =
                coordinator.handleCoordinationRequest(request);
        assertThatThrownBy(response::get)
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(IllegalStateException.class)
                .hasMessageContaining("invalid subtask 0 attempt 0");

        coordinator.notifyCheckpointComplete(CK1);
        waitForCoordinator(coordinator);
        assertThat(table.snapshotManager().latestSnapshotId()).isNull();

        coordinator.close();
    }

    @Test
    public void testCheckpointCompleteRequiresStagedFileInfo() throws Exception {
        FileStoreTable table = createFileStoreTable();
        OperatorCoordinator.Context context = createContext(2);
        PaimonWriterCoordinator coordinator = createCoordinator(table, context);
        coordinator.start();
        register(coordinator, 0);
        register(coordinator, 1);

        sendRequest(coordinator, fileInfoRequest(table, CK1, 0, GenericRow.of(1, 10L)));
        coordinator.notifyCheckpointComplete(CK1);
        waitForCoordinator(coordinator);

        Mockito.verify(context).failJob(Mockito.any(IllegalStateException.class));
        assertThat(table.snapshotManager().latestSnapshotId()).isNull();
        coordinator.close();
    }

    @Test
    public void testCheckpointWatermarkUsesMinWriterWatermark() throws Exception {
        FileStoreTable table = createFileStoreTable();
        PaimonWriterCoordinator coordinator = createCoordinator(table, 2);
        coordinator.start();

        register(coordinator, 0);
        register(coordinator, 1);
        sendRequest(coordinator, fileInfoRequest(table, CK1, 0, 0, 1000L, GenericRow.of(1, 10L)));
        sendRequest(coordinator, fileInfoRequest(table, CK1, 1, 0, 0L, GenericRow.of(2, 20L)));
        coordinator.notifyCheckpointComplete(CK1);
        waitForCoordinator(coordinator);

        assertThat(table.snapshotManager().latestSnapshot().watermark()).isEqualTo(0L);
        coordinator.close();
    }

    // ------------------------------------------------------------------------
    //  restore and recovered file-info tests
    // ------------------------------------------------------------------------

    @Test
    public void testRecoveredFileInfoWithoutCoordinatorRestoreWaitsForCheckpointComplete()
            throws Exception {
        FileStoreTable table = createFileStoreTable();
        PaimonWriterCoordinator coordinator = createCoordinator(table, 1);
        coordinator.start();
        register(coordinator, 0);

        sendRequest(
                coordinator,
                recoveredFileInfoRequest(
                        CK1, 0, commitUser, committables(table, CK1, GenericRow.of(1, 10L))));
        waitForCoordinator(coordinator);
        assertThat(table.snapshotManager().latestSnapshotId()).isNull();

        coordinator.notifyCheckpointComplete(CK1);
        waitForCoordinator(coordinator);
        assertResults(table, "1, 10");

        coordinator.close();
    }

    @Test
    public void testRecoveredFileInfoCommitsPendingCommittables() throws Exception {
        FileStoreTable table = createFileStoreTable();
        byte[] coordinatorState = createCoordinatorState(table, CK2);
        OperatorCoordinator.Context context = createContext(2);
        PaimonWriterCoordinator coordinator = createCoordinator(table, context);
        coordinator.resetToCheckpoint(CK2, coordinatorState);
        coordinator.start();
        register(coordinator, 0);
        register(coordinator, 1);

        sendRequest(
                coordinator,
                recoveredFileInfoRequest(
                        CK2, 0, commitUser, committables(table, CK1, GenericRow.of(1, 10L))));
        waitForCoordinator(coordinator);
        assertThat(table.snapshotManager().latestSnapshotId()).isNull();

        sendRequest(
                coordinator,
                recoveredFileInfoRequest(
                        CK2, 1, commitUser, committables(table, CK1, GenericRow.of(2, 20L))));
        waitForCoordinator(coordinator);

        Mockito.verify(context).failJob(Mockito.any(Throwable.class));
        assertResults(table, "1, 10", "2, 20");

        coordinator.close();
    }

    /**
     * Restored file info is recommitted and triggers the expected recovery failure, after which a
     * newer checkpoint is reported and completed normally.
     */
    @Test
    public void testCheckpointAfterRestoredCommitDoesNotFailJob() throws Exception {
        FileStoreTable table = createFileStoreTable();
        byte[] coordinatorState = createCoordinatorState(table, CK1);
        OperatorCoordinator.Context context = createContext(1);
        PaimonWriterCoordinator coordinator = createCoordinator(table, context);
        coordinator.resetToCheckpoint(CK1, coordinatorState);
        coordinator.start();
        register(coordinator, 0);

        sendRequest(
                coordinator,
                recoveredFileInfoRequest(
                        CK1, 0, commitUser, committables(table, CK1, GenericRow.of(1, 10L))));
        waitForCoordinator(coordinator);
        Mockito.verify(context).failJob(Mockito.any(Throwable.class));
        assertResults(table, "1, 10");

        sendRequest(coordinator, fileInfoRequest(table, CK2, 0, GenericRow.of(2, 20L)));
        coordinator.notifyCheckpointComplete(CK2);
        waitForCoordinator(coordinator);

        Mockito.verify(context, Mockito.times(1)).failJob(Mockito.any(Throwable.class));
        assertResults(table, "1, 10", "2, 20");

        coordinator.close();
    }

    /**
     * At a restored checkpoint, one subtask reports data and another reports an empty recovered
     * file-info payload.
     */
    @Test
    public void testEmptyRestoredFileInfoCompletesRestoredCheckpoint() throws Exception {
        FileStoreTable table = createFileStoreTable();
        byte[] coordinatorState = createCoordinatorState(table, CK1);
        OperatorCoordinator.Context context = createContext(2);
        PaimonWriterCoordinator coordinator = createCoordinator(table, context);
        coordinator.resetToCheckpoint(CK1, coordinatorState);
        coordinator.start();
        register(coordinator, 0);
        register(coordinator, 1);

        sendRequest(
                coordinator,
                recoveredFileInfoRequest(
                        CK1, 0, commitUser, committables(table, CK1, GenericRow.of(1, 10L))));
        waitForCoordinator(coordinator);
        assertThat(table.snapshotManager().latestSnapshotId()).isNull();

        sendRequest(
                coordinator, recoveredFileInfoRequest(CK1, 1, commitUser, Collections.emptyList()));
        waitForCoordinator(coordinator);

        Mockito.verify(context).failJob(Mockito.any(Throwable.class));
        assertResults(table, "1, 10");

        coordinator.close();
    }

    /**
     * During recovery to CK2, one recovered request cumulatively carries CK1 committables and the
     * other recovered request is empty.
     */
    @Test
    public void testEmptyFileInfoCompletesEarlierCheckpointBeforeRestoredCommit() throws Exception {
        FileStoreTable table = createFileStoreTable();
        byte[] coordinatorState = createCoordinatorState(table, CK2);
        OperatorCoordinator.Context context = createContext(2);
        PaimonWriterCoordinator coordinator = createCoordinator(table, context);
        coordinator.resetToCheckpoint(CK2, coordinatorState);
        coordinator.start();
        register(coordinator, 0);
        register(coordinator, 1);

        List<Committable> ck1Subtask0 = committables(table, CK1, GenericRow.of(1, 10L));
        sendRequest(coordinator, recoveredFileInfoRequest(CK2, 0, commitUser, ck1Subtask0));
        waitForCoordinator(coordinator);
        assertThat(table.snapshotManager().latestSnapshotId()).isNull();

        sendRequest(
                coordinator, recoveredFileInfoRequest(CK2, 1, commitUser, Collections.emptyList()));
        waitForCoordinator(coordinator);

        Mockito.verify(context).failJob(Mockito.any(Throwable.class));
        assertResults(table, "1, 10");

        coordinator.close();
    }

    // ------------------------------------------------------------------------
    //  abort and late-arrival tests
    // ------------------------------------------------------------------------

    /**
     * CK0 commit, CK1 abort but Task not fail, CK2 commit The aborted checkpoint creates no table
     * data, while completing the next checkpoint commits both the re-reported and newly produced
     * rows exactly once.
     */
    @Test
    public void testCheckpointAbortDoesNotDropCommittables() throws Exception {
        FileStoreTable table = createFileStoreTable();
        PaimonWriterCoordinator coordinator = createCoordinator(table, 1);
        coordinator.start();

        sendCheckpoint(coordinator, table, CK0, 0, GenericRow.of(0, 0L));
        coordinator.notifyCheckpointComplete(CK0);
        waitForCoordinator(coordinator);
        assertResults(table, "0, 0");

        List<Committable> ck1Committables = committables(table, CK1, GenericRow.of(1, 10L));
        sendRequest(coordinator, fileInfoRequest(CK1, 0, ck1Committables));
        coordinator.notifyCheckpointAborted(CK1);
        waitForCoordinator(coordinator);
        assertResults(table, "0, 0");

        sendRequest(
                coordinator,
                fileInfoRequest(CK2, 0, committables(table, CK2, GenericRow.of(2, 20L))));
        coordinator.notifyCheckpointComplete(CK2);
        waitForCoordinator(coordinator);
        assertResults(table, "0, 0", "1, 10", "2, 20");

        coordinator.close();
    }

    /** Only one of two subtasks reports a checkpoint before it is aborted. */
    @Test
    public void testPartialCheckpointAbortDoesNotFailJob() throws Exception {
        FileStoreTable table = createFileStoreTable();
        OperatorCoordinator.Context context = createContext(2);
        PaimonWriterCoordinator coordinator = createCoordinator(table, context);
        coordinator.start();

        register(coordinator, 0);
        register(coordinator, 1);
        List<Committable> subtask0Ck1 = committables(table, CK1, GenericRow.of(1, 10L));
        sendRequest(coordinator, fileInfoRequest(CK1, 0, subtask0Ck1));
        waitForCoordinator(coordinator);

        coordinator.notifyCheckpointAborted(CK1);
        waitForCoordinator(coordinator);
        Mockito.verify(context, Mockito.never()).failJob(Mockito.any(Throwable.class));
        assertThat(table.snapshotManager().latestSnapshotId()).isNull();

        sendRequest(
                coordinator,
                fileInfoRequest(CK2, 0, committables(table, CK2, GenericRow.of(2, 20L))));
        sendRequest(
                coordinator,
                fileInfoRequest(CK2, 1, committables(table, CK2, GenericRow.of(3, 30L))));
        coordinator.notifyCheckpointComplete(CK2);
        waitForCoordinator(coordinator);

        Mockito.verify(context, Mockito.never()).failJob(Mockito.any(Throwable.class));
        assertResults(table, "1, 10", "2, 20", "3, 30");

        coordinator.close();
    }

    /**
     * A file-info request arrives after the corresponding checkpoint has already been aborted. The
     * late request is retained as reliable pending file info and committed by the next complete
     * checkpoint envelope.
     */
    @Test
    public void testFileInfoAfterCheckpointAbortIsCommittedByLaterCheckpoint() throws Exception {
        FileStoreTable table = createFileStoreTable();
        OperatorCoordinator.Context context = createContext(2);
        PaimonWriterCoordinator coordinator = createCoordinator(table, context);
        coordinator.start();

        register(coordinator, 0);
        register(coordinator, 1);
        coordinator.notifyCheckpointAborted(CK1);
        waitForCoordinator(coordinator);

        sendRequest(coordinator, fileInfoRequest(table, CK1, 0, GenericRow.of(1, 10L)));
        waitForCoordinator(coordinator);

        Mockito.verify(context, Mockito.never()).failJob(Mockito.any(Throwable.class));
        assertThat(table.snapshotManager().latestSnapshotId()).isNull();

        sendRequest(coordinator, fileInfoRequest(table, CK2, 0, GenericRow.of(2, 20L)));
        sendRequest(coordinator, fileInfoRequest(table, CK2, 1, GenericRow.of(3, 30L)));
        coordinator.notifyCheckpointComplete(CK2);
        waitForCoordinator(coordinator);

        Mockito.verify(context, Mockito.never()).failJob(Mockito.any(Throwable.class));
        assertResults(table, "1, 10", "2, 20", "3, 30");

        coordinator.close();
    }

    /**
     * Scenario: PWC collects a checkpoint's file info, the checkpoint is aborted, and the same
     * attempt reports the same envelope again. Under ack-based reporting this is a protocol error:
     * if the first ACK was lost, the writer snapshot cannot complete and the attempt must failover.
     */
    @Test
    public void testDuplicateFileInfoAfterCollectedAbortIsRejected() throws Exception {
        FileStoreTable table = createFileStoreTable();
        OperatorCoordinator.Context context = createContext(1);
        PaimonWriterCoordinator coordinator = createCoordinator(table, context);
        coordinator.start();
        register(coordinator, 0);

        FileInfoRequest request = fileInfoRequest(table, CK1, 0, GenericRow.of(1, 10L));
        sendRequest(coordinator, request);
        waitForCoordinator(coordinator);
        coordinator.notifyCheckpointAborted(CK1);
        waitForCoordinator(coordinator);

        assertThatThrownBy(() -> sendRequest(coordinator, request))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Repeated file info envelope");

        coordinator.close();
    }

    /**
     * An earlier checkpoint has file info from only one subtask, while a later checkpoint receives
     * complete cumulative file info from all subtasks.
     */
    @Test
    public void testLaterCheckpointCompleteCanCommitEarlierPartialFileInfo() throws Exception {
        FileStoreTable table = createFileStoreTable();
        OperatorCoordinator.Context context = createContext(2);
        PaimonWriterCoordinator coordinator = createCoordinator(table, context);
        coordinator.start();

        register(coordinator, 0);
        register(coordinator, 1);
        List<Committable> ck1Subtask0 = committables(table, CK1, GenericRow.of(1, 10L));
        sendRequest(coordinator, fileInfoRequest(CK1, 0, ck1Subtask0));
        waitForCoordinator(coordinator);

        sendRequest(
                coordinator,
                fileInfoRequest(CK2, 0, committables(table, CK2, GenericRow.of(2, 20L))));
        sendRequest(
                coordinator,
                fileInfoRequest(CK2, 1, committables(table, CK2, GenericRow.of(3, 30L))));
        coordinator.notifyCheckpointComplete(CK2);
        waitForCoordinator(coordinator);

        Mockito.verify(context, Mockito.never()).failJob(Mockito.any(Throwable.class));
        assertResults(table, "1, 10", "2, 20", "3, 30");

        coordinator.close();
    }

    // ------------------------------------------------------------------------
    //  attempt and stale-message tests
    // ------------------------------------------------------------------------

    @Test
    public void testSubtaskFailoverReplacesUnstagedPendingFileInfo() throws Exception {
        FileStoreTable table = createFileStoreTable();
        PaimonWriterCoordinator coordinator = createCoordinator(table, 2);
        coordinator.start();

        register(coordinator, 0, 0);
        register(coordinator, 1, 0);
        sendRequest(coordinator, fileInfoRequest(table, CK1, 0, 0, GenericRow.of(1, 10L)));
        waitForCoordinator(coordinator);

        coordinator.executionAttemptFailed(0, 0, new RuntimeException("failover"));
        waitForCoordinator(coordinator);
        register(coordinator, 0, 1);

        List<Committable> recoveredSubtask0 = new ArrayList<>();
        recoveredSubtask0.addAll(committables(table, CK1, GenericRow.of(9, 90L)));
        recoveredSubtask0.addAll(committables(table, CK2, GenericRow.of(2, 20L)));
        sendRequest(coordinator, fileInfoRequest(CK2, 0, 1, recoveredSubtask0));
        sendRequest(coordinator, fileInfoRequest(table, CK2, 1, 0, GenericRow.of(3, 30L)));
        coordinator.notifyCheckpointComplete(CK2);
        waitForCoordinator(coordinator);

        assertResults(table, "2, 20", "3, 30", "9, 90");
        coordinator.close();
    }

    @Test
    public void testSubtaskFailoverReplacesFullyReceivedButUncompletedFileInfo() throws Exception {
        FileStoreTable table = createFileStoreTable();
        PaimonWriterCoordinator coordinator = createCoordinator(table, 2);
        coordinator.start();

        register(coordinator, 0, 0);
        register(coordinator, 1, 0);
        sendRequest(coordinator, fileInfoRequest(table, CK1, 0, 0, GenericRow.of(1, 10L)));
        sendRequest(coordinator, fileInfoRequest(table, CK1, 1, 0, GenericRow.of(4, 40L)));
        waitForCoordinator(coordinator);

        coordinator.executionAttemptFailed(0, 0, new RuntimeException("failover"));
        waitForCoordinator(coordinator);
        register(coordinator, 0, 1);

        List<Committable> recoveredSubtask0 = new ArrayList<>();
        recoveredSubtask0.addAll(committables(table, CK1, GenericRow.of(9, 90L)));
        recoveredSubtask0.addAll(committables(table, CK2, GenericRow.of(2, 20L)));
        sendRequest(coordinator, fileInfoRequest(CK2, 0, 1, recoveredSubtask0));
        sendRequest(coordinator, fileInfoRequest(table, CK2, 1, 0, GenericRow.of(3, 30L)));
        coordinator.notifyCheckpointComplete(CK2);
        waitForCoordinator(coordinator);

        assertResults(table, "2, 20", "3, 30", "4, 40", "9, 90");
        coordinator.close();
    }

    /**
     * A writer resends file info for a checkpoint that PWC has already committed. The table remains
     * committed once, and the registered gateway receives a second commit-complete event in
     * response to the stale resend, in addition to the original commit notification.
     */
    @Test
    public void testStaleFileInfoResendSendsCommitCompleteEvent() throws Exception {
        FileStoreTable table = createFileStoreTable();
        PaimonWriterCoordinator coordinator = createCoordinator(table, 1);
        coordinator.start();
        OperatorCoordinator.SubtaskGateway gateway = registerAndReturnGateway(coordinator, 0);

        FileInfoRequest request = fileInfoRequest(table, CK1, 0, GenericRow.of(1, 10L));
        sendRequest(coordinator, request);
        coordinator.notifyCheckpointComplete(CK1);
        waitForCoordinator(coordinator);
        assertResults(table, "1, 10");

        sendRequest(coordinator, request);
        waitForCoordinator(coordinator);

        Mockito.verify(gateway, Mockito.times(2)).sendEvent(Mockito.any(CommitCompleteEvent.class));

        coordinator.close();
    }

    private void sendCheckpoint(
            PaimonWriterCoordinator coordinator,
            FileStoreTable table,
            long checkpointId,
            int subtask,
            GenericRow... rows)
            throws Exception {
        register(coordinator, subtask);
        sendRequest(coordinator, fileInfoRequest(table, checkpointId, subtask, rows));
        waitForCoordinator(coordinator);
    }

    private FileInfoReceivedResponse sendRequest(
            PaimonWriterCoordinator coordinator, FileInfoRequest request) throws Exception {
        CoordinationResponse rawResponse = coordinator.handleCoordinationRequest(request).get();
        return CoordinationResponseUtils.unwrap(rawResponse);
    }

    private byte[] checkpointState(PaimonWriterCoordinator coordinator, long checkpointId)
            throws Exception {
        CompletableFuture<byte[]> result = new CompletableFuture<>();
        coordinator.checkpointCoordinator(checkpointId, result);
        return result.get();
    }

    private byte[] createCoordinatorState(FileStoreTable table, long checkpointId)
            throws Exception {
        PaimonWriterCoordinator coordinator = createCoordinator(table, 1);
        coordinator.start();
        byte[] state = checkpointState(coordinator, checkpointId);
        coordinator.close();
        return state;
    }

    private void register(PaimonWriterCoordinator coordinator, int subtask) {
        register(coordinator, subtask, 0);
    }

    private void register(PaimonWriterCoordinator coordinator, int subtask, int attemptNumber) {
        OperatorCoordinator.SubtaskGateway gateway =
                Mockito.mock(OperatorCoordinator.SubtaskGateway.class);
        when(gateway.sendEvent(Mockito.any(OperatorEvent.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
        coordinator.executionAttemptReady(subtask, attemptNumber, gateway);
    }

    private OperatorCoordinator.SubtaskGateway registerAndReturnGateway(
            PaimonWriterCoordinator coordinator, int subtask) {
        OperatorCoordinator.SubtaskGateway gateway =
                Mockito.mock(OperatorCoordinator.SubtaskGateway.class);
        when(gateway.sendEvent(Mockito.any(OperatorEvent.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
        coordinator.executionAttemptReady(subtask, 0, gateway);
        return gateway;
    }

    private FileInfoRequest fileInfoRequest(
            FileStoreTable table, long checkpointId, int subtask, GenericRow... rows)
            throws Exception {
        return fileInfoRequest(table, checkpointId, subtask, 0, rows);
    }

    private FileInfoRequest fileInfoRequest(
            FileStoreTable table,
            long checkpointId,
            int subtask,
            int attemptNumber,
            GenericRow... rows)
            throws Exception {
        return fileInfoRequest(table, checkpointId, subtask, attemptNumber, Long.MIN_VALUE, rows);
    }

    private FileInfoRequest fileInfoRequest(
            FileStoreTable table,
            long checkpointId,
            int subtask,
            int attemptNumber,
            long watermark,
            GenericRow... rows)
            throws Exception {
        return fileInfoRequest(
                checkpointId,
                subtask,
                attemptNumber,
                watermark,
                committables(table, checkpointId, rows));
    }

    private FileInfoRequest fileInfoRequest(
            long checkpointId, int subtask, List<Committable> committables) throws Exception {
        return fileInfoRequest(checkpointId, subtask, 0, committables);
    }

    private FileInfoRequest fileInfoRequest(
            long checkpointId, int subtask, int attemptNumber, List<Committable> committables)
            throws Exception {
        return fileInfoRequest(checkpointId, subtask, attemptNumber, Long.MIN_VALUE, committables);
    }

    private FileInfoRequest fileInfoRequest(
            long checkpointId,
            int subtask,
            int attemptNumber,
            long watermark,
            List<Committable> committables)
            throws Exception {
        return FileInfoRequest.fileInfo(
                checkpointId,
                subtask,
                attemptNumber,
                watermark,
                serialize(committables),
                committables.size());
    }

    private FileInfoRequest recoveredFileInfoRequest(
            long checkpointId,
            int subtask,
            String recoveredCommitUser,
            List<Committable> committables)
            throws Exception {
        return FileInfoRequest.recoveredFileInfo(
                checkpointId,
                subtask,
                0,
                Long.MIN_VALUE,
                serialize(committables),
                committables.size(),
                recoveredCommitUser);
    }

    private List<Committable> committables(
            FileStoreTable table, long checkpointId, GenericRow... rows) throws Exception {
        StreamTableWrite write =
                table.newStreamWriteBuilder().withCommitUser(commitUser).newWrite();
        for (GenericRow row : rows) {
            write.write(row);
        }
        List<Committable> committables = new ArrayList<>();
        for (CommitMessage message : write.prepareCommit(false, checkpointId)) {
            committables.add(new Committable(checkpointId, message));
        }
        write.close();
        return committables;
    }

    private byte[] serialize(List<Committable> committables) throws Exception {
        CommittableSerializer serializer = new CommittableSerializer(new CommitMessageSerializer());
        int total = 4;
        List<byte[]> bytes = new ArrayList<>();
        for (Committable committable : committables) {
            byte[] serialized = serializer.serialize(committable);
            bytes.add(serialized);
            total += 4 + serialized.length;
        }
        ByteBuffer buffer = ByteBuffer.allocate(total);
        buffer.putInt(committables.size());
        for (byte[] serialized : bytes) {
            buffer.putInt(serialized.length);
            buffer.put(serialized);
        }
        return buffer.array();
    }

    private PaimonWriterCoordinator createCoordinator(FileStoreTable table, int parallelism) {
        return createCoordinator(table, createContext(parallelism));
    }

    private OperatorCoordinator.Context createContext(int parallelism) {
        OperatorCoordinator.Context context = Mockito.mock(OperatorCoordinator.Context.class);
        when(context.getOperatorId()).thenReturn(operatorId);
        when(context.currentParallelism()).thenReturn(parallelism);
        when(context.getUserCodeClassloader())
                .thenReturn(Thread.currentThread().getContextClassLoader());
        return context;
    }

    private PaimonWriterCoordinator createCoordinator(
            FileStoreTable table, OperatorCoordinator.Context context) {
        Committer.Factory<Committable, ManifestCommittable> factory =
                commitContext ->
                        new StoreCommitter(
                                table,
                                table.newCommit(commitContext.commitUser())
                                        .ignoreEmptyCommit(
                                                !commitContext.streamingCheckpointEnabled()),
                                commitContext);
        return new PaimonWriterCoordinator(
                true,
                commitUser,
                factory,
                context,
                new PaimonWriterCoordinator.CoordinatorExecutorThreadFactory("PWC", context),
                null);
    }

    private void waitForCoordinator(PaimonWriterCoordinator coordinator) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        coordinator.runInCoordinatorThread(() -> future.complete(null));
        try {
            future.get();
        } catch (InterruptedException e) {
            throw new AssertionError("Interrupted while waiting for coordinator.", e);
        } catch (ExecutionException e) {
            ExceptionUtils.rethrow(ExceptionUtils.stripExecutionException(e));
        }
    }

    private FileStoreTable createFileStoreTable() throws Exception {
        Options conf = new Options();
        conf.set(CoreOptions.PATH, tablePath.toString());
        conf.setString("bucket", "1");
        conf.setString("bucket-key", "a");
        new SchemaManager(LocalFileIO.create(), tablePath)
                .createTable(
                        new Schema(
                                ROW_TYPE.getFields(),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                conf.toMap(),
                                ""));
        return FileStoreTableFactory.create(LocalFileIO.create(), conf);
    }

    private void assertResults(FileStoreTable table, String... expected) {
        TableRead read = table.newReadBuilder().newRead();
        List<String> actual = new ArrayList<>();
        table.newReadBuilder()
                .newScan()
                .plan()
                .splits()
                .forEach(
                        split -> {
                            try {
                                RecordReader<InternalRow> reader = read.createReader(split);
                                CloseableIterator<InternalRow> iterator =
                                        new RecordReaderIterator<>(reader);
                                while (iterator.hasNext()) {
                                    InternalRow row = iterator.next();
                                    actual.add(row.getInt(0) + ", " + row.getLong(1));
                                }
                                iterator.close();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
        Collections.sort(actual);
        assertThat(actual).isEqualTo(Arrays.asList(expected));
    }
}
