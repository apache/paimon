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
import org.apache.paimon.flink.sink.Committer;
import org.apache.paimon.flink.sink.RowDataStoreWriteOperator;
import org.apache.paimon.flink.sink.StoreCommitter;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestCommittableSerializer;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.SerializableSupplier;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.SerializedValue;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Test for {@link RowDataStoreWriteOperator}. */
public class PaimonWriterCoordinatorTest {
    private static final long CK0 = 7L;
    private static final long CK1 = 8L;
    private static final long CK2 = 9L;
    private static final List<GenericRow> CK0_ROW_DATA = Lists.newArrayList(GenericRow.of(0, 0L));
    private static final List<GenericRow> CK1_ROW_DATA =
            Lists.newArrayList(GenericRow.of(1, 10L), GenericRow.of(2, 20L));
    private static final List<GenericRow> CK1_RETRY_ROW_DATA =
            Lists.newArrayList(GenericRow.of(1, 10L), GenericRow.of(2, 20L), GenericRow.of(3, 30L));
    private static final List<GenericRow> CK2_ROW_DATA =
            Lists.newArrayList(GenericRow.of(3, 30L), GenericRow.of(4, 40L));

    private static final RowType ROW_TYPE =
            RowType.of(
                    new DataType[] {DataTypes.INT(), DataTypes.BIGINT()}, new String[] {"a", "b"});
    public String configuredStateDir;

    @TempDir public java.nio.file.Path tempDir;
    protected Path tablePath;
    public OperatorID operatorID;

    @BeforeEach
    public void before() {
        tablePath = new Path(tempDir.toString());
        initialCommitUser = UUID.randomUUID().toString();
        configuredStateDir = tempDir.toString() + "/pwc";
        operatorID = new OperatorID();
    }

    protected String initialCommitUser;

    // all normal
    @Test
    public void testCoordinatorDefault() throws Exception {
        boolean streamingCheckpointEnabled = true;
        FileStoreTable table = createFileStoreTable();
        PaimonWriterCoordinator coordinator = createCoordinator(table, streamingCheckpointEnabled);
        coordinator.start();

        doneForCK0(coordinator, table);

        tryCk(CK1, CK1_ROW_DATA, coordinator, table, true, true);
        assertResults(table, "0, 0", "1, 10", "2, 20");

        coordinator.close();
    }

    // =======================support recovery============================

    // JM normal; WriteTask failover
    // CK1 fail; recover from CK1; check; CK2 done; check;
    @Test
    public void testFailIntentionally() throws Exception {
        boolean streamingCheckpointEnabled = true;
        FileStoreTable table = createFileStoreTable();
        PaimonWriterCoordinator coordinator = createCoordinator(table, streamingCheckpointEnabled);
        coordinator.start();

        doneForCK0(coordinator, table);

        tryCk(CK1, CK1_ROW_DATA, coordinator, table, streamingCheckpointEnabled, false);
        assertThat(table.snapshotManager().latestSnapshotId() == CK0);
        coordinator.notifyCheckpointAborted(CK1);

        coordinator.resetToCheckpoint(CK0, null);
        assertResults(table, "0, 0");

        tryCk(CK2, CK2_ROW_DATA, coordinator, table, streamingCheckpointEnabled, true);
        assertResults(table, "0, 0", "3, 30", "4, 40");

        coordinator.close();
    }

    // JM failover; JM restart from CK0; CK1 fail
    // CK0 done; CK1 fail; recover from CK0; check;
    @Test
    public void testCoordinatorFailoverWithRecoveryCK0() throws Exception {
        FileStoreTable table = createFileStoreTable();
        boolean streamingCheckpointEnabled = true;
        PaimonWriterCoordinator coordinator1 = createCoordinator(table, streamingCheckpointEnabled);
        coordinator1.start();

        doneForCK0(coordinator1, table);

        tryCk(CK1, CK1_ROW_DATA, coordinator1, table, streamingCheckpointEnabled, false);
        assertThat(table.snapshotManager().latestSnapshotId() == CK0);
        coordinator1.close();

        PaimonWriterCoordinator coordinator2 = createCoordinator(table, streamingCheckpointEnabled);
        coordinator2.start();
        coordinator2.resetToCheckpoint(CK0, null);
        assertResults(table, "0, 0");

        coordinator2.close();
    }

    // Checkpoint coordinator not notice CK1 fail, replace
    @Test
    public void testCoordinatorFailoverWithRecoveryCK0withoutNotice() throws Exception {
        FileStoreTable table = createFileStoreTable();
        boolean streamingCheckpointEnabled = true;
        PaimonWriterCoordinator coordinator1 = createCoordinator(table, streamingCheckpointEnabled);
        coordinator1.start();

        doneForCK0(coordinator1, table);

        tryCk(CK1, CK1_ROW_DATA, coordinator1, table, streamingCheckpointEnabled, false);
        assertThat(table.snapshotManager().latestSnapshotId() == CK0);
        coordinator1.close();

        PaimonWriterCoordinator coordinator2 = createCoordinator(table, streamingCheckpointEnabled);
        coordinator2.start();
        coordinator2.resetToCheckpoint(CK0, null);
        assertResults(table, "0, 0");

        tryCk(CK1, CK1_RETRY_ROW_DATA, coordinator2, table, streamingCheckpointEnabled, true);
        assertResults(table, "0, 0", "1, 10", "2, 20", "3, 30");
        coordinator2.close();
    }

    // JM failover; Restart from CK1; restart CK2
    // CK1 done; trying CK2 ; recover from CK1; check; CK2 done; check;
    @Test
    public void testCoordinatorFailoverWithRecoveryFromCK1() throws Exception {
        FileStoreTable table = createFileStoreTable();
        boolean streamingCheckpointEnabled = true;

        PaimonWriterCoordinator coordinator1 = createCoordinator(table, streamingCheckpointEnabled);
        coordinator1.start();
        doneForCK0(coordinator1, table);
        assertThat(table.snapshotManager().latestSnapshotId() == CK1);

        tryCk(CK1, CK1_ROW_DATA, coordinator1, table, streamingCheckpointEnabled, false);
        coordinator1.close();

        PaimonWriterCoordinator coordinator2 = createCoordinator(table, streamingCheckpointEnabled);
        coordinator2.start();
        coordinator2.resetToCheckpoint(CK1, null);
        assertResults(table, "0, 0", "1, 10", "2, 20");

        coordinator2.close();
    }

    // ==================== Helper Methods ====================

    private void doneForCK0(PaimonWriterCoordinator coordinator, FileStoreTable table)
            throws Exception {
        tryCk(CK0, CK0_ROW_DATA, coordinator, table, true, true);
        assertResults(table, "0, 0");
    }

    private void tryCk(
            long ck,
            List<GenericRow> input,
            PaimonWriterCoordinator coordinator,
            FileStoreTable table,
            boolean streamingCheckpointEnabled,
            boolean doCommit)
            throws Exception {
        TaskOperatorEventGateway gateway = Mockito.mock(TaskOperatorEventGateway.class);
        CoordinatedFileInfoSender sender =
                new CoordinatedFileInfoSender(gateway, operatorID, streamingCheckpointEnabled);
        StreamTableWrite write =
                table.newStreamWriteBuilder().withCommitUser(initialCommitUser).newWrite();
        for (GenericRow row : input) {
            write.write(row);
        }
        for (CommitMessage committable : write.prepareCommit(false, ck)) {
            sender.collect(new Committable(ck, committable));
        }

        sender.sendToCoordinator(ck);

        ArgumentCaptor<SerializedValue> zeroCaptor = ArgumentCaptor.forClass(SerializedValue.class);
        verify(gateway).sendOperatorEventToCoordinator(eq(operatorID), zeroCaptor.capture());
        SerializedValue<OperatorEvent> capturedValue = zeroCaptor.getValue();
        OperatorEvent actualEvent =
                capturedValue.deserializeValue(OperatorEvent.class.getClassLoader());

        OperatorCoordinator.SubtaskGateway subtaskGateway =
                Mockito.mock(OperatorCoordinator.SubtaskGateway.class);
        // register subtask
        coordinator.executionAttemptReady(0, 2, subtaskGateway);

        coordinator.handleEventFromOperator(0, 2, actualEvent);
        if (doCommit) {
            coordinator.notifyCheckpointComplete(ck);
        }
        waitForCoordinatorToProcessActions(coordinator);
        write.close();
    }

    private PaimonWriterCoordinator createCoordinator(
            FileStoreTable table, boolean streamingCheckpointEnabled) {
        String initialCommitUser = "testUser";
        OperatorCoordinator.Context context = Mockito.mock(OperatorCoordinator.Context.class);
        when(context.getOperatorId()).thenReturn(operatorID);
        Committer.Factory committerFactory = createCommitterFactory(table);
        SerializableSupplier<?> committableSerializer = ManifestCommittableSerializer::new;
        when(context.currentParallelism()).thenReturn(1);
        PaimonWriterCoordinator.CoordinatorExecutorThreadFactory coordinatorThreadFactory =
                new PaimonWriterCoordinator.CoordinatorExecutorThreadFactory("PWC", context);
        Long endInputWatermark = 1000L;

        return new PaimonWriterCoordinator(
                configuredStateDir,
                streamingCheckpointEnabled,
                initialCommitUser,
                committerFactory,
                committableSerializer,
                context,
                coordinatorThreadFactory,
                endInputWatermark);
    }

    static void waitForCoordinatorToProcessActions(PaimonWriterCoordinator coordinator) {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        coordinator.runInCoordinatorThread(() -> future.complete(null));

        try {
            future.get();
        } catch (InterruptedException e) {
            throw new AssertionError("test interrupted");
        } catch (ExecutionException e) {
            ExceptionUtils.rethrow(ExceptionUtils.stripExecutionException(e));
        }
    }

    protected Committer.Factory<Committable, ManifestCommittable> createCommitterFactory(
            FileStoreTable table) {
        return context ->
                new StoreCommitter(
                        table,
                        table.newCommit(context.commitUser())
                                .ignoreEmptyCommit(!context.streamingCheckpointEnabled()),
                        context);
    }

    protected void assertResults(FileStoreTable table, String... expected) {
        TableRead read = table.newReadBuilder().newRead();
        List<String> actual = new ArrayList<>();
        table.newReadBuilder()
                .newScan()
                .plan()
                .splits()
                .forEach(
                        s -> {
                            try {
                                RecordReader<InternalRow> recordReader = read.createReader(s);
                                CloseableIterator<InternalRow> it =
                                        new RecordReaderIterator<>(recordReader);
                                while (it.hasNext()) {
                                    InternalRow row = it.next();
                                    actual.add(row.getInt(0) + ", " + row.getLong(1));
                                }
                                it.close();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
        Collections.sort(actual);
        assertThat(actual).isEqualTo(Arrays.asList(expected));
    }

    protected FileStoreTable createFileStoreTable() throws Exception {
        return createFileStoreTable(options -> {}, Collections.emptyList());
    }

    protected FileStoreTable createFileStoreTable(
            Consumer<Options> setOptions, List<String> partitionKeys) throws Exception {
        Options conf = new Options();
        conf.set(CoreOptions.PATH, tablePath.toString());
        conf.setString("bucket", "1");
        conf.setString("bucket-key", "a");
        setOptions.accept(conf);
        SchemaManager schemaManager = new SchemaManager(LocalFileIO.create(), tablePath);
        schemaManager.createTable(
                new Schema(
                        ROW_TYPE.getFields(),
                        partitionKeys,
                        Collections.emptyList(),
                        conf.toMap(),
                        ""));
        return FileStoreTableFactory.create(LocalFileIO.create(), conf);
    }
}
