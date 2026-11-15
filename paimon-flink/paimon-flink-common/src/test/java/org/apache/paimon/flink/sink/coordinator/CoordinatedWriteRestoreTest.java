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

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.operation.FileSystemWriteRestore;
import org.apache.paimon.operation.RestoreFiles;
import org.apache.paimon.operation.WriteRestore;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.util.SerializedValue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.paimon.CoreOptions.DELETE_ORDERING_GUARDS_ENABLED;
import static org.apache.paimon.data.BinaryRow.EMPTY_ROW;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_WRITER_COORDINATOR_ENABLED;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_WRITER_COORDINATOR_PAGE_SIZE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests writer recovery through the coordinator's serialized, paged RPC responses. */
class CoordinatedWriteRestoreTest extends TableTestBase {

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testGuardedCompaction(boolean captureEnabled) throws Exception {
        FileStoreTable table = createGuardedTable();
        table =
                table.copy(
                        Collections.singletonMap(
                                DELETE_ORDERING_GUARDS_ENABLED.key(),
                                Boolean.toString(captureEnabled)));
        write(table, ioManager, GenericRow.of(1, 2));
        assertThat(read(table)).isEmpty();

        RestoreFiles expected = restore(fileSystemRestore(table));
        assertThat(expected.deleteOrderingGuards()).hasSize(1);
        AtomicInteger pages = new AtomicInteger();
        CoordinatedWriteRestore writeRestore =
                coordinatedRestore(new TableWriteCoordinator(table), pages);
        RestoreFiles restored = restore(writeRestore);
        assertThat(pages.get()).isGreaterThan(1);

        commit(table, 1, compact(table, writeRestore, 1));

        assertThat(restored.deleteOrderingGuards())
                .containsExactlyElementsOf(expected.deleteOrderingGuards());
        assertThat(read(table)).isEmpty();
        assertThat(restore(fileSystemRestore(table)).deleteOrderingGuards())
                .containsExactlyElementsOf(expected.deleteOrderingGuards());
    }

    @Test
    void testGuardOnlyBucket() throws Exception {
        FileStoreTable table = createGuardedTable();
        RestoreFiles expected = restore(fileSystemRestore(table));
        assertThat(expected.dataFiles()).isEmpty();
        assertThat(expected.deleteOrderingGuards()).hasSize(1);
        assertThat(expected.totalBuckets()).isEqualTo(1);

        AtomicInteger pages = new AtomicInteger();
        RestoreFiles restored =
                restore(coordinatedRestore(new TableWriteCoordinator(table), pages));

        assertThat(restored.snapshot().id()).isEqualTo(expected.snapshot().id());
        assertThat(restored.dataFiles()).isEmpty();
        assertThat(restored.totalBuckets()).isEqualTo(expected.totalBuckets());
        assertThat(restored.deleteOrderingGuards())
                .containsExactlyElementsOf(expected.deleteOrderingGuards());
        assertThat(pages.get()).isGreaterThan(1);
    }

    @Test
    void testSnapshotPinnedGuardsAndStaleCompactionFence() throws Exception {
        FileStoreTable table = createGuardedTable();
        write(table, ioManager, GenericRow.of(1, 2));
        RestoreFiles original = restore(fileSystemRestore(table));
        TableWriteCoordinator coordinator = new TableWriteCoordinator(table);

        write(table, ioManager, GenericRow.ofKind(RowKind.DELETE, 2, 8));
        compact(table, EMPTY_ROW, 0, ioManager, true);
        RestoreFiles latest = restore(fileSystemRestore(table));
        assertThat(latest.deleteOrderingGuards()).hasSize(2);
        assertThat(latest.snapshot().id()).isGreaterThan(original.snapshot().id());

        CoordinatedWriteRestore writeRestore = coordinatedRestore(coordinator, new AtomicInteger());
        RestoreFiles restored = restore(writeRestore);
        assertThat(restored.snapshot().id()).isEqualTo(original.snapshot().id());
        assertThat(restored.deleteOrderingGuards())
                .containsExactlyElementsOf(original.deleteOrderingGuards());
        assertThat(restored.dataFiles()).containsExactlyElementsOf(original.dataFiles());

        List<CommitMessage> pending = compact(table, writeRestore, 1);
        assertThatThrownBy(() -> commit(table, 1, pending))
                .hasMessageContaining("guard sources changed")
                .hasMessageContaining("recompute compaction");
        assertThat(table.latestSnapshot().get().id()).isEqualTo(latest.snapshot().id());

        coordinator.checkpoint();
        RestoreFiles refreshed = restore(writeRestore);
        assertThat(refreshed.snapshot().id()).isEqualTo(latest.snapshot().id());
        assertThat(refreshed.deleteOrderingGuards())
                .containsExactlyInAnyOrderElementsOf(latest.deleteOrderingGuards());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testWithoutGuards(boolean hasSnapshot) throws Exception {
        FileStoreTable table = createTable();
        if (hasSnapshot) {
            write(table, ioManager, GenericRow.of(1, 2));
        }
        RestoreFiles expected = restore(fileSystemRestore(table));
        RestoreFiles restored =
                restore(coordinatedRestore(new TableWriteCoordinator(table), new AtomicInteger()));

        assertThat(restored.deleteOrderingGuards()).isEmpty();
        assertThat(restored.totalBuckets()).isEqualTo(expected.totalBuckets());
        assertThat(restored.dataFiles()).isEqualTo(expected.dataFiles());
        if (hasSnapshot) {
            assertThat(restored.snapshot().id()).isEqualTo(expected.snapshot().id());
        } else {
            assertThat(restored.snapshot()).isNull();
        }
    }

    private FileStoreTable createTable() throws Exception {
        catalog.createTable(
                identifier(),
                Schema.newBuilder()
                        .column("k", DataTypes.INT())
                        .column("seq", DataTypes.INT())
                        .primaryKey("k")
                        .option("bucket", "1")
                        .option("sequence.field", "seq")
                        .option("file.format", "parquet")
                        .option("num-sorted-run.compaction-trigger", "100")
                        .option(DELETE_ORDERING_GUARDS_ENABLED.key(), "true")
                        .option(SINK_WRITER_COORDINATOR_ENABLED.key(), "true")
                        .option(SINK_WRITER_COORDINATOR_PAGE_SIZE.key(), "128 b")
                        .build(),
                false);
        return getTable(identifier());
    }

    private FileStoreTable createGuardedTable() throws Exception {
        FileStoreTable table = createTable();
        write(table, ioManager, GenericRow.ofKind(RowKind.DELETE, 1, 4));
        compact(table, EMPTY_ROW, 0, ioManager, true);
        return table;
    }

    private FileSystemWriteRestore fileSystemRestore(FileStoreTable table) {
        return new FileSystemWriteRestore(
                table.coreOptions(),
                table.snapshotManager(),
                table.store().newScan(),
                table.store().newIndexFileHandler());
    }

    private CoordinatedWriteRestore coordinatedRestore(
            TableWriteCoordinator coordinator, AtomicInteger pages) throws Exception {
        TaskOperatorEventGateway gateway = mock(TaskOperatorEventGateway.class);
        when(gateway.sendRequestToCoordinator(any(), any()))
                .thenAnswer(
                        invocation -> {
                            SerializedValue<CoordinationRequest> serialized =
                                    invocation.getArgument(1);
                            CoordinationRequest request =
                                    serialized.deserializeValue(getClass().getClassLoader());
                            CoordinationResponse response;
                            if (request instanceof PagedCoordinationRequest) {
                                pages.incrementAndGet();
                                response = coordinator.scan((PagedCoordinationRequest) request);
                            } else {
                                response =
                                        new LatestIdentifierResponse(
                                                coordinator.latestCommittedIdentifier(
                                                        ((LatestIdentifierRequest) request)
                                                                .user()));
                            }
                            return CompletableFuture.completedFuture(
                                    CoordinationResponseUtils.wrap(response));
                        });
        return new CoordinatedWriteRestore(gateway, new OperatorID());
    }

    private RestoreFiles restore(WriteRestore writeRestore) {
        return writeRestore.restoreFiles(EMPTY_ROW, 0, false, false, false);
    }

    private List<CommitMessage> compact(
            FileStoreTable table, WriteRestore writeRestore, long identifier) throws Exception {
        try (TableWriteImpl<?> write = table.newWrite(commitUser).withIOManager(ioManager)) {
            write.withWriteRestore(writeRestore);
            write.compact(EMPTY_ROW, 0, true);
            return write.prepareCommit(true, identifier);
        }
    }

    private void commit(FileStoreTable table, long identifier, List<CommitMessage> messages)
            throws Exception {
        try (TableCommitImpl commit = table.newCommit(commitUser)) {
            commit.commit(identifier, messages);
        }
    }
}
