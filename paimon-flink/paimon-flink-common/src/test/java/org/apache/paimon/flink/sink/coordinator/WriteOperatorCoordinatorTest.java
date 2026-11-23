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
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.types.DataTypes;

import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.paimon.data.BinaryRow.EMPTY_ROW;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_WRITER_COORDINATOR_PAGE_SIZE;
import static org.apache.paimon.utils.InstantiationUtil.deserializeObject;
import static org.apache.paimon.utils.InstantiationUtil.serializeObject;
import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

/** Tests for refreshing writer metadata after coordinator recovery. */
@Timeout(60)
class WriteOperatorCoordinatorTest extends TableTestBase {

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testRecoveryRefreshesFilesAndLatestIdentifier(boolean globalRecovery) throws Exception {
        FileStoreTable table = createTable(1024 * 1024);
        commit(table, 1, GenericRow.of(1L, 3));

        try (WriteOperatorCoordinator coordinator = new WriteOperatorCoordinator(table)) {
            coordinator.start();
            CompletableFuture<byte[]> checkpoint = new CompletableFuture<>();
            coordinator.checkpointCoordinator(2, checkpoint);
            checkpoint.get(30, TimeUnit.SECONDS);
            assertThat(latestIdentifier(coordinator)).isEqualTo(1);

            // The checkpoint refresh precedes publication of its table snapshot.
            InternalRow[] rows = new InternalRow[21];
            for (int i = 0; i < 20; i++) {
                rows[i] = GenericRow.of((long) i + 2, 1);
            }
            rows[20] = GenericRow.of(1L, 4);
            commit(table, 2, rows);
            assertThat(scan(coordinator).snapshot().id()).isEqualTo(1);
            assertThat(latestIdentifier(coordinator)).isEqualTo(1);

            if (globalRecovery) {
                coordinator.resetToCheckpoint(2, checkpoint.get());
            } else {
                coordinator.subtaskReset(0, 2);
            }

            // No subsequent checkpoint should be needed to initialize the recovered writer.
            ScanCoordinationResponse restored = scan(coordinator);
            assertThat(restored.snapshot().id()).isEqualTo(2);
            assertThat(restored.extractDataFiles()).hasSize(2);
            assertThat(
                            restored.extractDataFiles().stream()
                                    .mapToLong(DataFileMeta::maxSequenceNumber)
                                    .max()
                                    .getAsLong())
                    .isEqualTo(21);
            assertThat(latestIdentifier(coordinator)).isEqualTo(2);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testResetBeforeStart(boolean restoredCheckpoint) throws Exception {
        FileStoreTable table = createTable(1024 * 1024);
        commit(table, 1, GenericRow.of(1L, 3));

        try (WriteOperatorCoordinator coordinator = new WriteOperatorCoordinator(table)) {
            coordinator.resetToCheckpoint(
                    restoredCheckpoint ? 1 : -1, restoredCheckpoint ? new byte[0] : null);
            coordinator.start();
            assertThat(scan(coordinator).snapshot().id()).isEqualTo(1);
            assertThat(latestIdentifier(coordinator)).isEqualTo(1);
        }
    }

    @Test
    void testSubtaskResetPreservesInFlightPages() throws Exception {
        FileStoreTable table = createTable(128);
        commit(table, 1, GenericRow.of(1L, 3));

        try (WriteOperatorCoordinator coordinator = new WriteOperatorCoordinator(table)) {
            coordinator.start();
            byte[] content = scanRequest();
            String requestId = UUID.randomUUID().toString();
            PagedCoordinationResponse firstPage =
                    request(coordinator, new PagedCoordinationRequest(content, requestId, null));
            assertThat(firstPage.nextPageToken()).isNotNull();

            commit(table, 2, GenericRow.of(1L, 4));
            coordinator.subtaskReset(0, 2);
            assertThat(scan(coordinator).snapshot().id()).isEqualTo(2);

            // An unaffected subtask may still be consuming a response from before the reset.
            ScanCoordinationResponse previous =
                    collectPages(coordinator, content, requestId, firstPage);
            assertThat(previous.snapshot().id()).isEqualTo(1);
            assertThat(previous.extractDataFiles()).hasSize(1);
        }
    }

    @Test
    void testFailedRefreshIsRetried() throws Exception {
        FileStoreTable table = createTable(1024 * 1024);
        commit(table, 1, GenericRow.of(1L, 3));
        FileStoreTable coordinatorTable = spy(table);

        try (WriteOperatorCoordinator coordinator =
                new WriteOperatorCoordinator(coordinatorTable)) {
            coordinator.start();
            commit(table, 2, GenericRow.of(1L, 4));
            doThrow(new RuntimeException("Expected refresh failure"))
                    .doReturn(table.latestSnapshot())
                    .when(coordinatorTable)
                    .latestSnapshot();
            coordinator.subtaskReset(0, 2);

            assertThatThrownBy(() -> scan(coordinator))
                    .hasRootCauseMessage("Expected refresh failure");
            assertThat(scan(coordinator).snapshot().id()).isEqualTo(2);
        }
    }

    @Test
    void testResetDuringRefreshIsNotLost() throws Exception {
        FileStoreTable table = createTable(1024 * 1024);
        commit(table, 1, GenericRow.of(1L, 3));
        FileStoreTable coordinatorTable = spy(table);
        CountDownLatch refreshing = new CountDownLatch(1);
        CountDownLatch continueRefresh = new CountDownLatch(1);

        try (WriteOperatorCoordinator coordinator =
                new WriteOperatorCoordinator(coordinatorTable)) {
            coordinator.start();
            commit(table, 2, GenericRow.of(1L, 4));
            Optional<Snapshot> snapshotBeforeReset = table.latestSnapshot();
            AtomicBoolean firstRefresh = new AtomicBoolean(true);
            doAnswer(
                            invocation -> {
                                if (firstRefresh.getAndSet(false)) {
                                    refreshing.countDown();
                                    if (!continueRefresh.await(30, TimeUnit.SECONDS)) {
                                        throw new IllegalStateException("Refresh was not released");
                                    }
                                    return snapshotBeforeReset;
                                }
                                return invocation.callRealMethod();
                            })
                    .when(coordinatorTable)
                    .latestSnapshot();

            coordinator.subtaskReset(0, 2);
            CompletableFuture<CoordinationResponse> pending =
                    coordinator.handleCoordinationRequest(
                            new PagedCoordinationRequest(
                                    scanRequest(), UUID.randomUUID().toString(), null));
            try {
                assertThat(refreshing.await(30, TimeUnit.SECONDS)).isTrue();
                commit(table, 3, GenericRow.of(1L, 5));
                coordinator.subtaskReset(1, 3);
            } finally {
                continueRefresh.countDown();
            }
            pending.get(30, TimeUnit.SECONDS);

            assertThat(scan(coordinator).snapshot().id()).isEqualTo(3);
        }
    }

    private FileStoreTable createTable(int pageSize) throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("version", DataTypes.INT())
                        .primaryKey("id")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.BUCKET_KEY.key(), "id")
                        .option(CoreOptions.WRITE_ONLY.key(), "true")
                        .option(SINK_WRITER_COORDINATOR_PAGE_SIZE.key(), pageSize + " b")
                        .build();
        catalog.createTable(identifier(), schema, false);
        return getTableDefault();
    }

    private void commit(FileStoreTable table, long identifier, InternalRow... rows)
            throws Exception {
        StreamWriteBuilder builder = table.newStreamWriteBuilder().withCommitUser(commitUser);
        try (StreamTableWrite write = builder.newWrite();
                StreamTableCommit commit = builder.newCommit()) {
            for (InternalRow row : rows) {
                write.write(row);
            }
            commit.commit(identifier, write.prepareCommit(false, identifier));
        }
    }

    private long latestIdentifier(WriteOperatorCoordinator coordinator) throws Exception {
        LatestIdentifierResponse response =
                request(coordinator, new LatestIdentifierRequest(commitUser));
        return response.latestIdentifier();
    }

    private ScanCoordinationResponse scan(WriteOperatorCoordinator coordinator) throws Exception {
        byte[] content = scanRequest();
        String requestId = UUID.randomUUID().toString();
        PagedCoordinationResponse firstPage =
                request(coordinator, new PagedCoordinationRequest(content, requestId, null));
        return collectPages(coordinator, content, requestId, firstPage);
    }

    private ScanCoordinationResponse collectPages(
            WriteOperatorCoordinator coordinator,
            byte[] content,
            String requestId,
            PagedCoordinationResponse page)
            throws Exception {
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        while (true) {
            result.write(page.content());
            if (page.nextPageToken() == null) {
                break;
            }
            page =
                    request(
                            coordinator,
                            new PagedCoordinationRequest(content, requestId, page.nextPageToken()));
        }
        return deserializeObject(result.toByteArray(), getClass().getClassLoader());
    }

    private byte[] scanRequest() throws Exception {
        return serializeObject(
                new ScanCoordinationRequest(serializeBinaryRow(EMPTY_ROW), 0, false, false, false));
    }

    private <T extends CoordinationResponse> T request(
            WriteOperatorCoordinator coordinator, CoordinationRequest request) throws Exception {
        return CoordinationResponseUtils.unwrap(
                coordinator.handleCoordinationRequest(request).get(30, TimeUnit.SECONDS));
    }
}
