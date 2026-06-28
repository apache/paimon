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
import org.apache.paimon.table.sink.CommitMessageImpl;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.util.SerializedValue;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link CoordinatedFileInfoSender}. */
class CoordinatedFileInfoSenderTest {

    @Test
    void testSendWaitsForAckBeforeReturning() throws Exception {
        TaskOperatorEventGateway gateway = Mockito.mock(TaskOperatorEventGateway.class);
        OperatorID operatorId = new OperatorID();
        CompletableFuture<CoordinationResponse> ack = new CompletableFuture<>();
        Mockito.when(gateway.sendRequestToCoordinator(Mockito.eq(operatorId), Mockito.any()))
                .thenReturn(ack);

        CoordinatedFileInfoSender sender = new CoordinatedFileInfoSender(gateway, operatorId);
        sender.setSubtaskId(3);
        sender.setAttemptNumber(4);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> send =
                    executor.submit(
                            () ->
                                    sender.sendToCoordinator(
                                            1L, Collections.singletonList(committable(1L))));

            Thread.sleep(100L);
            assertThat(send.isDone()).isFalse();

            ack.complete(ackResponse(1L, 3));
            send.get(5, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testSendEmptyFileInfo() throws Exception {
        TaskOperatorEventGateway gateway = Mockito.mock(TaskOperatorEventGateway.class);
        OperatorID operatorId = new OperatorID();
        Mockito.when(gateway.sendRequestToCoordinator(Mockito.eq(operatorId), Mockito.any()))
                .thenReturn(CompletableFuture.completedFuture(ackResponse(1L, 3)));

        CoordinatedFileInfoSender sender = new CoordinatedFileInfoSender(gateway, operatorId);
        sender.setSubtaskId(3);
        sender.setAttemptNumber(4);
        sender.sendToCoordinator(1L, Collections.emptyList());

        ArgumentCaptor<SerializedValue> captor = ArgumentCaptor.forClass(SerializedValue.class);
        Mockito.verify(gateway).sendRequestToCoordinator(Mockito.eq(operatorId), captor.capture());
        FileInfoRequest request = deserializeRequest(captor.getValue());

        assertThat(request.checkpointId()).isEqualTo(1L);
        assertThat(request.subtaskId()).isEqualTo(3);
        assertThat(request.attemptNumber()).isEqualTo(4);
        assertThat(request.committableCount()).isEqualTo(0);
        assertThat(CoordinatedFileInfoSender.deserializeCommittables(request.serializedData()))
                .isEmpty();
    }

    @Test
    void testFailedSendCanBeRetriedWithSameCommittables() throws Exception {
        TaskOperatorEventGateway gateway = Mockito.mock(TaskOperatorEventGateway.class);
        OperatorID operatorId = new OperatorID();
        CompletableFuture<CoordinationResponse> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("send failed"));
        Mockito.when(gateway.sendRequestToCoordinator(Mockito.eq(operatorId), Mockito.any()))
                .thenReturn(failed)
                .thenReturn(CompletableFuture.completedFuture(ackResponse(1L, 3)));

        CoordinatedFileInfoSender sender = new CoordinatedFileInfoSender(gateway, operatorId);
        sender.setSubtaskId(3);
        sender.setAttemptNumber(4);
        List<Committable> committables = Collections.singletonList(committable(1L));

        assertThatThrownBy(() -> sender.sendToCoordinator(1L, committables))
                .isInstanceOf(RuntimeException.class)
                .hasRootCauseMessage("send failed");

        sender.sendToCoordinator(1L, committables);

        ArgumentCaptor<SerializedValue> captor = ArgumentCaptor.forClass(SerializedValue.class);
        Mockito.verify(gateway, Mockito.times(2))
                .sendRequestToCoordinator(Mockito.eq(operatorId), captor.capture());
        FileInfoRequest retryRequest = deserializeRequest(captor.getAllValues().get(1));

        assertThat(retryRequest.checkpointId()).isEqualTo(1L);
        assertThat(retryRequest.subtaskId()).isEqualTo(3);
        assertThat(retryRequest.attemptNumber()).isEqualTo(4);
        assertThat(retryRequest.committableCount()).isEqualTo(1);

        List<Committable> retryCommittables =
                CoordinatedFileInfoSender.deserializeCommittables(retryRequest.serializedData());
        assertThat(retryCommittables).hasSize(1);
        assertThat(retryCommittables.get(0).checkpointId()).isEqualTo(1L);
    }

    @Test
    void testRecoveredFileInfoUsesSingleRequest() throws Exception {
        TaskOperatorEventGateway gateway = Mockito.mock(TaskOperatorEventGateway.class);
        OperatorID operatorId = new OperatorID();
        Mockito.when(gateway.sendRequestToCoordinator(Mockito.eq(operatorId), Mockito.any()))
                .thenReturn(CompletableFuture.completedFuture(ackResponse(1L, 3)));

        CoordinatedFileInfoSender sender = new CoordinatedFileInfoSender(gateway, operatorId);
        sender.setSubtaskId(3);
        sender.setAttemptNumber(4);
        sender.sendRecoveredFileInfoToCoordinator(
                1L, "commit-user", Collections.singletonList(committable(1L)));

        ArgumentCaptor<SerializedValue> captor = ArgumentCaptor.forClass(SerializedValue.class);
        Mockito.verify(gateway).sendRequestToCoordinator(Mockito.eq(operatorId), captor.capture());
        FileInfoRequest request = deserializeRequest(captor.getValue());
        assertThat(request.recovered()).isTrue();
        assertThat(request.checkpointId()).isEqualTo(1L);
        assertThat(request.subtaskId()).isEqualTo(3);
        assertThat(request.attemptNumber()).isEqualTo(4);
        assertThat(request.commitUser()).isEqualTo("commit-user");
        assertThat(request.committableCount()).isEqualTo(1);
        assertThat(CoordinatedFileInfoSender.deserializeCommittables(request.serializedData()))
                .hasSize(1);
    }

    private Committable committable(long checkpointId) {
        return new Committable(
                checkpointId,
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        0,
                        null,
                        DataIncrement.emptyIncrement(),
                        CompactIncrement.emptyIncrement()));
    }

    private CoordinationResponse ackResponse(long checkpointId, int subtaskId) {
        return CoordinationResponseUtils.wrap(
                new FileInfoReceivedResponse(checkpointId, subtaskId));
    }

    @SuppressWarnings("unchecked")
    private FileInfoRequest deserializeRequest(SerializedValue serializedValue) throws Exception {
        return (FileInfoRequest)
                ((SerializedValue<CoordinationRequest>) serializedValue)
                        .deserializeValue(Thread.currentThread().getContextClassLoader());
    }
}
