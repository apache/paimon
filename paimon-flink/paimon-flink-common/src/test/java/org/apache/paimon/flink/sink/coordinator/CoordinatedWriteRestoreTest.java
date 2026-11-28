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

import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.operation.RestoreFiles;
import org.apache.paimon.utils.InstantiationUtil;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.util.SerializedValue;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.paimon.data.BinaryRow.EMPTY_ROW;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests guard metadata transport through the coordinator's serialized, paged responses. */
class CoordinatedWriteRestoreTest {

    @ParameterizedTest
    @ValueSource(ints = {32, 4096})
    void testRestoresGuardReferences(int pageSize) throws Exception {
        List<IndexFileMeta> guards =
                Arrays.asList(
                        new IndexFileMeta(
                                "DELETE_ORDERING_GUARD", "guard-1", 128L, 2L, null, null, null),
                        new IndexFileMeta(
                                "DELETE_ORDERING_GUARD",
                                "guard-2",
                                256L,
                                3L,
                                null,
                                "external/guard-2",
                                null));
        ScanCoordinationResponse response =
                new ScanCoordinationResponse(
                        null, 4, Collections.emptyList(), null, null, null, guards);
        AtomicInteger pages = new AtomicInteger();

        RestoreFiles restored = restore(response, pageSize, pages);

        assertThat(restored.totalBuckets()).isEqualTo(4);
        assertThat(restored.dataFiles()).isEmpty();
        assertThat(restored.deleteOrderingGuards()).containsExactlyElementsOf(guards);
        if (pageSize == 32) {
            assertThat(pages.get()).isGreaterThan(1);
        } else {
            assertThat(pages.get()).isEqualTo(1);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {32, 4096})
    void testExistingResponseConstructorRestoresWithoutGuards(int pageSize) throws Exception {
        ScanCoordinationResponse response =
                new ScanCoordinationResponse(null, null, null, null, null, null);

        RestoreFiles restored = restore(response, pageSize, new AtomicInteger());

        assertThat(restored.snapshot()).isNull();
        assertThat(restored.totalBuckets()).isNull();
        assertThat(restored.dataFiles()).isNull();
        assertThat(restored.deleteOrderingGuards()).isEmpty();
    }

    private RestoreFiles restore(
            ScanCoordinationResponse response, int pageSize, AtomicInteger pages) throws Exception {
        byte[] content = InstantiationUtil.serializeObject(response);
        TaskOperatorEventGateway gateway = mock(TaskOperatorEventGateway.class);
        when(gateway.sendRequestToCoordinator(any(), any()))
                .thenAnswer(
                        invocation -> {
                            SerializedValue<CoordinationRequest> serialized =
                                    invocation.getArgument(1);
                            PagedCoordinationRequest request =
                                    (PagedCoordinationRequest)
                                            serialized.deserializeValue(
                                                    getClass().getClassLoader());
                            int start = request.pageToken() == null ? 0 : request.pageToken();
                            int end = Math.min(content.length, start + pageSize);
                            pages.incrementAndGet();
                            return CompletableFuture.completedFuture(
                                    CoordinationResponseUtils.wrap(
                                            new PagedCoordinationResponse(
                                                    Arrays.copyOfRange(content, start, end),
                                                    end == content.length ? null : end)));
                        });
        return new CoordinatedWriteRestore(gateway, new OperatorID())
                .restoreFiles(EMPTY_ROW, 0, false, false, false);
    }
}
