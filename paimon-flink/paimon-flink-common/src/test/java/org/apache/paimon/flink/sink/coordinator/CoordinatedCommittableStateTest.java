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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CoordinatedCommittableState}. */
class CoordinatedCommittableStateTest {

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void testSnapshotKeepsEmptyCheckpointForRestore() throws Exception {
        StateInitializationContext context = context(Collections.emptyList(), false);
        ListState<byte[]> flinkState = committableState(context);

        CoordinatedCommittableState state = new CoordinatedCommittableState();
        state.initialize(context);
        state.snapshot(1L);

        assertThat(state.pendingCommittables()).containsOnlyKeys(1L);
        assertThat(state.pendingCommittables().get(1L)).isEmpty();

        ArgumentCaptor<List<byte[]>> serializedState = ArgumentCaptor.forClass(List.class);
        Mockito.verify(flinkState).update(serializedState.capture());

        CoordinatedCommittableState restored = new CoordinatedCommittableState();
        restored.initialize(context(serializedState.getValue(), true));
        assertThat(restored.pendingCommittables()).containsOnlyKeys(1L);
        assertThat(restored.pendingCommittables().get(1L)).isEmpty();
    }

    @Test
    void testPendingCheckpointsAreReturnedInAscendingOrder() throws Exception {
        StateInitializationContext context = context(Collections.emptyList(), false);

        CoordinatedCommittableState state = new CoordinatedCommittableState();
        state.initialize(context);
        state.snapshot(3L);
        state.snapshot(1L);
        state.snapshot(2L);

        assertThat(new ArrayList<>(state.pendingCommittables().keySet()))
                .containsExactly(1L, 2L, 3L);
        assertThat(state.pendingCommittables().get(1L)).isEmpty();
        assertThat(state.pendingCommittables().get(2L)).isEmpty();
        assertThat(state.pendingCommittables().get(3L)).isEmpty();
    }

    @Test
    void testOnlyUnacknowledgedCommittablesAreReported() throws Exception {
        StateInitializationContext context = context(Collections.emptyList(), false);

        CoordinatedCommittableState state = new CoordinatedCommittableState();
        state.initialize(context);
        Committable ck1 = committable(1L);
        Committable ck2 = committable(2L);
        state.add(ck1);
        state.add(ck2);

        assertThat(state.unacknowledgedCommittables()).containsExactly(ck1, ck2);

        state.markAcknowledged(Collections.singletonList(ck1));
        assertThat(state.unacknowledgedCommittables()).containsExactly(ck2);

        state.markCommittedUpTo(1L);
        assertThat(state.pendingCommittables()).containsOnlyKeys(2L);
        assertThat(state.unacknowledgedCommittables()).containsExactly(ck2);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private StateInitializationContext context(Iterable<byte[]> committables, boolean restored)
            throws Exception {
        StateInitializationContext context = Mockito.mock(StateInitializationContext.class);
        OperatorStateStore operatorStateStore = Mockito.mock(OperatorStateStore.class);
        ListState<byte[]> committableState = Mockito.mock(ListState.class);
        Mockito.when(context.getOperatorStateStore()).thenReturn(operatorStateStore);
        Mockito.when(context.isRestored()).thenReturn(restored);
        Mockito.when(committableState.get()).thenReturn(committables);
        Mockito.when(operatorStateStore.getListState(Mockito.any(ListStateDescriptor.class)))
                .thenAnswer(
                        invocation -> {
                            ListStateDescriptor descriptor = invocation.getArgument(0);
                            if ("pwc_pending_committables".equals(descriptor.getName())) {
                                return committableState;
                            }
                            throw new IllegalArgumentException(
                                    "Unexpected state " + descriptor.getName());
                        });
        return context;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private ListState<byte[]> committableState(StateInitializationContext context)
            throws Exception {
        OperatorStateStore operatorStateStore = context.getOperatorStateStore();
        ListStateDescriptor descriptor =
                new ListStateDescriptor<>("pwc_pending_committables", byte[].class);
        return operatorStateStore.getListState(descriptor);
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
}
