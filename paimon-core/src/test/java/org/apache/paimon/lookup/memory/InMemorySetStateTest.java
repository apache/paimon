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

package org.apache.paimon.lookup.memory;

import org.apache.paimon.data.serializer.IntSerializer;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/** Tests for {@link InMemorySetState}. */
class InMemorySetStateTest {

    @Test
    void testRetractOnAbsentKeyIsNoOp() throws Exception {
        // predicate-filtered inserts never add the secondary key, but the refresh
        // path retracts DELETE/UPDATE_BEFORE rows unconditionally
        InMemorySetState<Integer, Integer> state =
                new InMemorySetState<>(IntSerializer.INSTANCE, IntSerializer.INSTANCE);

        state.add(1, 10);
        state.add(1, 20);

        assertThatCode(() -> state.retract(2, 10)).doesNotThrowAnyException();
        assertThatCode(() -> state.retract(1, 99)).doesNotThrowAnyException();
        assertThat(state.get(1)).containsExactlyInAnyOrder(10, 20);
        assertThat(state.get(2)).isEmpty();

        state.retract(1, 10);
        assertThat(state.get(1)).containsExactly(20);
    }
}
