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

import org.apache.paimon.flink.sink.CommittableSerializer;
import org.apache.paimon.table.sink.CommitMessageSerializer;

import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link CheckpointCommittablesSerializer}. */
public class CheckpointCommittablesSerializerTest {

    private final CheckpointCommittablesSerializer serializer =
            new CheckpointCommittablesSerializer(
                    new CommittableSerializer(new CommitMessageSerializer()));

    @Test
    public void testCurrentVersionIsV2() {
        assertThat(serializer.getVersion()).isEqualTo(2);
    }

    @Test
    public void testRoundTripPreservesIdleFlag() throws IOException {
        for (boolean idle : new boolean[] {true, false}) {
            CheckpointCommittables original =
                    new CheckpointCommittables(
                            42L, Collections.emptyList(), /* watermark */ 4242L, idle);
            CheckpointCommittables decoded =
                    serializer.deserialize(serializer.getVersion(), serializer.serialize(original));
            assertThat(decoded.checkpointId()).isEqualTo(42L);
            assertThat(decoded.watermark()).isEqualTo(4242L);
            assertThat(decoded.idle()).isEqualTo(idle);
            assertThat(decoded.committables()).isEmpty();
        }
    }

    @Test
    public void testV1PayloadDeserializesAsActive() throws IOException {
        // Hand-encode a v1 payload (no idle bit) so the reader is exercised on real bytes rather
        // than a spec that could drift alongside the serializer implementation.
        DataOutputSerializer out = new DataOutputSerializer(32);
        out.writeLong(7L); // checkpointId
        out.writeLong(1234L); // watermark
        out.writeInt(new CommittableSerializer(new CommitMessageSerializer()).getVersion());
        out.writeInt(0); // empty committables

        CheckpointCommittables decoded = serializer.deserialize(1, out.getCopyOfBuffer());
        assertThat(decoded.checkpointId()).isEqualTo(7L);
        assertThat(decoded.watermark()).isEqualTo(1234L);
        // v1 predates idle tracking; readers must default to ACTIVE so pre-upgrade payloads keep
        // participating in the min just like they did before.
        assertThat(decoded.idle()).isFalse();
        assertThat(decoded.committables()).isEmpty();
    }

    @Test
    public void testUnknownVersionRejected() throws IOException {
        byte[] bytes =
                serializer.serialize(new CheckpointCommittables(1L, Collections.emptyList(), 0L));
        assertThatThrownBy(() -> serializer.deserialize(99, bytes))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unknown version");
    }
}
