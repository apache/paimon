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
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.CommittableSerializer;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.CommitMessageSerializer;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializerTypeSerializerProxy;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.apache.paimon.flink.sink.coordinator.WriterCommittablesTest.committableEquals;
import static org.apache.paimon.manifest.ManifestCommittableSerializerTest.randomCompactIncrement;
import static org.apache.paimon.manifest.ManifestCommittableSerializerTest.randomNewFilesIncrement;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link CommittableEvent}. */
public class CommittableEventTest {

    private static final TypeSerializer<CheckpointCommittables> SERIALIZER =
            new SimpleVersionedSerializerTypeSerializerProxy<>(
                    () ->
                            new CheckpointCommittablesSerializer(
                                    new CommittableSerializer(new CommitMessageSerializer())));

    @Test
    public void testSerialization() throws Exception {
        DataIncrement dataIncrement = randomNewFilesIncrement();
        CompactIncrement compactIncrement = randomCompactIncrement();
        CommitMessage commitMessage =
                new CommitMessageImpl(createTestRow(), 1, 2, dataIncrement, compactIncrement);
        long checkpointId = 123L;
        long watermark = 4567L;
        Committable committable = new Committable(checkpointId, commitMessage);
        CheckpointCommittables entry =
                new CheckpointCommittables(
                        checkpointId, Collections.singletonList(committable), watermark);
        CommittableEvent event = CommittableEvent.create(checkpointId, entry, SERIALIZER);

        assertThat(event.getCheckpointId()).isEqualTo(checkpointId);
        CheckpointCommittables decoded = event.deserialize(SERIALIZER);
        assertThat(decoded.checkpointId()).isEqualTo(checkpointId);
        assertThat(decoded.watermark()).isEqualTo(watermark);
        assertThat(decoded.committables()).hasSize(1);
        assertThat(committableEquals(decoded.committables().get(0), committable)).isTrue();
    }

    @Test
    public void testSerializationWithEmptyCommittable() throws Exception {
        long checkpointId = 123L;
        CheckpointCommittables entry =
                new CheckpointCommittables(checkpointId, Collections.emptyList(), Long.MIN_VALUE);
        CommittableEvent event = CommittableEvent.create(checkpointId, entry, SERIALIZER);

        assertThat(event.getCheckpointId()).isEqualTo(checkpointId);
        CheckpointCommittables decoded = event.deserialize(SERIALIZER);
        assertThat(decoded.checkpointId()).isEqualTo(checkpointId);
        assertThat(decoded.watermark()).isEqualTo(Long.MIN_VALUE);
        assertThat(decoded.committables()).isEmpty();
    }

    private static BinaryRow createTestRow() {
        BinaryRow row = new BinaryRow(2);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeInt(0, 1024);
        writer.writeString(1, BinaryString.fromString("abc"));
        writer.complete();
        return row;
    }
}
