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
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.CommitMessageSerializer;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializerTypeSerializerProxy;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.flink.sink.coordinator.WriterCommittablesTest.committableEquals;
import static org.apache.paimon.manifest.ManifestCommittableSerializerTest.randomCompactIncrement;
import static org.apache.paimon.manifest.ManifestCommittableSerializerTest.randomNewFilesIncrement;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link RestoredCommittableEvent}. */
public class RestoredCommittableEventTest {

    private static final TypeSerializer<CheckpointCommittables> SERIALIZER =
            new SimpleVersionedSerializerTypeSerializerProxy<>(
                    () ->
                            new CheckpointCommittablesSerializer(
                                    new CommittableSerializer(new CommitMessageSerializer())));

    @Test
    public void testSerializationWithMultipleEntries() throws Exception {
        CommitMessage message1 =
                new CommitMessageImpl(
                        createTestRow(), 1, 2, randomNewFilesIncrement(), randomCompactIncrement());
        CommitMessage message2 =
                new CommitMessageImpl(
                        createTestRow(), 3, 4, randomNewFilesIncrement(), randomCompactIncrement());
        Committable committable1 = new Committable(1L, message1);
        Committable committable2 = new Committable(2L, message2);

        CheckpointCommittables entry1 =
                new CheckpointCommittables(1L, Collections.singletonList(committable1), 100L);
        CheckpointCommittables entry2 =
                new CheckpointCommittables(2L, Collections.singletonList(committable2), 500L);
        long restoredCheckpointId = 2L;
        RestoredCommittableEvent event =
                RestoredCommittableEvent.create(
                        restoredCheckpointId, Arrays.asList(entry1, entry2), SERIALIZER);

        assertThat(event.getRestoredCheckpointId()).isEqualTo(restoredCheckpointId);

        List<CheckpointCommittables> decoded = event.deserialize(SERIALIZER);
        assertThat(decoded).hasSize(2);

        assertThat(decoded.get(0).checkpointId()).isEqualTo(1L);
        assertThat(decoded.get(0).watermark()).isEqualTo(100L);
        assertThat(decoded.get(0).committables()).hasSize(1);
        assertThat(committableEquals(decoded.get(0).committables().get(0), committable1)).isTrue();

        assertThat(decoded.get(1).checkpointId()).isEqualTo(2L);
        assertThat(decoded.get(1).watermark()).isEqualTo(500L);
        assertThat(decoded.get(1).committables()).hasSize(1);
        assertThat(committableEquals(decoded.get(1).committables().get(0), committable2)).isTrue();
    }

    @Test
    public void testSerializationWithEmptyEntries() throws Exception {
        long restoredCheckpointId = 7L;
        RestoredCommittableEvent event =
                RestoredCommittableEvent.create(
                        restoredCheckpointId, Collections.emptyList(), SERIALIZER);

        assertThat(event.getRestoredCheckpointId()).isEqualTo(restoredCheckpointId);
        assertThat(event.deserialize(SERIALIZER)).isEmpty();
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
