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

package org.apache.paimon.index.pkvector;

import org.apache.paimon.io.DataOutputSerializer;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.apache.paimon.index.pkvector.PkVectorSegmentMeta.OrdinalLayout.FILE_POSITION;
import static org.apache.paimon.index.pkvector.PkVectorSegmentMeta.Role.ANN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PkVectorSegmentMeta}. */
class PkVectorSegmentMetaTest {

    @Test
    void testRoundTrip() {
        PkVectorSegmentMeta metadata =
                new PkVectorSegmentMeta(
                        ANN,
                        "1d4502f1-9cf0-4d86-8d8d-5cc9ac05e108",
                        7,
                        "VECTOR<FLOAT>(1024)",
                        "l2",
                        "ivf-pq",
                        Arrays.asList(
                                new PkVectorSegmentMeta.SourceFile("data-1", 3, 0, 100, 1024),
                                new PkVectorSegmentMeta.SourceFile("data-2", 3, 0, 50, 512)),
                        FILE_POSITION,
                        120,
                        42,
                        new byte[] {1, 2, 3},
                        new byte[] {4, 5, 6});

        PkVectorSegmentMeta restored = PkVectorSegmentMeta.deserialize(metadata.serialize());

        assertThat(restored.role()).isEqualTo(ANN);
        assertThat(restored.indexDefinitionId()).isEqualTo(metadata.indexDefinitionId());
        assertThat(restored.vectorFieldId()).isEqualTo(7);
        assertThat(restored.vectorTypeFingerprint()).isEqualTo("VECTOR<FLOAT>(1024)");
        assertThat(restored.metric()).isEqualTo("l2");
        assertThat(restored.algorithm()).isEqualTo("ivf-pq");
        assertThat(restored.sourceFiles()).isEqualTo(metadata.sourceFiles());
        assertThat(restored.ordinalLayout()).isEqualTo(FILE_POSITION);
        assertThat(restored.liveRowCountAtBuild()).isEqualTo(120);
        assertThat(restored.buildSnapshotId()).isEqualTo(42);
        assertThat(restored.optionsHash()).containsExactly(1, 2, 3);
        assertThat(restored.payloadMetadata()).containsExactly(4, 5, 6);
    }

    @Test
    void testRejectTrailingBytes() {
        PkVectorSegmentMeta metadata =
                new PkVectorSegmentMeta(
                        ANN,
                        "index",
                        1,
                        "VECTOR<FLOAT>(2)",
                        "l2",
                        "ivf-pq",
                        Arrays.asList(new PkVectorSegmentMeta.SourceFile("data", 1, 0, 1, 8)),
                        FILE_POSITION,
                        1,
                        1,
                        new byte[0]);
        byte[] bytes = Arrays.copyOf(metadata.serialize(), metadata.serialize().length + 1);

        assertThatThrownBy(() -> PkVectorSegmentMeta.deserialize(bytes))
                .hasMessageContaining("Unexpected trailing bytes");
    }

    @Test
    void testRejectsPreReleaseLayoutWithoutPayloadMetadata() throws Exception {
        DataOutputSerializer output = new DataOutputSerializer(128);
        output.writeInt(1);
        output.writeByte(ANN.ordinal());
        output.writeUTF("index");
        output.writeInt(7);
        output.writeUTF("VECTOR<FLOAT>(2)");
        output.writeUTF("l2");
        output.writeUTF("ivf-pq");
        output.writeInt(1);
        output.writeUTF("data-1");
        output.writeLong(3);
        output.writeInt(0);
        output.writeLong(10);
        output.writeLong(100);
        output.writeByte(FILE_POSITION.ordinal());
        output.writeLong(9);
        output.writeLong(42);
        output.writeInt(1);
        output.writeByte(1);

        assertThatThrownBy(() -> PkVectorSegmentMeta.deserialize(output.getCopyOfBuffer()))
                .hasMessageContaining("Failed to deserialize primary-key vector segment metadata");
    }
}
