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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PkVectorAnnSegmentMeta}. */
class PkVectorAnnSegmentMetaTest {

    @Test
    void testRoundTrip() {
        PkVectorAnnSegmentMeta metadata =
                new PkVectorAnnSegmentMeta(
                        "index-definition",
                        Arrays.asList(
                                new PkVectorSourceFile("data-1", 100),
                                new PkVectorSourceFile("data-2", 50)),
                        PkVectorAnnSegmentMeta.OrdinalLayout.FILE_POSITION,
                        new byte[] {1, 2, 3});

        PkVectorAnnSegmentMeta restored = PkVectorAnnSegmentMeta.deserialize(metadata.serialize());

        assertThat(restored.indexDefinitionId()).isEqualTo("index-definition");
        assertThat(restored.sourceFiles()).isEqualTo(metadata.sourceFiles());
        assertThat(restored.ordinalLayout())
                .isEqualTo(PkVectorAnnSegmentMeta.OrdinalLayout.FILE_POSITION);
        assertThat(restored.payloadMetadata()).containsExactly(1, 2, 3);
    }

    @Test
    void testRejectsTruncatedPayloadMetadata() throws Exception {
        DataOutputSerializer output = new DataOutputSerializer(128);
        output.writeInt(1);
        output.writeUTF("index");
        output.writeInt(1);
        output.writeUTF("data-1");
        output.writeLong(10);
        output.writeByte(PkVectorAnnSegmentMeta.OrdinalLayout.FILE_POSITION.ordinal());
        output.writeInt(1);

        assertThatThrownBy(() -> PkVectorAnnSegmentMeta.deserialize(output.getCopyOfBuffer()))
                .hasMessageContaining("Failed to deserialize ANN vector segment metadata");
    }
}
