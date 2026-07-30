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

package org.apache.paimon.index;

import org.apache.paimon.io.DataOutputSerializer;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DataEvolutionIndexSourceMeta}. */
class DataEvolutionIndexSourceMetaTest {

    @Test
    void testRoundTripAndDetection() {
        DataEvolutionIndexSourceMeta sourceMeta = new DataEvolutionIndexSourceMeta(42L);

        byte[] serialized = sourceMeta.serialize();

        assertThat(DataEvolutionIndexSourceMeta.isDataEvolutionMeta(serialized)).isTrue();
        assertThat(DataEvolutionIndexSourceMeta.deserialize(serialized).scanSnapshotId())
                .isEqualTo(42L);
        assertThat(DataEvolutionIndexSourceMeta.isDataEvolutionMeta(null)).isFalse();
        assertThat(DataEvolutionIndexSourceMeta.isDataEvolutionMeta(new byte[] {1})).isFalse();
    }

    @Test
    void testRejectsInvalidSnapshotId() {
        assertThatThrownBy(() -> new DataEvolutionIndexSourceMeta(0L))
                .hasMessageContaining("snapshot id must be positive");
    }

    @Test
    void testRejectsWrongMagicAndVersion() throws Exception {
        DataOutputSerializer wrongMagic = new DataOutputSerializer(16);
        wrongMagic.writeInt(1);
        wrongMagic.writeInt(1);
        wrongMagic.writeLong(1L);
        assertThatThrownBy(
                        () ->
                                DataEvolutionIndexSourceMeta.deserialize(
                                        wrongMagic.getCopyOfBuffer()))
                .hasMessageContaining("Not data-evolution index source metadata");

        byte[] wrongVersion = new DataEvolutionIndexSourceMeta(1L).serialize();
        wrongVersion[7] = 2;
        assertThatThrownBy(() -> DataEvolutionIndexSourceMeta.deserialize(wrongVersion))
                .hasMessageContaining("Unsupported data-evolution index source version");
    }

    @Test
    void testRejectsTruncatedAndTrailingBytes() {
        byte[] serialized = new DataEvolutionIndexSourceMeta(1L).serialize();

        assertThatThrownBy(
                        () ->
                                DataEvolutionIndexSourceMeta.deserialize(
                                        Arrays.copyOf(serialized, serialized.length - 1)))
                .hasMessageContaining("Failed to deserialize data-evolution index source metadata");

        byte[] trailing = Arrays.copyOf(serialized, serialized.length + 1);
        assertThatThrownBy(() -> DataEvolutionIndexSourceMeta.deserialize(trailing))
                .hasMessageContaining("Unexpected trailing bytes");
    }

    @Test
    void testReadsFromIndexFile() {
        DataEvolutionIndexSourceMeta sourceMeta = new DataEvolutionIndexSourceMeta(9L);
        IndexFileMeta indexFile =
                new IndexFileMeta(
                        "lumina",
                        "index-1",
                        1L,
                        10L,
                        new GlobalIndexMeta(0, 9, 1, null, null, sourceMeta.serialize()),
                        null);

        assertThat(DataEvolutionIndexSourceMeta.fromIndexFile(indexFile).scanSnapshotId())
                .isEqualTo(9L);
    }
}
