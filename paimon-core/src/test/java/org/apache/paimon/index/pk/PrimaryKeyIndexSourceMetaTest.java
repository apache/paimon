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

package org.apache.paimon.index.pk;

import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataOutputSerializer;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PrimaryKeyIndexSourceMeta}. */
class PrimaryKeyIndexSourceMetaTest {

    @Test
    void testMultipleSourceRoundTrip() {
        PrimaryKeyIndexSourceMeta metadata =
                new PrimaryKeyIndexSourceMeta(
                        Arrays.asList(
                                new PrimaryKeyIndexSourceFile("data-1", 10),
                                new PrimaryKeyIndexSourceFile("data-2", 20)));

        PrimaryKeyIndexSourceMeta restored =
                PrimaryKeyIndexSourceMeta.deserialize(metadata.serialize());

        assertThat(restored.sourceFiles()).isEqualTo(metadata.sourceFiles());
        assertThatThrownBy(restored::sourceFile)
                .hasMessageContaining("Expected exactly one source file");
    }

    @Test
    void testSingleSourceRoundTrip() {
        PrimaryKeyIndexSourceMeta metadata =
                new PrimaryKeyIndexSourceMeta(new PrimaryKeyIndexSourceFile("data-1", 0));

        PrimaryKeyIndexSourceMeta restored =
                PrimaryKeyIndexSourceMeta.deserialize(metadata.serialize());

        assertThat(restored.sourceFile()).isEqualTo(metadata.sourceFile());
    }

    @Test
    void testRejectsInvalidSourceFile() {
        assertThatThrownBy(() -> new PrimaryKeyIndexSourceFile("data-1", -1))
                .hasMessageContaining("row count must not be negative");
    }

    @Test
    void testRejectsUnsupportedVersion() throws Exception {
        DataOutputSerializer output = new DataOutputSerializer(64);
        output.writeInt(2);
        output.writeInt(1);
        output.writeUTF("data-1");
        output.writeLong(1);

        assertThatThrownBy(() -> PrimaryKeyIndexSourceMeta.deserialize(output.getCopyOfBuffer()))
                .hasMessageContaining("Unsupported index source version: 2");
    }

    @Test
    void testRejectsSourceCountBeforeAllocation() throws Exception {
        DataOutputSerializer output = new DataOutputSerializer(8);
        output.writeInt(1);
        output.writeInt(Integer.MAX_VALUE);

        assertThatThrownBy(() -> PrimaryKeyIndexSourceMeta.deserialize(output.getCopyOfBuffer()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("source file count")
                .hasMessageContaining("exceeds the maximum");
    }

    @Test
    void testRejectsTruncatedAndTrailingMetadata() throws Exception {
        DataOutputSerializer truncated = new DataOutputSerializer(64);
        truncated.writeInt(1);
        truncated.writeInt(1);
        truncated.writeUTF("data-1");
        assertThatThrownBy(() -> PrimaryKeyIndexSourceMeta.deserialize(truncated.getCopyOfBuffer()))
                .hasMessageContaining("Failed to deserialize index source metadata");

        DataOutputSerializer trailing = new DataOutputSerializer(64);
        trailing.writeInt(1);
        trailing.writeInt(1);
        trailing.writeUTF("data-1");
        trailing.writeLong(1);
        trailing.writeByte(42);
        assertThatThrownBy(() -> PrimaryKeyIndexSourceMeta.deserialize(trailing.getCopyOfBuffer()))
                .hasMessageContaining("Unexpected trailing bytes in index source metadata");
    }

    @Test
    void testReadsFromIndexFile() {
        PrimaryKeyIndexSourceMeta metadata =
                new PrimaryKeyIndexSourceMeta(new PrimaryKeyIndexSourceFile("data-1", 5));
        IndexFileMeta indexFile =
                new IndexFileMeta(
                        "btree",
                        "index-1",
                        10,
                        5,
                        new GlobalIndexMeta(0, 4, 1, null, null, metadata.serialize()),
                        null);

        assertThat(PrimaryKeyIndexSourceMeta.fromIndexFile(indexFile).sourceFile())
                .isEqualTo(metadata.sourceFile());
    }
}
