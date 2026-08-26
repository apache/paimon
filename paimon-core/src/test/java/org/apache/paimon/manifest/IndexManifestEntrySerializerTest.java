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

package org.apache.paimon.manifest;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.utils.ObjectSerializer;
import org.apache.paimon.utils.ObjectSerializerTestBase;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Random;

import static org.apache.paimon.index.IndexFileMetaSerializerTest.randomIndexFile;
import static org.apache.paimon.io.DataFileTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link IndexManifestEntrySerializer}. */
public class IndexManifestEntrySerializerTest extends ObjectSerializerTestBase<IndexManifestEntry> {

    @Test
    void testReadsGlobalIndexWithoutSourceMeta() {
        IndexManifestEntrySerializer serializer = new IndexManifestEntrySerializer();
        IndexManifestEntry entry =
                new IndexManifestEntry(
                        FileKind.ADD,
                        BinaryRow.EMPTY_ROW,
                        0,
                        new IndexFileMeta(
                                "btree",
                                "index-file",
                                100,
                                10,
                                new GlobalIndexMeta(0, 9, 7, null, new byte[] {1}),
                                null));
        InternalRow serialized = serializer.toRow(entry);
        assertThat(serialized.getInt(0)).isEqualTo(1);
        assertThat(serialized.getRow(10, GlobalIndexMeta.SCHEMA.getFieldCount()).getFieldCount())
                .isEqualTo(7);

        GlobalIndexMeta restored = serializer.fromRow(serialized).indexFile().globalIndexMeta();

        assertThat(restored.indexMeta()).containsExactly(1);
        assertThat(restored.sourceMeta()).isNull();
        assertThat(restored.buildSchemaId()).isNull();
    }

    @Test
    void testGlobalIndexMetadataRoundTrip() throws IOException {
        IndexManifestEntrySerializer serializer = new IndexManifestEntrySerializer();
        IndexManifestEntry entry =
                new IndexManifestEntry(
                        FileKind.ADD,
                        BinaryRow.EMPTY_ROW,
                        0,
                        new IndexFileMeta(
                                "ivf-pq",
                                "index-file",
                                100,
                                10,
                                new GlobalIndexMeta(
                                        0, 9, 7, null, new byte[] {3, 4}, new byte[] {1, 2}, 11L),
                                null));
        assertThat(serializer.toRow(entry).getInt(0)).isEqualTo(1);

        GlobalIndexMeta restored =
                serializer
                        .deserializeFromBytes(serializer.serializeToBytes(entry))
                        .indexFile()
                        .globalIndexMeta();

        assertThat(restored.indexMeta()).containsExactly(3, 4);
        assertThat(restored.sourceMeta()).containsExactly(1, 2);
        assertThat(restored.buildSchemaId()).isEqualTo(11L);
    }

    @Override
    protected ObjectSerializer<IndexManifestEntry> serializer() {
        return new IndexManifestEntrySerializer();
    }

    @Override
    protected IndexManifestEntry object() {
        return randomIndexEntry();
    }

    public static IndexManifestEntry randomIndexEntry() {
        Random rnd = new Random();
        return new IndexManifestEntry(
                rnd.nextBoolean() ? FileKind.ADD : FileKind.DELETE,
                row(rnd.nextInt()),
                rnd.nextInt(),
                randomIndexFile());
    }
}
