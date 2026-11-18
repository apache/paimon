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

import org.apache.paimon.deletionvectors.DeletionVectorsIndexFile;
import org.apache.paimon.globalindex.CentroidVectorIndexUtils;
import org.apache.paimon.globalindex.VectorIndexShardMeta;
import org.apache.paimon.utils.ObjectSerializer;
import org.apache.paimon.utils.ObjectSerializerTestBase;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link org.apache.paimon.index.IndexFileMetaSerializer}. */
public class IndexFileMetaSerializerTest extends ObjectSerializerTestBase<IndexFileMeta> {

    @Test
    void testRoutingModelKindRoundTrip() throws Exception {
        byte[] descriptor =
                VectorIndexShardMeta.routing(
                                "model", 1, Collections.singletonList(new Range(10, 1000)))
                        .serialize();
        IndexFileMeta routing =
                new IndexFileMeta(
                        "ivf-pq",
                        "model.index",
                        100,
                        0,
                        new GlobalIndexMeta(10, 1000, 7, null, descriptor),
                        null);
        IndexFileMetaSerializer serializer = new IndexFileMetaSerializer();
        assertThat(serializer.fromRow(serializer.toRow(routing))).isEqualTo(routing);
        assertThat(
                        CentroidVectorIndexUtils.isRoutingModel(
                                serializer.fromRow(serializer.toRow(routing))))
                .isTrue();

        org.apache.paimon.manifest.IndexManifestEntry entry =
                new org.apache.paimon.manifest.IndexManifestEntry(
                        org.apache.paimon.manifest.FileKind.ADD,
                        org.apache.paimon.data.BinaryRow.EMPTY_ROW,
                        0,
                        routing);
        org.apache.paimon.manifest.IndexManifestEntrySerializer manifest =
                new org.apache.paimon.manifest.IndexManifestEntrySerializer();
        assertThat(manifest.fromRow(manifest.toRow(entry))).isEqualTo(entry);
    }

    @Test
    void testGlobalIndexSourceMetaRoundTrip() {
        IndexFileMetaSerializer serializer = new IndexFileMetaSerializer();
        IndexFileMeta indexFile =
                new IndexFileMeta(
                        "ivf-pq",
                        "index-file",
                        100,
                        10,
                        new GlobalIndexMeta(0, 9, 7, null, new byte[] {3, 4}, new byte[] {1, 2}),
                        null);

        GlobalIndexMeta restored =
                serializer.fromRow(serializer.toRow(indexFile)).globalIndexMeta();

        assertThat(restored.sourceMeta()).containsExactly(1, 2);
        assertThat(restored.indexMeta()).containsExactly(3, 4);
    }

    @Test
    void testEqualityIncludesGlobalIndexMeta() {
        IndexFileMeta first =
                globalIndexFile(
                        new GlobalIndexMeta(
                                0, 9, 7, new int[] {8}, new byte[] {3}, new byte[] {1}));
        IndexFileMeta equal =
                globalIndexFile(
                        new GlobalIndexMeta(
                                0, 9, 7, new int[] {8}, new byte[] {3}, new byte[] {1}));
        IndexFileMeta different =
                globalIndexFile(
                        new GlobalIndexMeta(
                                0, 9, 7, new int[] {8}, new byte[] {3}, new byte[] {2}));

        assertThat(first).isEqualTo(equal).hasSameHashCodeAs(equal).isNotEqualTo(different);
    }

    private static IndexFileMeta globalIndexFile(GlobalIndexMeta globalIndexMeta) {
        return new IndexFileMeta("ivf-pq", "index-file", 100, 10, globalIndexMeta, null);
    }

    @Override
    protected ObjectSerializer<IndexFileMeta> serializer() {
        return new IndexFileMetaSerializer();
    }

    @Override
    protected IndexFileMeta object() {
        return randomIndexFile();
    }

    public static IndexFileMeta randomIndexFile() {
        Random rnd = new Random();
        if (rnd.nextBoolean()) {
            return randomHashIndexFile();
        } else {
            return randomDeletionVectorIndexFile();
        }
    }

    public static IndexFileMeta randomHashIndexFile() {
        Random rnd = new Random();
        return new IndexFileMeta(
                HashIndexFile.HASH_INDEX,
                "my_file_name" + rnd.nextLong(),
                rnd.nextInt(),
                rnd.nextInt(),
                null,
                null,
                null);
    }

    public static IndexFileMeta randomDeletionVectorIndexFile() {
        Random rnd = new Random();
        LinkedHashMap<String, DeletionVectorMeta> dvRanges = new LinkedHashMap<>();
        dvRanges.put(
                "my_file_name1",
                new DeletionVectorMeta(
                        "my_file_name1", rnd.nextInt(), rnd.nextInt(), rnd.nextLong()));
        dvRanges.put(
                "my_file_name2",
                new DeletionVectorMeta(
                        "my_file_name2", rnd.nextInt(), rnd.nextInt(), rnd.nextLong()));
        return new IndexFileMeta(
                DeletionVectorsIndexFile.DELETION_VECTORS_INDEX,
                "deletion_vectors_index_file_name" + rnd.nextLong(),
                rnd.nextInt(),
                rnd.nextInt(),
                dvRanges,
                null);
    }
}
