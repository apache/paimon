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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.DeletionFileKey;
import org.apache.paimon.deletionvectors.DeletionVectorsIndexFile;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataOutputSerializer;
import org.apache.paimon.utils.ObjectSerializer;
import org.apache.paimon.utils.ObjectSerializerTestBase;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link org.apache.paimon.index.IndexFileMetaSerializer}. */
public class IndexFileMetaSerializerTest extends ObjectSerializerTestBase<IndexFileMeta> {

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
        LinkedHashMap<DeletionFileKey, DeletionVectorMeta> dvRanges = new LinkedHashMap<>();
        dvRanges.put(
                DeletionFileKey.ofFileName("my_file_name1"),
                new DeletionVectorMeta(
                        "my_file_name1", rnd.nextInt(), rnd.nextInt(), rnd.nextLong()));
        dvRanges.put(
                DeletionFileKey.ofFileName("my_file_name2"),
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

    public static IndexFileMeta rowIdRangeDeletionVectorIndexFile() {
        DeletionFileKey rowIdRangeKey = DeletionFileKey.ofRange(new Range(10, 19));
        LinkedHashMap<DeletionFileKey, DeletionVectorMeta> dvRanges = new LinkedHashMap<>();
        dvRanges.put(rowIdRangeKey, new DeletionVectorMeta(rowIdRangeKey, 4, 5, 6L));
        return new IndexFileMeta(
                DeletionVectorsIndexFile.DELETION_VECTORS_INDEX,
                "deletion_vectors_index_file_name",
                100,
                9,
                dvRanges,
                null);
    }

    @Test
    public void testRowIdRangeDeletionVectorsRoundTrip() {
        IndexFileMeta indexFile = rowIdRangeDeletionVectorIndexFile();

        IndexFileMeta actual = serializer().fromRow(serializer().toRow(indexFile));

        assertThat(actual).isEqualTo(indexFile);
        assertThat(actual.dvRanges()).containsOnlyKeys(DeletionFileKey.ofRange(new Range(10, 19)));
    }

    @Test
    public void testRowIdRangeDeletionVectorsSerializeRoundTrip() throws IOException {
        IndexFileMeta indexFile = rowIdRangeDeletionVectorIndexFile();
        IndexFileMetaSerializer serializer = new IndexFileMetaSerializer();
        DataOutputSerializer out = new DataOutputSerializer(128);

        serializer.serialize(indexFile, out);
        IndexFileMeta actual =
                serializer.deserialize(new DataInputDeserializer(out.getCopyOfBuffer()));

        assertThat(actual).isEqualTo(indexFile);
    }

    @Test
    public void testRowIdRangeDeletionVectorsLegacyMarker() {
        IndexFileMeta indexFile = rowIdRangeDeletionVectorIndexFile();
        IndexFileMetaSerializer serializer = new IndexFileMetaSerializer();

        InternalRow row = serializer.toRow(indexFile);

        assertThat(serializer.fromRow(row)).isEqualTo(indexFile);
        assertThat(DeletionVectorMeta.isLegacyMarker(row.getArray(4))).isTrue();
        assertThat(row.isNullAt(7)).isFalse();
        // ensure that old path will fast-fail
        assertThatThrownBy(
                        () ->
                                IndexFileMetaSerializer.rowArrayDataToFileNameDvMetas(
                                        row.getArray(4)))
                .isInstanceOf(NullPointerException.class);
    }
}
