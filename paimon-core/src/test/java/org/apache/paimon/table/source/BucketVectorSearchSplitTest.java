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

package org.apache.paimon.table.source;

import org.apache.paimon.data.BinaryArray;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.index.DeletionVectorMeta;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceFile;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataOutputSerializer;
import org.apache.paimon.io.PojoDataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests the byte form of {@link BucketVectorSearchSplit}, which a reader outside the JVM reads. */
class BucketVectorSearchSplitTest {

    @Test
    void testRoundTrip() throws Exception {
        BucketVectorSearchSplit expected = split();

        // Compared whole rather than field by field: only that catches a field the format forgets
        // to carry.
        BucketVectorSearchSplit actual = deserialize(serialize(expected));
        assertThat(actual).isEqualTo(expected);
        assertPayload(actual.payloadFiles().get(0));
    }

    /** Java engines ship this split through object serialization, which uses the same form. */
    @Test
    void testJavaSerializationRoundTrip() throws Exception {
        BucketVectorSearchSplit expected = split();
        byte[] bytes = InstantiationUtil.serializeObject(expected);
        BucketVectorSearchSplit actual =
                InstantiationUtil.deserializeObject(
                        bytes, BucketVectorSearchSplit.class.getClassLoader());
        assertThat(actual).isEqualTo(expected);
        assertPayload(actual.payloadFiles().get(0));
    }

    /** Payloads go through {@code IndexFileMetaSerializer}, so every field it carries survives. */
    @Test
    void testRoundTripKeepsDeletionVectorRanges() throws Exception {
        LinkedHashMap<String, DeletionVectorMeta> dvRanges = new LinkedHashMap<>();
        dvRanges.put("data-1.orc", new DeletionVectorMeta("data-1.orc", 0, 8, 2L));
        BucketVectorSearchSplit expected = withDvRanges(dvRanges);

        assertThat(deserialize(serialize(expected))).isEqualTo(expected);
        assertThat(
                        InstantiationUtil.<BucketVectorSearchSplit>deserializeObject(
                                InstantiationUtil.serializeObject(expected),
                                BucketVectorSearchSplit.class.getClassLoader()))
                .isEqualTo(expected);
    }

    /** Two splits that compare equal have to serialize to the same bytes. */
    @Test
    void testSerializationIsCanonical() throws Exception {
        DataSplit dataSplit =
                dataSplit(Arrays.asList(dataFile("data-1.orc"), dataFile("data-2.orc")));
        List<Range> ranges = Collections.singletonList(new Range(0, 1));
        Map<String, List<Range>> ascending = new LinkedHashMap<>();
        ascending.put("data-1.orc", ranges);
        ascending.put("data-2.orc", ranges);
        Map<String, List<Range>> descending = new LinkedHashMap<>();
        descending.put("data-2.orc", ranges);
        descending.put("data-1.orc", ranges);

        List<IndexFileMeta> payloadFiles = split().payloadFiles();
        BucketVectorSearchSplit first =
                new BucketVectorSearchSplit(dataSplit, payloadFiles, ascending);
        BucketVectorSearchSplit second =
                new BucketVectorSearchSplit(dataSplit, payloadFiles, descending);
        assertThat(first).isEqualTo(second);
        assertThat(serialize(first)).isEqualTo(serialize(second));
    }

    /** A bucket with no vector payload still plans a split, so both collections can be empty. */
    @Test
    void testRoundTripWithoutPayloadsOrRowRanges() throws Exception {
        BucketVectorSearchSplit expected =
                new BucketVectorSearchSplit(
                        split().dataSplit(), Collections.emptyList(), Collections.emptyMap());
        assertThat(deserialize(serialize(expected))).isEqualTo(expected);
    }

    /** Names carry more than ASCII, so they have to survive unchanged. */
    @Test
    void testRoundTripNonAsciiNames() throws Exception {
        String indexType = "向量-ivf🚀";
        BucketVectorSearchSplit actual = deserialize(serialize(withPayloadIndexType(indexType)));
        assertThat(actual.payloadFiles().get(0).indexType()).isEqualTo(indexType);
    }

    @Test
    void testRejectsForeignBytes() throws Exception {
        byte[] wrongMagic = serialize(split());
        ByteBuffer.wrap(wrongMagic).putLong(0, 1);
        assertThatThrownBy(() -> deserialize(wrongMagic))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("wrong magic number");

        byte[] unsupportedVersion = serialize(split());
        ByteBuffer.wrap(unsupportedVersion).putInt(Long.BYTES, 2);
        assertThatThrownBy(() -> deserialize(unsupportedVersion))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unsupported BucketVectorSearchSplit version: 2");
    }

    private static byte[] serialize(BucketVectorSearchSplit split) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(256);
        split.serialize(out);
        return out.getCopyOfBuffer();
    }

    private static BucketVectorSearchSplit deserialize(byte[] bytes) throws IOException {
        return BucketVectorSearchSplit.deserialize(new DataInputDeserializer(bytes));
    }

    /**
     * Non-empty rows and stats on purpose: the nested BinaryRow layout is the part of this message
     * a reader is most likely to get wrong, and an empty row exercises none of it.
     */
    private static DataFileMeta dataFile(String fileName) {
        SimpleStats stats =
                new SimpleStats(
                        BinaryRow.singleColumn("min_value"),
                        BinaryRow.singleColumn("max_value"),
                        BinaryArray.fromLongArray(new Long[] {0L}));
        return new PojoDataFileMeta(
                fileName,
                1_234,
                6,
                BinaryRow.singleColumn("min_key"),
                BinaryRow.singleColumn("max_key"),
                stats,
                stats,
                3,
                9,
                7,
                1,
                Collections.emptyList(),
                Timestamp.fromEpochMillis(1_700_000_000_000L),
                0L,
                null,
                FileSource.COMPACT,
                null,
                null,
                40L,
                Arrays.asList("k", "v"),
                new long[] {3, 9});
    }

    private static DataSplit dataSplit(List<DataFileMeta> dataFiles) {
        return DataSplit.builder()
                .withSnapshot(11)
                .withPartition(BinaryRow.singleColumn(20250826))
                .withBucket(2)
                .withBucketPath("bucket-2")
                .withTotalBuckets(8)
                .withDataFiles(dataFiles)
                .build();
    }

    private static BucketVectorSearchSplit split() {
        DataSplit dataSplit = dataSplit(Collections.singletonList(dataFile("data-1.orc")));

        byte[] sourceMeta =
                new PrimaryKeyIndexSourceMeta(
                                1,
                                Collections.singletonList(
                                        new PrimaryKeyIndexSourceFile("data-1.orc", 6)))
                        .serialize();
        IndexFileMeta payload =
                new IndexFileMeta(
                        "ivf-pq",
                        "ann-0.idx",
                        5_000_000_000L,
                        6,
                        new GlobalIndexMeta(
                                40, 45, 7, new int[] {3, 5}, new byte[] {1, 2, 3}, sourceMeta),
                        "s3://vector-bucket/ann-0.idx");

        Map<String, List<Range>> ranges = new LinkedHashMap<>();
        ranges.put("data-1.orc", Arrays.asList(new Range(0, 1), new Range(4, 5)));
        return new BucketVectorSearchSplit(dataSplit, Collections.singletonList(payload), ranges);
    }

    private static BucketVectorSearchSplit withPayloadIndexType(String indexType) {
        BucketVectorSearchSplit split = split();
        IndexFileMeta payload = split.payloadFiles().get(0);
        IndexFileMeta renamed =
                new IndexFileMeta(
                        indexType,
                        payload.fileName(),
                        payload.fileSize(),
                        payload.rowCount(),
                        payload.globalIndexMeta(),
                        payload.externalPath());
        return new BucketVectorSearchSplit(
                split.dataSplit(), Collections.singletonList(renamed), split.rowRangesByFile());
    }

    private static BucketVectorSearchSplit withDvRanges(
            LinkedHashMap<String, DeletionVectorMeta> dvRanges) {
        BucketVectorSearchSplit base = split();
        IndexFileMeta payload = base.payloadFiles().get(0);
        IndexFileMeta withDvRanges =
                new IndexFileMeta(
                        payload.indexType(),
                        payload.fileName(),
                        payload.fileSize(),
                        payload.rowCount(),
                        dvRanges,
                        payload.externalPath(),
                        payload.globalIndexMeta());
        return new BucketVectorSearchSplit(
                base.dataSplit(), Collections.singletonList(withDvRanges), base.rowRangesByFile());
    }

    private static void assertPayload(IndexFileMeta payload) {
        assertThat(payload.indexType()).isEqualTo("ivf-pq");
        assertThat(payload.fileName()).isEqualTo("ann-0.idx");
        assertThat(payload.fileSize()).isEqualTo(5_000_000_000L);
        assertThat(payload.rowCount()).isEqualTo(6);
        assertThat(payload.externalPath()).isEqualTo("s3://vector-bucket/ann-0.idx");
        assertThat(payload.globalIndexMeta()).isNotNull();
        assertThat(payload.globalIndexMeta().rowRangeStart()).isEqualTo(40);
        assertThat(payload.globalIndexMeta().rowRangeEnd()).isEqualTo(45);
        assertThat(payload.globalIndexMeta().indexFieldId()).isEqualTo(7);
        assertThat(payload.globalIndexMeta().extraFieldIds()).containsExactly(3, 5);
        assertThat(payload.globalIndexMeta().indexMeta()).containsExactly(1, 2, 3);
        assertThat(PrimaryKeyIndexSourceMeta.fromIndexFile(payload).sourceFile().fileName())
                .isEqualTo("data-1.orc");
    }
}
