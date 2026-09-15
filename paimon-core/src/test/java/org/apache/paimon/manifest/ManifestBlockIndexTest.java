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
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RowRangeIndex;
import org.apache.paimon.utils.SerializationUtils;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.function.BiPredicate;

import static org.apache.paimon.manifest.ManifestSidecarTest.meta;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/** Complete sidecar payload format, compression and independent filtering. */
class ManifestBlockIndexTest {
    private final RowType type = RowType.of(DataTypes.INT(), DataTypes.STRING());
    private final ManifestSidecar.Settings defaults =
            new ManifestSidecar.Settings(true, true, true, true);

    private byte[] fixture(String field) throws IOException {
        Properties p = new Properties();
        try (java.io.InputStream in = getClass().getResourceAsStream("/manifest-sidecar.txt")) {
            p.load(in);
        }
        return Base64.getDecoder().decode(p.getProperty(field));
    }

    private byte[] partition(int p, String q) {
        BinaryRow row = new BinaryRow(2);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeInt(0, p);
        if (q == null) {
            writer.setNullAt(1);
        } else {
            writer.writeString(1, BinaryString.fromString(q));
        }
        writer.complete();
        return SerializationUtils.serializeBinaryRow(row);
    }

    private RowRangeIndex query(long point) {
        return RowRangeIndex.create(Collections.singletonList(new Range(point, point)));
    }

    private static BiPredicate<Integer, Integer> bucketFilter(int expected) {
        return new BiPredicate<Integer, Integer>() {
            @Override
            public boolean test(Integer bucket, Integer totalBuckets) {
                return bucket == expected;
            }
        };
    }

    private PartitionPredicate part(int value) {
        return PartitionPredicate.fromPredicate(type, new PredicateBuilder(type).equal(0, value));
    }

    @Test
    void partitionCoverageIsAlwaysGenerated() throws Exception {
        byte[] header = fixture("avroHeader");
        for (int mask = 0; mask < 4; mask++) {
            boolean rowIdEnabled = (mask & 1) != 0;
            boolean bucketEnabled = (mask & 2) != 0;
            ManifestSidecar.Settings settings =
                    new ManifestSidecar.Settings(true, true, rowIdEnabled, bucketEnabled);
            ManifestSidecar.Builder builder = new ManifestSidecar.Builder(settings, header);
            for (int block = 0; block < 2; block++) {
                builder.beginBlock(header.length + block * 100L, 100, 1);
                builder.add(100L + block * 100L, 10, partition(7 + block, "p"), 1, 4);
                builder.endBlock();
            }
            byte[] data = builder.serialize(header.length + 200, 2);
            assertThat(ByteBuffer.wrap(data).getInt(28 + header.length)).isEqualTo(2);
            for (int[] position : positions(data)) {
                assertThat(data[position[1]]).isEqualTo((byte) 1);
                assertThat(data[position[2]]).isEqualTo((byte) (rowIdEnabled ? 1 : 0));
                assertThat(data[position[3]]).isEqualTo((byte) (bucketEnabled ? 1 : 0));
            }
            ManifestFileMeta meta = meta("m", header.length + 200, 2);
            assertThat(ManifestSidecar.select(data, meta, query(999)).blocks())
                    .hasSize(rowIdEnabled ? 0 : 2);
            assertThat(ManifestSidecar.select(data, meta, null, part(99), type).blocks()).isEmpty();
            BiPredicate<Integer, Integer> buckets = bucketFilter(99);
            assertThat(ManifestSidecar.select(data, meta, null, null, type, buckets).blocks())
                    .hasSize(bucketEnabled ? 0 : 2);

            // Generation settings do not disable payloads already stored in a sidecar.
            byte[] existing = fixture("indexWithBuckets");
            ManifestFileMeta existingMeta = meta("manifest-golden", header.length + 400, 7);
            assertThat(ManifestSidecar.select(existing, existingMeta, query(999)).blocks())
                    .isEmpty();
            assertThat(
                            ManifestSidecar.select(existing, existingMeta, null, part(99), type)
                                    .blocks())
                    .isEmpty();
            assertThat(
                            ManifestSidecar.select(
                                            existing, existingMeta, null, null, type, buckets)
                                    .blocks())
                    .isEmpty();
        }
    }

    @Test
    void jointGoldenPreservesTuplesNullsAndDerivedOrdinals() throws Exception {
        byte[] a = partition(7, "left");
        byte[] b = partition(9, null);
        assertThat(a).isEqualTo(fixture("partitionA"));
        assertThat(b).isEqualTo(fixture("partitionB"));
        byte[] header = fixture("avroHeader");
        ManifestSidecar.Builder builder = new ManifestSidecar.Builder(defaults, header);
        builder.beginBlock(header.length, 100, 3);
        builder.add(0L, 10, a);
        builder.add(5L, 5, a);
        builder.add(20L, 5, b);
        builder.endBlock();
        builder.beginBlock(header.length + 100, 200, 2);
        builder.add((1L << 32) - 2, 5, b);
        builder.add(8254058425445L, 1, a);
        builder.endBlock();
        builder.beginBlock(header.length + 300, 100, 2);
        builder.add(20L, 5, a);
        builder.add(Long.MAX_VALUE, 1, b);
        builder.endBlock();
        byte[] data = builder.serialize(header.length + 400, 7);
        assertThat(data).isEqualTo(fixture("indexWithPartitions"));
        ManifestFileMeta meta = meta("manifest-golden", header.length + 400, 7);
        PartitionPredicate filter = spy(part(7));
        assertThat(ManifestSidecar.select(data, meta, query(20), filter, type).blocks())
                .extracting(block -> block.firstRecord)
                .containsExactly(0L, 5L);
        verify(filter, times(2)).test(any(BinaryRow.class));
        PartitionPredicate nullFilter =
                PartitionPredicate.fromPredicate(type, new PredicateBuilder(type).isNull(1));
        assertThat(ManifestSidecar.select(data, meta, null, nullFilter, type).blocks()).hasSize(3);
        assertThat(ManifestSidecar.select(data, meta, null, part(99), type).blocks()).isEmpty();
        // Missing partition payloads cannot be pruned by dictionary misses.
        assertThat(ManifestSidecar.select(fixture("index"), meta, null, part(99), type).blocks())
                .hasSize(3);
    }

    @Test
    void unavailableDimensionsAreIndependentAndDoNotPoisonLaterBlocks() throws Exception {
        ManifestSidecar.Settings settings = defaults;
        byte[] header = fixture("avroHeader");
        ManifestSidecar.Builder builder = new ManifestSidecar.Builder(settings, header);
        builder.beginBlock(header.length, 100, 1);
        builder.add(null, 10, partition(7, "left"));
        builder.endBlock();
        builder.beginBlock(header.length + 100, 100, 1);
        builder.add(200L, 10, null);
        builder.endBlock();
        builder.beginBlock(header.length + 200, 100, 1);
        builder.add(300L, 10, partition(7, "left")); // an existing dictionary ID remains usable
        builder.endBlock();
        byte[] data = builder.serialize(header.length + 300, 3);
        ManifestFileMeta meta = meta("m", header.length + 300, 3);
        assertThat(ManifestSidecar.select(data, meta, null, part(9), type).blocks())
                .extracting(block -> block.firstRecord)
                .containsExactly(1L);
        assertThat(ManifestSidecar.select(data, meta, query(999), part(7), type).blocks())
                .extracting(block -> block.firstRecord)
                .containsExactly(0L);
        assertThat(ManifestSidecar.select(data, meta, query(200), part(9), type).blocks())
                .extracting(block -> block.firstRecord)
                .containsExactly(1L);
    }

    private List<int[]> positions(byte[] data) {
        ByteBuffer in = ByteBuffer.wrap(data);
        in.position(24);
        int header = in.getInt();
        in.position(in.position() + header);
        int partitions = in.getInt();
        for (int i = 0; i < partitions; i++) {
            int length = in.getInt();
            in.position(in.position() + length);
        }
        int blocks = in.getInt();
        List<int[]> result = new ArrayList<>();
        for (int i = 0; i < blocks; i++) {
            int block = in.position();
            in.position(block + 24);
            int partition = skipPayload(in);
            int row = skipPayload(in);
            int bucket = skipPayload(in);
            result.add(new int[] {block, partition, row, bucket});
        }
        return result;
    }

    private int skipPayload(ByteBuffer in) {
        int position = in.position();
        if (in.get() != 0) {
            int length = in.getInt();
            in.position(in.position() + length);
        }
        return position;
    }

    private byte[] checksum(byte[] data) throws Exception {
        byte[] hash =
                MessageDigest.getInstance("SHA-256").digest(Arrays.copyOf(data, data.length - 32));
        System.arraycopy(hash, 0, data, data.length - 32, 32);
        return data;
    }

    @Test
    void absentRowOrBucketFiltersKeepRemainingDimensions() throws Exception {
        byte[] data = fixture("indexWithBuckets");
        ManifestFileMeta meta = meta("manifest-golden", fixture("avroHeader").length + 400, 7);
        assertThat(
                        ManifestSidecar.select(data, meta, null, part(7), type, bucketFilter(1))
                                .blocks())
                .extracting(block -> block.firstRecord)
                .containsExactly(0L);
        assertThat(ManifestSidecar.select(data, meta, query(20), part(7), type, null).blocks())
                .extracting(block -> block.firstRecord)
                .containsExactly(0L, 5L);
        assertThat(ManifestSidecar.select(data, meta, null, null, type, null).blocks())
                .extracting(block -> block.firstRecord)
                .containsExactly(0L, 3L, 5L);
    }

    @Test
    void bucketPayloadGoldenAndTotalBucketsArePreserved() throws Exception {
        byte[] header = fixture("avroHeader");
        byte[] a = partition(7, "left");
        byte[] b = partition(9, null);
        ManifestSidecar.Builder builder = new ManifestSidecar.Builder(defaults, header);
        builder.beginBlock(header.length, 100, 3);
        builder.add(0L, 10, a, 1, 4);
        builder.add(5L, 5, a, 1, 4);
        builder.add(20L, 5, b, 1, 8);
        builder.endBlock();
        builder.beginBlock(header.length + 100, 200, 2);
        builder.add((1L << 32) - 2, 5, b, 2, 4);
        builder.add(8254058425445L, 1, a, 2, 8);
        builder.endBlock();
        builder.beginBlock(header.length + 300, 100, 2);
        builder.add(20L, 5, a, 0, 1);
        builder.add(Long.MAX_VALUE, 1, b, 3, 4);
        builder.endBlock();
        byte[] data = builder.serialize(header.length + 400, 7);
        assertThat(data).isEqualTo(fixture("indexWithBuckets"));
        ManifestFileMeta meta = meta("manifest-golden", header.length + 400, 7);
        BiPredicate<Integer, Integer> bucket = bucketFilter(1);
        assertThat(ManifestSidecar.select(data, meta, null, null, type, bucket).blocks())
                .extracting(block -> block.firstRecord)
                .containsExactly(0L);
        // Existing row-id and partition coverage remains independently usable.
        assertThat(
                        ManifestSidecar.select(data, meta, query(0), part(7), type, bucketFilter(2))
                                .blocks())
                .isEmpty();
        BiPredicate<Integer, Integer> filter = (bucketId, total) -> bucketId == 2 && total == 8;
        assertThat(ManifestSidecar.select(data, meta, null, null, type, filter).blocks())
                .extracting(block -> block.firstRecord)
                .containsExactly(3L);
        // The caller can omit a bucket filter which requires an actual entry partition.
        assertThat(ManifestSidecar.select(data, meta, null, null, type, null).blocks()).hasSize(3);
        for (String unavailable : new String[] {"index", "indexWithPartitions"}) {
            assertThat(
                            ManifestSidecar.select(
                                            fixture(unavailable),
                                            meta,
                                            null,
                                            null,
                                            type,
                                            bucketFilter(99))
                                    .blocks())
                    .hasSize(3);
        }
    }

    @Test
    void unknownOrInvalidBucketPayloadIsUnavailable() throws Exception {
        ManifestSidecar.Settings settings = defaults;
        byte[] header = fixture("avroHeader");
        for (Integer[] pair :
                Arrays.asList(
                        new Integer[] {null, null},
                        new Integer[] {-1, 4},
                        new Integer[] {4, 4},
                        new Integer[] {0, 0})) {
            ManifestSidecar.Builder builder = new ManifestSidecar.Builder(settings, header);
            builder.beginBlock(header.length, 100, 2);
            builder.add(100L, 10, partition(7, "left"), 1, 4);
            builder.add(200L, 10, partition(7, "left"), pair[0], pair[1]);
            builder.endBlock();
            builder.beginBlock(header.length + 100, 100, 1);
            builder.add(300L, 10, partition(7, "left"), 1, 4);
            builder.endBlock();
            byte[] data = builder.serialize(header.length + 200, 3);
            int bucket = positions(data).get(0)[3];
            assertThat(data[bucket]).isZero();
            assertThat(positions(data).get(1)[0]).isEqualTo(bucket + 1);
            ManifestFileMeta meta = meta("m", header.length + 200, 3);
            assertThat(
                            ManifestSidecar.select(data, meta, null, null, type, bucketFilter(99))
                                    .blocks())
                    .extracting(block -> block.firstRecord)
                    .containsExactly(0L);
            assertThat(
                            ManifestSidecar.select(
                                            data, meta, query(999), null, type, bucketFilter(99))
                                    .blocks())
                    .isEmpty();
        }
    }

    @Test
    void largePayloadsKeepExactCoverage() throws Exception {
        byte[] header = fixture("avroHeader");
        ManifestSidecar.Builder builder = new ManifestSidecar.Builder(defaults, header);
        int blocks = 33;
        int entriesPerBlock = 4097;
        int entries = blocks * entriesPerBlock;
        int blockBytes = 1024 * 1024;
        for (int block = 0; block < blocks; block++) {
            builder.beginBlock(
                    header.length + (long) block * blockBytes, blockBytes, entriesPerBlock);
            for (int i = 0; i < entriesPerBlock; i++) {
                int entry = block * entriesPerBlock + i;
                builder.add(entry * 2L, 1, partition(entry, null), i, entriesPerBlock + 1);
            }
            builder.endBlock();
        }
        long fileSize = header.length + (long) blocks * blockBytes;
        byte[] data = builder.serialize(fileSize, entries);
        ManifestFileMeta meta = meta("m", fileSize, entries);
        long last = (entries - 1L) * 2;
        assertThat(ManifestSidecar.select(data, meta, query(last)).blocks())
                .extracting(block -> block.firstRecord)
                .containsExactly((blocks - 1L) * entriesPerBlock);
        assertThat(ManifestSidecar.select(data, meta, query(last - 1)).blocks()).isEmpty();
        assertThat(ManifestSidecar.select(data, meta, null, part(entries), type).blocks())
                .isEmpty();
        assertThat(
                        ManifestSidecar.select(
                                        data, meta, null, null, type, bucketFilter(entriesPerBlock))
                                .blocks())
                .isEmpty();
    }

    @Test
    void absentPayloadsOmitLengthFieldsForEveryDimensionCombination() throws Exception {
        byte[] header = fixture("avroHeader");
        ManifestSidecar.Builder builder = new ManifestSidecar.Builder(defaults, header);
        for (int mask = 0; mask < 8; mask++) {
            builder.beginBlock(header.length + mask * 100L, 100, 1);
            builder.add(
                    (mask & 2) == 0 ? null : 100L + mask,
                    1,
                    (mask & 1) == 0 ? null : partition(7, "left"),
                    (mask & 4) == 0 ? null : 0,
                    (mask & 4) == 0 ? null : 1);
            builder.endBlock();
        }
        byte[] data = builder.serialize(header.length + 800, 8);
        List<int[]> positions = positions(data);
        int[] presentSizes = {10, 25, 10};
        for (int mask = 0; mask < 8; mask++) {
            for (int dimension = 0; dimension < 3; dimension++) {
                int start = positions.get(mask)[dimension + 1];
                int end =
                        dimension < 2
                                ? positions.get(mask)[dimension + 2]
                                : mask < 7 ? positions.get(mask + 1)[0] : data.length - 32;
                boolean present = (mask & (1 << dimension)) != 0;
                assertThat(data[start]).isEqualTo((byte) (present ? 1 : 0));
                assertThat(end - start).isEqualTo(present ? presentSizes[dimension] : 1);
            }
        }
        ManifestFileMeta meta = meta("m", header.length + 800, 8);
        assertThat(ManifestSidecar.select(data, meta, null, part(99), type).blocks())
                .extracting(block -> block.firstRecord)
                .containsExactly(0L, 2L, 4L, 6L);
        assertThat(ManifestSidecar.select(data, meta, query(999)).blocks())
                .extracting(block -> block.firstRecord)
                .containsExactly(0L, 1L, 4L, 5L);
        BiPredicate<Integer, Integer> buckets = bucketFilter(99);
        assertThat(ManifestSidecar.select(data, meta, null, null, type, buckets).blocks())
                .extracting(block -> block.firstRecord)
                .containsExactly(0L, 1L, 2L, 3L);
        assertThat(ManifestSidecar.select(data, meta, query(999), part(99), type, buckets).blocks())
                .extracting(block -> block.firstRecord)
                .containsExactly(0L);
    }

    @Test
    void deltaVarintsCompressSortedPayloadsWithoutCoarseningRowIds() throws Exception {
        byte[] header = fixture("avroHeader");
        ManifestSidecar.Builder builder = new ManifestSidecar.Builder(defaults, header);
        int count = 10000;
        builder.beginBlock(header.length, 100, count);
        for (int i = count - 1; i >= 0; i--) {
            builder.add(i * 4L, 3, partition(i, null), i, count);
        }
        builder.endBlock();
        byte[] data = builder.serialize(header.length + 100, count);
        int[] block = positions(data).get(0);
        assertThat(ByteBuffer.wrap(data).getInt(block[1] + 1)).isEqualTo(4 + count);
        assertThat(ByteBuffer.wrap(data).getInt(block[2] + 1)).isEqualTo(20 + 2 * (count - 1));
        assertThat(ByteBuffer.wrap(data).getInt(block[3] + 1)).isEqualTo(4 + 2 + 5 * (count - 1));
        ManifestFileMeta meta = meta("m", header.length + 100, count);
        assertThat(ManifestSidecar.select(data, meta, query(3)).blocks()).isEmpty();
        assertThat(
                        ManifestSidecar.select(
                                        data, meta, query(4), part(9999), type, bucketFilter(9999))
                                .blocks())
                .hasSize(1);
    }

    @Test
    void unpartitionedTablesStillRecordTheEmptyPartition() throws Exception {
        byte[] header = fixture("avroHeader");
        ManifestSidecar.Builder builder =
                new ManifestSidecar.Builder(
                        new ManifestSidecar.Settings(true, true, false, false), header);
        builder.beginBlock(header.length, 100, 1);
        builder.add(null, 0, SerializationUtils.serializeBinaryRow(BinaryRow.EMPTY_ROW));
        builder.endBlock();
        byte[] data = builder.serialize(header.length + 100, 1);
        assertThat(ByteBuffer.wrap(data).getInt(28 + header.length)).isEqualTo(1);
        int[] block = positions(data).get(0);
        assertThat(data[block[1]]).isEqualTo((byte) 1);
        assertThat(data[block[2]]).isZero();
        assertThat(data[block[3]]).isZero();
        assertThat(
                        ManifestSidecar.select(
                                        data,
                                        meta("m", header.length + 100, 1),
                                        null,
                                        null,
                                        RowType.of())
                                .blocks())
                .hasSize(1);
    }

    @Test
    void rowMissSkipsPartitionAndBucketDecoding() throws Exception {
        byte[] data =
                replacePayload(fixture("indexWithBuckets"), 0, 1, compressedPayload(2, 999, 1));
        data = replacePayload(data, 0, 3, compressedPayload(2, 0, 0));
        BiPredicate<Integer, Integer> buckets = mock(BiPredicate.class);
        assertThat(
                        ManifestSidecar.select(
                                        data, goldenMeta(), query(15), part(7), type, buckets)
                                .blocks())
                .isEmpty();
        verifyNoInteractions(buckets);
    }

    @Test
    void partitionMissSkipsBucketDecodingWithOrWithoutRowQuery() throws Exception {
        byte[] data = replacePayload(fixture("indexWithBuckets"), 0, 3, compressedPayload(2, 0, 0));
        for (RowRangeIndex rows : Arrays.asList(null, query(0))) {
            BiPredicate<Integer, Integer> buckets = mock(BiPredicate.class);
            assertThat(
                            ManifestSidecar.select(
                                            data, goldenMeta(), rows, part(99), type, buckets)
                                    .blocks())
                    .isEmpty();
            verifyNoInteractions(buckets);
        }
    }

    @Test
    void absentPartitionFilterDoesNotDecodePartitionIds() throws Exception {
        byte[] data =
                replacePayload(fixture("indexWithBuckets"), 0, 1, compressedPayload(2, 999, 1));
        BiPredicate<Integer, Integer> buckets = spy(bucketFilter(1));
        assertThat(
                        ManifestSidecar.select(data, goldenMeta(), query(20), null, type, buckets)
                                .blocks())
                .extracting(block -> block.firstRecord)
                .containsExactly(0L);
        verify(buckets).test(1, 4);
        verify(buckets).test(0, 1);
        verify(buckets).test(3, 4);
        verifyNoMoreInteractions(buckets);
    }

    @Test
    void matchesSkipUnusedDeltas() throws Exception {
        byte[] partitions =
                replacePayload(fixture("indexWithBuckets"), 0, 1, compressedPayload(2, 0, 999));
        assertThat(
                        ManifestSidecar.select(partitions, goldenMeta(), query(0), part(7), type)
                                .blocks())
                .hasSize(1);
        assertThatThrownBy(
                        () ->
                                ManifestSidecar.select(
                                        partitions, goldenMeta(), query(0), part(99), type))
                .isInstanceOf(IOException.class);
        byte[] buckets =
                replacePayload(
                        fixture("indexWithBuckets"),
                        0,
                        3,
                        compressedPayload(2, (1L << 32) | 4, Long.MAX_VALUE));
        assertThat(
                        ManifestSidecar.select(
                                        buckets,
                                        goldenMeta(),
                                        query(0),
                                        null,
                                        type,
                                        bucketFilter(1))
                                .blocks())
                .hasSize(1);
        assertThatThrownBy(
                        () ->
                                ManifestSidecar.select(
                                        buckets,
                                        goldenMeta(),
                                        query(0),
                                        null,
                                        type,
                                        bucketFilter(99)))
                .isInstanceOf(IOException.class);

        byte[] header = fixture("avroHeader");
        ManifestSidecar.Builder builder = new ManifestSidecar.Builder(defaults, header);
        builder.beginBlock(header.length, 100, 3);
        for (long first : new long[] {0, 20, 40}) {
            builder.add(first, 10);
        }
        builder.endBlock();
        byte[] rows =
                replacePayload(
                        builder.serialize(header.length + 100, 3),
                        0,
                        2,
                        rowPayload(3, 0, 49, 9, 11, 9, 99));
        ManifestFileMeta meta = meta("m", header.length + 100, 3);
        assertThat(ManifestSidecar.select(rows, meta, query(0)).blocks()).hasSize(1);
        assertThat(ManifestSidecar.select(rows, meta, query(20)).blocks()).hasSize(1);
        assertThat(ManifestSidecar.select(rows, meta, query(100)).blocks()).isEmpty();
        assertThatThrownBy(() -> ManifestSidecar.select(rows, meta, query(35)))
                .isInstanceOf(IOException.class);
    }

    @Test
    void malformedCompressedPayloadsFailWhenConsumed() throws Exception {
        List<byte[]> badRows =
                Arrays.asList(
                        rowPayload(2, 0, 24, 9), // Missing an endpoint.
                        rowPayload(2, 0, 24, 9, 0), // Overlapping intervals.
                        rowPayload(2, 0, 24, Long.MAX_VALUE, 0), // Exceeds the envelope.
                        rowPayload(2, 0, 24, 9, 11, 0), // More values than declared.
                        rowPayload(1, Long.MAX_VALUE, 1), // Envelope overflows.
                        rowPayload(1, -1, 24),
                        rowPayload(1, 0, -1),
                        Arrays.copyOf(rowPayload(1, 0, 24), 19), // Truncated fixed-width envelope.
                        rowPayload(1, 0, 24, 0)); // Unexpected value for a single interval.
        for (byte[] payload : badRows) {
            byte[] data = replacePayload(fixture("indexWithBuckets"), 0, 2, payload);
            assertThatThrownBy(() -> ManifestSidecar.select(data, goldenMeta(), query(15)))
                    .isInstanceOf(IOException.class);
        }
        for (byte[] payload :
                Arrays.asList(compressedPayload(2, 999, 0), compressedPayload(2, 0, 0))) {
            byte[] data = replacePayload(fixture("indexWithBuckets"), 0, 1, payload);
            assertThatThrownBy(
                            () ->
                                    ManifestSidecar.select(
                                            data, goldenMeta(), query(0), part(99), type))
                    .isInstanceOf(IOException.class);
        }
        for (byte[] payload :
                Arrays.asList(
                        compressedPayload(2, 0, 4),
                        compressedPayload(2, 1L << 31, 4),
                        compressedPayload(2, 0, 0))) {
            byte[] data = replacePayload(fixture("indexWithBuckets"), 0, 3, payload);
            assertThatThrownBy(
                            () ->
                                    ManifestSidecar.select(
                                            data,
                                            goldenMeta(),
                                            query(0),
                                            null,
                                            type,
                                            bucketFilter(99)))
                    .isInstanceOf(IOException.class);
        }
    }

    @Test
    void malformedDeltaVarintsFailWhenConsumed() throws Exception {
        byte[] overlong = new byte[10];
        Arrays.fill(overlong, (byte) 0x80);
        for (byte[] deltas :
                Arrays.asList(
                        new byte[] {(byte) 0x80},
                        new byte[] {(byte) 0x80, (byte) 0x80}, // Truncated delta.
                        new byte[] {(byte) 0x81, 0, 0}, // Noncanonical delta.
                        overlong)) {
            for (int dimension = 1; dimension <= 3; dimension++) {
                ByteArrayOutputStream payload = new ByteArrayOutputStream();
                payload.write(dimension == 2 ? rowPayload(2, 0, 24) : compressedPayload(2));
                payload.write(deltas);
                byte[] data =
                        replacePayload(
                                fixture("indexWithBuckets"), 0, dimension, payload.toByteArray());
                int dim = dimension;
                assertThatThrownBy(
                                () ->
                                        ManifestSidecar.select(
                                                data,
                                                goldenMeta(),
                                                query(0),
                                                dim == 1 ? part(99) : null,
                                                type,
                                                dim == 3 ? bucketFilter(99) : null))
                        .isInstanceOf(IOException.class);
            }
        }
    }

    @Test
    void invalidCountFramingAndDirectoryFailEvenWhenFiltersMiss() throws Exception {
        List<byte[]> bad =
                Arrays.asList(
                        new byte[0],
                        new byte[3], // Incomplete int count.
                        compressedPayload(-1, 1, 1),
                        compressedPayload(0, 1, 1),
                        compressedPayload(4, 1, 1),
                        compressedPayload(Integer.MAX_VALUE, 1, 1),
                        compressedPayload(1), // Count without payload data.
                        compressedPayload(2, 1));
        for (int dimension = 1; dimension <= 3; dimension++) {
            for (byte[] payload : bad) {
                byte[] data = replacePayload(fixture("indexWithBuckets"), 0, dimension, payload);
                assertThatThrownBy(() -> ManifestSidecar.select(data, goldenMeta(), query(999)))
                        .isInstanceOf(IOException.class);
            }
            byte[] data = fixture("indexWithBuckets");
            int position = positions(data).get(0)[dimension];
            ByteBuffer.wrap(data).putInt(position + 1, Integer.MAX_VALUE);
            checksum(data);
            assertThatThrownBy(() -> ManifestSidecar.select(data, goldenMeta(), query(999)))
                    .isInstanceOf(IOException.class);
        }
        byte[] missingEnvelope =
                replacePayload(
                        fixture("indexWithBuckets"), 0, 2, Arrays.copyOf(rowPayload(1, 0, 24), 19));
        assertThatThrownBy(() -> ManifestSidecar.select(missingEnvelope, goldenMeta(), null))
                .isInstanceOf(IOException.class);
        byte[] data = fixture("indexWithBuckets");
        int block = positions(data).get(0)[0];
        ByteBuffer.wrap(data).putLong(block + 16, 2);
        checksum(data);
        assertThatThrownBy(() -> ManifestSidecar.select(data, goldenMeta(), query(999)))
                .isInstanceOf(IOException.class);
    }

    @Test
    void unknownEncodingsRemainIndependent() throws Exception {
        for (int dimension = 1; dimension <= 3; dimension++) {
            byte[] data =
                    replacePayload(
                            fixture("indexWithBuckets"), 0, dimension, new byte[] {(byte) 0x80});
            data[positions(data).get(0)[dimension]] = (byte) 202;
            checksum(data);
            assertThat(
                            ManifestSidecar.select(
                                            data,
                                            goldenMeta(),
                                            query(dimension == 2 ? 999 : 0),
                                            part(dimension == 1 ? 99 : 7),
                                            type,
                                            bucketFilter(dimension == 3 ? 99 : 1))
                                    .blocks())
                    .extracting(block -> block.firstRecord)
                    .containsExactly(0L);
        }
    }

    private ManifestFileMeta goldenMeta() throws Exception {
        return meta("manifest-golden", fixture("avroHeader").length + 400, 7);
    }

    private byte[] replacePayload(byte[] data, int block, int dimension, byte[] payload)
            throws Exception {
        int start = positions(data).get(block)[dimension];
        int end =
                data[start] == 0 ? start + 1 : start + 5 + ByteBuffer.wrap(data).getInt(start + 1);
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(buffer);
        out.write(data, 0, start);
        out.writeByte(1);
        out.writeInt(payload.length);
        out.write(payload);
        out.write(data, end, data.length - end);
        return checksum(buffer.toByteArray());
    }

    private static byte[] compressedPayload(int count, long... values) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(buffer);
        out.writeInt(count);
        out.write(deltaBytes(values));
        return buffer.toByteArray();
    }

    private static byte[] rowPayload(int count, long min, long span, long... values)
            throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(buffer);
        out.writeInt(count);
        out.writeLong(min);
        out.writeLong(span);
        out.write(deltaBytes(values));
        return buffer.toByteArray();
    }

    private static byte[] deltaBytes(long... values) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (long value : values) {
            while ((value & ~0x7fL) != 0) {
                out.write((int) (value & 0x7f) | 0x80);
                value >>>= 7;
            }
            out.write((int) value);
        }
        return out.toByteArray();
    }
}
