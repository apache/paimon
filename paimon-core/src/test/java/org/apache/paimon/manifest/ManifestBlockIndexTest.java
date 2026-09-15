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
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RowRangeIndex;
import org.apache.paimon.utils.SerializationUtils;

import org.junit.jupiter.api.Test;

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

import static org.apache.paimon.manifest.ManifestSidecarTest.MAX_BYTES;
import static org.apache.paimon.manifest.ManifestSidecarTest.meta;
import static org.apache.paimon.manifest.ManifestSidecarTest.settings;
import static org.apache.paimon.manifest.ManifestSidecarTest.sidecarOptions;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/** Independent partition/row-ID payloads and conservative resource degradation. */
class ManifestBlockIndexTest {
    private final RowType type = RowType.of(DataTypes.INT(), DataTypes.STRING());
    private final ManifestSidecar.Settings defaults = settings(sidecarOptions(), 2);

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
    void payloadGenerationCanBeDisabledIndependently() throws Exception {
        byte[] header = fixture("avroHeader");
        for (int mask = 0; mask < 8; mask++) {
            boolean partitionEnabled = (mask & 1) != 0;
            boolean rowIdEnabled = (mask & 2) != 0;
            boolean bucketEnabled = (mask & 4) != 0;
            ManifestSidecar.Settings settings =
                    new ManifestSidecar.Settings(
                            16 * 1024 * 1024L, partitionEnabled, rowIdEnabled, bucketEnabled);
            ManifestSidecar.Builder builder = new ManifestSidecar.Builder(settings, header);
            for (int block = 0; block < 2; block++) {
                builder.beginBlock(header.length + block * 100L, 100, 1);
                builder.add(100L + block * 100L, 10, partition(7 + block, "p"), 1, 4);
                builder.endBlock();
            }
            byte[] data = builder.serialize("m", header.length + 200, 2);
            assertThat(ByteBuffer.wrap(data).getInt(64 + header.length))
                    .isEqualTo(partitionEnabled ? 2 : 0);
            for (int[] position : positions(data)) {
                assertThat(data[position[1]]).isEqualTo((byte) (partitionEnabled ? 1 : 0));
                assertThat(data[position[2]]).isEqualTo((byte) (rowIdEnabled ? 1 : 0));
                assertThat(data[position[3]]).isEqualTo((byte) (bucketEnabled ? 1 : 0));
            }
            ManifestFileMeta meta = meta("m", header.length + 200, 2);
            assertThat(ManifestSidecar.select(data, meta, query(999), settings).blocks())
                    .hasSize(rowIdEnabled ? 0 : 2);
            assertThat(ManifestSidecar.select(data, meta, null, part(99), type, settings).blocks())
                    .hasSize(partitionEnabled ? 0 : 2);
            BiPredicate<Integer, Integer> buckets = bucketFilter(99);
            assertThat(
                            ManifestSidecar.select(data, meta, null, null, type, buckets, settings)
                                    .blocks())
                    .hasSize(bucketEnabled ? 0 : 2);

            // Generation settings do not disable payloads already stored in a sidecar.
            byte[] existing = fixture("indexWithBuckets");
            ManifestFileMeta existingMeta = meta("manifest-golden", header.length + 400, 7);
            assertThat(
                            ManifestSidecar.select(existing, existingMeta, query(999), settings)
                                    .blocks())
                    .isEmpty();
            assertThat(
                            ManifestSidecar.select(
                                            existing, existingMeta, null, part(99), type, settings)
                                    .blocks())
                    .isEmpty();
            assertThat(
                            ManifestSidecar.select(
                                            existing,
                                            existingMeta,
                                            null,
                                            null,
                                            type,
                                            buckets,
                                            settings)
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
        byte[] data = builder.serialize("manifest-golden", header.length + 400, 7);
        assertThat(data).isEqualTo(fixture("indexWithPartitions"));
        ManifestFileMeta meta = meta("manifest-golden", header.length + 400, 7);
        PartitionPredicate filter = spy(part(7));
        assertThat(ManifestSidecar.select(data, meta, query(20), filter, type, defaults).blocks())
                .extracting(block -> block.firstRecord)
                .containsExactly(0L, 5L);
        verify(filter, times(2)).test(any(BinaryRow.class));
        PartitionPredicate nullFilter =
                PartitionPredicate.fromPredicate(type, new PredicateBuilder(type).isNull(1));
        assertThat(ManifestSidecar.select(data, meta, null, nullFilter, type, defaults).blocks())
                .hasSize(3);
        assertThat(ManifestSidecar.select(data, meta, null, part(99), type, defaults).blocks())
                .isEmpty();
        // Missing partition payloads cannot be pruned by dictionary misses.
        assertThat(
                        ManifestSidecar.select(
                                        fixture("index"), meta, null, part(99), type, defaults)
                                .blocks())
                .hasSize(3);
    }

    @Test
    void unavailableDimensionsAreIndependentAndDoNotPoisonLaterBlocks() throws Exception {
        Options options = sidecarOptions();
        options.set(MAX_BYTES, new MemorySize(512));
        ManifestSidecar.Settings settings = settings(options, 2);
        byte[] header = fixture("avroHeader");
        ManifestSidecar.Builder builder = new ManifestSidecar.Builder(settings, header);
        builder.beginBlock(header.length, 100, 1);
        builder.add(null, 10, partition(7, "left"));
        builder.endBlock();
        builder.beginBlock(header.length + 100, 100, 1);
        builder.add(200L, 10, partition(9, String.join("", Collections.nCopies(600, "x"))));
        builder.endBlock();
        builder.beginBlock(header.length + 200, 100, 1);
        builder.add(300L, 10, partition(7, "left")); // an existing dictionary ID remains usable
        builder.endBlock();
        byte[] data = builder.serialize("m", header.length + 300, 3);
        ManifestFileMeta meta = meta("m", header.length + 300, 3);
        assertThat(ManifestSidecar.select(data, meta, null, part(9), type, settings).blocks())
                .extracting(block -> block.firstRecord)
                .containsExactly(1L);
        assertThat(ManifestSidecar.select(data, meta, query(999), part(7), type, settings).blocks())
                .extracting(block -> block.firstRecord)
                .containsExactly(0L);
        assertThat(ManifestSidecar.select(data, meta, query(200), part(9), type, settings).blocks())
                .extracting(block -> block.firstRecord)
                .containsExactly(1L);
    }

    @Test
    void coarseningContinuesThroughTheEntireBlockAndDetectsUnknownRows() throws Exception {
        Options options = sidecarOptions();
        options.set(MAX_BYTES, new MemorySize(512));
        ManifestSidecar.Settings settings = settings(options, 2);
        byte[] header = fixture("avroHeader");
        for (boolean unknown : new boolean[] {false, true}) {
            ManifestSidecar.Builder builder = new ManifestSidecar.Builder(settings, header);
            builder.beginBlock(header.length, 100, 66);
            for (int i = 0; i < 64; i++) {
                builder.add(100L + i * 1000L, 10, partition(7, "left"));
            }
            builder.add(10L, 10, partition(7, "left"));
            builder.add(unknown ? null : Long.MAX_VALUE, 1, partition(7, "left"));
            builder.endBlock();
            byte[] data = builder.serialize("m", header.length + 100, 66);
            ManifestFileMeta meta = meta("m", header.length + 100, 66);
            for (long point : new long[] {10, 100, 200, Long.MAX_VALUE}) {
                assertThat(ManifestSidecar.select(data, meta, query(point), settings).blocks())
                        .hasSize(1);
            }
            assertThat(ManifestSidecar.select(data, meta, query(0), settings).blocks())
                    .hasSize(unknown ? 1 : 0);
            assertThat(ManifestSidecar.select(data, meta, null, part(9), type, settings).blocks())
                    .isEmpty();
        }
    }

    private List<int[]> positions(byte[] data) {
        ByteBuffer in = ByteBuffer.wrap(data);
        in.position(60);
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
    void rowMissSkipsPartitionAndBucketPayloads() throws Exception {
        byte[] data = fixture("indexWithBuckets");
        int[] first = positions(data).get(0);
        ByteBuffer.wrap(data).putInt(first[1] + 9, -1);
        ByteBuffer.wrap(data).putInt(first[3] + 9, -1);
        checksum(data);
        ManifestFileMeta meta = meta("manifest-golden", fixture("avroHeader").length + 400, 7);
        BiPredicate<Integer, Integer> buckets = mock(BiPredicate.class);
        assertThat(
                        ManifestSidecar.select(
                                        data, meta, query(15), part(7), type, buckets, defaults)
                                .blocks())
                .isEmpty();
        verifyNoInteractions(buckets);
    }

    @Test
    void partitionMissSkipsBucketMatchingWithOrWithoutRowFilter() throws Exception {
        byte[] data = fixture("indexWithBuckets");
        ByteBuffer.wrap(data).putInt(positions(data).get(0)[3] + 9, -1);
        checksum(data);
        ManifestFileMeta meta = meta("manifest-golden", fixture("avroHeader").length + 400, 7);
        for (RowRangeIndex rows : Arrays.asList(null, query(0))) {
            BiPredicate<Integer, Integer> buckets = mock(BiPredicate.class);
            assertThat(
                            ManifestSidecar.select(
                                            data, meta, rows, part(99), type, buckets, defaults)
                                    .blocks())
                    .isEmpty();
            verifyNoInteractions(buckets);
        }
    }

    @Test
    void absentPartitionFilterKeepsRowAndBucketMatching() throws Exception {
        byte[] data = fixture("indexWithBuckets");
        ByteBuffer.wrap(data).putInt(positions(data).get(0)[1] + 9, 999);
        checksum(data);
        ManifestFileMeta meta = meta("manifest-golden", fixture("avroHeader").length + 400, 7);
        BiPredicate<Integer, Integer> buckets = spy(bucketFilter(1));
        assertThat(
                        ManifestSidecar.select(data, meta, query(20), null, type, buckets, defaults)
                                .blocks())
                .extracting(block -> block.firstRecord)
                .containsExactly(0L);
        verify(buckets).test(1, 4);
        verify(buckets).test(0, 1);
        verify(buckets).test(3, 4);
        verifyNoMoreInteractions(buckets);
    }

    @Test
    void absentRowOrBucketFiltersKeepRemainingDimensions() throws Exception {
        byte[] data = fixture("indexWithBuckets");
        ManifestFileMeta meta = meta("manifest-golden", fixture("avroHeader").length + 400, 7);
        assertThat(
                        ManifestSidecar.select(
                                        data, meta, null, part(7), type, bucketFilter(1), defaults)
                                .blocks())
                .extracting(block -> block.firstRecord)
                .containsExactly(0L);
        assertThat(
                        ManifestSidecar.select(data, meta, query(20), part(7), type, null, defaults)
                                .blocks())
                .extracting(block -> block.firstRecord)
                .containsExactly(0L, 5L);
        assertThat(ManifestSidecar.select(data, meta, null, null, type, null, defaults).blocks())
                .extracting(block -> block.firstRecord)
                .containsExactly(0L, 3L, 5L);
    }

    @Test
    void partitionAndBucketMatchesSkipUnusedPayloadElements() throws Exception {
        byte[] partitions = fixture("indexWithBuckets");
        int[] first = positions(partitions).get(0);
        ByteBuffer.wrap(partitions).putInt(first[1] + 13, -1);
        checksum(partitions);
        ManifestFileMeta meta = meta("manifest-golden", fixture("avroHeader").length + 400, 7);
        assertThat(
                        ManifestSidecar.select(partitions, meta, query(0), part(7), type, defaults)
                                .blocks())
                .extracting(block -> block.firstRecord)
                .containsExactly(0L);
        assertThatThrownBy(
                        () ->
                                ManifestSidecar.select(
                                        partitions, meta, query(0), part(99), type, defaults))
                .isInstanceOf(IOException.class);

        byte[] buckets = fixture("indexWithBuckets");
        ByteBuffer.wrap(buckets).putInt(first[3] + 17, -1);
        checksum(buckets);
        assertThat(
                        ManifestSidecar.select(
                                        buckets,
                                        meta,
                                        query(0),
                                        null,
                                        type,
                                        bucketFilter(1),
                                        defaults)
                                .blocks())
                .extracting(block -> block.firstRecord)
                .containsExactly(0L);
        assertThatThrownBy(
                        () ->
                                ManifestSidecar.select(
                                        buckets,
                                        meta,
                                        query(0),
                                        null,
                                        type,
                                        bucketFilter(99),
                                        defaults))
                .isInstanceOf(IOException.class);
    }

    @Test
    void skippedPayloadsStillRequireValidFramingAndDirectory() throws Exception {
        byte[] good = fixture("indexWithBuckets");
        int[] first = positions(good).get(0);
        ManifestFileMeta meta = meta("manifest-golden", fixture("avroHeader").length + 400, 7);
        List<byte[]> invalid = new ArrayList<>();
        byte[] bad = good.clone();
        ByteBuffer.wrap(bad).putInt(first[1] + 5, 0);
        invalid.add(bad);
        bad = good.clone();
        ByteBuffer.wrap(bad).putInt(first[3] + 1, -1);
        invalid.add(bad);
        bad = good.clone();
        ByteBuffer.wrap(bad).putInt(first[2] + 5, 0);
        invalid.add(bad);
        bad = good.clone();
        ByteBuffer.wrap(bad).putLong(positions(good).get(1)[0], 0);
        invalid.add(bad);
        bad = good.clone();
        ByteBuffer.wrap(bad).putLong(first[0] + 16, 2);
        invalid.add(bad);
        for (byte[] data : invalid) {
            checksum(data);
            assertThatThrownBy(
                            () ->
                                    ManifestSidecar.select(
                                            data, meta, query(15), part(7), type, defaults))
                    .isInstanceOf(IOException.class);
        }
    }

    @Test
    void unknownUnsignedEncodingsSkipOnlyTheirDimensionAndMalformedPayloadsFail() throws Exception {
        byte[] good = fixture("indexWithBuckets");
        int[] first = positions(good).get(0);
        ManifestFileMeta meta = meta("manifest-golden", fixture("avroHeader").length + 400, 7);
        byte[] data = good.clone();
        data[first[1]] = (byte) 200;
        assertThat(
                        ManifestSidecar.select(
                                        checksum(data), meta, query(0), part(99), type, defaults)
                                .blocks())
                .extracting(block -> block.firstRecord)
                .containsExactly(0L);
        data = good.clone();
        data[first[2]] = (byte) 201;
        assertThat(
                        ManifestSidecar.select(
                                        checksum(data), meta, query(16), part(7), type, defaults)
                                .blocks())
                .extracting(block -> block.firstRecord)
                .containsExactly(0L);
        data = good.clone();
        data[first[3]] = (byte) 202;
        // Unknown encodings skip their payload without decoding even an invalid pair count.
        ByteBuffer.wrap(data).putInt(first[3] + 5, 0);
        checksum(data);
        BiPredicate<Integer, Integer> noBucket = bucketFilter(99);
        assertThat(
                        ManifestSidecar.select(
                                        data, meta, query(20), part(7), type, noBucket, defaults)
                                .blocks())
                .extracting(block -> block.firstRecord)
                .containsExactly(0L);
        assertThat(
                        ManifestSidecar.select(
                                        data, meta, query(999), part(7), type, noBucket, defaults)
                                .blocks())
                .isEmpty();
        assertThat(
                        ManifestSidecar.select(
                                        data, meta, query(20), part(99), type, noBucket, defaults)
                                .blocks())
                .isEmpty();
        for (int position : new int[] {first[1], first[2], first[3]}) {
            byte[] bad = good.clone();
            bad[position] = 0; // encoding 0 cannot have a length or payload bytes
            checksum(bad);
            assertThatThrownBy(() -> ManifestSidecar.select(bad, meta, query(0), defaults))
                    .isInstanceOf(IOException.class);
            byte[] invalidLength = good.clone();
            invalidLength[position] = (byte) 255;
            ByteBuffer.wrap(invalidLength).putInt(position + 1, -1);
            checksum(invalidLength);
            assertThatThrownBy(
                            () -> ManifestSidecar.select(invalidLength, meta, query(0), defaults))
                    .isInstanceOf(IOException.class);
        }
        // A checksummed directory with missing bytes/entries must still be rejected.
        byte[] bad = good.clone();
        ByteBuffer.wrap(bad).putLong(first[0] + 16, 2);
        checksum(bad);
        assertThatThrownBy(() -> ManifestSidecar.select(bad, meta, query(0), defaults))
                .isInstanceOf(IOException.class);
        byte[] badRange = good.clone();
        // Row payload begins after its encoding and length, then the range-count integer.
        ByteBuffer.wrap(badRange).putLong(first[2] + 9 + 16, 9L);
        checksum(badRange);
        assertThatThrownBy(
                        () ->
                                ManifestSidecar.select(
                                        badRange, meta, query(15), part(7), type, defaults))
                .isInstanceOf(IOException.class);
        byte[] badId = good.clone();
        ByteBuffer.wrap(badId).putInt(first[1] + 9, 999);
        checksum(badId);
        assertThatThrownBy(
                        () ->
                                ManifestSidecar.select(
                                        badId, meta, query(0), part(7), type, defaults))
                .isInstanceOf(IOException.class);
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
        byte[] data = builder.serialize("manifest-golden", header.length + 400, 7);
        assertThat(data).isEqualTo(fixture("indexWithBuckets"));
        ManifestFileMeta meta = meta("manifest-golden", header.length + 400, 7);
        BiPredicate<Integer, Integer> bucket = bucketFilter(1);
        assertThat(ManifestSidecar.select(data, meta, null, null, type, bucket, defaults).blocks())
                .extracting(block -> block.firstRecord)
                .containsExactly(0L);
        // Existing row-id and partition coverage remains independently usable.
        assertThat(
                        ManifestSidecar.select(
                                        data,
                                        meta,
                                        query(0),
                                        part(7),
                                        type,
                                        bucketFilter(2),
                                        defaults)
                                .blocks())
                .isEmpty();
        BiPredicate<Integer, Integer> filter = (bucketId, total) -> bucketId == 2 && total == 8;
        assertThat(ManifestSidecar.select(data, meta, null, null, type, filter, defaults).blocks())
                .extracting(block -> block.firstRecord)
                .containsExactly(3L);
        // The caller can omit a bucket filter which requires an actual entry partition.
        assertThat(ManifestSidecar.select(data, meta, null, null, type, null, defaults).blocks())
                .hasSize(3);
        for (String unavailable : new String[] {"index", "indexWithPartitions"}) {
            assertThat(
                            ManifestSidecar.select(
                                            fixture(unavailable),
                                            meta,
                                            null,
                                            null,
                                            type,
                                            bucketFilter(99),
                                            defaults)
                                    .blocks())
                    .hasSize(3);
        }
    }

    @Test
    void unknownInvalidOrOverBudgetBucketPayloadIsUnavailable() throws Exception {
        Options options = sidecarOptions();
        options.set(MAX_BYTES, new MemorySize(512));
        ManifestSidecar.Settings settings = settings(options, 2);
        byte[] header = fixture("avroHeader");
        for (Integer[] pair :
                Arrays.asList(
                        new Integer[] {null, null},
                        new Integer[] {-1, 4},
                        new Integer[] {4, 4},
                        new Integer[] {0, 0},
                        new Integer[] {2, 8})) {
            ManifestSidecar.Builder builder = new ManifestSidecar.Builder(settings, header);
            int extraPairs = pair[0] != null && pair[0] == 2 ? 65 : 0;
            builder.beginBlock(header.length, 100, 2 + extraPairs);
            builder.add(100L, 10, partition(7, "left"), 1, 4);
            builder.add(200L, 10, partition(7, "left"), pair[0], pair[1]);
            for (int i = 0; i < extraPairs; i++) {
                builder.add(200L, 10, partition(7, "left"), i, 100);
            }
            builder.endBlock();
            builder.beginBlock(header.length + 100, 100, 1);
            builder.add(300L, 10, partition(7, "left"), 1, 4);
            builder.endBlock();
            byte[] data = builder.serialize("m", header.length + 200, 3 + extraPairs);
            int bucket = positions(data).get(0)[3];
            assertThat(data[bucket]).isZero();
            assertThat(positions(data).get(1)[0]).isEqualTo(bucket + 1);
            ManifestFileMeta meta = meta("m", header.length + 200, 3 + extraPairs);
            assertThat(
                            ManifestSidecar.select(
                                            data,
                                            meta,
                                            null,
                                            null,
                                            type,
                                            bucketFilter(99),
                                            settings)
                                    .blocks())
                    .extracting(block -> block.firstRecord)
                    .containsExactly(0L);
            assertThat(
                            ManifestSidecar.select(
                                            data,
                                            meta,
                                            query(999),
                                            null,
                                            type,
                                            bucketFilter(99),
                                            settings)
                                    .blocks())
                    .isEmpty();
        }
    }

    @Test
    void malformedBucketPayloadInvalidatesTheContainer() throws Exception {
        byte[] good = fixture("indexWithBuckets");
        int payload = positions(good).get(0)[3] + 1;
        ManifestFileMeta meta = meta("manifest-golden", fixture("avroHeader").length + 400, 7);
        for (int[] mutation :
                new int[][] {
                    {payload, -2},
                    {payload, Integer.MAX_VALUE},
                    {payload, 0},
                    {payload + 4, 0},
                    {payload + 8, -1},
                    {payload + 12, 1},
                    {payload + 16, 0}
                }) {
            byte[] bad = good.clone();
            ByteBuffer.wrap(bad).putInt(mutation[0], mutation[1]);
            checksum(bad);
            assertThatThrownBy(
                            () ->
                                    ManifestSidecar.select(
                                            bad,
                                            meta,
                                            null,
                                            null,
                                            type,
                                            bucketFilter(99),
                                            defaults))
                    .isInstanceOf(IOException.class);
        }
    }

    @Test
    void payloadsCanExceedFormerLimitsWithinByteBudget() throws Exception {
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
        byte[] data = builder.serialize("m", fileSize, entries);
        assertThat(data.length).isLessThanOrEqualTo(defaults.maxBytes);
        ManifestFileMeta meta = meta("m", fileSize, entries);
        long last = (entries - 1L) * 2;
        assertThat(ManifestSidecar.select(data, meta, query(last), defaults).blocks())
                .extracting(block -> block.firstRecord)
                .containsExactly((blocks - 1L) * entriesPerBlock);
        assertThat(ManifestSidecar.select(data, meta, query(last - 1), defaults).blocks())
                .isEmpty();
        assertThat(ManifestSidecar.select(data, meta, null, part(entries), type, defaults).blocks())
                .isEmpty();
        assertThat(
                        ManifestSidecar.select(
                                        data,
                                        meta,
                                        null,
                                        null,
                                        type,
                                        bucketFilter(entriesPerBlock),
                                        defaults)
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
        byte[] data = builder.serialize("m", header.length + 800, 8);
        List<int[]> positions = positions(data);
        int[] presentSizes = {13, 25, 17};
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
        assertThat(ManifestSidecar.select(data, meta, null, part(99), type, defaults).blocks())
                .extracting(block -> block.firstRecord)
                .containsExactly(0L, 2L, 4L, 6L);
        assertThat(ManifestSidecar.select(data, meta, query(999), defaults).blocks())
                .extracting(block -> block.firstRecord)
                .containsExactly(0L, 1L, 4L, 5L);
        BiPredicate<Integer, Integer> buckets = bucketFilter(99);
        assertThat(ManifestSidecar.select(data, meta, null, null, type, buckets, defaults).blocks())
                .extracting(block -> block.firstRecord)
                .containsExactly(0L, 1L, 2L, 3L);
        assertThat(
                        ManifestSidecar.select(
                                        data, meta, query(999), part(99), type, buckets, defaults)
                                .blocks())
                .extracting(block -> block.firstRecord)
                .containsExactly(0L);
    }

    @Test
    void tightByteBudgetKeepsAllDescriptorsOrOmitsTheWholeFile() throws Exception {
        Options options = sidecarOptions();
        options.set(MAX_BYTES, new MemorySize(250));
        ManifestSidecar.Settings settings = settings(options, 2);
        byte[] header = fixture("avroHeader");
        ManifestSidecar.Builder builder = new ManifestSidecar.Builder(settings, header);
        for (int i = 0; i < 3; i++) {
            builder.beginBlock(header.length + 100L * i, 100, 1);
            builder.add(i * 100L, 10, partition(7, "left"));
            builder.endBlock();
        }
        byte[] data = builder.serialize("m", header.length + 300, 3);
        assertThat(data.length).isLessThanOrEqualTo(250);
        assertThat(
                        ManifestSidecar.select(
                                        data,
                                        meta("m", header.length + 300, 3),
                                        query(999),
                                        part(99),
                                        type,
                                        settings)
                                .blocks())
                .extracting(block -> block.firstRecord)
                .containsExactly(0L, 1L, 2L);
        builder.beginBlock(header.length + 300, 100, 1);
        builder.add(300L, 1, partition(7, "left"));
        builder.endBlock();
        assertThat(builder.serialize("m", header.length + 400, 4)).isNull();
    }
}
