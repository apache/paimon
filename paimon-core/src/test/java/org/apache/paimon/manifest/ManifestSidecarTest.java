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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.SingleSegments;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.avro.AvroFileFormat;
import org.apache.paimon.fs.ByteArraySeekableStream;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.manifest.ManifestSidecar.ManifestSidecarSegment;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.CompatibilityUtils;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.PathFactory;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RowRangeIndex;
import org.apache.paimon.utils.SegmentsCache;
import org.apache.paimon.utils.SerializationUtils;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.zip.CRC32;

import static org.apache.paimon.TestKeyValueGenerator.DEFAULT_PART_TYPE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/** Golden file format, physical block positions, completeness and allocation bounds. */
class ManifestSidecarTest {

    private static final String GENERATE_GOLDEN_FILES_PROPERTY =
            "generateManifestSidecarGoldenFiles";

    @TempDir java.nio.file.Path temp;

    static ManifestFileMeta meta(String name, long size, long entries) {
        ManifestFileMeta meta = mock(ManifestFileMeta.class);
        when(meta.fileName()).thenReturn(name);
        when(meta.fileSize()).thenReturn(size);
        when(meta.extraFiles())
                .thenReturn(Collections.singletonList(name + ManifestSidecar.SUFFIX));
        when(meta.numAddedFiles()).thenReturn(entries);
        return meta;
    }

    static byte[] readGoldenFile() throws IOException {
        String resource = "/compatibility/manifest-sidecar-v1";
        try (InputStream input = ManifestSidecarTest.class.getResourceAsStream(resource)) {
            assertThat(input).as("Golden file %s", resource).isNotNull();
            return IOUtils.readFully(input, false);
        }
    }

    static byte[] verifyGoldenFile(byte[] current) throws IOException {
        if (Boolean.parseBoolean(
                System.getProperties().getProperty(GENERATE_GOLDEN_FILES_PROPERTY))) {
            CompatibilityUtils.writeCompatibilityFile("manifest-sidecar-v1", current);
            return current;
        }
        byte[] golden = readGoldenFile();
        assertThat(current).isEqualTo(golden);
        return golden;
    }

    static byte[] header() throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(output, null);
        encoder.writeFixed(new byte[] {'O', 'b', 'j', 1});
        // Fix both metadata order and sync marker for deterministic golden files.
        encoder.writeMapStart();
        encoder.setItemCount(2);
        encoder.startItem();
        encoder.writeString("avro.codec");
        encoder.writeBytes("null".getBytes(StandardCharsets.UTF_8));
        encoder.startItem();
        encoder.writeString("avro.schema");
        encoder.writeBytes("\"long\"".getBytes(StandardCharsets.UTF_8));
        encoder.writeMapEnd();
        encoder.writeFixed(new byte[16]);
        encoder.flush();
        return output.toByteArray();
    }

    static byte[] partition(int p, String q) {
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

    static byte[] testSidecar() throws IOException {
        byte[] header = header();
        byte[] a = partition(7, "left");
        byte[] b = partition(9, null);
        ManifestSidecar.Builder builder = new ManifestSidecar.Builder(header, true, true);
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
        return builder.serialize(header.length + 400, 7);
    }

    private ManifestFileMeta testMeta() throws IOException {
        return meta("manifest-golden", header().length + 400, 7);
    }

    @Test
    void emptyManifestHasACompleteSidecar() throws Exception {
        byte[] header = header();
        byte[] data = new ManifestSidecar.Builder(header, true, true).serialize(header.length, 0);
        assertThat(ByteBuffer.wrap(data).getInt()).isEqualTo(0x504d5343);
        assertThat(ManifestSidecar.select(data, meta("empty", header.length, 0), null).blocks())
                .isEmpty();
    }

    @Test
    void crc32FooterAndTruncatedSidecars() throws Exception {
        byte[] good = testSidecar();
        ManifestFileMeta meta = testMeta();
        int limit = good.length - Integer.BYTES;
        // Independently generated CRC32, stored in big-endian byte order.
        assertThat(ByteBuffer.wrap(good, limit, Integer.BYTES).getInt()).isEqualTo(0xdf82cd30);
        assertThat(ManifestSidecar.select(good, meta, null).blocks()).hasSize(3);
        for (int position = limit; position < good.length; position++) {
            byte[] bad = good.clone();
            bad[position] ^= 1;
            assertThatThrownBy(() -> ManifestSidecar.select(bad, meta, null))
                    .isInstanceOf(IOException.class);
        }
        for (int length = 0; length < good.length; length++) {
            byte[] truncated = Arrays.copyOf(good, length);
            assertThatThrownBy(() -> ManifestSidecar.select(truncated, meta, null))
                    .isInstanceOf(IOException.class);
        }
    }

    @Test
    void buildAndReadSelectedBlocksFromPhysicalManifests() throws Exception {
        FileIO io = LocalFileIO.create();
        ManifestTestDataGenerator generator = ManifestTestDataGenerator.builder().build();
        List<ManifestEntry> entries = new ArrayList<>();
        for (int i = 0; i < 4000; i++) {
            ManifestEntry entry = generator.next();
            entries.add(
                    ManifestEntry.create(
                            i % 2 == 0 ? FileKind.ADD : FileKind.DELETE,
                            entry.partition(),
                            1,
                            4,
                            entry.file().newFirstRowId(i * 1000000L)));
        }
        Path sourcePath = new Path(temp.toString(), "manifest-source");
        ManifestFileMeta source;
        try (ManifestAvroWriter writer = writer(io, sourcePath)) {
            writer.write(entries);
            writer.close();
            source = writer.result().get(0);
        }
        Path rewrittenPath = new Path(temp.toString(), "manifest-rewritten");
        ManifestFileMeta rewritten;
        try (ManifestAvroWriter writer = writer(io, rewrittenPath);
                ManifestAvroReader reader = new ManifestAvroReader(io.newInputStream(sourcePath))) {
            writer.writeEncodedManifest(reader, source);
            writer.close();
            rewritten = writer.result().get(0);
        }
        for (ManifestFileMeta meta : Arrays.asList(source, rewritten)) {
            Path path = new Path(temp.toString(), meta.fileName());
            byte[] data =
                    ManifestSidecar.build(io, path, meta.fileSize(), entries.size(), true, true);
            ManifestSidecar.Selection selected =
                    ManifestSidecar.select(
                            data,
                            meta,
                            RowRangeIndex.create(
                                    Arrays.asList(
                                            new Range(1000000000L, 1000000000L),
                                            new Range(3000000000L, 3000000000L))),
                            null,
                            DEFAULT_PART_TYPE,
                            (bucket, totalBuckets) -> bucket == 1 && totalBuckets == 4);
            assertThat(selected.blocks()).hasSize(2);
            List<ManifestEntry> expected = new ArrayList<>();
            for (ManifestSidecar.Block block : selected.blocks()) {
                expected.addAll(
                        entries.subList(
                                (int) block.firstRecord,
                                (int) (block.firstRecord + block.recordCount)));
            }
            List<ManifestEntry> actual = new ArrayList<>();
            try (ManifestAvroReader reader =
                            new ManifestAvroReader(
                                    ManifestSidecar.openManifest(io, path, selected));
                    CloseableIterator<InternalRow> rows =
                            reader.read(ManifestEntry.MANIFEST_ROW_TYPE, null, null)) {
                ManifestEntrySerializer serializer = new ManifestEntrySerializer();
                while (rows.hasNext()) {
                    actual.add(serializer.fromRow(rows.next()));
                }
            }
            assertThat(actual).containsExactlyElementsOf(expected);
        }
    }

    private ManifestAvroWriter writer(FileIO io, Path path) {
        PathFactory paths = mock(PathFactory.class);
        when(paths.newPath()).thenReturn(path);
        return new ManifestAvroWriter(
                io,
                new FileSystemSchemaManager(io, new Path(temp.toUri())),
                DEFAULT_PART_TYPE,
                (AvroFileFormat) FileFormat.fromIdentifier("avro", new Options()),
                new ManifestEntrySerializer(),
                "zstd",
                paths,
                Long.MAX_VALUE,
                new CoreOptions(new Options()));
    }

    @Test
    void rowIdCoverageAndBlockOrdinals() throws Exception {
        byte[] header = header();
        byte[] data = testSidecar();
        ManifestFileMeta meta = testMeta();
        for (long point :
                new long[] {
                    0,
                    9,
                    20,
                    24,
                    (1L << 32) - 2,
                    1L << 32,
                    (1L << 32) + 2,
                    8254058425445L,
                    Long.MAX_VALUE
                }) {
            assertThat(select(data, meta, point).blocks()).as("row %s", point).isNotEmpty();
        }
        for (long point :
                new long[] {
                    10, 19, 25, (1L << 32) - 3, (1L << 32) + 3, 8254058425444L, Long.MAX_VALUE - 1
                }) {
            assertThat(select(data, meta, point).blocks()).as("row %s", point).isEmpty();
        }
        ManifestSidecar.Selection selected = select(data, meta, 20);
        assertThat(selected.blocks()).extracting(b -> b.firstRecord).containsExactly(0L, 5L);
        assertThat(selected.blocks())
                .extracting(b -> b.offset)
                .containsExactly((long) header.length, header.length + 300L);
        assertThat(selected.blocks()).extracting(b -> b.length).containsExactly(100L, 100L);

        ManifestSidecar.Selection gap = select(data, meta, 16);

        assertThat(gap.blocks()).isEmpty();
        RowRangeIndex query =
                RowRangeIndex.create(Arrays.asList(new Range(10, 19), new Range(25, 40)));
        assertThat(ManifestSidecar.select(data, meta, query).blocks()).isEmpty();
        assertThat(query.ranges()).containsExactly(new Range(10, 19), new Range(25, 40));
    }

    @Test
    void minMaxSkipsExactIntersectionChecksAndHandlesOneInterval() throws Exception {
        byte[] header = header();
        ManifestSidecar.Builder builder = new ManifestSidecar.Builder(header, true, true);
        builder.beginBlock(header.length, 100, 2);
        builder.add(0L, 10);
        builder.add(20L, 10);
        builder.endBlock();
        builder.beginBlock(header.length + 100, 100, 2);
        builder.add(100L, 10);
        builder.add(200L, 10);
        builder.endBlock();
        builder.beginBlock(header.length + 200, 100, 1);
        builder.add(1L << 32, 10);
        builder.endBlock();
        byte[] data = builder.serialize(header.length + 300, 5);
        ManifestFileMeta meta = meta("m", header.length + 300, 5);
        RowRangeIndex outside =
                spy(RowRangeIndex.create(Collections.singletonList(new Range(50, 59))));
        ManifestSidecar.Selection none = ManifestSidecar.select(data, meta, outside);
        assertThat(none.blocks()).isEmpty();

        // Only the three envelopes are tested; no individual interval intersection is evaluated.
        verify(outside, times(3)).intersects(anyLong(), anyLong());
        verify(outside).intersects(0, 29);
        verify(outside).intersects(100, 209);
        verify(outside).intersects(1L << 32, (1L << 32) + 9);

        RowRangeIndex one =
                spy(
                        RowRangeIndex.create(
                                Collections.singletonList(
                                        new Range((1L << 32) + 9, (1L << 32) + 9))));
        ManifestSidecar.Selection hit = ManifestSidecar.select(data, meta, one);
        assertThat(hit.blocks()).extracting(b -> b.firstRecord).containsExactly(4L);

        // A one-interval block needs no second intersection check after its envelope matches.
        verify(one, times(3)).intersects(anyLong(), anyLong());
    }

    @Test
    void singleIntervalHandlesBoundariesAndAbsentQueries() throws Exception {
        byte[] header = header();
        for (Range range :
                Arrays.asList(
                        new Range(0, 0),
                        new Range(42, 51),
                        new Range(Long.MAX_VALUE, Long.MAX_VALUE))) {
            ManifestSidecar.Builder builder = new ManifestSidecar.Builder(header, true, true);
            builder.beginBlock(header.length, 100, 1);
            builder.add(range.from, range.to - range.from + 1);
            builder.endBlock();
            byte[] data = builder.serialize(header.length + 100, 1);
            ManifestFileMeta meta = meta("m", header.length + 100, 1);
            assertThat(ManifestSidecar.select(data, meta, null).blocks()).hasSize(1);
            assertThat(
                            ManifestSidecar.select(
                                            data,
                                            meta,
                                            RowRangeIndex.create(Collections.emptyList()))
                                    .blocks())
                    .isEmpty();
            for (long point : new long[] {range.from, range.to}) {
                RowRangeIndex query =
                        spy(
                                RowRangeIndex.create(
                                        Collections.singletonList(new Range(point, point))));
                assertThat(ManifestSidecar.select(data, meta, query).blocks()).hasSize(1);
                verify(query).intersects(range.from, range.to);
            }
            long missing = range.from > 0 ? range.from - 1 : range.to + 1;
            assertThat(select(data, meta, missing).blocks()).isEmpty();
        }
    }

    @Test
    void hugeRangesAreNotExpandedAndInvalidCoverageRetainsBlocks() throws Exception {
        byte[] header = header();
        ManifestSidecar.Builder builder = new ManifestSidecar.Builder(header, true, true);
        builder.beginBlock(header.length, 100, 2);
        builder.add(0L, Long.MAX_VALUE);
        builder.add(Long.MAX_VALUE, 1);
        builder.endBlock();
        byte[] data = builder.serialize(header.length + 100, 2);
        assertThat(data.length).isLessThan(512);
        assertThat(select(data, meta("m", header.length + 100, 2), Long.MAX_VALUE).blocks())
                .hasSize(1);
        for (Long first : Arrays.asList(null, -1L, Long.MAX_VALUE)) {
            builder = new ManifestSidecar.Builder(header, true, true);
            builder.beginBlock(header.length, 100, 1);
            builder.add(first, 2);
            builder.endBlock();
            assertThat(
                            select(
                                            builder.serialize(header.length + 100, 1),
                                            meta("m", header.length + 100, 1),
                                            100)
                                    .blocks())
                    .hasSize(1);
        }
        for (long count : new long[] {0, -1}) {
            builder = new ManifestSidecar.Builder(header, true, true);
            builder.beginBlock(header.length, 100, 1);
            builder.add(0L, count);
            builder.endBlock();
            assertThat(
                            select(
                                            builder.serialize(header.length + 100, 1),
                                            meta("m", header.length + 100, 1),
                                            100)
                                    .blocks())
                    .hasSize(1);
        }
        builder = new ManifestSidecar.Builder(header, true, true);
        builder.beginBlock(header.length, 100, 64);
        for (int i = 0; i < 64; i++) {
            builder.add(i * 10L, 1);
        }
        builder.endBlock();
        assertThat(
                        select(
                                        builder.serialize(header.length + 100, 64),
                                        meta("m", header.length + 100, 64),
                                        5)
                                .blocks())
                .isEmpty();
    }

    @Test
    void cacheRespectsElementThreshold() throws Exception {
        byte[] data = testSidecar();
        Path path = new Path(temp.toString(), "manifest-golden");
        Path sidecar = ManifestSidecar.path(path);
        Files.write(temp.resolve(sidecar.getName()), data);
        ManifestFileMeta meta = testMeta();
        FileIO io = spy(LocalFileIO.create());
        SegmentsCache<Object> tooSmall =
                new SegmentsCache<>(1024, MemorySize.ofMebiBytes(1), data.length - 1L, null, false);
        assertThat(readCached(io, path, meta, tooSmall).blocks()).hasSize(2);
        assertThat(readCached(io, path, meta, tooSmall).blocks()).hasSize(2);
        assertThat(tooSmall.getIfPresents(sidecar)).isNull();
        verify(io, times(2)).newInputStream(sidecar);

        SegmentsCache<Object> cache =
                new SegmentsCache<>(1024, MemorySize.ofMebiBytes(1), data.length, null, false);
        assertThat(readCached(io, path, meta, cache).blocks()).hasSize(2);
        assertThat(readCached(io, path, meta, cache).blocks()).hasSize(2);
        verify(io, times(3)).newInputStream(sidecar);
    }

    @Test
    void cachedSegmentsRequireSidecarType() throws Exception {
        byte[] data = testSidecar();
        Path path = new Path(temp.toString(), "manifest-golden");
        Path sidecar = ManifestSidecar.path(path);
        Files.write(temp.resolve(sidecar.getName()), data);
        SegmentsCache<Object> cache =
                new SegmentsCache<>(1024, MemorySize.ofMebiBytes(1), Long.MAX_VALUE, null, false);
        cache.put(sidecar, new SingleSegments(MemorySegment.wrap(data), data.length));
        FileIO io = spy(LocalFileIO.create());

        assertThat(readCached(io, path, testMeta(), cache).blocks()).hasSize(2);
        assertThat(cache.getIfPresents(sidecar)).isInstanceOf(ManifestSidecarSegment.class);
        ManifestSidecarSegment cached = (ManifestSidecarSegment) cache.getIfPresents(sidecar);
        assertThat(cached.bytes()).containsExactly(data);
        assertThat(cached.totalMemorySize()).isEqualTo(data.length);
        assertThat(readCached(io, path, testMeta(), cache).blocks()).hasSize(2);
        verify(io, times(1)).newInputStream(sidecar);
    }

    @Test
    void missingAndInvalidSidecarsAreNotCached() throws Exception {
        byte[] data = testSidecar();
        Path path = new Path(temp.toString(), "manifest-golden");
        Path sidecar = ManifestSidecar.path(path);
        ManifestFileMeta meta = testMeta();
        FileIO io = spy(LocalFileIO.create());
        SegmentsCache<Object> cache =
                new SegmentsCache<>(1024, MemorySize.ofMebiBytes(1), Long.MAX_VALUE, null, false);
        assertThat(readCached(io, path, meta, cache)).isNull();
        assertThat(cache.getIfPresents(sidecar)).isNull();
        byte[] corrupt = data.clone();
        corrupt[0] ^= 1;
        Files.write(temp.resolve(sidecar.getName()), corrupt);
        assertThat(readCached(io, path, meta, cache)).isNull();
        assertThat(cache.getIfPresents(sidecar)).isNull();
        Files.write(temp.resolve(sidecar.getName()), data);
        assertThat(readCached(io, path, meta, cache).blocks()).hasSize(2);
        assertThat(readCached(io, path, meta, cache).blocks()).hasSize(2);
        verify(io, times(3)).newInputStream(sidecar);
    }

    @Test
    void cachedBytesPreservePerQueryCancellation() throws Exception {
        Path path = new Path(temp.toString(), "manifest-golden");
        Path sidecar = ManifestSidecar.path(path);
        Files.write(temp.resolve(sidecar.getName()), testSidecar());
        ManifestFileMeta meta = testMeta();
        FileIO io = spy(LocalFileIO.create());
        SegmentsCache<Object> cache =
                new SegmentsCache<>(1024, MemorySize.ofMebiBytes(1), Long.MAX_VALUE, null, false);
        RowRangeIndex cancelled = mock(RowRangeIndex.class);
        when(cancelled.intersects(anyLong(), anyLong()))
                .thenThrow(new CancellationException("cancelled"));
        assertThatThrownBy(
                        () ->
                                ManifestSidecar.read(
                                        io, path, meta, cancelled, null, null, null, cache))
                .isInstanceOf(CancellationException.class);
        assertThat(cache.getIfPresents(sidecar)).isNull();

        assertThat(readCached(io, path, meta, cache).blocks()).hasSize(2);
        assertThatThrownBy(
                        () ->
                                ManifestSidecar.read(
                                        io, path, meta, cancelled, null, null, null, cache))
                .isInstanceOf(CancellationException.class);
        assertThat(readCached(io, path, meta, cache).blocks()).hasSize(2);
        verify(io, times(2)).newInputStream(sidecar);
    }

    private ManifestSidecar.Selection readCached(
            FileIO io, Path path, ManifestFileMeta meta, SegmentsCache<Object> cache) {
        return ManifestSidecar.read(
                io,
                path,
                meta,
                RowRangeIndex.create(Collections.singletonList(new Range(20, 20))),
                null,
                null,
                null,
                cache);
    }

    @Test
    void corruptMissingIncompleteAndMismatchedIndexesFallback() throws Exception {
        Path manifest = new Path(temp.toString(), "manifest-golden");
        java.nio.file.Path index = temp.resolve("manifest-golden" + ManifestSidecar.SUFFIX);
        ManifestFileMeta meta = testMeta();
        RowRangeIndex query = RowRangeIndex.create(Collections.singletonList(new Range(11, 11)));

        assertThat(ManifestSidecar.read(LocalFileIO.create(), manifest, meta, query)).isNull();
        byte[] good = testSidecar();
        for (int position : new int[] {0, 9, 11, 15, 16, 55, 63, 67, 75, good.length - 1}) {
            byte[] bad = good.clone();
            bad[position] ^= 2;
            Files.write(index, bad);
            assertThat(ManifestSidecar.read(LocalFileIO.create(), manifest, meta, query)).isNull();
        }
        // A valid checksum cannot make an unsupported container version readable.
        for (int version : new int[] {0, 2, 99}) {
            byte[] bad = good.clone();
            bad[4] = (byte) version;
            int limit = bad.length - Integer.BYTES;
            CRC32 crc = new CRC32();
            crc.update(bad, 0, limit);
            ByteBuffer.wrap(bad, limit, Integer.BYTES).putInt((int) crc.getValue());
            assertThatThrownBy(() -> ManifestSidecar.select(bad, meta, query))
                    .isInstanceOf(IOException.class);
        }
        Files.write(index, Arrays.copyOf(good, good.length - 1));
        assertThat(ManifestSidecar.read(LocalFileIO.create(), manifest, meta, query)).isNull();
        Files.write(index, good);
        assertThat(ManifestSidecar.read(LocalFileIO.create(), manifest, meta, query).blocks())
                .isEmpty();
        // The sidecar is bound to physical coverage, not to a particular file name.
        assertThat(
                        ManifestSidecar.select(
                                        good,
                                        meta("renamed", meta.fileSize(), 7),
                                        RowRangeIndex.create(
                                                Collections.singletonList(new Range(20, 20))))
                                .blocks())
                .extracting(block -> block.firstRecord)
                .containsExactly(0L, 5L);
        assertThatThrownBy(
                        () ->
                                ManifestSidecar.select(
                                        good, meta("renamed", meta.fileSize() + 1, 7), query))
                .isInstanceOf(IOException.class);
        assertThatThrownBy(
                        () ->
                                ManifestSidecar.select(
                                        good, meta("renamed", meta.fileSize(), 8), query))
                .isInstanceOf(IOException.class);
    }

    @Test
    void ioFailuresFallBackWithoutInspectingNestedExceptions() throws Exception {
        Path path = new Path(temp.toString(), "m");
        IOException suppressed = new IOException("read failed");
        suppressed.addSuppressed(new CancellationException("cancelled during close"));
        for (IOException failure :
                Arrays.asList(
                        new IOException("read failed"),
                        new java.net.SocketTimeoutException("timeout"),
                        new java.io.InterruptedIOException("no thread interruption flag"),
                        new IOException("wrapped", new InterruptedException("interrupted")),
                        suppressed)) {
            FileIO fileIO = mock(FileIO.class);
            when(fileIO.newInputStream(ManifestSidecar.path(path))).thenThrow(failure);
            assertThat(ManifestSidecar.read(fileIO, path, meta("m", 1, 1), null)).isNull();
            assertThat(Thread.currentThread().isInterrupted()).isFalse();
        }
    }

    @Test
    void interruptedThreadDoesNotFallBackOnIoFailure() throws Exception {
        Path path = new Path(temp.toString(), "m");
        IOException failure = new IOException("read failed");
        FileIO fileIO = mock(FileIO.class);
        when(fileIO.newInputStream(ManifestSidecar.path(path))).thenThrow(failure);
        try {
            Thread.currentThread().interrupt();
            assertThatThrownBy(() -> ManifestSidecar.read(fileIO, path, meta("m", 1, 1), null))
                    .isInstanceOf(java.io.UncheckedIOException.class)
                    .hasCauseReference(failure);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void uncheckedFailuresPropagateUnchanged() throws Exception {
        Path path = new Path(temp.toString(), "m");
        for (Throwable failure :
                Arrays.asList(
                        new IllegalStateException("unexpected failure"),
                        new java.io.UncheckedIOException(new IOException("wrapped I/O")),
                        new CancellationException("cancelled"),
                        new AssertionError("error"))) {
            FileIO fileIO = mock(FileIO.class);
            when(fileIO.newInputStream(ManifestSidecar.path(path))).thenThrow(failure);
            assertThatThrownBy(() -> ManifestSidecar.read(fileIO, path, meta("m", 1, 1), null))
                    .isSameAs(failure);
        }
    }

    @Test
    void indexReadsUseBoundedBulkRequests() throws Exception {
        byte[] header = header();
        for (int blockCount : new int[] {5000, 25000, 131073}) {
            ManifestSidecar.Builder builder = new ManifestSidecar.Builder(header, true, true);
            for (int blockNumber = 0; blockNumber < blockCount; blockNumber++) {
                builder.beginBlock(header.length + blockNumber * 100L, 100, 1);
                builder.add((long) blockNumber, 1);
                builder.endBlock();
            }
            long size = header.length + blockCount * 100L;
            byte[] data = builder.serialize(size, blockCount);
            ManifestFileMeta meta = meta("manifest-large", size, blockCount);
            CountingInput stream = new CountingInput(data, Integer.MAX_VALUE);
            Path path = new Path(temp.toString(), meta.fileName());
            FileIO io = mock(FileIO.class);
            when(io.newInputStream(ManifestSidecar.path(path))).thenReturn(stream);
            ManifestSidecar.Selection actual =
                    ManifestSidecar.read(
                            io,
                            path,
                            meta,
                            RowRangeIndex.create(Collections.singletonList(new Range(0, 0))));
            assertThat(actual.blocks()).hasSize(1);
            assertThat(actual.blocks().get(0).offset).isEqualTo(header.length);
            assertThat(stream.readLengths).hasSize((data.length + (1 << 20) - 1) / (1 << 20));
            assertThat(stream.requests).allMatch(request -> request <= 1 << 20);
            assertThat(stream.closed).isTrue();
        }
    }

    @Test
    void sidecarsLargerThanTheFormerDefaultLimitAreReadCompletely() throws Exception {
        byte[] header = header();
        BinaryRow partition = new BinaryRow(1);
        BinaryRowWriter rowWriter = new BinaryRowWriter(partition);
        byte[] value = new byte[17 * 1024 * 1024];
        rowWriter.writeBinary(0, value, 0, value.length);
        rowWriter.complete();
        ManifestSidecar.Builder builder = new ManifestSidecar.Builder(header, true, true);
        builder.beginBlock(header.length, 100, 1);
        builder.add(0L, 1, SerializationUtils.serializeBinaryRow(partition));
        builder.endBlock();
        byte[] data = builder.serialize(header.length + 100, 1);
        assertThat(data.length).isGreaterThan(16 * 1024 * 1024);
        Path path = new Path(temp.toString(), "manifest-large");
        CountingInput stream = new CountingInput(data, Integer.MAX_VALUE);
        FileIO io = mock(FileIO.class);
        when(io.newInputStream(ManifestSidecar.path(path))).thenReturn(stream);
        assertThat(
                        ManifestSidecar.read(
                                        io,
                                        path,
                                        meta("manifest-large", header.length + 100, 1),
                                        null)
                                .blocks())
                .hasSize(1);
        assertThat(stream.readLengths.stream().mapToInt(Integer::intValue).sum())
                .isEqualTo(data.length);
        assertThat(stream.requests).allMatch(request -> request <= 1 << 20);
        assertThat(stream.closed).isTrue();
    }

    @Test
    void indexShortReadsReadTheWholeFile() throws Exception {
        byte[] data = testSidecar();
        Path path = new Path(temp.toString(), "manifest-golden");
        for (int maxRead : new int[] {Integer.MAX_VALUE, 7}) {
            CountingInput stream = new CountingInput(data, maxRead);
            FileIO io = mock(FileIO.class);
            when(io.newInputStream(ManifestSidecar.path(path))).thenReturn(stream);
            ManifestSidecar.Selection actual =
                    ManifestSidecar.read(
                            io,
                            path,
                            testMeta(),
                            RowRangeIndex.create(Collections.singletonList(new Range(20, 20))));
            assertThat(actual.blocks())
                    .extracting(block -> block.firstRecord)
                    .containsExactly(0L, 5L);
            assertThat(stream.closed).isTrue();
        }
    }

    @Test
    void adjacentBlocksShareReadsForSingleByteConsumers() throws Exception {
        byte[] header = header();
        byte[] body = new byte[400];
        for (int position = 0; position < body.length; position++) {
            body[position] = (byte) position;
        }
        ManifestSidecar.Selection selected =
                ManifestSidecar.select(
                        testSidecar(),
                        testMeta(),
                        RowRangeIndex.create(
                                Arrays.asList(
                                        new Range(0, 0),
                                        new Range(8254058425445L, 8254058425445L))));
        byte[] manifest = Arrays.copyOf(header, header.length + body.length);
        System.arraycopy(body, 0, manifest, header.length, body.length);
        CountingInput stream = new CountingInput(manifest, Integer.MAX_VALUE);
        Path path = new Path(temp.toString(), "manifest-golden");
        FileIO io = mock(FileIO.class);
        when(io.newInputStream(path)).thenReturn(stream);
        ByteArrayOutputStream actual = new ByteArrayOutputStream();
        try (InputStream input = ManifestSidecar.openManifest(io, path, selected)) {
            int value;
            while ((value = input.read()) != -1) {
                actual.write(value);
            }
            assertThat(input.read(new byte[1], 0, 0)).isZero();
        }
        assertThat(actual.toByteArray()).isEqualTo(Arrays.copyOf(manifest, header.length + 300));
        assertThat(stream.readLengths).containsExactly(300);
        assertThat(stream.seeks).containsExactly((long) header.length);
        assertThat(stream.closed).isTrue();
    }

    @Test
    void blockReadsSkipGapsAndEmptySelections() throws Exception {
        byte[] header = header();
        byte[] manifest = Arrays.copyOf(header, header.length + 400);
        Arrays.fill(manifest, header.length + 100, header.length + 300, (byte) 7);
        Path path = new Path(temp.toString(), "manifest-golden");
        for (long point : new long[] {20, 16}) {
            CountingInput stream = new CountingInput(manifest, Integer.MAX_VALUE);
            FileIO io = mock(FileIO.class);
            when(io.newInputStream(path)).thenReturn(stream);
            byte[] actual;
            try (InputStream input =
                    ManifestSidecar.openManifest(
                            io, path, select(testSidecar(), testMeta(), point))) {
                actual = IOUtils.readFully(input, false);
            }
            if (point == 20) {
                assertThat(actual).isEqualTo(Arrays.copyOf(header, header.length + 200));
                assertThat(stream.readLengths).containsExactly(100, 100);
                assertThat(stream.seeks)
                        .containsExactly((long) header.length, header.length + 300L);
            } else {
                assertThat(actual).isEqualTo(header);
                assertThat(stream.readLengths).isEmpty();
                assertThat(stream.seeks).isEmpty();
                verifyNoInteractions(io);
            }
            assertThat(stream.closed).isEqualTo(point == 20);
        }
    }

    @Test
    void cachedBlocksAreSharedByDifferentSelectionsWithoutOpeningTheManifest() throws Exception {
        byte[] header = header();
        byte[] manifest = Arrays.copyOf(header, header.length + 400);
        for (int i = header.length; i < manifest.length; i++) {
            manifest[i] = (byte) i;
        }
        Path path = new Path(temp.toString(), "manifest-golden");
        SegmentsCache<Object> cache =
                new SegmentsCache<>(1024, MemorySize.ofMebiBytes(1), 400, null, false);
        cache.put(
                new ManifestSidecar.BlockCacheKey(path, header.length, 100),
                new SingleSegments(MemorySegment.wrap(new byte[100]), 100));
        FileIO io = mock(FileIO.class);
        CountingInput stream = new CountingInput(manifest, Integer.MAX_VALUE);
        when(io.newInputStream(path)).thenReturn(stream);
        ManifestSidecar.Selection all =
                ManifestSidecar.select(
                        testSidecar(),
                        testMeta(),
                        RowRangeIndex.create(
                                Collections.singletonList(new Range(0, Long.MAX_VALUE))));
        try (InputStream in = ManifestSidecar.openManifest(io, path, all, cache)) {
            assertThat(IOUtils.readFully(in, false)).isEqualTo(manifest);
        }
        assertThat(stream.readLengths).containsExactly(400);
        assertThat(stream.seeks).containsExactly((long) header.length);
        assertThat(cache.estimatedSize()).isEqualTo(3);
        assertThat(cache.getIfPresents(path)).isNull();
        when(io.newInputStream(path)).thenThrow(new IOException("Must use cached blocks"));
        for (long point : new long[] {20, 8254058425445L, 0}) {
            ManifestSidecar.Selection selected = select(testSidecar(), testMeta(), point);
            ByteArrayOutputStream expected = new ByteArrayOutputStream();
            expected.write(header);
            for (ManifestSidecar.Block block : selected.blocks()) {
                expected.write(manifest, (int) block.offset, (int) block.length);
            }
            try (InputStream in = ManifestSidecar.openManifest(io, path, selected, cache)) {
                assertThat(IOUtils.readFully(in, false)).isEqualTo(expected.toByteArray());
            }
        }
        verify(io, times(1)).newInputStream(path);

        Path other = new Path(temp.toString(), "other/manifest-golden");
        byte[] otherBytes = manifest.clone();
        otherBytes[header.length] ^= 1;
        when(io.newInputStream(other)).thenReturn(new CountingInput(otherBytes, Integer.MAX_VALUE));
        try (InputStream in =
                ManifestSidecar.openManifest(
                        io, other, select(testSidecar(), testMeta(), 0), cache)) {
            assertThat(IOUtils.readFully(in, false))
                    .isEqualTo(Arrays.copyOf(otherBytes, header.length + 100));
        }
        verify(io).newInputStream(other);
    }

    @Test
    void mixedHitsAndMissesReadOnlyUncachedBlocks() throws Exception {
        byte[] header = header();
        byte[] manifest = Arrays.copyOf(header, header.length + 400);
        FileIO io = mock(FileIO.class);
        Path path = new Path(temp.toString(), "manifest-golden");
        CountingInput cold = new CountingInput(manifest, Integer.MAX_VALUE);
        CountingInput mixed = new CountingInput(manifest, Integer.MAX_VALUE);
        when(io.newInputStream(path)).thenReturn(cold, mixed);
        SegmentsCache<Object> cache =
                new SegmentsCache<>(1024, MemorySize.ofMebiBytes(1), 400, null, false);
        try (InputStream in =
                ManifestSidecar.openManifest(
                        io, path, select(testSidecar(), testMeta(), 8254058425445L), cache)) {
            IOUtils.readFully(in, false);
        }
        ManifestSidecar.Selection all =
                ManifestSidecar.select(
                        testSidecar(),
                        testMeta(),
                        RowRangeIndex.create(
                                Collections.singletonList(new Range(0, Long.MAX_VALUE))));
        try (InputStream in = ManifestSidecar.openManifest(io, path, all, cache)) {
            assertThat(IOUtils.readFully(in, false)).isEqualTo(manifest);
        }
        assertThat(mixed.readLengths).containsExactly(100, 100);
        assertThat(mixed.seeks).containsExactly((long) header.length, header.length + 300L);
        try (InputStream in = ManifestSidecar.openManifest(io, path, all, cache)) {
            assertThat(IOUtils.readFully(in, false)).isEqualTo(manifest);
        }
        verify(io, times(2)).newInputStream(path);
    }

    @Test
    void truncatedCoalescedReadsDoNotPopulateTheBlockCache() throws Exception {
        byte[] header = header();
        byte[] manifest = Arrays.copyOf(header, header.length + 400);
        FileIO io = mock(FileIO.class);
        Path path = new Path(temp.toString(), "manifest-golden");
        CountingInput truncated =
                new CountingInput(Arrays.copyOf(manifest, manifest.length - 1), 7);
        CountingInput complete = new CountingInput(manifest, 7);
        when(io.newInputStream(path)).thenReturn(truncated, complete);
        SegmentsCache<Object> cache =
                new SegmentsCache<>(1024, MemorySize.ofMebiBytes(1), 400, null, false);
        ManifestSidecar.Selection all =
                ManifestSidecar.select(
                        testSidecar(),
                        testMeta(),
                        RowRangeIndex.create(
                                Collections.singletonList(new Range(0, Long.MAX_VALUE))));
        try (InputStream in = ManifestSidecar.openManifest(io, path, all, cache)) {
            assertThatThrownBy(() -> IOUtils.readFully(in, false)).isInstanceOf(EOFException.class);
        }
        assertThat(truncated.closed).isTrue();
        assertThat(cache.estimatedSize()).isZero();
        try (InputStream in = ManifestSidecar.openManifest(io, path, all, cache)) {
            assertThat(IOUtils.readFully(in, false)).isEqualTo(manifest);
        }
        assertThat(complete.closed).isTrue();
        assertThat(cache.estimatedSize()).isEqualTo(3);
    }

    @Test
    void evictedBlocksAreReadAgainWithinTheSharedBudget() throws Exception {
        byte[] header = header();
        byte[] manifest = Arrays.copyOf(header, header.length + 400);
        Path path = new Path(temp.toString(), "manifest-golden");
        FileIO io = mock(FileIO.class);
        when(io.newInputStream(path))
                .thenAnswer(ignored -> new CountingInput(manifest, Integer.MAX_VALUE));
        SegmentsCache<Object> cache =
                new SegmentsCache<>(1024, MemorySize.ofBytes(1300), 400, null, false);
        ManifestSidecar.Selection all =
                ManifestSidecar.select(
                        testSidecar(),
                        testMeta(),
                        RowRangeIndex.create(
                                Collections.singletonList(new Range(0, Long.MAX_VALUE))));
        for (int round = 0; round < 2; round++) {
            try (InputStream in = ManifestSidecar.openManifest(io, path, all, cache)) {
                assertThat(IOUtils.readFully(in, false)).isEqualTo(manifest);
            }
            assertThat(cache.totalCacheBytes()).isLessThanOrEqualTo(1300);
            assertThat(cache.estimatedSize()).isEqualTo(1);
        }
        verify(io, times(2)).newInputStream(path);
    }

    @Test
    void oversizedBlocksUseBoundedReadsWithoutModifyingPreviouslyCachedBytes() throws Exception {
        byte[] header = header();
        int cachedLength = (4 << 20) + 17;
        int uncachedLength = 2 * (4 << 20) + 31;
        ManifestSidecar.Builder builder = new ManifestSidecar.Builder(header, true, true);
        builder.beginBlock(header.length, cachedLength, 1);
        builder.add(0L, 1);
        builder.endBlock();
        builder.beginBlock(header.length + cachedLength, uncachedLength, 1);
        builder.add(100L, 1);
        builder.endBlock();
        byte[] manifest = Arrays.copyOf(header, header.length + cachedLength + uncachedLength);
        Arrays.fill(manifest, header.length, header.length + cachedLength, (byte) 7);
        Arrays.fill(manifest, header.length + cachedLength, manifest.length, (byte) 9);
        byte[] data = builder.serialize(manifest.length, 2);
        ManifestFileMeta meta = meta("large", manifest.length, 2);
        Path path = new Path(temp.toString(), "large");
        FileIO io = mock(FileIO.class);
        CountingInput cold = new CountingInput(manifest, Integer.MAX_VALUE);
        CountingInput mixed = new CountingInput(manifest, Integer.MAX_VALUE);
        when(io.newInputStream(path)).thenReturn(cold, mixed);
        SegmentsCache<Object> cache =
                new SegmentsCache<>(1024, MemorySize.ofMebiBytes(8), cachedLength, null, false);
        ManifestSidecar.Selection first = select(data, meta, 0);
        byte[] expected = Arrays.copyOf(manifest, header.length + cachedLength);
        try (InputStream in = ManifestSidecar.openManifest(io, path, first, cache)) {
            assertThat(IOUtils.readFully(in, false)).isEqualTo(expected);
        }
        assertThat(cold.readLengths).containsExactly(4 << 20, 17);
        ManifestSidecar.Selection all =
                ManifestSidecar.select(
                        data,
                        meta,
                        RowRangeIndex.create(
                                Collections.singletonList(new Range(0, Long.MAX_VALUE))));
        try (InputStream in = ManifestSidecar.openManifest(io, path, all, cache)) {
            assertThat(IOUtils.readFully(in, false)).isEqualTo(manifest);
        }
        assertThat(mixed.readLengths).containsExactly(4 << 20, 4 << 20, 31);
        assertThat(cache.estimatedSize()).isEqualTo(1);
        assertThat(
                        cache.getIfPresents(
                                new ManifestSidecar.BlockCacheKey(
                                        path, header.length + cachedLength, uncachedLength)))
                .isNull();
        try (InputStream in = ManifestSidecar.openManifest(io, path, first, cache)) {
            assertThat(IOUtils.readFully(in, false)).isEqualTo(expected);
        }
        verify(io, times(2)).newInputStream(path);
    }

    @Test
    void largeBlockSpansUseBoundedReads() throws Exception {
        byte[] header = header();
        ManifestSidecar.Builder builder = new ManifestSidecar.Builder(header, true, true);
        long offset = header.length;
        for (int length : new int[] {2 << 20, 2 << 20, 2 << 20, 2 << 20, 1 << 20}) {
            builder.beginBlock(offset, length, 1);
            builder.add(20L, 1);
            builder.endBlock();
            offset += length;
        }
        byte[] data = builder.serialize(offset, 5);
        byte[] manifest = Arrays.copyOf(header, (int) offset);
        Path path = new Path(temp.toString(), "manifest-large");
        for (boolean withCache : new boolean[] {false, true}) {
            CountingInput stream = new CountingInput(manifest, Integer.MAX_VALUE);
            FileIO io = mock(FileIO.class);
            when(io.newInputStream(path)).thenReturn(stream);
            SegmentsCache<Object> cache =
                    withCache
                            ? new SegmentsCache<>(
                                    1024, MemorySize.ofMebiBytes(16), 4 << 20, null, false)
                            : null;
            try (InputStream input =
                    ManifestSidecar.openManifest(
                            io, path, select(data, meta("manifest-large", offset, 5), 20), cache)) {
                assertThat(IOUtils.readFully(input, false)).isEqualTo(manifest);
            }
            assertThat(stream.readLengths).containsExactly(4 << 20, 4 << 20, 1 << 20);
            if (withCache) {
                assertThat(stream.seeks)
                        .containsExactly(
                                (long) header.length,
                                header.length + (4L << 20),
                                header.length + (8L << 20));
                assertThat(cache.estimatedSize()).isEqualTo(5);
            } else {
                assertThat(stream.seeks).containsExactly((long) header.length);
            }
            assertThat(stream.closed).isTrue();
        }
    }

    @Test
    void blockShortReadsAndTruncation() throws Exception {
        byte[] header = header();
        Path path = new Path(temp.toString(), "manifest-golden");
        ManifestSidecar.Selection selected =
                ManifestSidecar.select(
                        testSidecar(),
                        testMeta(),
                        RowRangeIndex.create(
                                Collections.singletonList(new Range(0, Long.MAX_VALUE))));
        for (int bodyLength : new int[] {400, 399}) {
            byte[] manifest = Arrays.copyOf(header, header.length + bodyLength);
            CountingInput stream = new CountingInput(manifest, 7);
            FileIO io = mock(FileIO.class);
            when(io.newInputStream(path)).thenReturn(stream);
            try (InputStream input = ManifestSidecar.openManifest(io, path, selected)) {
                if (bodyLength == 400) {
                    assertThat(IOUtils.readFully(input, false)).isEqualTo(manifest);
                } else {
                    assertThatThrownBy(() -> IOUtils.readFully(input, false))
                            .isInstanceOf(EOFException.class);
                }
            }
            assertThat(stream.closed).isTrue();
        }
    }

    private static class CountingInput extends ByteArraySeekableStream {
        private final int maxRead;
        private final List<Integer> requests = new ArrayList<>();
        private final List<Integer> readLengths = new ArrayList<>();
        private final List<Long> seeks = new ArrayList<>();
        private boolean closed;

        private CountingInput(byte[] data, int maxRead) {
            super(data);
            this.maxRead = maxRead;
        }

        @Override
        public int read(byte[] bytes, int offset, int length) throws IOException {
            requests.add(length);
            int count = super.read(bytes, offset, Math.min(length, maxRead));
            if (count > 0) {
                readLengths.add(count);
            }
            return count;
        }

        @Override
        public void seek(long position) throws IOException {
            seeks.add(position);
            super.seek(position);
        }

        @Override
        public void close() throws IOException {
            closed = true;
            super.close();
        }
    }

    private ManifestSidecar.Selection select(byte[] data, ManifestFileMeta meta, long point)
            throws IOException {
        return ManifestSidecar.select(
                data,
                meta,
                RowRangeIndex.create(Collections.singletonList(new Range(point, point))));
    }
}
