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
import org.apache.paimon.data.SingleSegments;
import org.apache.paimon.fs.ByteArraySeekableStream;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.manifest.ManifestSidecar.ManifestSidecarSegment;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RowRangeIndex;
import org.apache.paimon.utils.SegmentsCache;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CancellationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/** Cross-language format, physical block positions, completeness and allocation bounds. */
class ManifestSidecarTest {
    @TempDir java.nio.file.Path temp;
    private final ManifestSidecar.Settings settings =
            new ManifestSidecar.Settings(new CoreOptions(sidecarOptions()), 2);

    static Options sidecarOptions() {
        Options options = new Options();
        options.set(CoreOptions.MANIFEST_SIDECAR_READ, true);
        options.set(CoreOptions.DATA_EVOLUTION_ENABLED, true);
        options.set(CoreOptions.BUCKET, 4);
        return options;
    }

    static ManifestFileMeta meta(String name, long size, long entries) {
        ManifestFileMeta meta = mock(ManifestFileMeta.class);
        when(meta.fileName()).thenReturn(name);
        when(meta.fileSize()).thenReturn(size);
        when(meta.extraFiles())
                .thenReturn(Collections.singletonList(name + ManifestSidecar.SUFFIX));
        when(meta.numAddedFiles()).thenReturn(entries);
        return meta;
    }

    private Properties fixture() throws IOException {
        Properties properties = new Properties();
        try (java.io.InputStream input = getClass().getResourceAsStream("/manifest-sidecar.txt")) {
            properties.load(input);
        }
        return properties;
    }

    private byte[] header() throws IOException {
        return Base64.getDecoder().decode(fixture().getProperty("avroHeader"));
    }

    private byte[] golden() throws IOException {
        return Base64.getDecoder().decode(fixture().getProperty("index"));
    }

    private ManifestFileMeta goldenMeta() throws IOException {
        return meta("manifest-golden", header().length + 400, 7);
    }

    @Test
    void crossLanguageFormatAndBlockOrdinals() throws Exception {
        byte[] header = header();
        ManifestSidecar.Builder builder = new ManifestSidecar.Builder(settings, header);
        builder.beginBlock(header.length, 100, 3);
        builder.add(0L, 10);
        builder.add(5L, 5);
        builder.add(20L, 5);
        builder.endBlock();
        builder.beginBlock(header.length + 100, 200, 2);
        builder.add((1L << 32) - 2, 5);
        builder.add(8254058425445L, 1);
        builder.endBlock();
        builder.beginBlock(header.length + 300, 100, 2);
        builder.add(20L, 5);
        builder.add(Long.MAX_VALUE, 1);
        builder.endBlock();
        byte[] data = builder.serialize("manifest-golden", header.length + 400, 7);
        assertThat(data).isEqualTo(golden());
        ManifestFileMeta meta = goldenMeta();
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
        assertThat(ManifestSidecar.select(data, meta, query, settings).blocks()).isEmpty();
        assertThat(query.ranges()).containsExactly(new Range(10, 19), new Range(25, 40));
    }

    @Test
    void settingsUseMemorySizesAndFollowTheManifestTarget() {
        Options options = sidecarOptions();
        assertThat(new ManifestSidecar.Settings(new CoreOptions(options), 2).maxBytes)
                .isEqualTo(16 * 1024 * 1024);
        options.setString(CoreOptions.MANIFEST_TARGET_FILE_SIZE.key(), "12 mb");
        assertThat(new ManifestSidecar.Settings(new CoreOptions(options), 2).maxBytes)
                .isEqualTo(24 * 1024 * 1024);
        options.setString(CoreOptions.MANIFEST_TARGET_FILE_SIZE.key(), "64 mb");
        assertThat(new ManifestSidecar.Settings(new CoreOptions(options), 2).maxBytes)
                .isEqualTo(128 * 1024 * 1024);
        options.setString(CoreOptions.MANIFEST_TARGET_FILE_SIZE.key(), "1 gb");
        assertThat(new ManifestSidecar.Settings(new CoreOptions(options), 2).maxBytes)
                .isEqualTo(Integer.MAX_VALUE - 1);
        options.set(CoreOptions.MANIFEST_TARGET_FILE_SIZE, new MemorySize(Long.MAX_VALUE));
        assertThat(new CoreOptions(options).manifestSidecarMaxSize().getBytes())
                .isEqualTo(Long.MAX_VALUE);
        assertThat(new ManifestSidecar.Settings(new CoreOptions(options), 2).maxBytes)
                .isEqualTo(Integer.MAX_VALUE - 1);

        options.setString(CoreOptions.MANIFEST_SIDECAR_MAX_BYTES.key(), "512 kb");
        options.setString(CoreOptions.MANIFEST_TARGET_FILE_SIZE.key(), "1 gb");
        assertThat(new ManifestSidecar.Settings(new CoreOptions(options), 2).maxBytes)
                .isEqualTo(512 * 1024);
        options.set(CoreOptions.MANIFEST_SIDECAR_MAX_BYTES, new MemorySize(Integer.MAX_VALUE - 1L));
        assertThat(new ManifestSidecar.Settings(new CoreOptions(options), 2).maxBytes)
                .isEqualTo(Integer.MAX_VALUE - 1);
        for (long bytes : new long[] {0, 127, Integer.MAX_VALUE, Long.MAX_VALUE}) {
            options.set(CoreOptions.MANIFEST_SIDECAR_MAX_BYTES, new MemorySize(bytes));
            assertThat(new ManifestSidecar.Settings(new CoreOptions(options), 2).maxBytes)
                    .isEqualTo((int) Math.min(bytes, Integer.MAX_VALUE - 1L));
        }
        options.setString(CoreOptions.MANIFEST_SIDECAR_MAX_BYTES.key(), "-1 bytes");
        assertThatThrownBy(() -> new ManifestSidecar.Settings(new CoreOptions(options), 2))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void insufficientByteBudgetSkipsSidecarIo() throws Exception {
        FileIO io = mock(FileIO.class);
        Path path = new Path(temp.toString(), "manifest-golden");
        ManifestFileMeta meta = goldenMeta();
        for (int bytes : new int[] {0, 1, 127}) {
            Options options = sidecarOptions();
            options.set(CoreOptions.MANIFEST_SIDECAR_MAX_BYTES, new MemorySize(bytes));
            ManifestSidecar.Settings settings =
                    new ManifestSidecar.Settings(new CoreOptions(options), 2);
            assertThat(ManifestSidecar.read(io, path, meta, null, settings)).isNull();
            assertThat(ManifestSidecar.build(io, path, meta.fileSize(), 7, settings)).isNull();
        }
        verifyNoInteractions(io);
    }

    @Test
    void minMaxSkipsExactIntersectionChecksAndHandlesOneInterval() throws Exception {
        byte[] header = header();
        ManifestSidecar.Builder builder = new ManifestSidecar.Builder(settings, header);
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
        byte[] data = builder.serialize("m", header.length + 300, 5);
        ManifestFileMeta meta = meta("m", header.length + 300, 5);
        RowRangeIndex outside =
                spy(RowRangeIndex.create(Collections.singletonList(new Range(50, 59))));
        ManifestSidecar.Selection none = ManifestSidecar.select(data, meta, outside, settings);
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
        ManifestSidecar.Selection hit = ManifestSidecar.select(data, meta, one, settings);
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
            ManifestSidecar.Builder builder = new ManifestSidecar.Builder(settings, header);
            builder.beginBlock(header.length, 100, 1);
            builder.add(range.from, range.to - range.from + 1);
            builder.endBlock();
            byte[] data = builder.serialize("m", header.length + 100, 1);
            ManifestFileMeta meta = meta("m", header.length + 100, 1);
            assertThat(ManifestSidecar.select(data, meta, null, settings).blocks()).hasSize(1);
            assertThat(
                            ManifestSidecar.select(
                                            data,
                                            meta,
                                            RowRangeIndex.create(Collections.emptyList()),
                                            settings)
                                    .blocks())
                    .isEmpty();
            for (long point : new long[] {range.from, range.to}) {
                RowRangeIndex query =
                        spy(
                                RowRangeIndex.create(
                                        Collections.singletonList(new Range(point, point))));
                assertThat(ManifestSidecar.select(data, meta, query, settings).blocks()).hasSize(1);
                verify(query).intersects(range.from, range.to);
            }
            long missing = range.from > 0 ? range.from - 1 : range.to + 1;
            assertThat(select(data, meta, missing).blocks()).isEmpty();
        }
    }

    @Test
    void malformedConsumedIntervalsStillFallBack() throws Exception {
        int firstBlockIntervals = 60 + 4 + header().length + 4 + 4 + 24 + 1 + 5 + 4;
        ManifestFileMeta meta = goldenMeta();
        for (long[] mutation : new long[][] {{0, -1}, {8, -1}, {8, 30}, {16, 9}, {24, 19}}) {
            byte[] data = golden();
            ByteBuffer.wrap(data).putLong(firstBlockIntervals + (int) mutation[0], mutation[1]);
            byte[] hash =
                    MessageDigest.getInstance("SHA-256")
                            .digest(Arrays.copyOf(data, data.length - 32));
            System.arraycopy(hash, 0, data, data.length - 32, 32);
            Files.write(temp.resolve("manifest-golden" + ManifestSidecar.SUFFIX), data);
            for (RowRangeIndex query :
                    Collections.singletonList(
                            RowRangeIndex.create(Collections.singletonList(new Range(15, 15))))) {
                assertThat(
                                ManifestSidecar.read(
                                        LocalFileIO.create(),
                                        new Path(temp.toString(), "manifest-golden"),
                                        meta,
                                        query,
                                        settings))
                        .isNull();
            }
        }
    }

    @Test
    void rowBoundsAndMatchesSkipUnusedIntervals() throws Exception {
        byte[] header = header();
        ManifestSidecar.Builder builder = new ManifestSidecar.Builder(settings, header);
        builder.beginBlock(header.length, 100, 3);
        builder.add(0L, 10);
        builder.add(20L, 10);
        builder.add(40L, 10);
        builder.endBlock();
        byte[] data = builder.serialize("m", header.length + 100, 3);
        int intervals = 60 + 4 + header.length + 4 + 4 + 24 + 1 + 5 + 4;
        // A checksummed invalid tail must not be visited once the answer is known.
        ByteBuffer.wrap(data).putLong(intervals + 32, 19);
        byte[] hash =
                MessageDigest.getInstance("SHA-256").digest(Arrays.copyOf(data, data.length - 32));
        System.arraycopy(hash, 0, data, data.length - 32, 32);
        ManifestFileMeta meta = meta("m", header.length + 100, 3);
        assertThat(select(data, meta, 0).blocks()).hasSize(1);
        assertThat(select(data, meta, 20).blocks()).hasSize(1);
        assertThat(select(data, meta, 100).blocks()).isEmpty();
        assertThat(
                        ManifestSidecar.select(
                                        data,
                                        meta,
                                        RowRangeIndex.create(Collections.emptyList()),
                                        settings)
                                .blocks())
                .isEmpty();
        assertThatThrownBy(() -> select(data, meta, 35)).isInstanceOf(IOException.class);
    }

    @Test
    void hugeRangesAreNotExpandedAndInvalidCoverageRetainsBlocks() throws Exception {
        byte[] header = header();
        ManifestSidecar.Builder builder = new ManifestSidecar.Builder(settings, header);
        builder.beginBlock(header.length, 100, 2);
        builder.add(0L, Long.MAX_VALUE);
        builder.add(Long.MAX_VALUE, 1);
        builder.endBlock();
        byte[] data = builder.serialize("m", header.length + 100, 2);
        assertThat(data.length).isLessThan(512);
        assertThat(select(data, meta("m", header.length + 100, 2), Long.MAX_VALUE).blocks())
                .hasSize(1);
        for (Long first : Arrays.asList(null, -1L, Long.MAX_VALUE)) {
            builder = new ManifestSidecar.Builder(settings, header);
            builder.beginBlock(header.length, 100, 1);
            builder.add(first, 2);
            builder.endBlock();
            assertThat(
                            select(
                                            builder.serialize("m", header.length + 100, 1),
                                            meta("m", header.length + 100, 1),
                                            100)
                                    .blocks())
                    .hasSize(1);
        }
        for (long count : new long[] {0, -1}) {
            builder = new ManifestSidecar.Builder(settings, header);
            builder.beginBlock(header.length, 100, 1);
            builder.add(0L, count);
            builder.endBlock();
            assertThat(
                            select(
                                            builder.serialize("m", header.length + 100, 1),
                                            meta("m", header.length + 100, 1),
                                            100)
                                    .blocks())
                    .hasSize(1);
        }
        Options options = sidecarOptions();
        options.set(CoreOptions.MANIFEST_SIDECAR_MAX_BYTES, new MemorySize(512));
        builder =
                new ManifestSidecar.Builder(
                        new ManifestSidecar.Settings(new CoreOptions(options), 2), header);
        builder.beginBlock(header.length, 100, 64);
        for (int i = 0; i < 64; i++) {
            builder.add(i * 10L, 1);
        }
        builder.endBlock();
        assertThat(
                        select(
                                        builder.serialize("m", header.length + 100, 64),
                                        meta("m", header.length + 100, 64),
                                        5)
                                .blocks())
                .hasSize(1);
        options.set(CoreOptions.MANIFEST_SIDECAR_MAX_BYTES, new MemorySize(128));
        builder =
                new ManifestSidecar.Builder(
                        new ManifestSidecar.Settings(new CoreOptions(options), 2), header);
        assertThat(builder.serialize("m", 1, 2)).isNull();
    }

    @Test
    void cacheRespectsElementThresholdAndPerReadByteBudget() throws Exception {
        byte[] data = golden();
        Path path = new Path(temp.toString(), "manifest-golden");
        Path sidecar = ManifestSidecar.path(path);
        Files.write(temp.resolve(sidecar.getName()), data);
        ManifestFileMeta meta = goldenMeta();
        FileIO io = spy(LocalFileIO.create());
        SegmentsCache<Path> tooSmall =
                new SegmentsCache<>(1024, MemorySize.ofMebiBytes(1), data.length - 1L, null, false);
        assertThat(readCached(io, path, meta, settings, tooSmall).blocks()).hasSize(2);
        assertThat(readCached(io, path, meta, settings, tooSmall).blocks()).hasSize(2);
        assertThat(tooSmall.getIfPresents(sidecar)).isNull();
        verify(io, times(2)).newInputStream(sidecar);

        SegmentsCache<Path> cache =
                new SegmentsCache<>(1024, MemorySize.ofMebiBytes(1), data.length, null, false);
        assertThat(readCached(io, path, meta, settings, cache).blocks()).hasSize(2);
        Options options = sidecarOptions();
        options.set(CoreOptions.MANIFEST_SIDECAR_MAX_BYTES, new MemorySize(data.length - 1));
        assertThat(
                        readCached(
                                io,
                                path,
                                meta,
                                new ManifestSidecar.Settings(new CoreOptions(options), 2),
                                cache))
                .isNull();
        assertThat(readCached(io, path, meta, settings, cache).blocks()).hasSize(2);
        verify(io, times(3)).newInputStream(sidecar);
    }

    @Test
    void cachedSegmentsRequireSidecarType() throws Exception {
        byte[] data = golden();
        Path path = new Path(temp.toString(), "manifest-golden");
        Path sidecar = ManifestSidecar.path(path);
        Files.write(temp.resolve(sidecar.getName()), data);
        SegmentsCache<Path> cache =
                new SegmentsCache<>(1024, MemorySize.ofMebiBytes(1), Long.MAX_VALUE, null, false);
        cache.put(sidecar, new SingleSegments(MemorySegment.wrap(data), data.length));
        FileIO io = spy(LocalFileIO.create());

        assertThat(readCached(io, path, goldenMeta(), settings, cache).blocks()).hasSize(2);
        assertThat(cache.getIfPresents(sidecar)).isInstanceOf(ManifestSidecarSegment.class);
        ManifestSidecarSegment cached = (ManifestSidecarSegment) cache.getIfPresents(sidecar);
        assertThat(cached.bytes()).containsExactly(data);
        assertThat(cached.totalMemorySize()).isEqualTo(data.length);
        assertThat(readCached(io, path, goldenMeta(), settings, cache).blocks()).hasSize(2);
        verify(io, times(1)).newInputStream(sidecar);
    }

    @Test
    void missingAndInvalidSidecarsAreNotCached() throws Exception {
        byte[] data = golden();
        Path path = new Path(temp.toString(), "manifest-golden");
        Path sidecar = ManifestSidecar.path(path);
        ManifestFileMeta meta = goldenMeta();
        FileIO io = spy(LocalFileIO.create());
        SegmentsCache<Path> cache =
                new SegmentsCache<>(1024, MemorySize.ofMebiBytes(1), Long.MAX_VALUE, null, false);
        assertThat(readCached(io, path, meta, settings, cache)).isNull();
        assertThat(cache.getIfPresents(sidecar)).isNull();
        byte[] corrupt = data.clone();
        corrupt[0] ^= 1;
        Files.write(temp.resolve(sidecar.getName()), corrupt);
        assertThat(readCached(io, path, meta, settings, cache)).isNull();
        assertThat(cache.getIfPresents(sidecar)).isNull();
        Files.write(temp.resolve(sidecar.getName()), data);
        assertThat(readCached(io, path, meta, settings, cache).blocks()).hasSize(2);
        assertThat(readCached(io, path, meta, settings, cache).blocks()).hasSize(2);
        verify(io, times(3)).newInputStream(sidecar);
    }

    @Test
    void cachedBytesPreservePerQueryCancellation() throws Exception {
        Path path = new Path(temp.toString(), "manifest-golden");
        Path sidecar = ManifestSidecar.path(path);
        Files.write(temp.resolve(sidecar.getName()), golden());
        ManifestFileMeta meta = goldenMeta();
        FileIO io = spy(LocalFileIO.create());
        SegmentsCache<Path> cache =
                new SegmentsCache<>(1024, MemorySize.ofMebiBytes(1), Long.MAX_VALUE, null, false);
        RowRangeIndex cancelled = mock(RowRangeIndex.class);
        when(cancelled.intersects(anyLong(), anyLong()))
                .thenThrow(new CancellationException("cancelled"));
        assertThatThrownBy(
                        () ->
                                ManifestSidecar.read(
                                        io, path, meta, cancelled, null, null, null, settings,
                                        cache))
                .isInstanceOf(CancellationException.class);
        assertThat(cache.getIfPresents(sidecar)).isNull();

        assertThat(readCached(io, path, meta, settings, cache).blocks()).hasSize(2);
        assertThatThrownBy(
                        () ->
                                ManifestSidecar.read(
                                        io, path, meta, cancelled, null, null, null, settings,
                                        cache))
                .isInstanceOf(CancellationException.class);
        assertThat(readCached(io, path, meta, settings, cache).blocks()).hasSize(2);
        verify(io, times(2)).newInputStream(sidecar);
    }

    private ManifestSidecar.Selection readCached(
            FileIO io,
            Path path,
            ManifestFileMeta meta,
            ManifestSidecar.Settings settings,
            SegmentsCache<Path> cache) {
        return ManifestSidecar.read(
                io,
                path,
                meta,
                RowRangeIndex.create(Collections.singletonList(new Range(20, 20))),
                null,
                null,
                null,
                settings,
                cache);
    }

    @Test
    void corruptMissingIncompleteAndMismatchedIndexesFallback() throws Exception {
        Path manifest = new Path(temp.toString(), "manifest-golden");
        java.nio.file.Path index = temp.resolve("manifest-golden" + ManifestSidecar.SUFFIX);
        ManifestFileMeta meta = goldenMeta();
        RowRangeIndex query = RowRangeIndex.create(Collections.singletonList(new Range(11, 11)));

        assertThat(ManifestSidecar.read(LocalFileIO.create(), manifest, meta, query, settings))
                .isNull();
        byte[] good = golden();
        for (int position : new int[] {0, 9, 11, 15, 16, 55, 63, 67, 75, good.length - 1}) {
            byte[] bad = good.clone();
            bad[position] ^= 2;
            Files.write(index, bad);
            assertThat(ManifestSidecar.read(LocalFileIO.create(), manifest, meta, query, settings))
                    .isNull();
        }
        // A valid checksum cannot make an unsupported container version readable.
        for (int version : new int[] {0, 2, 99}) {
            byte[] bad = good.clone();
            ByteBuffer.wrap(bad).putInt(8, version);
            byte[] hash =
                    MessageDigest.getInstance("SHA-256")
                            .digest(Arrays.copyOf(bad, bad.length - 32));
            System.arraycopy(hash, 0, bad, bad.length - 32, 32);
            assertThatThrownBy(() -> ManifestSidecar.select(bad, meta, query, settings))
                    .isInstanceOf(IOException.class);
        }
        Files.write(index, Arrays.copyOf(good, good.length - 1));
        assertThat(ManifestSidecar.read(LocalFileIO.create(), manifest, meta, query, settings))
                .isNull();
        Files.write(index, good);
        assertThat(
                        ManifestSidecar.read(LocalFileIO.create(), manifest, meta, query, settings)
                                .blocks())
                .isEmpty();
        assertThatThrownBy(
                        () ->
                                ManifestSidecar.select(
                                        good, meta("other", meta.fileSize(), 7), query, settings))
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
            assertThat(ManifestSidecar.read(fileIO, path, meta("m", 1, 1), null, settings))
                    .isNull();
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
            assertThatThrownBy(
                            () ->
                                    ManifestSidecar.read(
                                            fileIO, path, meta("m", 1, 1), null, settings))
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
            assertThatThrownBy(
                            () ->
                                    ManifestSidecar.read(
                                            fileIO, path, meta("m", 1, 1), null, settings))
                    .isSameAs(failure);
        }
    }

    @Test
    void indexReadsUseBoundedBulkRequests() throws Exception {
        byte[] header = header();
        for (int blockCount : new int[] {5000, 25000, 131073}) {
            ManifestSidecar.Builder builder = new ManifestSidecar.Builder(settings, header);
            for (int blockNumber = 0; blockNumber < blockCount; blockNumber++) {
                builder.beginBlock(header.length + blockNumber * 100L, 100, 1);
                builder.add((long) blockNumber, 1);
                builder.endBlock();
            }
            long size = header.length + blockCount * 100L;
            byte[] data = builder.serialize("manifest-large", size, blockCount);
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
                            RowRangeIndex.create(Collections.singletonList(new Range(0, 0))),
                            settings);
            assertThat(actual.blocks()).hasSize(1);
            assertThat(actual.blocks().get(0).offset).isEqualTo(header.length);
            assertThat(stream.readLengths).hasSize((data.length + (1 << 20) - 1) / (1 << 20));
            assertThat(stream.requests).allMatch(request -> request <= 1 << 20);
            assertThat(stream.closed).isTrue();
        }
    }

    @Test
    void indexShortReadsAndExactBudget() throws Exception {
        byte[] data = golden();
        Options options = sidecarOptions();
        options.set(CoreOptions.MANIFEST_SIDECAR_MAX_BYTES, new MemorySize(data.length));
        Path path = new Path(temp.toString(), "manifest-golden");
        for (int maxRead : new int[] {Integer.MAX_VALUE, 7}) {
            CountingInput stream = new CountingInput(data, maxRead);
            FileIO io = mock(FileIO.class);
            when(io.newInputStream(ManifestSidecar.path(path))).thenReturn(stream);
            ManifestSidecar.Selection actual =
                    ManifestSidecar.read(
                            io,
                            path,
                            goldenMeta(),
                            RowRangeIndex.create(Collections.singletonList(new Range(20, 20))),
                            new ManifestSidecar.Settings(new CoreOptions(options), 2));
            assertThat(actual.blocks())
                    .extracting(block -> block.firstRecord)
                    .containsExactly(0L, 5L);
            assertThat(stream.closed).isTrue();
        }
    }

    @Test
    void indexOverBudgetStopsAfterOneExtraByte() throws Exception {
        Options options = sidecarOptions();
        options.set(CoreOptions.MANIFEST_SIDECAR_MAX_BYTES, new MemorySize(128));
        Path path = new Path(temp.toString(), "manifest-golden");
        CountingInput stream = new CountingInput(golden(), Integer.MAX_VALUE);
        FileIO io = mock(FileIO.class);
        when(io.newInputStream(ManifestSidecar.path(path))).thenReturn(stream);
        assertThat(
                        ManifestSidecar.read(
                                io,
                                path,
                                goldenMeta(),
                                RowRangeIndex.create(Collections.singletonList(new Range(20, 20))),
                                new ManifestSidecar.Settings(new CoreOptions(options), 2)))
                .isNull();
        assertThat(stream.readLengths).containsExactly(129);
        assertThat(stream.closed).isTrue();
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
                        golden(),
                        goldenMeta(),
                        RowRangeIndex.create(
                                Arrays.asList(
                                        new Range(0, 0),
                                        new Range(8254058425445L, 8254058425445L))),
                        settings);
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
                    ManifestSidecar.openManifest(io, path, select(golden(), goldenMeta(), point))) {
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
        SegmentsCache<Path> cache =
                new SegmentsCache<>(1024, MemorySize.ofMebiBytes(1), 400, null, false);
        cache.put(
                path,
                header.length,
                100,
                new SingleSegments(MemorySegment.wrap(new byte[100]), 100));
        FileIO io = mock(FileIO.class);
        CountingInput stream = new CountingInput(manifest, Integer.MAX_VALUE);
        when(io.newInputStream(path)).thenReturn(stream);
        ManifestSidecar.Selection all =
                ManifestSidecar.select(
                        golden(),
                        goldenMeta(),
                        RowRangeIndex.create(
                                Collections.singletonList(new Range(0, Long.MAX_VALUE))),
                        settings);
        try (InputStream in = ManifestSidecar.openManifest(io, path, all, cache)) {
            assertThat(IOUtils.readFully(in, false)).isEqualTo(manifest);
        }
        assertThat(stream.readLengths).containsExactly(400);
        assertThat(stream.seeks).containsExactly((long) header.length);
        assertThat(cache.estimatedSize()).isEqualTo(3);
        assertThat(cache.getIfPresents(path)).isNull();
        when(io.newInputStream(path)).thenThrow(new IOException("Must use cached blocks"));
        for (long point : new long[] {20, 8254058425445L, 0}) {
            ManifestSidecar.Selection selected = select(golden(), goldenMeta(), point);
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
                ManifestSidecar.openManifest(io, other, select(golden(), goldenMeta(), 0), cache)) {
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
        SegmentsCache<Path> cache =
                new SegmentsCache<>(1024, MemorySize.ofMebiBytes(1), 400, null, false);
        try (InputStream in =
                ManifestSidecar.openManifest(
                        io, path, select(golden(), goldenMeta(), 8254058425445L), cache)) {
            IOUtils.readFully(in, false);
        }
        ManifestSidecar.Selection all =
                ManifestSidecar.select(
                        golden(),
                        goldenMeta(),
                        RowRangeIndex.create(
                                Collections.singletonList(new Range(0, Long.MAX_VALUE))),
                        settings);
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
        SegmentsCache<Path> cache =
                new SegmentsCache<>(1024, MemorySize.ofMebiBytes(1), 400, null, false);
        ManifestSidecar.Selection all =
                ManifestSidecar.select(
                        golden(),
                        goldenMeta(),
                        RowRangeIndex.create(
                                Collections.singletonList(new Range(0, Long.MAX_VALUE))),
                        settings);
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
        SegmentsCache<Path> cache =
                new SegmentsCache<>(1024, MemorySize.ofBytes(1300), 400, null, false);
        ManifestSidecar.Selection all =
                ManifestSidecar.select(
                        golden(),
                        goldenMeta(),
                        RowRangeIndex.create(
                                Collections.singletonList(new Range(0, Long.MAX_VALUE))),
                        settings);
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
        int cachedLength = (1 << 20) + 17;
        int uncachedLength = 2 * (1 << 20) + 31;
        ManifestSidecar.Builder builder = new ManifestSidecar.Builder(settings, header);
        builder.beginBlock(header.length, cachedLength, 1);
        builder.add(0L, 1);
        builder.endBlock();
        builder.beginBlock(header.length + cachedLength, uncachedLength, 1);
        builder.add(100L, 1);
        builder.endBlock();
        byte[] manifest = Arrays.copyOf(header, header.length + cachedLength + uncachedLength);
        Arrays.fill(manifest, header.length, header.length + cachedLength, (byte) 7);
        Arrays.fill(manifest, header.length + cachedLength, manifest.length, (byte) 9);
        byte[] data = builder.serialize("large", manifest.length, 2);
        ManifestFileMeta meta = meta("large", manifest.length, 2);
        Path path = new Path(temp.toString(), "large");
        FileIO io = mock(FileIO.class);
        CountingInput cold = new CountingInput(manifest, Integer.MAX_VALUE);
        CountingInput mixed = new CountingInput(manifest, Integer.MAX_VALUE);
        when(io.newInputStream(path)).thenReturn(cold, mixed);
        SegmentsCache<Path> cache =
                new SegmentsCache<>(1024, MemorySize.ofMebiBytes(8), cachedLength, null, false);
        ManifestSidecar.Selection first = select(data, meta, 0);
        byte[] expected = Arrays.copyOf(manifest, header.length + cachedLength);
        try (InputStream in = ManifestSidecar.openManifest(io, path, first, cache)) {
            assertThat(IOUtils.readFully(in, false)).isEqualTo(expected);
        }
        assertThat(cold.readLengths).containsExactly(1 << 20, 17);
        ManifestSidecar.Selection all =
                ManifestSidecar.select(
                        data,
                        meta,
                        RowRangeIndex.create(
                                Collections.singletonList(new Range(0, Long.MAX_VALUE))),
                        settings);
        try (InputStream in = ManifestSidecar.openManifest(io, path, all, cache)) {
            assertThat(IOUtils.readFully(in, false)).isEqualTo(manifest);
        }
        assertThat(mixed.readLengths).containsExactly(1 << 20, 1 << 20, 31);
        assertThat(cache.estimatedSize()).isEqualTo(1);
        assertThat(cache.getIfPresents(path, header.length + cachedLength, uncachedLength))
                .isNull();
        try (InputStream in = ManifestSidecar.openManifest(io, path, first, cache)) {
            assertThat(IOUtils.readFully(in, false)).isEqualTo(expected);
        }
        verify(io, times(2)).newInputStream(path);
    }

    @Test
    void largeBlockSpansUseBoundedReads() throws Exception {
        byte[] header = header();
        ManifestSidecar.Builder builder = new ManifestSidecar.Builder(settings, header);
        long offset = header.length;
        for (int length : new int[] {512 * 1024, 512 * 1024, 257}) {
            builder.beginBlock(offset, length, 1);
            builder.add(20L, 1);
            builder.endBlock();
            offset += length;
        }
        byte[] data = builder.serialize("manifest-large", offset, 3);
        byte[] manifest = Arrays.copyOf(header, (int) offset);
        CountingInput stream = new CountingInput(manifest, Integer.MAX_VALUE);
        FileIO io = mock(FileIO.class);
        Path path = new Path(temp.toString(), "manifest-large");
        when(io.newInputStream(path)).thenReturn(stream);
        try (InputStream input =
                ManifestSidecar.openManifest(
                        io, path, select(data, meta("manifest-large", offset, 3), 20))) {
            assertThat(IOUtils.readFully(input, false)).isEqualTo(manifest);
        }
        assertThat(stream.readLengths).containsExactly(1 << 20, 257);
        assertThat(stream.seeks).containsExactly((long) header.length);
        assertThat(stream.closed).isTrue();
    }

    @Test
    void blockShortReadsAndTruncation() throws Exception {
        byte[] header = header();
        Path path = new Path(temp.toString(), "manifest-golden");
        ManifestSidecar.Selection selected =
                ManifestSidecar.select(
                        golden(),
                        goldenMeta(),
                        RowRangeIndex.create(
                                Collections.singletonList(new Range(0, Long.MAX_VALUE))),
                        settings);
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

    @Test
    void selectedStreamsSeekInVirtualCoordinates() throws Exception {
        byte[] header = header();
        byte[] manifest = Arrays.copyOf(header, header.length + 400);
        for (int i = header.length; i < manifest.length; i++) {
            manifest[i] = (byte) i;
        }
        ByteArrayOutputStream expected = new ByteArrayOutputStream();
        expected.write(header);
        expected.write(manifest, header.length, 100);
        expected.write(manifest, header.length + 300, 100);
        byte[] selectedBytes = expected.toByteArray();
        ManifestSidecar.Selection selected = select(golden(), goldenMeta(), 20);
        Path path = new Path(temp.toString(), "manifest-golden");
        for (int maxElementSize : new int[] {0, 50, 400}) {
            SegmentsCache<Path> cache =
                    maxElementSize == 0
                            ? null
                            : new SegmentsCache<>(
                                    1024, MemorySize.ofMebiBytes(1), maxElementSize, null, false);
            FileIO io = mock(FileIO.class);
            when(io.newInputStream(path)).thenAnswer(ignored -> new CountingInput(manifest, 7));
            try (SeekableInputStream input =
                    ManifestSidecar.openManifest(io, path, selected, cache)) {
                for (int position :
                        new int[] {
                            0,
                            header.length + 117,
                            header.length - 1,
                            selectedBytes.length,
                            header.length + 100,
                            header.length + 17
                        }) {
                    input.seek(position);
                    assertThat(input.getPos()).isEqualTo(position);
                    assertThat(IOUtils.readFully(input, false))
                            .isEqualTo(
                                    Arrays.copyOfRange(
                                            selectedBytes, position, selectedBytes.length));
                    assertThat(input.getPos()).isEqualTo(selectedBytes.length);
                }
                assertThatThrownBy(() -> input.seek(-1)).isInstanceOf(IOException.class);
                assertThatThrownBy(() -> input.seek(selectedBytes.length + 1))
                        .isInstanceOf(EOFException.class);
            }
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
                RowRangeIndex.create(Collections.singletonList(new Range(point, point))),
                settings);
    }
}
