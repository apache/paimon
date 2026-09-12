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
import org.apache.paimon.fs.ByteArraySeekableStream;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RowRangeIndex;

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
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Cross-language format, physical block positions, completeness and allocation bounds. */
class ManifestRowIdIndexTest {
    @TempDir java.nio.file.Path temp;
    private final ManifestRowIdIndex.Settings settings =
            new ManifestRowIdIndex.Settings(new Options());

    static ManifestFileMeta meta(String name, long size, long entries) {
        ManifestFileMeta meta = mock(ManifestFileMeta.class);
        when(meta.fileName()).thenReturn(name);
        when(meta.fileSize()).thenReturn(size);
        when(meta.extraFiles())
                .thenReturn(Collections.singletonList(name + ManifestRowIdIndex.SUFFIX));
        when(meta.numAddedFiles()).thenReturn(entries);
        return meta;
    }

    private Properties fixture() throws IOException {
        Properties properties = new Properties();
        try (java.io.InputStream input =
                getClass().getResourceAsStream("/manifest-row-id-index-v2.txt")) {
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
        ManifestRowIdIndex.Builder builder = new ManifestRowIdIndex.Builder(settings, header);
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
        ManifestRowIdIndex.Selection selected = select(data, meta, 20);
        assertThat(selected.blocks()).extracting(b -> b.firstRecord).containsExactly(0L, 5L);
        assertThat(selected.blocks())
                .extracting(b -> b.offset)
                .containsExactly((long) header.length, header.length + 300L);
        assertThat(selected.blocks()).extracting(b -> b.length).containsExactly(100L, 100L);

        ManifestRowIdIndex.Selection gap = select(data, meta, 16);

        assertThat(gap.blocks()).isEmpty();
        RowRangeIndex query =
                RowRangeIndex.create(Arrays.asList(new Range(10, 19), new Range(25, 40)));
        assertThat(ManifestRowIdIndex.select(data, meta, query, settings).blocks()).isEmpty();
        assertThat(query.ranges()).containsExactly(new Range(10, 19), new Range(25, 40));
    }

    @Test
    void minMaxSkipsExactIntersectionChecksAndHandlesOneInterval() throws Exception {
        byte[] header = header();
        ManifestRowIdIndex.Builder builder = new ManifestRowIdIndex.Builder(settings, header);
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
        ManifestRowIdIndex.Selection none =
                ManifestRowIdIndex.select(data, meta, outside, settings);
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
        ManifestRowIdIndex.Selection hit = ManifestRowIdIndex.select(data, meta, one, settings);
        assertThat(hit.blocks()).extracting(b -> b.firstRecord).containsExactly(4L);

        // A one-interval block needs no second intersection check after its envelope matches.
        verify(one, times(3)).intersects(anyLong(), anyLong());
    }

    @Test
    void malformedIntervalsStillFallbackAfterMinMaxRejectionOrAnEarlyHit() throws Exception {
        byte[] data = golden();
        int firstBlockIntervals = 68 + 4 + header().length + 4 + 36;
        // Make the second interval overlap the first, keeping the envelope unchanged.
        ByteBuffer.wrap(data).putLong(firstBlockIntervals + 16, 9L);
        byte[] hash =
                MessageDigest.getInstance("SHA-256").digest(Arrays.copyOf(data, data.length - 32));
        System.arraycopy(hash, 0, data, data.length - 32, 32);
        Files.write(temp.resolve("manifest-golden" + ManifestRowIdIndex.SUFFIX), data);
        ManifestFileMeta meta = goldenMeta();

        for (long point : new long[] {30, 0}) {
            RowRangeIndex query =
                    RowRangeIndex.create(Collections.singletonList(new Range(point, point)));
            assertThat(
                            ManifestRowIdIndex.read(
                                    LocalFileIO.create(),
                                    new Path(temp.toString(), "manifest-golden"),
                                    meta,
                                    query,
                                    settings))
                    .isNull();
        }
    }

    @Test
    void hugeRangesAreNotExpandedAndInvalidCoverageDisablesIndex() throws Exception {
        byte[] header = header();
        ManifestRowIdIndex.Builder builder = new ManifestRowIdIndex.Builder(settings, header);
        builder.beginBlock(header.length, 100, 2);
        builder.add(0L, Long.MAX_VALUE);
        builder.add(Long.MAX_VALUE, 1);
        builder.endBlock();
        byte[] data = builder.serialize("m", header.length + 100, 2);
        assertThat(data.length).isLessThan(512);
        assertThat(select(data, meta("m", header.length + 100, 2), Long.MAX_VALUE).blocks())
                .hasSize(1);
        for (Long first : Arrays.asList(null, -1L, Long.MAX_VALUE)) {
            builder = new ManifestRowIdIndex.Builder(settings, header);
            builder.beginBlock(header.length, 100, 1);
            builder.add(first, 2);
            builder.endBlock();
            assertThat(builder.serialize("m", header.length + 100, 1)).isNull();
        }
        for (long count : new long[] {0, -1}) {
            builder = new ManifestRowIdIndex.Builder(settings, header);
            builder.beginBlock(header.length, 100, 1);
            builder.add(0L, count);
            assertThat(builder.serialize("m", 1, 1)).isNull();
        }
        Options options = new Options();
        options.set(CoreOptions.MANIFEST_ROW_ID_INDEX_MAX_RANGES, 1);
        builder = new ManifestRowIdIndex.Builder(new ManifestRowIdIndex.Settings(options), header);
        builder.beginBlock(header.length, 100, 2);
        builder.add(0L, 1);
        builder.add(10L, 1);
        assertThat(builder.serialize("m", 1, 2)).isNull();
        options.set(CoreOptions.MANIFEST_ROW_ID_INDEX_MAX_BYTES, 128);
        builder = new ManifestRowIdIndex.Builder(new ManifestRowIdIndex.Settings(options), header);
        assertThat(builder.serialize("m", 1, 2)).isNull();
    }

    @Test
    void corruptMissingIncompleteAndMismatchedIndexesFallback() throws Exception {
        Path manifest = new Path(temp.toString(), "manifest-golden");
        java.nio.file.Path index = temp.resolve("manifest-golden" + ManifestRowIdIndex.SUFFIX);
        ManifestFileMeta meta = goldenMeta();
        RowRangeIndex query = RowRangeIndex.create(Collections.singletonList(new Range(11, 11)));

        assertThat(ManifestRowIdIndex.read(LocalFileIO.create(), manifest, meta, query, settings))
                .isNull();
        byte[] good = golden();
        for (int position : new int[] {0, 9, 11, 15, 16, 55, 63, 67, 75, good.length - 1}) {
            byte[] bad = good.clone();
            bad[position] ^= 2;
            Files.write(index, bad);
            assertThat(
                            ManifestRowIdIndex.read(
                                    LocalFileIO.create(), manifest, meta, query, settings))
                    .isNull();
        }
        // Self-consistent checksum cannot turn an unsupported or incomplete envelope into an index.
        for (int position : new int[] {9, 11, 15}) {
            byte[] bad = good.clone();
            bad[position] = 0;
            byte[] hash =
                    MessageDigest.getInstance("SHA-256")
                            .digest(Arrays.copyOf(bad, bad.length - 32));
            System.arraycopy(hash, 0, bad, bad.length - 32, 32);
            assertThatThrownBy(() -> ManifestRowIdIndex.select(bad, meta, query, settings))
                    .isInstanceOf(IOException.class);
        }
        Files.write(index, Arrays.copyOf(good, good.length - 1));
        assertThat(ManifestRowIdIndex.read(LocalFileIO.create(), manifest, meta, query, settings))
                .isNull();
        Files.write(index, good);
        assertThat(
                        ManifestRowIdIndex.read(
                                        LocalFileIO.create(), manifest, meta, query, settings)
                                .blocks())
                .isEmpty();
        assertThatThrownBy(
                        () ->
                                ManifestRowIdIndex.select(
                                        good, meta("other", meta.fileSize(), 7), query, settings))
                .isInstanceOf(IOException.class);
    }

    @Test
    void ioTimeoutFallsBackButInterruptionAndFatalErrorsPropagate() {
        ManifestFileMeta manifest = meta("m", 1, 1);
        RowRangeIndex query = RowRangeIndex.create(Collections.singletonList(new Range(1, 1)));
        Path path = new Path(temp.toString(), "m");

        LocalFileIO timedOut =
                new LocalFileIO() {
                    @Override
                    public org.apache.paimon.fs.SeekableInputStream newInputStream(Path path)
                            throws IOException {
                        throw new java.net.SocketTimeoutException("timeout");
                    }
                };
        assertThat(ManifestRowIdIndex.read(timedOut, path, manifest, query, settings)).isNull();
        assertThat(Thread.currentThread().isInterrupted()).isFalse();
        LocalFileIO interrupted =
                new LocalFileIO() {
                    @Override
                    public org.apache.paimon.fs.SeekableInputStream newInputStream(Path path)
                            throws IOException {
                        throw new java.io.InterruptedIOException("stop");
                    }
                };
        try {
            assertThatThrownBy(
                            () ->
                                    ManifestRowIdIndex.read(
                                            interrupted, path, manifest, query, settings))
                    .isInstanceOf(java.io.UncheckedIOException.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
        LocalFileIO failed =
                new LocalFileIO() {
                    @Override
                    public org.apache.paimon.fs.SeekableInputStream newInputStream(Path path) {
                        throw new AssertionError("fatal");
                    }
                };
        assertThatThrownBy(() -> ManifestRowIdIndex.read(failed, path, manifest, query, settings))
                .isInstanceOf(AssertionError.class);
    }

    @Test
    void suppressedCancellationInterruptionAndFatalErrorsPropagate() {
        ManifestFileMeta manifest = meta("m", 1, 1);
        RowRangeIndex query = RowRangeIndex.create(Collections.singletonList(new Range(1, 1)));
        for (Throwable closeFailure :
                Arrays.asList(
                        new CancellationException("cancelled"),
                        new java.io.InterruptedIOException("interrupted"),
                        new AssertionError("fatal"))) {
            AtomicBoolean closed = new AtomicBoolean();
            LocalFileIO fileIO =
                    new LocalFileIO() {
                        @Override
                        public org.apache.paimon.fs.SeekableInputStream newInputStream(Path path) {
                            return new ByteArraySeekableStream(new byte[0]) {
                                @Override
                                public int read(byte[] bytes, int offset, int length)
                                        throws IOException {
                                    throw new IOException("read failed");
                                }

                                @Override
                                public void close() throws IOException {
                                    super.close();
                                    closed.set(true);
                                    if (closeFailure instanceof IOException) {
                                        throw (IOException) closeFailure;
                                    }
                                    if (closeFailure instanceof Error) {
                                        throw (Error) closeFailure;
                                    }
                                    throw (RuntimeException) closeFailure;
                                }
                            };
                        }
                    };
            try {
                assertThatThrownBy(
                                () ->
                                        ManifestRowIdIndex.read(
                                                fileIO,
                                                new Path(temp.toString(), "m"),
                                                manifest,
                                                query,
                                                settings))
                        .isInstanceOf(
                                closeFailure instanceof java.io.InterruptedIOException
                                        ? java.io.UncheckedIOException.class
                                        : closeFailure.getClass());
                assertThat(Thread.currentThread().isInterrupted())
                        .isEqualTo(closeFailure instanceof java.io.InterruptedIOException);
                assertThat(closed).isTrue();
            } finally {
                Thread.interrupted();
            }
        }
    }

    @Test
    void ordinaryExceptionCyclesFallBack() {
        IOException first = new IOException("first");
        IOException second = new IOException("second");
        first.initCause(second);
        second.addSuppressed(first);
        LocalFileIO fileIO =
                new LocalFileIO() {
                    @Override
                    public org.apache.paimon.fs.SeekableInputStream newInputStream(Path path)
                            throws IOException {
                        throw first;
                    }
                };
        assertThat(
                        ManifestRowIdIndex.read(
                                fileIO,
                                new Path(temp.toString(), "m"),
                                meta("m", 1, 1),
                                RowRangeIndex.create(Collections.singletonList(new Range(1, 1))),
                                settings))
                .isNull();
        assertThat(Thread.currentThread().isInterrupted()).isFalse();
    }

    @Test
    void indexReadsUseBoundedBulkRequests() throws Exception {
        byte[] header = header();
        for (int blockCount : new int[] {5000, 25000}) {
            ManifestRowIdIndex.Builder builder = new ManifestRowIdIndex.Builder(settings, header);
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
            when(io.newInputStream(ManifestRowIdIndex.path(path))).thenReturn(stream);
            ManifestRowIdIndex.Selection actual =
                    ManifestRowIdIndex.read(
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
        Options options = new Options();
        options.set(CoreOptions.MANIFEST_ROW_ID_INDEX_MAX_BYTES, data.length);
        Path path = new Path(temp.toString(), "manifest-golden");
        for (int maxRead : new int[] {Integer.MAX_VALUE, 7}) {
            CountingInput stream = new CountingInput(data, maxRead);
            FileIO io = mock(FileIO.class);
            when(io.newInputStream(ManifestRowIdIndex.path(path))).thenReturn(stream);
            ManifestRowIdIndex.Selection actual =
                    ManifestRowIdIndex.read(
                            io,
                            path,
                            goldenMeta(),
                            RowRangeIndex.create(Collections.singletonList(new Range(20, 20))),
                            new ManifestRowIdIndex.Settings(options));
            assertThat(actual.blocks())
                    .extracting(block -> block.firstRecord)
                    .containsExactly(0L, 5L);
            assertThat(stream.closed).isTrue();
        }
    }

    @Test
    void indexOverBudgetStopsAfterOneExtraByte() throws Exception {
        Options options = new Options();
        options.set(CoreOptions.MANIFEST_ROW_ID_INDEX_MAX_BYTES, 128);
        Path path = new Path(temp.toString(), "manifest-golden");
        CountingInput stream = new CountingInput(golden(), Integer.MAX_VALUE);
        FileIO io = mock(FileIO.class);
        when(io.newInputStream(ManifestRowIdIndex.path(path))).thenReturn(stream);
        assertThat(
                        ManifestRowIdIndex.read(
                                io,
                                path,
                                goldenMeta(),
                                RowRangeIndex.create(Collections.singletonList(new Range(20, 20))),
                                new ManifestRowIdIndex.Settings(options)))
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
        ManifestRowIdIndex.Selection selected =
                ManifestRowIdIndex.select(
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
        try (InputStream input = ManifestRowIdIndex.openManifest(io, path, selected)) {
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
                    ManifestRowIdIndex.openManifest(
                            io, path, select(golden(), goldenMeta(), point))) {
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
            }
            assertThat(stream.closed).isTrue();
        }
    }

    @Test
    void largeBlockSpansUseBoundedReads() throws Exception {
        byte[] header = header();
        ManifestRowIdIndex.Builder builder = new ManifestRowIdIndex.Builder(settings, header);
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
                ManifestRowIdIndex.openManifest(
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
        ManifestRowIdIndex.Selection selected =
                ManifestRowIdIndex.select(
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
            try (InputStream input = ManifestRowIdIndex.openManifest(io, path, selected)) {
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

    private ManifestRowIdIndex.Selection select(byte[] data, ManifestFileMeta meta, long point)
            throws IOException {
        return ManifestRowIdIndex.select(
                data,
                meta,
                RowRangeIndex.create(Collections.singletonList(new Range(point, point))),
                settings);
    }
}
