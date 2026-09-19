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

package org.apache.paimon.format.parquet;

import org.apache.paimon.fs.FileRange;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.VectoredReadable;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.utils.IOUtils;

import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for bounded per-reader tail caching and positioned I/O delegation. */
class ParquetTailInputStreamTest {

    @Test
    void testTailAndMetadataShareOneRead() throws Exception {
        CountingInputStream delegate = new CountingInputStream(400_000);
        try (ParquetTailInputStream in =
                new ParquetTailInputStream(delegate, delegate.data.length)) {
            assertThat(delegate.requests).isEmpty();
            in.seek(delegate.data.length - 8);
            byte[] trailer = new byte[8];
            IOUtils.readFully(in, trailer);
            assertThat(trailer)
                    .containsExactly(
                            Arrays.copyOfRange(
                                    delegate.data, delegate.data.length - 8, delegate.data.length));
            in.seek(delegate.data.length - 100_000);
            byte[] metadata = new byte[4096];
            IOUtils.readFully(in, metadata);
            assertThat(metadata)
                    .containsExactly(
                            Arrays.copyOfRange(
                                    delegate.data,
                                    delegate.data.length - 100_000,
                                    delegate.data.length - 100_000 + 4096));
            assertThat(delegate.requests).hasSize(1);
            assertThat(delegate.requests.get(0))
                    .containsExactly(delegate.data.length - 128 * 1024, 128 * 1024);
        }
        assertThat(delegate.closeCount).isEqualTo(1);
    }

    @Test
    void testLargeMetadataFallsThroughAndCrossesTailBoundary() throws Exception {
        CountingInputStream delegate = new CountingInputStream(400_000);
        try (ParquetTailInputStream in =
                new ParquetTailInputStream(delegate, delegate.data.length)) {
            in.seek(delegate.data.length - 8);
            in.read();
            in.seek(0);
            byte[] all = new byte[delegate.data.length];
            IOUtils.readFully(in, all);
            assertThat(all).containsExactly(delegate.data);
            assertThat(in.getPos()).isEqualTo(delegate.data.length);
            assertThat(in.read()).isEqualTo(-1);
            assertThat(in.read(all, 0, 0)).isZero();
        }
    }

    @Test
    void testVectoredReadsKeepNativeDelegateAndUseCachedTail() throws Exception {
        CountingInputStream delegate = new CountingInputStream(400_000);
        try (ParquetTailInputStream in =
                new ParquetTailInputStream(delegate, delegate.data.length)) {
            in.seek(399_999);
            in.read();
            FileRange uncached = FileRange.createFileRange(100, 32);
            FileRange cached = FileRange.createFileRange(399_000, 64);
            in.readVectored(Arrays.asList(cached, uncached));
            assertThat(uncached.getData().get(5, TimeUnit.SECONDS))
                    .containsExactly(Arrays.copyOfRange(delegate.data, 100, 132));
            assertThat(cached.getData().get(5, TimeUnit.SECONDS))
                    .containsExactly(Arrays.copyOfRange(delegate.data, 399_000, 399_064));
            assertThat(delegate.vectoredCalls).isEqualTo(1);
            assertThat(delegate.requests).hasSize(2);
            assertThat(in.getPos()).isEqualTo(400_000);
            in.readVectored(Collections.emptyList());
            FileRange empty = FileRange.createFileRange(400_000, 0);
            in.readVectored(Collections.singletonList(empty));
            assertThat(empty.getData().get()).isEmpty();
            assertThatThrownBy(
                            () ->
                                    in.readVectored(
                                            Arrays.asList(
                                                    FileRange.createFileRange(399_000, 20),
                                                    FileRange.createFileRange(399_010, 20))))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void testPerStreamCacheAndClosedState() throws Exception {
        CountingInputStream first = new CountingInputStream(10);
        CountingInputStream second = new CountingInputStream(10);
        second.data[9] = 99;
        ParquetTailInputStream a = new ParquetTailInputStream(first, 10);
        ParquetTailInputStream b = new ParquetTailInputStream(second, 10);
        a.seek(9);
        b.seek(9);
        assertThat(a.read()).isNotEqualTo(b.read());
        a.close();
        a.close();
        assertThat(first.closeCount).isEqualTo(1);
        assertThatThrownBy(a::read).isInstanceOf(IOException.class);
        assertThatThrownBy(() -> a.pread(9, new byte[1], 0, 1)).isInstanceOf(IOException.class);
        assertThatThrownBy(() -> b.seek(11)).isInstanceOf(EOFException.class);
        b.close();
    }

    @Test
    void testCachedRangesFillCallerProvidedBuffers() throws Exception {
        CountingInputStream delegate = new CountingInputStream(400_000);
        try (ParquetTailInputStream in =
                new ParquetTailInputStream(delegate, delegate.data.length)) {
            byte[] supplied = new byte[64];
            byte[] empty = new byte[0];
            FileRange cached = FileRange.createFileRange(399_000, supplied);
            FileRange zeroLength = FileRange.createFileRange(400_000, empty);
            in.readVectored(Arrays.asList(cached, zeroLength));
            assertThat(cached.getData().get(5, TimeUnit.SECONDS)).isSameAs(supplied);
            assertThat(supplied)
                    .containsExactly(Arrays.copyOfRange(delegate.data, 399_000, 399_064));
            assertThat(zeroLength.getData().get(5, TimeUnit.SECONDS)).isSameAs(empty);
            assertThat(delegate.requests).hasSize(1);
            assertThat(delegate.vectoredCalls).isZero();
        }
    }

    @Test
    void testCorruptFooterStillClosesInput() throws Exception {
        CountingInputStream delegate = new CountingInputStream(400_000);
        LocalFileIO fileIO =
                new LocalFileIO() {
                    @Override
                    public boolean isObjectStore() {
                        return true;
                    }

                    @Override
                    public SeekableInputStream newInputStream(Path path, long fileSize) {
                        assertThat(fileSize).isEqualTo(delegate.data.length);
                        return delegate;
                    }
                };
        ParquetInputFile file =
                ParquetInputFile.fromPath(fileIO, new Path("/file.parquet"), delegate.data.length);
        ParquetInputStream stream = file.newStream();
        assertThatThrownBy(
                        () ->
                                ParquetFileReader.readFooter(
                                        file, ParquetReadOptions.builder().build(), stream, true))
                .isInstanceOf(RuntimeException.class);
        assertThat(delegate.closeCount).isEqualTo(1);
    }

    private static class CountingInputStream extends SeekableInputStream
            implements VectoredReadable {
        final byte[] data;
        final List<long[]> requests = new ArrayList<>();
        long position;
        int vectoredCalls;
        int closeCount;

        CountingInputStream(int length) {
            data = new byte[length];
            for (int i = 0; i < length; i++) {
                data[i] = (byte) (i * 31);
            }
        }

        @Override
        public void seek(long desired) {
            position = desired;
        }

        @Override
        public long getPos() {
            return position;
        }

        @Override
        public int read() {
            return position == data.length ? -1 : data[(int) position++] & 0xff;
        }

        @Override
        public int read(byte[] bytes, int offset, int length) {
            int count = pread(position, bytes, offset, length);
            if (count > 0) {
                position += count;
            }
            return count;
        }

        @Override
        public int pread(long start, byte[] bytes, int offset, int length) {
            requests.add(new long[] {start, length});
            int count = (int) Math.min(length, data.length - start);
            if (count <= 0) {
                return length == 0 ? 0 : -1;
            }
            System.arraycopy(data, (int) start, bytes, offset, count);
            return count;
        }

        @Override
        public void readVectored(List<? extends FileRange> ranges) {
            vectoredCalls++;
            for (FileRange range : ranges) {
                byte[] bytes = new byte[range.getLength()];
                pread(range.getOffset(), bytes, 0, bytes.length);
                range.getData().complete(bytes);
            }
        }

        @Override
        public void close() {
            closeCount++;
        }
    }
}
