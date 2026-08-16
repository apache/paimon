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

package org.apache.paimon.vector.index;

import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.VectoredReadable;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link NativeVectorGlobalIndexReader.SeekableStreamVectorIndexInput}. */
public class SeekableStreamVectorIndexInputTest {

    @Test
    public void testVectoredReadableInputUsesParallelPositionReads() throws Exception {
        byte[] data = data(128 * 1024);
        TestVectoredSeekableInputStream input = new TestVectoredSeekableInputStream(data, 2);
        NativeVectorGlobalIndexReader.SeekableStreamVectorIndexInput indexInput =
                new NativeVectorGlobalIndexReader.SeekableStreamVectorIndexInput(input);

        byte[][] buffers = new byte[][] {new byte[64], new byte[64]};
        indexInput.pread(new long[] {0, 32 * 1024}, buffers);

        assertThat(buffers[0]).isEqualTo(slice(data, 0, 64));
        assertThat(buffers[1]).isEqualTo(slice(data, 32 * 1024, 64));
        assertThat(input.positionReads).hasValue(2);
        assertThat(input.sequentialReads).hasValue(0);
        assertThat(input.maxActiveReads).hasValue(2);
        assertThat(input.positionReadBuffers.stream().anyMatch(buffer -> buffer == buffers[0]))
                .isTrue();
        assertThat(input.positionReadBuffers.stream().anyMatch(buffer -> buffer == buffers[1]))
                .isTrue();
    }

    @Test
    public void testFallbackToSequentialReadWhenRangesOverlap() {
        byte[] data = data(1024);
        TestVectoredSeekableInputStream input = new TestVectoredSeekableInputStream(data, 0);
        NativeVectorGlobalIndexReader.SeekableStreamVectorIndexInput indexInput =
                new NativeVectorGlobalIndexReader.SeekableStreamVectorIndexInput(input);

        byte[][] buffers = new byte[][] {new byte[64], new byte[64]};
        indexInput.pread(new long[] {0, 32}, buffers);

        assertThat(buffers[0]).isEqualTo(slice(data, 0, 64));
        assertThat(buffers[1]).isEqualTo(slice(data, 32, 64));
        assertThat(input.positionReads).hasValue(0);
        assertThat(input.sequentialReads).hasValue(2);
    }

    private static byte[] data(int length) {
        byte[] data = new byte[length];
        for (int i = 0; i < length; i++) {
            data[i] = (byte) i;
        }
        return data;
    }

    private static byte[] slice(byte[] data, int offset, int length) {
        byte[] expected = new byte[length];
        System.arraycopy(data, offset, expected, 0, length);
        return expected;
    }

    private static class TestVectoredSeekableInputStream extends SeekableInputStream
            implements VectoredReadable {

        private final byte[] data;
        private final CountDownLatch readsStarted;
        private final CountDownLatch finishReads = new CountDownLatch(1);
        private final AtomicInteger activeReads = new AtomicInteger();
        private final AtomicInteger positionReads = new AtomicInteger();
        private final AtomicInteger sequentialReads = new AtomicInteger();
        private final AtomicInteger maxActiveReads = new AtomicInteger();
        private final CopyOnWriteArrayList<byte[]> positionReadBuffers =
                new CopyOnWriteArrayList<>();

        private int position;

        private TestVectoredSeekableInputStream(byte[] data, int expectedPositionReads) {
            this.data = data;
            this.readsStarted = new CountDownLatch(expectedPositionReads);
            if (expectedPositionReads == 0) {
                finishReads.countDown();
            }
        }

        @Override
        public void seek(long desired) {
            position = (int) desired;
        }

        @Override
        public long getPos() {
            return position;
        }

        @Override
        public int read() {
            return data[position++];
        }

        @Override
        public int read(byte[] buffer, int offset, int length) {
            System.arraycopy(data, position, buffer, offset, length);
            position += length;
            sequentialReads.incrementAndGet();
            return length;
        }

        @Override
        public void close() {}

        @Override
        public int pread(long position, byte[] buffer, int offset, int length) throws IOException {
            positionReadBuffers.add(buffer);
            int active = activeReads.incrementAndGet();
            maxActiveReads.accumulateAndGet(active, Math::max);
            readsStarted.countDown();
            try {
                if (!readsStarted.await(5, TimeUnit.SECONDS)) {
                    throw new IOException("Timed out waiting for parallel vector index reads");
                }
                finishReads.countDown();
                System.arraycopy(data, (int) position, buffer, offset, length);
                positionReads.incrementAndGet();
                return length;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            } finally {
                activeReads.decrementAndGet();
            }
        }
    }
}
