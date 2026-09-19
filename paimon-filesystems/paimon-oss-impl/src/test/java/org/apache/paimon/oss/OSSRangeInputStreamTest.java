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

package org.apache.paimon.oss;

import org.apache.paimon.fs.FileRange;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.OSSObject;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/** Tests for exact OSS range reads, independent of an OSS service. */
class OSSRangeInputStreamTest {

    @Test
    void testKnownLengthOpenAndSeekIssueNoRequests() throws Exception {
        OSSClient client = mock(OSSClient.class);
        OSSFileIO fileIO =
                new OSSFileIO() {
                    @Override
                    OSSClient ossClient(Path path) {
                        return client;
                    }
                };
        try (SeekableInputStream in =
                fileIO.newInputStream(new Path("oss://bucket/a.parquet"), 99)) {
            assertThat(in).isInstanceOf(OSSRangeInputStream.class);
            in.seek(98);
            assertThat(in.getPos()).isEqualTo(98);
        }
        verifyNoInteractions(client);
    }

    @Test
    void testExactPositionedReadAndEndOfFile() throws Exception {
        byte[] data = data(100_000);
        AtomicInteger closed = new AtomicInteger();
        OSSClient client = client(data, closed);
        try (OSSRangeInputStream in =
                new OSSRangeInputStream(client, "bucket", "key", data.length, null)) {
            in.seek(123);
            byte[] result = new byte[8];
            assertThat(in.pread(data.length - 3, result, 2, 6)).isEqualTo(3);
            assertThat(Arrays.copyOfRange(result, 2, 5))
                    .containsExactly(Arrays.copyOfRange(data, data.length - 3, data.length));
            assertThat(in.getPos()).isEqualTo(123);
            assertThat(in.pread(data.length, result, 0, result.length)).isEqualTo(-1);
            assertThat(in.pread(data.length, result, 0, 0)).isZero();
            in.seek(data.length);
            assertThat(in.read()).isEqualTo(-1);
            assertThat(in.read(result, 0, 0)).isZero();
            assertThatThrownBy(() -> in.seek(-1)).isInstanceOf(EOFException.class);
            assertThatThrownBy(() -> in.seek(data.length + 1L)).isInstanceOf(EOFException.class);
        }
        ArgumentCaptor<GetObjectRequest> request = ArgumentCaptor.forClass(GetObjectRequest.class);
        verify(client).getObject(request.capture());
        assertThat(request.getValue().getRange()).containsExactly(data.length - 3, data.length - 1);
        assertThat(closed.get()).isEqualTo(1);
    }

    @Test
    void testBufferAndRetryAfterTruncatedRead() throws Exception {
        byte[] data = data(100_000);
        OSSClient client = client(data, new AtomicInteger());
        OSSObject shortObject = new OSSObject();
        shortObject.setObjectContent(new ByteArrayInputStream(new byte[3]));
        OSSObject fullObject = new OSSObject();
        fullObject.setObjectContent(new ByteArrayInputStream(Arrays.copyOf(data, 64 * 1024)));
        when(client.getObject(any(GetObjectRequest.class))).thenReturn(shortObject, fullObject);
        try (OSSRangeInputStream in =
                new OSSRangeInputStream(client, "bucket", "key", data.length, null)) {
            assertThatThrownBy(in::read).isInstanceOf(EOFException.class);
            assertThat(in.getPos()).isZero();
            assertThat(in.read()).isEqualTo(data[0] & 0xff);
            in.seek(9);
            assertThat(in.read()).isEqualTo(data[9] & 0xff);
        }
        verify(client, times(2)).getObject(any(GetObjectRequest.class));
    }

    @Test
    void testVectoredReadsAreBoundedAndDoNotMovePosition() throws Exception {
        byte[] data = data(2_000_000);
        AtomicInteger closed = new AtomicInteger();
        OSSClient client = client(data, closed);
        try (OSSRangeInputStream in =
                new OSSRangeInputStream(client, "bucket", "key", data.length, null)) {
            in.seek(17);
            List<FileRange> ranges =
                    Arrays.asList(
                            FileRange.createFileRange(1_200_000, 128),
                            FileRange.createFileRange(100, 256),
                            FileRange.createFileRange(600_000, 192));
            in.readVectored(ranges);
            for (FileRange range : ranges) {
                assertThat(range.getData().get(10, TimeUnit.SECONDS))
                        .containsExactly(
                                Arrays.copyOfRange(
                                        data,
                                        (int) range.getOffset(),
                                        (int) range.getOffset() + range.getLength()));
            }
            assertThat(in.getPos()).isEqualTo(17);
            in.readVectored(Collections.emptyList());
        }
        assertThat(closed.get()).isEqualTo(3);
        verify(client, times(3)).getObject(any(GetObjectRequest.class));
    }

    @Test
    void testFailureCompletesRangeExceptionally() throws Exception {
        OSSClient client = mock(OSSClient.class);
        when(client.getObject(any(GetObjectRequest.class)))
                .thenThrow(new IllegalStateException("failure"));
        try (OSSRangeInputStream in =
                new OSSRangeInputStream(client, "bucket", "key", 1024, null)) {
            FileRange range = FileRange.createFileRange(0, 10);
            in.readVectored(Collections.singletonList(range));
            assertThatThrownBy(() -> range.getData().get(10, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(IOException.class);
        }
    }

    @Test
    void testMissingObjectKeepsFileNotFoundSemantics() throws Exception {
        OSSClient client = mock(OSSClient.class);
        when(client.getObject(any(GetObjectRequest.class)))
                .thenThrow(
                        new OSSException(
                                "missing", "NoSuchKey", "request", "host", null, null, "GET"));
        try (OSSRangeInputStream in = new OSSRangeInputStream(client, "bucket", "key", 10, null)) {
            assertThatThrownBy(in::read).isInstanceOf(FileNotFoundException.class);
        }
    }

    @Test
    void testCloseReleasesAnActiveRequest() throws Exception {
        CountDownLatch reading = new CountDownLatch(1);
        CountDownLatch closed = new CountDownLatch(1);
        OSSObject object = new OSSObject();
        object.setObjectContent(
                new InputStream() {
                    @Override
                    public int read() throws IOException {
                        reading.countDown();
                        try {
                            if (!closed.await(10, TimeUnit.SECONDS)) {
                                throw new IOException("Close timed out");
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IOException(e);
                        }
                        throw new IOException("Closed");
                    }

                    @Override
                    public void close() {
                        closed.countDown();
                    }
                });
        OSSClient client = mock(OSSClient.class);
        when(client.getObject(any(GetObjectRequest.class))).thenReturn(object);
        OSSRangeInputStream in = new OSSRangeInputStream(client, "bucket", "key", 10, null);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Integer> read = executor.submit(() -> in.pread(0, new byte[1], 0, 1));
            assertThat(reading.await(10, TimeUnit.SECONDS)).isTrue();
            in.close();
            assertThatThrownBy(() -> read.get(10, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(IOException.class);
            assertThatThrownBy(in::read).isInstanceOf(IOException.class);
            in.close();
        } finally {
            in.close();
            executor.shutdownNow();
        }
    }

    private static byte[] data(int length) {
        byte[] data = new byte[length];
        for (int i = 0; i < length; i++) {
            data[i] = (byte) (i * 31);
        }
        return data;
    }

    @ParameterizedTest
    @ValueSource(strings = {"io", "runtime", "error"})
    void testCloseAttemptsAllRequestsWhenClosingThrows(String kind) throws Exception {
        Throwable failure =
                "io".equals(kind)
                        ? new IOException("close failure")
                        : "runtime".equals(kind)
                                ? new IllegalStateException("close failure")
                                : new AssertionError("close failure");
        CountDownLatch reading = new CountDownLatch(3);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger closed = new AtomicInteger();
        OSSClient client = mock(OSSClient.class);
        when(client.getObject(any(GetObjectRequest.class)))
                .thenAnswer(
                        invocation -> {
                            OSSObject object = new OSSObject();
                            object.setObjectContent(
                                    new InputStream() {
                                        private final AtomicBoolean attempted = new AtomicBoolean();

                                        @Override
                                        public int read() throws IOException {
                                            reading.countDown();
                                            try {
                                                if (!release.await(10, TimeUnit.SECONDS)) {
                                                    throw new IOException("Read timed out");
                                                }
                                            } catch (InterruptedException e) {
                                                Thread.currentThread().interrupt();
                                                throw new IOException(e);
                                            }
                                            throw new IOException("Read released");
                                        }

                                        @Override
                                        public void close() throws IOException {
                                            if (attempted.compareAndSet(false, true)) {
                                                closed.incrementAndGet();
                                                org.apache.paimon.utils.ExceptionUtils
                                                        .rethrowIOException(failure);
                                            }
                                        }
                                    });
                            return object;
                        });
        OSSRangeInputStream in = new OSSRangeInputStream(client, "bucket", "key", 10, null);
        ExecutorService executor = Executors.newFixedThreadPool(3);
        List<Future<Integer>> reads = new ArrayList<>();
        try {
            for (int i = 0; i < 3; i++) {
                reads.add(executor.submit(() -> in.pread(0, new byte[1], 0, 1)));
            }
            assertThat(reading.await(10, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(in::close).isSameAs(failure);
            assertThat(closed.get()).isEqualTo(3);
            in.close();
        } finally {
            release.countDown();
            executor.shutdown();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            in.close();
        }
        for (Future<Integer> read : reads) {
            assertThatThrownBy(() -> read.get(10, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(IOException.class);
        }
    }

    @Test
    void testRandomReadsAcrossBufferBoundaries() throws Exception {
        byte[] data = data(180_000);
        OSSClient client = client(data, new AtomicInteger());
        Random random = new Random(20260908);
        try (OSSRangeInputStream in =
                new OSSRangeInputStream(client, "bucket", "key", data.length, null)) {
            for (int i = 0; i < 500; i++) {
                int position = random.nextInt(data.length + 1);
                in.seek(position);
                byte[] bytes = new byte[random.nextInt(90_000)];
                int count = in.read(bytes, 0, bytes.length);
                if (count > 0) {
                    assertThat(Arrays.copyOf(bytes, count))
                            .containsExactly(Arrays.copyOfRange(data, position, position + count));
                    assertThat(in.getPos()).isEqualTo(position + count);
                } else {
                    assertThat(count).isEqualTo(bytes.length == 0 ? 0 : -1);
                }
                long saved = in.getPos();
                int offset = random.nextInt(data.length);
                byte[] other = new byte[31];
                int n = in.pread(offset, other, 0, other.length);
                assertThat(Arrays.copyOf(other, n))
                        .containsExactly(Arrays.copyOfRange(data, offset, offset + n));
                assertThat(in.getPos()).isEqualTo(saved);
            }
            assertThatThrownBy(
                            () ->
                                    in.readVectored(
                                            Collections.singletonList(
                                                    FileRange.createFileRange(data.length - 1, 2))))
                    .isInstanceOf(EOFException.class);
        }
    }

    private static OSSClient client(byte[] data, AtomicInteger closed) {
        OSSClient client = mock(OSSClient.class);
        when(client.getObject(any(GetObjectRequest.class)))
                .thenAnswer(
                        invocation -> {
                            long[] range =
                                    ((GetObjectRequest) invocation.getArgument(0)).getRange();
                            byte[] bytes =
                                    Arrays.copyOfRange(data, (int) range[0], (int) range[1] + 1);
                            OSSObject object = new OSSObject();
                            object.setObjectContent(
                                    new ByteArrayInputStream(bytes) {
                                        @Override
                                        public void close() {
                                            closed.incrementAndGet();
                                        }
                                    });
                            return object;
                        });
        return client;
    }

    @Test
    void testHadoopStatisticsCountLogicalReadsWithoutDoubleCountingBufferFills() throws Exception {
        byte[] data = data(100_000);
        OSSClient client = client(data, new AtomicInteger());
        FileSystem.Statistics statistics = new FileSystem.Statistics("oss");
        try (OSSRangeInputStream in =
                new OSSRangeInputStream(client, "bucket", "key", data.length, statistics)) {
            in.read();
            in.read(new byte[10]);
            assertThat(statistics.getReadOps()).isEqualTo(1);
            assertThat(statistics.getBytesRead()).isEqualTo(11);
            in.pread(5000, new byte[10], 0, 10);
            in.seek(90_000);
            in.read(new byte[10_000]);
            assertThat(statistics.getReadOps()).isEqualTo(3);
            assertThat(statistics.getBytesRead()).isEqualTo(10_021);
        }
    }
}
