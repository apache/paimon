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

package org.apache.paimon.format.mosaic;

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the stream pool of {@link MosaicInputFileAdapter}. */
class MosaicInputFileAdapterTest {

    @Test
    void testConcurrentReadsAreCappedAtMaxStreams() throws Exception {
        CountDownLatch readsStarted = new CountDownLatch(2);
        CountDownLatch releaseReads = new CountDownLatch(1);
        AtomicInteger opened = new AtomicInteger();
        LocalFileIO fileIO =
                new LocalFileIO() {
                    @Override
                    public SeekableInputStream newInputStream(Path path) {
                        opened.incrementAndGet();
                        return new BlockingStream(readsStarted, releaseReads);
                    }
                };
        MosaicInputFileAdapter adapter =
                new MosaicInputFileAdapter(fileIO, new Path("file:/tmp/mosaic-adapter-test"), 2);

        List<Thread> readers = new ArrayList<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        for (int i = 0; i < 3; i++) {
            Thread thread =
                    new Thread(
                            () -> {
                                try {
                                    adapter.readFully(0, new byte[4], 0, 4);
                                } catch (Throwable t) {
                                    failure.set(t);
                                }
                            });
            thread.start();
            readers.add(thread);
        }
        readsStarted.await();
        // Two reads hold the two streams; the third must wait instead of opening another one.
        readers.get(0).join(200);
        assertThat(opened.get()).isEqualTo(2);
        assertThat(readers.stream().filter(Thread::isAlive).count()).isEqualTo(3);

        releaseReads.countDown();
        for (Thread thread : readers) {
            thread.join();
        }
        assertThat(failure.get()).isNull();
        assertThat(opened.get()).isEqualTo(2);
        adapter.close();
    }

    @Test
    void testFailedExtraStreamOpenReleasesItsSlot() throws Exception {
        CountDownLatch readsStarted = new CountDownLatch(1);
        CountDownLatch releaseReads = new CountDownLatch(1);
        CountDownLatch thirdOpen = new CountDownLatch(1);
        AtomicInteger opens = new AtomicInteger();
        LocalFileIO fileIO =
                new LocalFileIO() {
                    @Override
                    public SeekableInputStream newInputStream(Path path) throws IOException {
                        // The second open (the first extra stream) fails once.
                        int open = opens.incrementAndGet();
                        if (open == 2) {
                            throw new IOException("open failed");
                        }
                        if (open == 3) {
                            thirdOpen.countDown();
                        }
                        return new BlockingStream(readsStarted, releaseReads);
                    }
                };
        MosaicInputFileAdapter adapter =
                new MosaicInputFileAdapter(fileIO, new Path("file:/tmp/mosaic-adapter-test"), 2);
        AtomicReference<Throwable> holderFailure = new AtomicReference<>();
        Thread holder = new Thread(() -> read(adapter, holderFailure));
        holder.start();
        readsStarted.await();

        // The failed open must not keep the second slot reserved.
        AtomicReference<Throwable> failed = new AtomicReference<>();
        read(adapter, failed);
        assertThat(failed.get()).isInstanceOf(IOException.class).hasMessage("open failed");
        CountDownLatch secondRead = new CountDownLatch(1);
        AtomicReference<Throwable> retryFailure = new AtomicReference<>();
        Thread retry =
                new Thread(
                        () -> {
                            read(adapter, retryFailure);
                            secondRead.countDown();
                        });
        retry.start();
        // The retry must open its own stream while the first one is still held.
        thirdOpen.await();
        releaseReads.countDown();
        retry.join();
        holder.join();
        assertThat(retryFailure.get()).isNull();
        assertThat(holderFailure.get()).isNull();
        assertThat(opens.get()).isEqualTo(3);
        adapter.close();
    }

    @Test
    void testCloseClosesEveryStreamAndRejectsLaterReads() throws Exception {
        CountDownLatch readsStarted = new CountDownLatch(2);
        CountDownLatch releaseReads = new CountDownLatch(1);
        List<BlockingStream> streams = new ArrayList<>();
        LocalFileIO fileIO =
                new LocalFileIO() {
                    @Override
                    public SeekableInputStream newInputStream(Path path) {
                        BlockingStream stream = new BlockingStream(readsStarted, releaseReads);
                        synchronized (streams) {
                            streams.add(stream);
                        }
                        return stream;
                    }
                };
        MosaicInputFileAdapter adapter =
                new MosaicInputFileAdapter(fileIO, new Path("file:/tmp/mosaic-adapter-test"), 3);
        List<Thread> readers = new ArrayList<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        for (int i = 0; i < 2; i++) {
            Thread thread = new Thread(() -> read(adapter, failure));
            thread.start();
            readers.add(thread);
        }
        readsStarted.await();
        releaseReads.countDown();
        for (Thread thread : readers) {
            thread.join();
        }
        assertThat(failure.get()).isNull();
        assertThat(streams).hasSize(2);

        adapter.close();
        assertThat(streams).allMatch(stream -> stream.closeCount == 1);
        AtomicReference<Throwable> afterClose = new AtomicReference<>();
        read(adapter, afterClose);
        assertThat(afterClose.get()).isInstanceOf(IOException.class);
        // Closing again is a no-op.
        adapter.close();
        assertThat(streams).allMatch(stream -> stream.closeCount == 1);
    }

    @Test
    void testCloseWakesWaitingReader() throws Exception {
        CountDownLatch readsStarted = new CountDownLatch(1);
        CountDownLatch releaseReads = new CountDownLatch(1);
        LocalFileIO fileIO =
                new LocalFileIO() {
                    @Override
                    public SeekableInputStream newInputStream(Path path) {
                        return new BlockingStream(readsStarted, releaseReads);
                    }
                };
        MosaicInputFileAdapter adapter =
                new MosaicInputFileAdapter(fileIO, new Path("file:/tmp/mosaic-adapter-test"), 1);
        AtomicReference<Throwable> first = new AtomicReference<>();
        AtomicReference<Throwable> second = new AtomicReference<>();
        Thread holder = new Thread(() -> read(adapter, first));
        holder.start();
        readsStarted.await();
        Thread waiter = new Thread(() -> read(adapter, second));
        waiter.start();
        waiter.join(200);
        assertThat(waiter.isAlive()).isTrue();

        adapter.close();
        waiter.join();
        assertThat(second.get()).isInstanceOf(IOException.class);
        releaseReads.countDown();
        holder.join();
        // The read that held the stream completes; the stream is closed by close().
        assertThat(first.get()).isNull();
    }

    private static void read(MosaicInputFileAdapter adapter, AtomicReference<Throwable> failure) {
        try {
            adapter.readFully(0, new byte[4], 0, 4);
        } catch (Throwable t) {
            failure.set(t);
        }
    }

    /** A stream whose reads block until released, to hold a pooled stream busy. */
    private static class BlockingStream extends SeekableInputStream {

        private final CountDownLatch started;
        private final CountDownLatch release;

        private BlockingStream(CountDownLatch started, CountDownLatch release) {
            this.started = started;
            this.release = release;
        }

        @Override
        public void seek(long desired) {}

        @Override
        public long getPos() {
            return 0;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            started.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
            return len;
        }

        @Override
        public int read() {
            return -1;
        }

        @Override
        public void close() {
            closeCount++;
        }

        private volatile int closeCount;
    }
}
