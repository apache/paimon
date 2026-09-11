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
        public void close() {}
    }
}
