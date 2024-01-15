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

package org.apache.paimon.utils;

import org.apache.paimon.data.RandomAccessInputView;
import org.apache.paimon.data.SimpleCollectingOutputView;
import org.apache.paimon.data.serializer.Serializer;
import org.apache.paimon.memory.ArraySegmentPool;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.reader.RecordReader;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * A class to help parallel execution.
 *
 * @param <T> Record Type.
 * @param <E> Extra message of one {@link RecordReader}.
 */
public class ParallelExecution<T, E> implements Closeable {

    private final Serializer<T> serializer;
    private final BlockingQueue<MemorySegment> idlePages;
    private final BlockingQueue<ParallelBatch<T, E>> results;
    private final ExecutorService executorService;

    private final AtomicReference<Throwable> exception;

    private final CountDownLatch latch;

    public ParallelExecution(
            Serializer<T> serializer,
            int pageSize,
            int parallelism,
            List<Supplier<Pair<RecordReader<T>, E>>> readers) {
        this.serializer = serializer;
        int totalPages = parallelism * 2;
        this.idlePages = new ArrayBlockingQueue<>(totalPages);
        for (int i = 0; i < totalPages; i++) {
            idlePages.add(MemorySegment.allocateHeapMemory(pageSize));
        }
        this.executorService =
                new ThreadPoolExecutor(
                        parallelism,
                        parallelism,
                        1,
                        TimeUnit.SECONDS,
                        new LinkedBlockingQueue<>(),
                        new ExecutorThreadFactory(Thread.currentThread().getName() + "-parallel"));
        this.results = new LinkedBlockingQueue<>();
        this.exception = new AtomicReference<>();
        this.latch = new CountDownLatch(readers.size());

        for (Supplier<Pair<RecordReader<T>, E>> readerSupplier : readers) {
            Serializer<T> duplicate = this.serializer.duplicate();
            executorService.submit(() -> asyncRead(readerSupplier, duplicate));
        }
    }

    @Nullable
    public ParallelBatch<T, E> take() throws InterruptedException, IOException {
        ParallelBatch<T, E> element;
        do {
            if (latch.getCount() == 0 && results.isEmpty()) {
                return null;
            }

            element = results.poll(2, TimeUnit.SECONDS);

            if (exception.get() != null) {
                throw new IOException(exception.get());
            }
        } while (element == null);
        return element;
    }

    private void asyncRead(
            Supplier<Pair<RecordReader<T>, E>> readerSupplier, Serializer<T> serializer) {
        Pair<RecordReader<T>, E> pair = readerSupplier.get();
        try (CloseableIterator<T> iterator = pair.getLeft().toCloseableIterator()) {
            int count = 0;
            SimpleCollectingOutputView outputView = null;

            while (iterator.hasNext()) {
                T next = iterator.next();

                while (true) {
                    if (outputView == null) {
                        outputView = newOutputView();
                        count = 0;
                    }

                    try {
                        serializer.serialize(next, outputView);
                        count++;
                        break;
                    } catch (EOFException e) {
                        sendToResults(outputView, count, pair.getRight());
                        outputView = null;
                    }
                }
            }

            if (outputView != null) {
                sendToResults(outputView, count, pair.getRight());
            }

            latch.countDown();
        } catch (Throwable e) {
            this.exception.set(e);
        }
    }

    private SimpleCollectingOutputView newOutputView() throws InterruptedException {
        MemorySegment page = idlePages.take();
        return new SimpleCollectingOutputView(
                new ArraySegmentPool(Collections.singletonList(page)), page.size());
    }

    private void sendToResults(SimpleCollectingOutputView outputView, int count, E extraMessage) {
        results.add(iterator(outputView.getCurrentSegment(), count, extraMessage));
    }

    @Override
    public void close() throws IOException {
        this.executorService.shutdownNow();
    }

    private ParallelBatch<T, E> iterator(MemorySegment page, int numRecords, E extraMessage) {
        RandomAccessInputView inputView =
                new RandomAccessInputView(
                        new ArrayList<>(Collections.singletonList(page)), page.size());
        return new ParallelBatch<T, E>() {

            int numReturn = 0;

            @Nullable
            @Override
            public T next() throws IOException {
                if (numReturn >= numRecords) {
                    return null;
                }

                numReturn++;
                return serializer.deserialize(inputView);
            }

            @Override
            public void releaseBatch() {
                idlePages.add(page);
            }

            @Override
            public E extraMessage() {
                return extraMessage;
            }
        };
    }

    /** A batch provides next and extra message. */
    public interface ParallelBatch<T, E> {

        @Nullable
        T next() throws IOException;

        void releaseBatch();

        E extraMessage();
    }
}
