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

import org.apache.paimon.reader.RecordReader;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/** A {@link RecordReader} to use ASYNC_EXECUTOR to read records async. */
public class AsyncRecordReader<T> implements RecordReader<T> {

    private static final ExecutorService ASYNC_EXECUTOR =
            Executors.newCachedThreadPool(new ExecutorThreadFactory("paimon-reader-async-thread"));

    private final BlockingQueue<Element> queue;
    private final Future<Void> future;

    private boolean isEnd = false;

    public AsyncRecordReader(IOExceptionSupplier<RecordReader<T>> supplier) {
        this.queue = new LinkedBlockingQueue<>();
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        this.future = ASYNC_EXECUTOR.submit(() -> asyncRead(supplier, classLoader));
    }

    private Void asyncRead(IOExceptionSupplier<RecordReader<T>> supplier, ClassLoader classLoader)
            throws IOException {
        // set classloader, otherwise, its classloader belongs to its creator. It is possible that
        // its creator's classloader has already exited, which will cause subsequent reads to report
        // exceptions
        Thread.currentThread().setContextClassLoader(classLoader);

        try (RecordReader<T> reader = supplier.get()) {
            while (true) {
                RecordIterator<T> batch = reader.readBatch();
                if (batch == null) {
                    queue.add(new Element(true, null));
                    return null;
                }

                queue.add(new Element(false, batch));
            }
        }
    }

    @Nullable
    @Override
    public RecordIterator<T> readBatch() throws IOException {
        if (isEnd) {
            return null;
        }

        try {
            Element element;
            do {
                element = queue.poll(2, TimeUnit.SECONDS);
                checkException();
            } while (element == null);

            if (element.isEnd) {
                isEnd = true;
                return null;
            }

            return element.batch;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }

    private void checkException() throws IOException, InterruptedException {
        if (future.isDone()) {
            try {
                future.get();
            } catch (ExecutionException e) {
                throw new IOException(e.getCause());
            }
        }
    }

    @Override
    public void close() throws IOException {
        future.cancel(true);
    }

    private class Element {

        private final boolean isEnd;
        private final RecordIterator<T> batch;

        private Element(boolean isEnd, RecordIterator<T> batch) {
            this.isEnd = isEnd;
            this.batch = batch;
        }
    }
}
