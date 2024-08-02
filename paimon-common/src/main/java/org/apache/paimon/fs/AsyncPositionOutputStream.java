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

package org.apache.paimon.fs;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.paimon.utils.ThreadUtils.newDaemonThreadFactory;

/** A {@link PositionOutputStream} which uses a async thread to write data. */
public class AsyncPositionOutputStream extends PositionOutputStream {

    public static final ExecutorService EXECUTOR_SERVICE =
            Executors.newCachedThreadPool(newDaemonThreadFactory("AsyncOutputStream"));

    private final PositionOutputStream out;
    private final LinkedBlockingQueue<AsyncEvent> eventQueue;
    private final AtomicReference<Throwable> exception;
    private final Future<?> future;

    private volatile boolean isClosed;
    private long position;

    public AsyncPositionOutputStream(PositionOutputStream out) {
        this.out = out;
        this.eventQueue = new LinkedBlockingQueue<>();
        this.exception = new AtomicReference<>();
        this.isClosed = false;
        this.position = 0;
        this.future = EXECUTOR_SERVICE.submit(this::execute);
    }

    private void execute() {
        try {
            doWork();
        } catch (Throwable e) {
            exception.set(e);
            throw new RuntimeException(e);
        }
    }

    private void doWork() throws InterruptedException, IOException {
        try {
            while (!isClosed) {
                AsyncEvent event = eventQueue.poll(1, TimeUnit.SECONDS);
                if (event == null) {
                    continue;
                }
                if (event instanceof EndEvent) {
                    return;
                }
                if (event instanceof DataEvent) {
                    DataEvent dataEvent = (DataEvent) event;
                    out.write(dataEvent.data);
                }
                if (event instanceof FlushEvent) {
                    out.flush();
                }
            }
        } finally {
            out.close();
        }
    }

    @Override
    public long getPos() throws IOException {
        checkException();
        return position;
    }

    @Override
    public void write(int b) throws IOException {
        checkException();
        position++;
        putEvent(new DataEvent(new byte[] {(byte) b}, 0, 1));
    }

    @Override
    public void write(byte[] b) throws IOException {
        checkException();
        position += b.length;
        putEvent(new DataEvent(b, 0, b.length));
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        checkException();
        position += len;
        putEvent(new DataEvent(b, off, len));
    }

    @Override
    public void flush() throws IOException {
        checkException();
        putEvent(new FlushEvent());
    }

    @Override
    public void close() throws IOException {
        checkException();
        putEvent(new EndEvent());
        try {
            this.future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } finally {
            isClosed = true;
        }
    }

    private void putEvent(AsyncEvent event) {
        try {
            eventQueue.put(event);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private void checkException() throws IOException {
        Throwable throwable = exception.get();
        if (throwable != null) {
            if (throwable instanceof IOException) {
                throw (IOException) throwable;
            }
            throw new IOException(throwable);
        }
    }

    private interface AsyncEvent {}

    private static class DataEvent implements AsyncEvent {

        private final byte[] data;

        public DataEvent(byte[] input, int offset, int length) {
            byte[] data = new byte[length];
            System.arraycopy(input, offset, data, 0, length);
            this.data = data;
        }
    }

    private static class FlushEvent implements AsyncEvent {}

    private static class EndEvent implements AsyncEvent {}
}
