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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.utils.FixLenByteArrayOutputStream;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
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

    public static final int AWAIT_TIMEOUT_SECONDS = 10;
    public static final int BUFFER_SIZE = 1024 * 64;
    public static final int MAX_BUFFER = 1024;

    private final PositionOutputStream out;
    private final FixLenByteArrayOutputStream buffer;
    private final LinkedBlockingQueue<byte[]> bufferQueue;
    private final LinkedBlockingQueue<AsyncEvent> eventQueue;
    private final AtomicReference<Throwable> exception;
    private final Future<?> future;

    private int totalBuffers;
    private long position;
    private boolean closed = false;

    public AsyncPositionOutputStream(PositionOutputStream out) {
        this.out = out;
        this.bufferQueue = new LinkedBlockingQueue<>();
        this.eventQueue = new LinkedBlockingQueue<>();
        this.exception = new AtomicReference<>();
        this.position = 0;
        this.future = EXECUTOR_SERVICE.submit(this::execute);
        this.buffer = new FixLenByteArrayOutputStream();
        this.buffer.setBuffer(new byte[BUFFER_SIZE]);
        this.totalBuffers = 1;
    }

    @VisibleForTesting
    LinkedBlockingQueue<byte[]> getBufferQueue() {
        return bufferQueue;
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
            while (true) {
                AsyncEvent event = eventQueue.poll(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                if (event == null) {
                    continue;
                }
                if (event instanceof EndEvent) {
                    return;
                }
                if (event instanceof DataEvent) {
                    DataEvent dataEvent = (DataEvent) event;
                    out.write(dataEvent.data, 0, dataEvent.length);
                    bufferQueue.add(dataEvent.data);
                }
                if (event instanceof FlushEvent) {
                    out.flush();
                    ((FlushEvent) event).latch.countDown();
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

    private void flushBuffer() throws IOException {
        if (buffer.getCount() == 0) {
            return;
        }
        putEvent(new DataEvent(buffer.getBuffer(), buffer.getCount()));
        byte[] byteArray;
        if (totalBuffers >= MAX_BUFFER) {
            while (true) {
                checkException();
                try {
                    byteArray = bufferQueue.poll(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    if (byteArray != null) {
                        break;
                    }
                } catch (InterruptedException e) {
                    sendEndEvent();
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        } else {
            byteArray = bufferQueue.poll();
        }
        if (byteArray == null) {
            byteArray = new byte[BUFFER_SIZE];
            totalBuffers++;
        }
        buffer.setBuffer(byteArray);
        buffer.setCount(0);
    }

    @Override
    public void write(int b) throws IOException {
        checkException();
        position++;
        while (buffer.write((byte) b) != 1) {
            flushBuffer();
        }
    }

    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        checkException();
        position += len;
        while (true) {
            int written = buffer.write(b, off, len);
            off += written;
            len -= written;
            if (len == 0) {
                return;
            }
            flushBuffer();
        }
    }

    @Override
    public void flush() throws IOException {
        if (closed) {
            throw new IOException("Already closed");
        }
        checkException();
        flushBuffer();
        FlushEvent event = new FlushEvent();
        putEvent(event);
        while (true) {
            try {
                boolean await = event.latch.await(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                if (await) {
                    return;
                }
                checkException();
            } catch (InterruptedException e) {
                sendEndEvent();
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        checkException();
        flushBuffer();
        sendEndEvent();
        try {
            this.future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
        this.closed = true;
    }

    private void sendEndEvent() {
        putEvent(new EndEvent());
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
            if (throwable instanceof RuntimeException) {
                throw (RuntimeException) throwable;
            }
            throw new IOException(throwable);
        }
    }

    private interface AsyncEvent {}

    private static class DataEvent implements AsyncEvent {

        private final byte[] data;
        private final int length;

        public DataEvent(byte[] data, int length) {
            this.data = data;
            this.length = length;
        }
    }

    private static class FlushEvent implements AsyncEvent {
        private final CountDownLatch latch = new CountDownLatch(1);
    }

    private static class EndEvent implements AsyncEvent {}
}
