package org.apache.paimon.utils;

import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReader.RecordIterator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

/** A {@link RecordReader} to use ASYNC_EXECUTOR to read records async. */
public class AsyncRecordReader<T> implements RecordReader<T> {

    private static final ExecutorService ASYNC_EXECUTOR =
            Executors.newCachedThreadPool(new ExecutorThreadFactory("paimon-reader-async-thread"));

    private final BlockingQueue<Element> queue;
    private final Future<Void> future;

    public AsyncRecordReader(RecordReader<T> reader) {
        this.queue = new LinkedBlockingQueue<>();
        this.future =
                ASYNC_EXECUTOR.submit(
                        () -> {
                            try {
                                while (true) {
                                    RecordIterator<T> batch = reader.readBatch();
                                    if (batch == null) {
                                        queue.add(new Element(true, null));
                                        return null;
                                    }

                                    queue.add(new Element(false, batch));
                                }
                            } finally {
                                reader.close();
                            }
                        });
    }

    @Nullable
    @Override
    public RecordIterator<T> readBatch() throws IOException {
        try {
            Element element = queue.take();
            if (element.isEnd) {
                return null;
            }

            return element.batch;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
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
