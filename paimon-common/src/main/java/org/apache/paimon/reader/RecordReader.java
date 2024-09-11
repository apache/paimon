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

package org.apache.paimon.reader;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.Filter;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * The reader that reads the batches of records.
 *
 * @since 0.4.0
 */
@Public
public interface RecordReader<T> extends Closeable {

    /**
     * Reads one batch. The method should return null when reaching the end of the input.
     *
     * <p>The returned iterator object and any contained objects may be held onto by the source for
     * some time, so it should not be immediately reused by the reader.
     */
    @Nullable
    RecordIterator<T> readBatch() throws IOException;

    /** Closes the reader and should release all resources. */
    @Override
    void close() throws IOException;

    /**
     * An internal iterator interface which presents a more restrictive API than {@link Iterator}.
     */
    interface RecordIterator<T> {

        /**
         * Gets the next record from the iterator. Returns null if this iterator has no more
         * elements.
         */
        @Nullable
        T next() throws IOException;

        /**
         * Releases the batch that this iterator iterated over. This is not supposed to close the
         * reader and its resources, but is simply a signal that this iterator is not used anymore.
         * This method can be used as a hook to recycle/reuse heavyweight object structures.
         */
        void releaseBatch();

        /** Returns an iterator that applies {@code function} to each element. */
        default <R> RecordReader.RecordIterator<R> transform(Function<T, R> function) {
            RecordReader.RecordIterator<T> thisIterator = this;
            return new RecordReader.RecordIterator<R>() {
                @Nullable
                @Override
                public R next() throws IOException {
                    T next = thisIterator.next();
                    if (next == null) {
                        return null;
                    }
                    return function.apply(next);
                }

                @Override
                public void releaseBatch() {
                    thisIterator.releaseBatch();
                }
            };
        }

        /** Filters a {@link RecordIterator}. */
        default RecordIterator<T> filter(Filter<T> filter) {
            RecordIterator<T> thisIterator = this;
            return new RecordIterator<T>() {
                @Nullable
                @Override
                public T next() throws IOException {
                    while (true) {
                        T next = thisIterator.next();
                        if (next == null) {
                            return null;
                        }
                        if (filter.test(next)) {
                            return next;
                        }
                    }
                }

                @Override
                public void releaseBatch() {
                    thisIterator.releaseBatch();
                }
            };
        }
    }

    // -------------------------------------------------------------------------
    //                     Util methods
    // -------------------------------------------------------------------------

    /**
     * Performs the given action for each remaining element in {@link RecordReader} until all
     * elements have been processed or the action throws an exception.
     */
    default void forEachRemaining(Consumer<? super T> action) throws IOException {
        RecordReader.RecordIterator<T> batch;
        T record;

        try {
            while ((batch = readBatch()) != null) {
                while ((record = batch.next()) != null) {
                    action.accept(record);
                }
                batch.releaseBatch();
            }
        } finally {
            close();
        }
    }

    /**
     * Performs the given action for each remaining element with row position in {@link
     * RecordReader} until all elements have been processed or the action throws an exception.
     */
    default void forEachRemainingWithPosition(BiConsumer<Long, ? super T> action)
            throws IOException {
        FileRecordIterator<T> batch;
        T record;

        try {
            while ((batch = (FileRecordIterator<T>) readBatch()) != null) {
                while ((record = batch.next()) != null) {
                    action.accept(batch.returnedPosition(), record);
                }
                batch.releaseBatch();
            }
        } finally {
            close();
        }
    }

    /** Returns a {@link RecordReader} that applies {@code function} to each element. */
    default <R> RecordReader<R> transform(Function<T, R> function) {
        RecordReader<T> thisReader = this;
        return new RecordReader<R>() {
            @Nullable
            @Override
            public RecordIterator<R> readBatch() throws IOException {
                RecordIterator<T> iterator = thisReader.readBatch();
                if (iterator == null) {
                    return null;
                }
                return iterator.transform(function);
            }

            @Override
            public void close() throws IOException {
                thisReader.close();
            }
        };
    }

    /** Filters a {@link RecordReader}. */
    default RecordReader<T> filter(Filter<T> filter) {
        RecordReader<T> thisReader = this;
        return new RecordReader<T>() {
            @Nullable
            @Override
            public RecordIterator<T> readBatch() throws IOException {
                RecordIterator<T> iterator = thisReader.readBatch();
                if (iterator == null) {
                    return null;
                }
                return iterator.filter(filter);
            }

            @Override
            public void close() throws IOException {
                thisReader.close();
            }
        };
    }

    /** Convert this reader to a {@link CloseableIterator}. */
    default CloseableIterator<T> toCloseableIterator() {
        return new RecordReaderIterator<>(this);
    }
}
