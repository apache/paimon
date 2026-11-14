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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.IOUtils;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Streams bucket-local guard segments alongside sorted data, retaining one strongest bound per key.
 *
 * <p>Guards bypass data serializers, spills and changelog inputs. Keys with only guards produce no
 * rows. Each instance belongs to one ordered read and must be closed with the data reader.
 */
public class DeleteOrderingGuardReader implements Closeable {

    private final Comparator<InternalRow> keyComparator;
    private final List<Cursor> cursors = new ArrayList<>();
    private final PriorityQueue<Cursor> heap;

    public DeleteOrderingGuardReader(
            List<RecordReader<KeyValue>> segments, Comparator<InternalRow> keyComparator)
            throws IOException {
        this.keyComparator = keyComparator;
        this.heap =
                new PriorityQueue<>((a, b) -> keyComparator.compare(a.next.key(), b.next.key()));
        try {
            for (RecordReader<KeyValue> segment : segments) {
                Cursor cursor = new Cursor(segment);
                cursors.add(cursor);
                if (cursor.advance()) {
                    heap.add(cursor);
                }
            }
        } catch (IOException | RuntimeException | Error failure) {
            // Include not-yet-initialized readers if the first batch failed.
            List<Closeable> toClose = new ArrayList<>(cursors);
            toClose.addAll(segments.subList(cursors.size(), segments.size()));
            for (Closeable segment : toClose) {
                try {
                    segment.close();
                } catch (Throwable cleanupFailure) {
                    failure.addSuppressed(cleanupFailure);
                }
            }
            throw failure;
        }
    }

    @Nullable
    private KeyValue forKey(InternalRow key, Comparator<KeyValue> comparator) {
        KeyValue strongest = null;
        while (!heap.isEmpty() && keyComparator.compare(heap.peek().next.key(), key) <= 0) {
            Cursor cursor = heap.remove();
            if (keyComparator.compare(cursor.next.key(), key) == 0
                    && (strongest == null || comparator.compare(cursor.next, strongest) > 0)) {
                strongest = cursor.next;
            }
            try {
                if (cursor.advance()) {
                    heap.add(cursor);
                }
            } catch (IOException failure) {
                throw new UncheckedIOException("Failed to read delete ordering guards.", failure);
            }
        }
        return strongest;
    }

    /** Read one key's metadata for consolidation, including keys with no ordinary data. */
    @Nullable
    public KeyValue nextKey(Comparator<KeyValue> comparator) throws IOException {
        if (heap.isEmpty()) {
            return null;
        }
        try {
            return forKey(heap.peek().next.key(), comparator);
        } catch (UncheckedIOException failure) {
            throw failure.getCause();
        }
    }

    public <T> MergeFunctionWrapper<T> wrap(
            MergeFunctionWrapper<T> wrapped, @Nullable FieldsComparator sequenceComparator) {
        return new Resolver<>(wrapped, sequenceComparator);
    }

    /** Native ordering: source sequence, internal sequence, then retract before add. */
    public static Comparator<KeyValue> sequenceComparator(
            @Nullable FieldsComparator sourceComparator) {
        return (a, b) -> {
            int result =
                    sourceComparator == null ? 0 : sourceComparator.compare(a.value(), b.value());
            if (result == 0) {
                result = Long.compare(a.sequenceNumber(), b.sequenceNumber());
            }
            return result == 0 ? Boolean.compare(a.isAdd(), b.isAdd()) : result;
        };
    }

    /** The returned reader owns both resources, including early close. */
    public <T> RecordReader<T> withDataReader(RecordReader<T> data) {
        return new RecordReader<T>() {
            @Nullable
            @Override
            public RecordIterator<T> readBatch() throws IOException {
                return data.readBatch();
            }

            @Override
            public void close() throws IOException {
                closeResources(Arrays.asList(data, DeleteOrderingGuardReader.this));
            }
        };
    }

    @Override
    public void close() throws IOException {
        closeResources(cursors);
        cursors.clear();
        heap.clear();
    }

    private static void closeResources(List<? extends AutoCloseable> resources) throws IOException {
        try {
            IOUtils.closeAll(resources);
        } catch (IOException failure) {
            throw failure;
        } catch (Exception failure) {
            throw new IOException("Failed to close delete ordering guard readers.", failure);
        }
    }

    private static class Cursor implements Closeable {
        private final RecordReader<KeyValue> reader;
        @Nullable private RecordReader.RecordIterator<KeyValue> batch;
        private KeyValue next;
        private boolean exhausted;
        private boolean closed;

        private Cursor(RecordReader<KeyValue> reader) {
            this.reader = reader;
        }

        private boolean advance() throws IOException {
            while (!exhausted) {
                if (batch == null) {
                    batch = reader.readBatch();
                    if (batch == null) {
                        exhausted = true;
                        break;
                    }
                }
                next = batch.next();
                if (next != null) {
                    return true;
                }
                batch.releaseBatch();
                batch = null;
            }
            next = null;
            return false;
        }

        @Override
        public void close() throws IOException {
            if (!closed) {
                closed = true;
                try {
                    if (batch != null) {
                        batch.releaseBatch();
                        batch = null;
                    }
                } finally {
                    reader.close();
                }
            }
        }
    }

    private class Resolver<T> implements MergeFunctionWrapper<T> {
        private final MergeFunctionWrapper<T> wrapped;
        private final Comparator<KeyValue> comparator;
        @Nullable private KeyValue strongest;
        private boolean initialized;

        private Resolver(
                MergeFunctionWrapper<T> wrapped, @Nullable FieldsComparator sourceComparator) {
            this.wrapped = wrapped;
            this.comparator = sequenceComparator(sourceComparator);
            wrapped.withDeleteOrderingGuardResolver(this::resolve);
        }

        @Override
        public void reset() {
            wrapped.reset();
            strongest = null;
            initialized = false;
        }

        @Override
        public void add(KeyValue kv) {
            if (!initialized) {
                strongest = forKey(kv.key(), comparator);
                initialized = true;
            }
            wrapped.add(kv);
        }

        @Override
        public T getResult() {
            return wrapped.getResult();
        }

        @Nullable
        private KeyValue resolve(@Nullable KeyValue row) {
            return row == null || strongest == null || comparator.compare(row, strongest) > 0
                    ? row
                    : null;
        }
    }
}
