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
import org.apache.paimon.types.RowType;
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
 * Streams bucket-local guard segments alongside sorted data, retaining only one key's bounds.
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

    private List<KeyValue> forKey(InternalRow key) {
        List<KeyValue> result = new ArrayList<>();
        while (!heap.isEmpty() && keyComparator.compare(heap.peek().next.key(), key) <= 0) {
            Cursor cursor = heap.remove();
            if (keyComparator.compare(cursor.next.key(), key) == 0) {
                result.add(cursor.next);
            }
            try {
                if (cursor.advance()) {
                    heap.add(cursor);
                }
            } catch (IOException failure) {
                throw new UncheckedIOException("Failed to read delete ordering guards.", failure);
            }
        }
        return result;
    }

    /** Read one key's metadata for consolidation, including keys with no ordinary data. */
    @Nullable
    public List<KeyValue> nextKey() throws IOException {
        if (heap.isEmpty()) {
            return null;
        }
        try {
            return forKey(heap.peek().next.key());
        } catch (UncheckedIOException failure) {
            throw failure.getCause();
        }
    }

    public <T> MergeFunctionWrapper<T> wrap(
            MergeFunctionWrapper<T> wrapped,
            MergeFunctionFactory<KeyValue> factory,
            @Nullable RowType readType,
            @Nullable FieldsComparator sequenceComparator) {
        MergeFunctionFactory<KeyValue> nativeFactory =
                factory instanceof LookupMergeFunction.Factory
                        ? ((LookupMergeFunction.Factory) factory).wrapped()
                        : factory;
        return new Resolver<>(wrapped, nativeFactory.create(readType), sequenceComparator);
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
        private final MergeFunction<KeyValue> beforeFunction;
        private final Comparator<KeyValue> comparator;
        private List<KeyValue> guards;
        private int nextGuard;

        private Resolver(
                MergeFunctionWrapper<T> wrapped,
                MergeFunction<KeyValue> beforeFunction,
                @Nullable FieldsComparator sourceComparator) {
            this.wrapped = wrapped;
            this.beforeFunction = beforeFunction;
            this.comparator = sequenceComparator(sourceComparator);
            wrapped.withDeleteOrderingGuardBeforeImage(this::beforeImage);
        }

        @Override
        public void reset() {
            wrapped.reset();
            guards = null;
            nextGuard = 0;
        }

        @Override
        public void add(KeyValue kv) {
            if (guards == null) {
                guards = forKey(kv.key());
                guards.sort(comparator);
            }
            while (nextGuard < guards.size()
                    && comparator.compare(guards.get(nextGuard), kv) <= 0) {
                wrapped.addDeleteOrderingGuard(guards.get(nextGuard++));
            }
            wrapped.add(kv);
        }

        @Override
        public T getResult() {
            while (guards != null && nextGuard < guards.size()) {
                wrapped.addDeleteOrderingGuard(guards.get(nextGuard++));
            }
            return wrapped.getResult();
        }

        private KeyValue beforeImage(KeyValue before) {
            if (before == null || guards == null || guards.isEmpty()) {
                return before;
            }
            beforeFunction.reset();
            boolean added = false;
            for (KeyValue guard : guards) {
                if (!added && comparator.compare(before, guard) < 0) {
                    beforeFunction.add(before);
                    added = true;
                }
                beforeFunction.addDeleteOrderingGuard(guard);
            }
            if (!added) {
                beforeFunction.add(before);
            }
            KeyValue result = beforeFunction.getResult();
            // Native functions may reuse their KeyValue, but reset allocates fresh row state.
            return new KeyValue()
                    .replace(
                            result.key(),
                            result.sequenceNumber(),
                            result.valueKind(),
                            result.value())
                    .setLevel(before.level());
        }
    }
}
