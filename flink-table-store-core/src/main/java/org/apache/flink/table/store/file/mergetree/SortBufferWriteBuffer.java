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

package org.apache.flink.table.store.file.mergetree;

import org.apache.flink.table.store.annotation.VisibleForTesting;
import org.apache.flink.table.store.codegen.CodeGenUtils;
import org.apache.flink.table.store.codegen.NormalizedKeyComputer;
import org.apache.flink.table.store.codegen.RecordComparator;
import org.apache.flink.table.store.data.BinaryRow;
import org.apache.flink.table.store.data.InternalRow;
import org.apache.flink.table.store.data.serializer.BinaryRowSerializer;
import org.apache.flink.table.store.data.serializer.InternalRowSerializer;
import org.apache.flink.table.store.data.serializer.InternalSerializers;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.KeyValueSerializer;
import org.apache.flink.table.store.file.disk.IOManager;
import org.apache.flink.table.store.file.memory.MemorySegmentPool;
import org.apache.flink.table.store.file.mergetree.compact.MergeFunction;
import org.apache.flink.table.store.file.mergetree.compact.ReducerMergeFunctionWrapper;
import org.apache.flink.table.store.file.sort.BinaryExternalSortBuffer;
import org.apache.flink.table.store.file.sort.BinaryInMemorySortBuffer;
import org.apache.flink.table.store.file.sort.SortBuffer;
import org.apache.flink.table.store.file.utils.MutableObjectIterator;
import org.apache.flink.table.store.types.BigIntType;
import org.apache.flink.table.store.types.DataType;
import org.apache.flink.table.store.types.RowKind;
import org.apache.flink.table.store.types.RowType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/** A {@link WriteBuffer} which stores records in {@link BinaryInMemorySortBuffer}. */
public class SortBufferWriteBuffer implements WriteBuffer {

    private final RowType keyType;
    private final RowType valueType;
    private final KeyValueSerializer serializer;
    private final SortBuffer buffer;

    public SortBufferWriteBuffer(
            RowType keyType,
            RowType valueType,
            MemorySegmentPool memoryPool,
            boolean spillable,
            int sortMaxFan,
            IOManager ioManager) {
        this.keyType = keyType;
        this.valueType = valueType;
        this.serializer = new KeyValueSerializer(keyType, valueType);

        // user key + sequenceNumber
        List<DataType> sortKeyTypes = new ArrayList<>(keyType.getFieldTypes());
        sortKeyTypes.add(new BigIntType(false));

        // for sort binary buffer
        NormalizedKeyComputer normalizedKeyComputer =
                CodeGenUtils.newNormalizedKeyComputer(sortKeyTypes, "MemTableKeyComputer");
        RecordComparator keyComparator =
                CodeGenUtils.newRecordComparator(sortKeyTypes, "MemTableComparator");

        if (memoryPool.freePages() < 3) {
            throw new IllegalArgumentException(
                    "Write buffer requires a minimum of 3 page memory, please increase write buffer memory size.");
        }
        InternalRowSerializer serializer =
                InternalSerializers.create(KeyValue.schema(keyType, valueType));
        BinaryInMemorySortBuffer inMemorySortBuffer =
                BinaryInMemorySortBuffer.createBuffer(
                        normalizedKeyComputer, serializer, keyComparator, memoryPool);
        this.buffer =
                ioManager != null && spillable
                        ? new BinaryExternalSortBuffer(
                                new BinaryRowSerializer(serializer.getArity()),
                                keyComparator,
                                memoryPool.pageSize(),
                                inMemorySortBuffer,
                                ioManager,
                                sortMaxFan)
                        : inMemorySortBuffer;
    }

    @Override
    public boolean put(long sequenceNumber, RowKind valueKind, InternalRow key, InternalRow value)
            throws IOException {
        return buffer.write(serializer.toRow(key, sequenceNumber, valueKind, value));
    }

    @Override
    public int size() {
        return buffer.size();
    }

    @Override
    public long memoryOccupancy() {
        return buffer.getOccupancy();
    }

    @Override
    public boolean flushMemory() throws IOException {
        return buffer.flushMemory();
    }

    @Override
    public void forEach(
            Comparator<InternalRow> keyComparator,
            MergeFunction<KeyValue> mergeFunction,
            @Nullable KvConsumer rawConsumer,
            KvConsumer mergedConsumer)
            throws IOException {
        // TODO do not use iterator
        MergeIterator mergeIterator =
                new MergeIterator(
                        rawConsumer, buffer.sortedIterator(), keyComparator, mergeFunction);
        while (mergeIterator.hasNext()) {
            mergedConsumer.accept(mergeIterator.next());
        }
    }

    @Override
    public void clear() {
        buffer.clear();
    }

    @VisibleForTesting
    SortBuffer buffer() {
        return buffer;
    }

    private class MergeIterator {
        @Nullable private final KvConsumer rawConsumer;
        private final MutableObjectIterator<BinaryRow> kvIter;
        private final Comparator<InternalRow> keyComparator;
        private final ReducerMergeFunctionWrapper mergeFunctionWrapper;

        // previously read kv
        private KeyValueSerializer previous;
        private BinaryRow previousRow;
        // reads the next kv
        private KeyValueSerializer current;
        private BinaryRow currentRow;

        private KeyValue result;
        private boolean advanced;

        private MergeIterator(
                @Nullable KvConsumer rawConsumer,
                MutableObjectIterator<BinaryRow> kvIter,
                Comparator<InternalRow> keyComparator,
                MergeFunction<KeyValue> mergeFunction)
                throws IOException {
            this.rawConsumer = rawConsumer;
            this.kvIter = kvIter;
            this.keyComparator = keyComparator;
            this.mergeFunctionWrapper = new ReducerMergeFunctionWrapper(mergeFunction);

            int totalFieldCount = keyType.getFieldCount() + 2 + valueType.getFieldCount();
            this.previous = new KeyValueSerializer(keyType, valueType);
            this.previousRow = new BinaryRow(totalFieldCount);
            this.current = new KeyValueSerializer(keyType, valueType);
            this.currentRow = new BinaryRow(totalFieldCount);
            readOnce();
            this.advanced = false;
        }

        public boolean hasNext() throws IOException {
            advanceIfNeeded();
            return previousRow != null;
        }

        public KeyValue next() throws IOException {
            advanceIfNeeded();
            if (previousRow == null) {
                return null;
            }
            advanced = false;
            return result;
        }

        private void advanceIfNeeded() throws IOException {
            if (advanced) {
                return;
            }
            advanced = true;

            do {
                swapSerializers();
                if (previousRow == null) {
                    return;
                }
                mergeFunctionWrapper.reset();
                mergeFunctionWrapper.add(previous.getReusedKv());

                while (readOnce()) {
                    if (keyComparator.compare(
                                    previous.getReusedKv().key(), current.getReusedKv().key())
                            != 0) {
                        break;
                    }
                    mergeFunctionWrapper.add(current.getReusedKv());
                    swapSerializers();
                }
                result = mergeFunctionWrapper.getResult();
            } while (result == null);
        }

        private boolean readOnce() throws IOException {
            try {
                currentRow = kvIter.next(currentRow);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            if (currentRow != null) {
                current.fromRow(currentRow);
                if (rawConsumer != null) {
                    rawConsumer.accept(current.getReusedKv());
                }
            }
            return currentRow != null;
        }

        private void swapSerializers() {
            KeyValueSerializer tmp = previous;
            BinaryRow tmpRow = previousRow;
            previous = current;
            previousRow = currentRow;
            current = tmp;
            currentRow = tmpRow;
        }
    }

    private class RawIterator implements Iterator<KeyValue> {
        private final MutableObjectIterator<BinaryRow> kvIter;
        private final KeyValueSerializer current;

        private BinaryRow currentRow;
        private boolean advanced;

        private RawIterator(MutableObjectIterator<BinaryRow> kvIter) {
            this.kvIter = kvIter;
            this.current = new KeyValueSerializer(keyType, valueType);
            this.currentRow =
                    new BinaryRow(keyType.getFieldCount() + 2 + valueType.getFieldCount());
            this.advanced = false;
        }

        @Override
        public boolean hasNext() {
            if (!advanced) {
                advanceNext();
            }
            return currentRow != null;
        }

        @Override
        public KeyValue next() {
            if (!hasNext()) {
                return null;
            }
            advanced = false;
            return current.getReusedKv();
        }

        private void advanceNext() {
            try {
                currentRow = kvIter.next(currentRow);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            if (currentRow != null) {
                current.fromRow(currentRow);
            }
            advanced = true;
        }
    }
}
