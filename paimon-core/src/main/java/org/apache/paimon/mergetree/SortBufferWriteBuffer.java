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

package org.apache.paimon.mergetree;

import org.apache.paimon.KeyValue;
import org.apache.paimon.KeyValueSerializer;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.NormalizedKeyComputer;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.BinaryRowSerializer;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.mergetree.compact.MergeFunction;
import org.apache.paimon.mergetree.compact.ReducerMergeFunctionWrapper;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.sort.BinaryExternalSortBuffer;
import org.apache.paimon.sort.BinaryInMemorySortBuffer;
import org.apache.paimon.sort.SortBuffer;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.MutableObjectIterator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.IntStream;

/** A {@link WriteBuffer} which stores records in {@link BinaryInMemorySortBuffer}. */
public class SortBufferWriteBuffer implements WriteBuffer {

    private final RowType keyType;
    private final RowType valueType;
    private final KeyValueSerializer serializer;
    private final SortBuffer buffer;

    public SortBufferWriteBuffer(
            RowType keyType,
            RowType valueType,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MemorySegmentPool memoryPool,
            boolean spillable,
            MemorySize maxDiskSize,
            int sortMaxFan,
            CompressOptions compression,
            IOManager ioManager) {
        this.keyType = keyType;
        this.valueType = valueType;
        this.serializer = new KeyValueSerializer(keyType, valueType);

        // key fields
        IntStream sortFields = IntStream.range(0, keyType.getFieldCount());

        // user define sequence fields
        if (userDefinedSeqComparator != null) {
            IntStream udsFields =
                    IntStream.of(userDefinedSeqComparator.compareFields())
                            .map(operand -> operand + keyType.getFieldCount() + 2);
            sortFields = IntStream.concat(sortFields, udsFields);
        }

        // sequence field
        sortFields = IntStream.concat(sortFields, IntStream.of(keyType.getFieldCount()));

        int[] sortFieldArray = sortFields.toArray();

        // row type
        List<DataType> fieldTypes = new ArrayList<>(keyType.getFieldTypes());
        fieldTypes.add(new BigIntType(false));
        fieldTypes.add(new TinyIntType(false));
        fieldTypes.addAll(valueType.getFieldTypes());

        NormalizedKeyComputer normalizedKeyComputer =
                CodeGenUtils.newNormalizedKeyComputer(fieldTypes, sortFieldArray);
        RecordComparator keyComparator =
                CodeGenUtils.newRecordComparator(fieldTypes, sortFieldArray, true);

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
                                sortMaxFan,
                                compression,
                                maxDiskSize)
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
}
