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

import org.apache.flink.runtime.operators.sort.QuickSort;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.generated.NormalizedKeyComputer;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.operators.sort.BinaryInMemorySortBuffer;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.runtime.util.MemorySegmentPool;
import org.apache.flink.table.store.codegen.CodeGenUtils;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.KeyValueSerializer;
import org.apache.flink.table.store.file.mergetree.compact.MergeFunction;
import org.apache.flink.table.store.file.mergetree.compact.MergeFunctionHelper;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.MutableObjectIterator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/** A {@link MemTable} which stores records in {@link BinaryInMemorySortBuffer}. */
public class SortBufferMemTable implements MemTable {

    private final RowType keyType;
    private final RowType valueType;
    private final KeyValueSerializer serializer;
    private final BinaryInMemorySortBuffer buffer;
    private final @Nullable TableSchema tableSchema;

    public SortBufferMemTable(
            RowType keyType,
            RowType valueType,
            MemorySegmentPool memoryPool,
            @Nullable TableSchema tableSchema) {
        this.keyType = keyType;
        this.valueType = valueType;
        this.serializer = new KeyValueSerializer(keyType, valueType);
        this.tableSchema = tableSchema;

        // user key + sequenceNumber
        List<LogicalType> sortKeyTypes = new ArrayList<>(keyType.getChildren());
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
        this.buffer =
                BinaryInMemorySortBuffer.createBuffer(
                        normalizedKeyComputer,
                        InternalSerializers.create(KeyValue.schema(keyType, valueType)),
                        new BinaryRowDataSerializer(sortKeyTypes.size()),
                        keyComparator,
                        memoryPool);
    }

    @Override
    public boolean put(long sequenceNumber, RowKind valueKind, RowData key, RowData value)
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
    public Iterator<KeyValue> rawIterator() {
        return new RawIterator(buffer.getIterator(), tableSchema);
    }

    @Override
    public Iterator<KeyValue> mergeIterator(
            Comparator<RowData> keyComparator, MergeFunction mergeFunction) {
        new QuickSort().sort(buffer);
        return new MergeIterator(buffer.getIterator(), keyComparator, mergeFunction, tableSchema);
    }

    @Override
    public void clear() {
        buffer.reset();
    }

    private class MergeIterator implements Iterator<KeyValue> {
        private final MutableObjectIterator<BinaryRowData> kvIter;
        private final Comparator<RowData> keyComparator;
        private final MergeFunctionHelper mergeFunctionHelper;

        // holds the merged value
        private KeyValueSerializer previous;
        private BinaryRowData previousRow;
        // reads the next kv
        private KeyValueSerializer current;
        private BinaryRowData currentRow;
        private boolean advanced;

        private MergeIterator(
                MutableObjectIterator<BinaryRowData> kvIter,
                Comparator<RowData> keyComparator,
                MergeFunction mergeFunction,
                @Nullable TableSchema tableSchema) {
            this.kvIter = kvIter;
            this.keyComparator = keyComparator;
            this.mergeFunctionHelper = new MergeFunctionHelper(mergeFunction);

            int totalFieldCount = keyType.getFieldCount() + 2 + valueType.getFieldCount();
            this.previous = new KeyValueSerializer(keyType, valueType, tableSchema);
            this.previousRow = new BinaryRowData(totalFieldCount);
            this.current = new KeyValueSerializer(keyType, valueType, tableSchema);
            this.currentRow = new BinaryRowData(totalFieldCount);
            readOnce();
            this.advanced = false;
        }

        @Override
        public boolean hasNext() {
            advanceIfNeeded();
            return previousRow != null;
        }

        @Override
        public KeyValue next() {
            advanceIfNeeded();
            if (previousRow == null) {
                return null;
            }
            advanced = false;
            return previous.getReusedKv();
        }

        private void advanceIfNeeded() {
            if (advanced) {
                return;
            }
            advanced = true;

            RowData result;
            do {
                swapSerializers();
                if (previousRow == null) {
                    return;
                }
                mergeFunctionHelper.reset();
                mergeFunctionHelper.add(previous.getReusedKv().value());

                while (readOnce()) {
                    if (keyComparator.compare(
                                    previous.getReusedKv().key(), current.getReusedKv().key())
                            != 0) {
                        break;
                    }
                    mergeFunctionHelper.add(current.getReusedKv().value());
                    swapSerializers();
                }
                result = mergeFunctionHelper.getValue();
            } while (result == null);
            previous.getReusedKv().setValue(result);
        }

        private boolean readOnce() {
            try {
                currentRow = kvIter.next(currentRow);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            if (currentRow != null) {
                current.fromRow(currentRow);
            }
            return currentRow != null;
        }

        private void swapSerializers() {
            KeyValueSerializer tmp = previous;
            BinaryRowData tmpRow = previousRow;
            previous = current;
            previousRow = currentRow;
            current = tmp;
            currentRow = tmpRow;
        }
    }

    private class RawIterator implements Iterator<KeyValue> {
        private final MutableObjectIterator<BinaryRowData> kvIter;
        private final KeyValueSerializer current;

        private BinaryRowData currentRow;
        private boolean advanced;

        private RawIterator(MutableObjectIterator<BinaryRowData> kvIter, TableSchema tableSchema) {
            this.kvIter = kvIter;
            this.current = new KeyValueSerializer(keyType, valueType, tableSchema);
            this.currentRow =
                    new BinaryRowData(keyType.getFieldCount() + 2 + valueType.getFieldCount());
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
