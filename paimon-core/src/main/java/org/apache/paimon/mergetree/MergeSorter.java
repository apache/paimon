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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.SortEngine;
import org.apache.paimon.KeyValue;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.JoinedRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.memory.CachelessSegmentPool;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.mergetree.compact.ConcatRecordReader;
import org.apache.paimon.mergetree.compact.ConcatRecordReader.ReaderSupplier;
import org.apache.paimon.mergetree.compact.MergeFunctionWrapper;
import org.apache.paimon.mergetree.compact.SortMergeReader;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.sort.BinaryExternalSortBuffer;
import org.apache.paimon.sort.SortBuffer;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.MutableObjectIterator;
import org.apache.paimon.utils.OffsetRow;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.IntStream;

import static org.apache.paimon.schema.SystemColumns.SEQUENCE_NUMBER;
import static org.apache.paimon.schema.SystemColumns.VALUE_KIND;

/** The merge sorter to sort and merge readers with key overlap. */
public class MergeSorter {

    private final RowType keyType;
    private RowType valueType;

    private final SortEngine sortEngine;
    private final int spillThreshold;
    private final int spillSortMaxNumFiles;
    private final String compression;
    private final MemorySize maxDiskSize;

    private final MemorySegmentPool memoryPool;

    @Nullable private IOManager ioManager;

    public MergeSorter(
            CoreOptions options,
            RowType keyType,
            RowType valueType,
            @Nullable IOManager ioManager) {
        this.sortEngine = options.sortEngine();
        this.spillThreshold = options.sortSpillThreshold();
        this.spillSortMaxNumFiles = options.localSortMaxNumFileHandles();
        this.compression = options.spillCompression();
        this.keyType = keyType;
        this.valueType = valueType;
        this.memoryPool =
                new CachelessSegmentPool(options.sortSpillBufferSize(), options.pageSize());
        this.ioManager = ioManager;
        this.maxDiskSize = options.writeBufferSpillDiskSize();
    }

    public MemorySegmentPool memoryPool() {
        return memoryPool;
    }

    public RowType valueType() {
        return valueType;
    }

    public void setIOManager(IOManager ioManager) {
        this.ioManager = ioManager;
    }

    public void setProjectedValueType(RowType projectedType) {
        this.valueType = projectedType;
    }

    public <T> RecordReader<T> mergeSort(
            List<ReaderSupplier<KeyValue>> lazyReaders,
            Comparator<InternalRow> keyComparator,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MergeFunctionWrapper<T> mergeFunction)
            throws IOException {
        if (ioManager != null && lazyReaders.size() > spillThreshold) {
            return spillMergeSort(
                    lazyReaders, keyComparator, userDefinedSeqComparator, mergeFunction);
        }

        List<RecordReader<KeyValue>> readers = new ArrayList<>(lazyReaders.size());
        for (ReaderSupplier<KeyValue> supplier : lazyReaders) {
            try {
                readers.add(supplier.get());
            } catch (IOException e) {
                // if one of the readers creating failed, we need to close them all.
                readers.forEach(IOUtils::closeQuietly);
                throw e;
            }
        }

        return SortMergeReader.createSortMergeReader(
                readers, keyComparator, userDefinedSeqComparator, mergeFunction, sortEngine);
    }

    private <T> RecordReader<T> spillMergeSort(
            List<ReaderSupplier<KeyValue>> readers,
            Comparator<InternalRow> keyComparator,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MergeFunctionWrapper<T> mergeFunction)
            throws IOException {
        ExternalSorterWithLevel sorter = new ExternalSorterWithLevel(userDefinedSeqComparator);
        ConcatRecordReader.create(readers).forIOEachRemaining(sorter::put);
        sorter.flushMemory();

        NoReusingMergeIterator<T> iterator = sorter.newIterator(keyComparator, mergeFunction);
        return new RecordReader<T>() {

            private boolean read = false;

            @Nullable
            @Override
            public RecordIterator<T> readBatch() {
                if (read) {
                    return null;
                }

                read = true;
                return new RecordIterator<T>() {
                    @Override
                    public T next() throws IOException {
                        return iterator.next();
                    }

                    @Override
                    public void releaseBatch() {}
                };
            }

            @Override
            public void close() {
                sorter.clear();
            }
        };
    }

    /**
     * Here can not use {@link SortBufferWriteBuffer} for two reasons:
     *
     * <p>1.Changelog-producer: full-compaction and lookup need to know the level of the KeyValue.
     *
     * <p>2.Changelog-producer: full-compaction and lookup need to store the reference of
     * update_before.
     */
    private class ExternalSorterWithLevel {

        private final SortBuffer buffer;

        public ExternalSorterWithLevel(@Nullable FieldsComparator userDefinedSeqComparator) {
            if (memoryPool.freePages() < 3) {
                throw new IllegalArgumentException(
                        "Write buffer requires a minimum of 3 page memory, please increase write buffer memory size.");
            }

            // key fields
            IntStream sortFields = IntStream.range(0, keyType.getFieldCount());

            // user define sequence fields
            if (userDefinedSeqComparator != null) {
                IntStream udsFields =
                        IntStream.of(userDefinedSeqComparator.compareFields())
                                .map(operand -> operand + keyType.getFieldCount() + 3);
                sortFields = IntStream.concat(sortFields, udsFields);
            }

            // sequence field
            sortFields = IntStream.concat(sortFields, IntStream.of(keyType.getFieldCount()));

            // row type
            List<DataField> fields = new ArrayList<>(keyType.getFields());
            fields.add(new DataField(0, SEQUENCE_NUMBER, new BigIntType(false)));
            fields.add(new DataField(1, VALUE_KIND, new TinyIntType(false)));
            fields.add(new DataField(2, "_LEVEL", new IntType(false)));
            fields.addAll(valueType.getFields());

            this.buffer =
                    BinaryExternalSortBuffer.create(
                            ioManager,
                            new RowType(fields),
                            sortFields.toArray(),
                            memoryPool,
                            spillSortMaxNumFiles,
                            compression,
                            maxDiskSize);
        }

        public boolean put(KeyValue keyValue) throws IOException {
            GenericRow meta = new GenericRow(3);
            meta.setField(0, keyValue.sequenceNumber());
            meta.setField(1, keyValue.valueKind().toByteValue());
            meta.setField(2, keyValue.level());
            JoinedRow row =
                    new JoinedRow()
                            .replace(
                                    new JoinedRow().replace(keyValue.key(), meta),
                                    keyValue.value());
            return buffer.write(row);
        }

        public boolean flushMemory() throws IOException {
            return buffer.flushMemory();
        }

        public void clear() {
            buffer.clear();
        }

        public <T> NoReusingMergeIterator<T> newIterator(
                Comparator<InternalRow> keyComparator, MergeFunctionWrapper<T> mergeFunction)
                throws IOException {
            return new NoReusingMergeIterator<>(
                    buffer.sortedIterator(), keyComparator, mergeFunction);
        }
    }

    private class NoReusingMergeIterator<T> {

        private final MutableObjectIterator<BinaryRow> kvIter;
        private final Comparator<InternalRow> keyComparator;
        private final MergeFunctionWrapper<T> mergeFunc;

        private KeyValue left;

        private boolean isEnd;

        private NoReusingMergeIterator(
                MutableObjectIterator<BinaryRow> kvIter,
                Comparator<InternalRow> keyComparator,
                MergeFunctionWrapper<T> mergeFunction) {
            this.kvIter = kvIter;
            this.keyComparator = keyComparator;
            this.mergeFunc = mergeFunction;
            this.isEnd = false;
        }

        public T next() throws IOException {
            if (isEnd) {
                return null;
            }

            T result;
            do {
                mergeFunc.reset();
                InternalRow key = null;
                KeyValue keyValue;
                while ((keyValue = readOnce()) != null) {
                    if (key != null && keyComparator.compare(keyValue.key(), key) != 0) {
                        break;
                    }
                    key = keyValue.key();
                    mergeFunc.add(keyValue);
                }
                left = keyValue;
                if (key == null) {
                    return null;
                }
                result = mergeFunc.getResult();
            } while (result == null);
            return result;
        }

        private KeyValue readOnce() throws IOException {
            if (left != null) {
                KeyValue ret = left;
                left = null;
                return ret;
            }
            BinaryRow row = kvIter.next();
            if (row == null) {
                isEnd = true;
                return null;
            }

            int keyArity = keyType.getFieldCount();
            int valueArity = valueType.getFieldCount();
            return new KeyValue()
                    .replace(
                            new OffsetRow(keyArity, 0).replace(row),
                            row.getLong(keyArity),
                            RowKind.fromByteValue(row.getByte(keyArity + 1)),
                            new OffsetRow(valueArity, keyArity + 3).replace(row))
                    .setLevel(row.getInt(keyArity + 2));
        }
    }
}
