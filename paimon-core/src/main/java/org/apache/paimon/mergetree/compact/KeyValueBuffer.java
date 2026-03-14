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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.disk.ExternalBuffer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.InMemoryBuffer;
import org.apache.paimon.disk.RowBuffer;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.memory.UnlimitedSegmentPool;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.KeyValueWithLevelNoReusingSerializer;
import org.apache.paimon.utils.LazyField;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

/** A buffer to cache {@link KeyValue}s. */
public interface KeyValueBuffer {

    void reset();

    void put(KeyValue kv);

    CloseableIterator<KeyValue> iterator();

    /** A {@link KeyValueBuffer} implemented by hybrid. */
    class HybridBuffer implements KeyValueBuffer {

        private final int threshold;
        private final ListBuffer listBuffer;
        private final LazyField<BinaryBuffer> lazyBinaryBuffer;

        private @Nullable BinaryBuffer binaryBuffer;

        public HybridBuffer(int threshold, LazyField<BinaryBuffer> lazyBinaryBuffer) {
            this.threshold = threshold;
            this.listBuffer = new ListBuffer();
            this.lazyBinaryBuffer = lazyBinaryBuffer;
        }

        @Nullable
        @VisibleForTesting
        BinaryBuffer binaryBuffer() {
            return binaryBuffer;
        }

        @Override
        public void reset() {
            listBuffer.reset();
            if (binaryBuffer != null) {
                binaryBuffer.reset();
                binaryBuffer = null;
            }
        }

        @Override
        public void put(KeyValue kv) {
            if (binaryBuffer != null) {
                binaryBuffer.put(kv);
            } else {
                listBuffer.put(kv);
                if (listBuffer.list.size() > threshold) {
                    spillToBinary();
                }
            }
        }

        private void spillToBinary() {
            BinaryBuffer binaryBuffer = lazyBinaryBuffer.get();
            try (CloseableIterator<KeyValue> iterator = listBuffer.iterator()) {
                while (iterator.hasNext()) {
                    binaryBuffer.put(iterator.next());
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            this.listBuffer.reset();
            this.binaryBuffer = binaryBuffer;
        }

        @Override
        public CloseableIterator<KeyValue> iterator() {
            if (binaryBuffer != null) {
                return binaryBuffer.iterator();
            }
            return listBuffer.iterator();
        }
    }

    /** A {@link KeyValueBuffer} implemented by a list. */
    class ListBuffer implements KeyValueBuffer {

        private final List<KeyValue> list = new ArrayList<>();

        @Override
        public CloseableIterator<KeyValue> iterator() {
            return CloseableIterator.adapterForIterator(list.iterator());
        }

        @Override
        public void reset() {
            list.clear();
        }

        @Override
        public void put(KeyValue kv) {
            list.add(kv);
        }
    }

    /** A {@link KeyValueBuffer} implemented by binary with spilling. */
    class BinaryBuffer implements KeyValueBuffer {

        private final RowBuffer buffer;
        private final KeyValueWithLevelNoReusingSerializer kvSerializer;

        public BinaryBuffer(RowBuffer buffer, KeyValueWithLevelNoReusingSerializer kvSerializer) {
            this.buffer = buffer;
            this.kvSerializer = kvSerializer;
        }

        @Override
        public void reset() {
            buffer.reset();
        }

        @Override
        public void put(KeyValue kv) {
            try {
                boolean success = buffer.put(kvSerializer.toRow(kv));
                if (!success) {
                    throw new RuntimeException("This is a bug!");
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public CloseableIterator<KeyValue> iterator() {
            @SuppressWarnings("resource")
            RowBuffer.RowBufferIterator iterator = buffer.newIterator();
            return new CloseableIterator<KeyValue>() {

                private boolean hasNextWasCalled = false;
                private boolean nextResult = false;

                @Override
                public boolean hasNext() {
                    if (!hasNextWasCalled) {
                        nextResult = iterator.advanceNext();
                        hasNextWasCalled = true;
                    }
                    return nextResult;
                }

                @Override
                public KeyValue next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }
                    hasNextWasCalled = false;
                    return kvSerializer.fromRow(iterator.getRow().copy());
                }

                @Override
                public void close() {
                    iterator.close();
                }
            };
        }
    }

    static BinaryBuffer createBinaryBuffer(
            CoreOptions options,
            RowType keyType,
            RowType valueType,
            @Nullable IOManager ioManager) {
        KeyValueWithLevelNoReusingSerializer kvSerializer =
                new KeyValueWithLevelNoReusingSerializer(keyType, valueType);
        MemorySegmentPool pool =
                ioManager == null
                        ? new UnlimitedSegmentPool(options.pageSize())
                        : new HeapMemorySegmentPool(
                                options.lookupMergeBufferSize(), options.pageSize());
        InternalRowSerializer serializer = new InternalRowSerializer(kvSerializer.fieldTypes());
        RowBuffer buffer =
                ioManager == null
                        ? new InMemoryBuffer(pool, serializer)
                        : new ExternalBuffer(
                                ioManager,
                                pool,
                                serializer,
                                options.writeBufferSpillDiskSize(),
                                options.spillCompressOptions());
        return new BinaryBuffer(buffer, kvSerializer);
    }

    static HybridBuffer createHybridBuffer(
            CoreOptions options,
            RowType keyType,
            RowType valueType,
            @Nullable IOManager ioManager) {
        Supplier<BinaryBuffer> binarySupplier =
                () -> createBinaryBuffer(options, keyType, valueType, ioManager);
        int threshold = options == null ? 1024 : options.lookupMergeRecordsThreshold();
        return new HybridBuffer(threshold, new LazyField<>(binarySupplier));
    }

    static void insertInto(
            KeyValueBuffer buffer, KeyValue highLevel, Comparator<KeyValue> comparator) {
        List<KeyValue> newCandidates = new ArrayList<>();
        try (CloseableIterator<KeyValue> iterator = buffer.iterator()) {
            while (iterator.hasNext()) {
                KeyValue candidate = iterator.next();
                if (highLevel != null && comparator.compare(highLevel, candidate) < 0) {
                    newCandidates.add(highLevel);
                    newCandidates.add(candidate);
                    highLevel = null;
                } else {
                    newCandidates.add(candidate);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (highLevel != null) {
            newCandidates.add(highLevel);
        }
        buffer.reset();
        for (KeyValue kv : newCandidates) {
            buffer.put(kv);
        }
    }
}
