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

package org.apache.flink.table.store.file.mergetree.compact;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.utils.RecordReader;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * This reader is to read a list of {@link RecordReader}, which is already sorted by key and
 * sequence number, and perform a sort merge algorithm. {@link KeyValue}s with the same key will
 * also be combined during sort merging.
 *
 * <p>NOTE: {@link KeyValue}s from the same {@link RecordReader} must not contain the same key.
 */
public class SortMergeReader<T> implements RecordReader<T> {

    private final Comparator<RowData> userKeyComparator;
    private final MergeFunctionWrapper<T> mergeFunctionWrapper;
    private final List<RecordReader<KeyValue>> nextBatchReaders;
    private final RecordIterator<KeyValue>[] nextIterators;
    private final KeyValue[] leaves;
    private final List<Integer> nodeIndexes;
    private final Map<RowData, TreeSet<Element>> sameKeyMap;
    private final int size;
    private int remainReaderCount;
    private final int[] nodes;

    public SortMergeReader(
            List<RecordReader<KeyValue>> readers,
            Comparator<RowData> userKeyComparator,
            MergeFunctionWrapper<T> mergeFunctionWrapper) {
        this.nextBatchReaders = new ArrayList<>(readers);
        this.userKeyComparator = userKeyComparator;
        this.mergeFunctionWrapper = mergeFunctionWrapper;

        this.size = readers.size();
        this.nextIterators = new RecordIterator[size];
        this.nodeIndexes = new ArrayList<>(size);
        this.sameKeyMap = new HashMap<>(size);
        this.leaves = new KeyValue[size];
        this.nodes = new int[size];
        this.remainReaderCount = size;
    }

    @Nullable
    @Override
    public RecordIterator<T> readBatch() throws IOException {

        if (remainReaderCount > 0) {
            int winner = 0;
            for (int i = 0; i < size; i++) {
                leaves[i] = getIterator(i);
                if (compareLeavesByIndex(i, winner, null)) {
                    winner = i;
                }
            }

            Arrays.fill(nodes, winner);

            for (int i = size - 1; i >= 0; i--) {
                adjust(i, null);
            }
        }

        return remainReaderCount > 0 ? new SortMergeIterator() : null;
    }

    public void setLeavesValueFromIterator(int index) throws IOException {

        if (nextIterators[index] != null) {
            KeyValue next = nextIterators[index].next();
            if (next == null) {
                nextIterators[index].releaseBatch();
                KeyValue keyValue = getIterator(index);
                leaves[index] = keyValue;
            } else {
                leaves[index] = next;
            }
        }
    }

    private KeyValue getIterator(int index) throws IOException {
        while (true) {
            RecordIterator<KeyValue> iterator = nextBatchReaders.get(index).readBatch();
            if (iterator == null) {
                // no more batches, permanently remove this reader
                nextBatchReaders.get(index).close();
                nextIterators[index] = null;
                remainReaderCount--;
                return null;
            }
            KeyValue kv = iterator.next();
            if (kv == null) {
                // empty iterator, clean up and try next batch
                iterator.releaseBatch();
            } else {
                // found next kv
                nextIterators[index] = iterator;
                return kv;
            }
        }
    }

    public boolean compareLeavesByIndex(int index1, int index2, KeyValue keyValue) {
        KeyValue value1 = leaves[index1];
        KeyValue value2 = leaves[index2];

        if (value1 == null) {
            return false;
        }
        if (value2 == null) {
            return true;
        }

        int compare = userKeyComparator.compare(value1.key(), value2.key());

        if (compare == 0) {
            // Avoid previous value to add to same keyMap.
            if (keyValue == null || userKeyComparator.compare(value1.key(), keyValue.key()) != 0) {
                TreeSet<Element> aDefault =
                        sameKeyMap.getOrDefault(
                                value1.key(),
                                new TreeSet<>(Comparator.comparingLong(o -> o.sequenceNumber)));
                aDefault.add(new Element(value1.sequenceNumber(), index1));
                aDefault.add(new Element(value2.sequenceNumber(), index2));
                sameKeyMap.put(value1.key(), aDefault);
            }
            return value1.sequenceNumber() < value2.sequenceNumber();
        }

        return compare < 0;
    }

    public void adjust(int index, KeyValue keyValue) {
        int parent = (index + size) / 2;
        while (parent > 0) {
            if (compareLeavesByIndex(nodes[parent], index, keyValue)) {
                int temp = nodes[parent];
                nodes[parent] = index;
                index = temp;
            }
            parent = parent / 2;
        }
        nodes[0] = index;
    }

    @Override
    public void close() throws IOException {
        for (RecordReader<KeyValue> reader : nextBatchReaders) {
            reader.close();
        }
    }

    /** The iterator iterates on {@link SortMergeReader}. */
    private class SortMergeIterator implements RecordIterator<T> {

        private boolean released = false;

        @Override
        public T next() throws IOException {
            while (true) {
                KeyValue value = nextImpl();
                if (value == null) {
                    return null;
                }
                T result = mergeFunctionWrapper.getResult();
                if (result != null) {
                    return result;
                }
            }
        }

        private KeyValue nextImpl() throws IOException {

            if (nodeIndexes.size() > 0) {
                KeyValue keyValue = leaves[nodeIndexes.get(nodeIndexes.size() - 1)];
                for (int i = 0; i < nodeIndexes.size(); i++) {
                    Integer index = nodeIndexes.get(i);
                    setLeavesValueFromIterator(index);
                    // Last value will change, so we do not have to avoid previous value to add to
                    // same keyMap.
                    adjust(index, nodeIndexes.size() - 1 == i ? null : keyValue);
                }
                nodeIndexes.clear();
            }

            mergeFunctionWrapper.reset();
            KeyValue keyValue = leaves[nodes[0]];
            if (keyValue != null) {
                TreeSet<Element> elements = sameKeyMap.remove(keyValue.key());
                if (elements == null) {
                    nodeIndexes.add(nodes[0]);
                    mergeFunctionWrapper.add(keyValue);
                } else {
                    elements.forEach(
                            element -> {
                                mergeFunctionWrapper.add(leaves[element.nodeIndex]);
                                nodeIndexes.add(element.nodeIndex);
                            });
                    keyValue = leaves[elements.last().nodeIndex];
                }
            }
            return keyValue;
        }

        @Override
        public void releaseBatch() {
            released = true;
        }
    }

    private static class Element {

        private final long sequenceNumber;
        private final int nodeIndex;

        public Element(long sequenceNumber, int nodeIndex) {
            this.sequenceNumber = sequenceNumber;
            this.nodeIndex = nodeIndex;
        }
    }
}
