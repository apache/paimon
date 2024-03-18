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
import org.apache.paimon.utils.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/** {@link SortMergeReader} implemented with min-heap. */
public class SortMergeReaderWithMinHeap<T> implements SortMergeReader<T> {

    private final List<RecordReader<KeyValue>> nextBatchReaders;
    private final Comparator<InternalRow> userKeyComparator;
    private final MergeFunctionWrapper<T> mergeFunctionWrapper;

    private final PriorityQueue<Element> minHeap;
    private final List<Element> polled;

    public SortMergeReaderWithMinHeap(
            List<RecordReader<KeyValue>> readers,
            Comparator<InternalRow> userKeyComparator,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MergeFunctionWrapper<T> mergeFunctionWrapper) {
        this.nextBatchReaders = new ArrayList<>(readers);
        this.userKeyComparator = userKeyComparator;
        this.mergeFunctionWrapper = mergeFunctionWrapper;

        this.minHeap =
                new PriorityQueue<>(
                        (e1, e2) -> {
                            int result = userKeyComparator.compare(e1.kv.key(), e2.kv.key());
                            if (result != 0) {
                                return result;
                            }
                            if (userDefinedSeqComparator != null) {
                                result =
                                        userDefinedSeqComparator.compare(
                                                e1.kv.value(), e2.kv.value());
                                if (result != 0) {
                                    return result;
                                }
                            }
                            return Long.compare(e1.kv.sequenceNumber(), e2.kv.sequenceNumber());
                        });
        this.polled = new ArrayList<>();
    }

    @Nullable
    @Override
    public RecordIterator<T> readBatch() throws IOException {
        for (RecordReader<KeyValue> reader : nextBatchReaders) {
            while (true) {
                RecordIterator<KeyValue> iterator = reader.readBatch();
                if (iterator == null) {
                    // no more batches, permanently remove this reader
                    reader.close();
                    break;
                }
                KeyValue kv = iterator.next();
                if (kv == null) {
                    // empty iterator, clean up and try next batch
                    iterator.releaseBatch();
                } else {
                    // found next kv
                    minHeap.offer(new Element(kv, iterator, reader));
                    break;
                }
            }
        }
        nextBatchReaders.clear();

        return minHeap.isEmpty() ? null : new SortMergeIterator();
    }

    @Override
    public void close() throws IOException {
        for (RecordReader<KeyValue> reader : nextBatchReaders) {
            reader.close();
        }
        for (Element element : minHeap) {
            element.iterator.releaseBatch();
            element.reader.close();
        }
        for (Element element : polled) {
            element.iterator.releaseBatch();
            element.reader.close();
        }
    }

    /** The iterator iterates on {@link SortMergeReaderWithMinHeap}. */
    private class SortMergeIterator implements RecordIterator<T> {

        private boolean released = false;

        @Override
        public T next() throws IOException {
            while (true) {
                boolean hasMore = nextImpl();
                if (!hasMore) {
                    return null;
                }
                T result = mergeFunctionWrapper.getResult();
                if (result != null) {
                    return result;
                }
            }
        }

        private boolean nextImpl() throws IOException {
            Preconditions.checkState(
                    !released, "SortMergeIterator#advanceNext is called after release");
            Preconditions.checkState(
                    nextBatchReaders.isEmpty(),
                    "SortMergeIterator#advanceNext is called even if the last call returns null. "
                            + "This is a bug.");

            // add previously polled elements back to priority queue
            for (Element element : polled) {
                if (element.update()) {
                    // still kvs left, add back to priority queue
                    minHeap.offer(element);
                } else {
                    // reach end of batch, clean up
                    element.iterator.releaseBatch();
                    nextBatchReaders.add(element.reader);
                }
            }
            polled.clear();

            // there are readers reaching end of batch, so we end current batch
            if (!nextBatchReaders.isEmpty()) {
                return false;
            }

            mergeFunctionWrapper.reset();
            InternalRow key =
                    Preconditions.checkNotNull(minHeap.peek(), "Min heap is empty. This is a bug.")
                            .kv
                            .key();

            // fetch all elements with the same key
            // note that the same iterator should not produce the same keys, so this code is correct
            while (!minHeap.isEmpty()) {
                Element element = minHeap.peek();
                if (userKeyComparator.compare(key, element.kv.key()) != 0) {
                    break;
                }
                minHeap.poll();
                mergeFunctionWrapper.add(element.kv);
                polled.add(element);
            }
            return true;
        }

        @Override
        public void releaseBatch() {
            released = true;
        }
    }

    private static class Element {
        private KeyValue kv;
        private final RecordIterator<KeyValue> iterator;
        private final RecordReader<KeyValue> reader;

        private Element(
                KeyValue kv, RecordIterator<KeyValue> iterator, RecordReader<KeyValue> reader) {
            this.kv = kv;
            this.iterator = iterator;
            this.reader = reader;
        }

        // IMPORTANT: Must not call this for elements still in priority queue!
        private boolean update() throws IOException {
            KeyValue nextKv = iterator.next();
            if (nextKv == null) {
                return false;
            }
            kv = nextKv;
            return true;
        }
    }
}
