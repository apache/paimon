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
import java.util.Comparator;
import java.util.List;

/** {@link SortMergeReader} implemented with loser-tree. */
public class SortMergeReaderWithLoserTree<T> implements SortMergeReader<T> {

    private final MergeFunctionWrapper<T> mergeFunctionWrapper;
    private final LoserTree<KeyValue> loserTree;

    public SortMergeReaderWithLoserTree(
            List<RecordReader<KeyValue>> readers,
            Comparator<InternalRow> userKeyComparator,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MergeFunctionWrapper<T> mergeFunctionWrapper) {
        this.mergeFunctionWrapper = mergeFunctionWrapper;
        this.loserTree =
                new LoserTree<>(
                        readers,
                        (e1, e2) -> userKeyComparator.compare(e2.key(), e1.key()),
                        createSequenceComparator(userDefinedSeqComparator));
    }

    private Comparator<KeyValue> createSequenceComparator(
            @Nullable FieldsComparator userDefinedSeqComparator) {
        if (userDefinedSeqComparator == null) {
            return (e1, e2) -> Long.compare(e2.sequenceNumber(), e1.sequenceNumber());
        }

        return (o1, o2) -> {
            int result = userDefinedSeqComparator.compare(o2.value(), o1.value());
            if (result != 0) {
                return result;
            }
            return Long.compare(o2.sequenceNumber(), o1.sequenceNumber());
        };
    }

    /** Compared with heapsort, {@link LoserTree} will only produce one batch. */
    @Nullable
    @Override
    public RecordIterator<T> readBatch() throws IOException {
        loserTree.initializeIfNeeded();
        return loserTree.peekWinner() == null ? null : new SortMergeIterator();
    }

    @Override
    public void close() throws IOException {
        loserTree.close();
    }

    /** The iterator iterates on {@link SortMergeReaderWithLoserTree}. */
    private class SortMergeIterator implements RecordIterator<T> {

        private boolean released = false;

        @Nullable
        @Override
        public T next() throws IOException {
            while (true) {
                loserTree.adjustForNextLoop();
                KeyValue winner = loserTree.popWinner();
                if (winner == null) {
                    return null;
                }
                mergeFunctionWrapper.reset();
                mergeFunctionWrapper.add(winner);

                T result = merge();
                if (result != null) {
                    return result;
                }
            }
        }

        private T merge() {
            Preconditions.checkState(
                    !released, "SortMergeIterator#nextImpl is called after release");

            while (loserTree.peekWinner() != null) {
                mergeFunctionWrapper.add(loserTree.popWinner());
            }
            return mergeFunctionWrapper.getResult();
        }

        @Override
        public void releaseBatch() {
            released = true;
        }
    }
}
