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

import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.utils.ExceptionUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * A variant of the loser tree. In the LSM-Tree architecture, there will be duplicate Keys in
 * multiple {@link RecordReader}, and these Keys need to be merged. In the loser tree, we return in
 * the order of the Keys, but because the returned objects may be reused in the {@link RecordReader}
 * or the {@link MergeFunction}, for a single {@link RecordReader}, we cannot get the next Key
 * immediately after returning a Key, and we need to wait until the same Key in all {@link
 * RecordReader} is returned before proceeding to the next Key.
 *
 * <p>The process of building the loser tree is the same as a regular loser tree. The difference is
 * that in the process of adjusting the tree, we need to record the index of the same key and the
 * state of the winner/loser for subsequent quick adjustment of the position of the winner.
 *
 * <p>Detailed design can refer to https://cwiki.apache.org/confluence/x/9Ak0Dw.
 */
public class LoserTree<T> implements Closeable {
    private final int[] tree;
    private final int size;
    private final List<LeafIterator<T>> leaves;

    /**
     * if comparator.compare('a', 'b') > 0, then 'a' is the winner. In the following implementation,
     * we always let 'a' represent the parent node.
     */
    private final Comparator<T> firstComparator;

    /** same as firstComparator, but mainly used to compare sequenceNumber. */
    private final Comparator<T> secondComparator;

    private boolean initialized;

    public LoserTree(
            List<RecordReader<T>> nextBatchReaders,
            Comparator<T> firstComparator,
            Comparator<T> secondComparator) {
        this.size = nextBatchReaders.size();
        this.leaves = new ArrayList<>(size);
        this.tree = new int[size];
        // if e1 and e2 are both null, it doesn't matter who becomes the new winner. But if
        // firstComparator returns 0, it means that secondComparator must be used to compare again.
        this.firstComparator =
                (e1, e2) -> e1 == null ? -1 : (e2 == null ? 1 : firstComparator.compare(e1, e2));
        this.secondComparator =
                (e1, e2) -> e1 == null ? -1 : (e2 == null ? 1 : secondComparator.compare(e1, e2));
        this.initialized = false;

        for (RecordReader<T> reader : nextBatchReaders) {
            LeafIterator<T> iterator = new LeafIterator<>(reader);
            this.leaves.add(iterator);
        }
    }

    /** Initialize the loser tree in the same way as the regular loser tree. */
    public void initializeIfNeeded() throws IOException {
        if (!initialized) {
            Arrays.fill(tree, -1);
            for (int i = size - 1; i >= 0; i--) {
                leaves.get(i).advanceIfAvailable();
                adjust(i);
            }
            initialized = true;
        }
    }

    /** Adjust the Key that needs to be returned in the next round. */
    public void adjustForNextLoop() throws IOException {
        LeafIterator<T> winner = leaves.get(tree[0]);
        while (winner.state == State.WINNER_POPPED) {
            winner.advanceIfAvailable();
            adjust(tree[0]);
            winner = leaves.get(tree[0]);
        }
    }

    /** Pop the current winner and update its state to {@link State#WINNER_POPPED}. */
    public T popWinner() {
        LeafIterator<T> winner = leaves.get(tree[0]);
        if (winner.state == State.WINNER_POPPED) {
            // if the winner has already been popped, it means that all the same key has been
            // processed.
            return null;
        }
        T result = winner.pop();
        adjust(tree[0]);
        return result;
    }

    /** Peek the current winner, mainly for key comparisons. */
    public T peekWinner() {
        return leaves.get(tree[0]).state != State.WINNER_POPPED ? leaves.get(tree[0]).peek() : null;
    }

    /**
     * Adjust the winner from bottom to top. Using different {@link State}, we can quickly compare
     * whether all the current same keys have been processed.
     */
    private void adjust(int winner) {
        for (int parent = (winner + this.size) / 2; parent > 0 && winner >= 0; parent /= 2) {
            LeafIterator<T> winnerNode = leaves.get(winner);
            LeafIterator<T> parentNode;
            if (this.tree[parent] == -1) {
                // initialize the tree.
                winnerNode.state = State.LOSER_WITH_NEW_KEY;
            } else {
                parentNode = leaves.get(this.tree[parent]);
                switch (winnerNode.state) {
                    case WINNER_WITH_NEW_KEY:
                        adjustWithNewWinnerKey(parent, parentNode, winnerNode);
                        break;
                    case WINNER_WITH_SAME_KEY:
                        adjustWithSameWinnerKey(parent, parentNode, winnerNode);
                        break;
                    case WINNER_POPPED:
                        if (winnerNode.firstSameKeyIndex < 0) {
                            // fast path, which means that the same key is not yet processed in the
                            // current tree.
                            parent = -1;
                        } else {
                            // fast path. Directly exchange positions with the same key that has not
                            // yet been processed, no need to compare level by level.
                            parent = winnerNode.firstSameKeyIndex;
                            parentNode = leaves.get(this.tree[parent]);
                            winnerNode.state = State.LOSER_POPPED;
                            parentNode.state = State.WINNER_WITH_SAME_KEY;
                        }
                        break;
                    default:
                        throw new UnsupportedOperationException(
                                "unknown state for " + winnerNode.state.name());
                }
            }

            // if the winner loses, exchange nodes.
            if (!winnerNode.state.isWinner()) {
                int tmp = winner;
                winner = this.tree[parent];
                this.tree[parent] = tmp;
            }
        }
        this.tree[0] = winner;
    }

    /** The winner node has the same userKey as the global winner. */
    private void adjustWithSameWinnerKey(
            int index, LeafIterator<T> parentNode, LeafIterator<T> winnerNode) {
        switch (parentNode.state) {
            case LOSER_WITH_SAME_KEY:
                // the key of the previous loser is the same as the key of the current winner,
                // only the sequence needs to be compared.
                T parentKey = parentNode.peek();
                T childKey = winnerNode.peek();
                int secondResult = secondComparator.compare(parentKey, childKey);
                if (secondResult > 0) {
                    parentNode.state = State.WINNER_WITH_SAME_KEY;
                    winnerNode.state = State.LOSER_WITH_SAME_KEY;
                    parentNode.setFirstSameKeyIndex(index);
                } else {
                    winnerNode.setFirstSameKeyIndex(index);
                }
                return;
            case LOSER_WITH_NEW_KEY:
            case LOSER_POPPED:
                return;
            default:
                throw new UnsupportedOperationException(
                        "unknown state for " + parentNode.state.name());
        }
    }

    /**
     * The userKey of the new local winner node is different from that of the previous global
     * winner.
     */
    private void adjustWithNewWinnerKey(
            int index, LeafIterator<T> parentNode, LeafIterator<T> winnerNode) {
        switch (parentNode.state) {
            case LOSER_WITH_NEW_KEY:
                // when the new winner is also a new key, it needs to be compared.
                T parentKey = parentNode.peek();
                T childKey = winnerNode.peek();
                int firstResult = firstComparator.compare(parentKey, childKey);
                if (firstResult == 0) {
                    // if the compared keys are the same, we need to update the state of the node
                    // and record the index of the same key for the winner.
                    int secondResult = secondComparator.compare(parentKey, childKey);
                    if (secondResult < 0) {
                        parentNode.state = State.LOSER_WITH_SAME_KEY;
                        winnerNode.setFirstSameKeyIndex(index);
                    } else {
                        winnerNode.state = State.LOSER_WITH_SAME_KEY;
                        parentNode.state = State.WINNER_WITH_NEW_KEY;
                        parentNode.setFirstSameKeyIndex(index);
                    }
                } else if (firstResult > 0) {
                    // the two keys are completely different and just need to update the state.
                    parentNode.state = State.WINNER_WITH_NEW_KEY;
                    winnerNode.state = State.LOSER_WITH_NEW_KEY;
                }
                return;
            case LOSER_WITH_SAME_KEY:
                // A node in the WINNER_WITH_NEW_KEY state cannot encounter a node in the
                // LOSER_WITH_SAME_KEY state.
                throw new RuntimeException(
                        "This is a bug. Please file an issue. A node in the WINNER_WITH_NEW_KEY "
                                + "state cannot encounter a node in the LOSER_WITH_SAME_KEY state.");
            case LOSER_POPPED:
                // this case will only happen during adjustForNextLoop.
                parentNode.state = State.WINNER_POPPED;
                parentNode.firstSameKeyIndex = -1;
                winnerNode.state = State.LOSER_WITH_NEW_KEY;
                return;
            default:
                throw new UnsupportedOperationException(
                        "unknown state for " + parentNode.state.name());
        }
    }

    @Override
    public void close() throws IOException {
        IOException exception = null;
        for (LeafIterator<T> iterator : leaves) {
            try {
                iterator.close();
            } catch (IOException e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }
        }
        if (exception != null) {
            throw exception;
        }
    }

    /** Leaf node, used to manage {@link RecordReader}. */
    private static class LeafIterator<T> implements Closeable {
        /** The reader that reads the batches of records. */
        private final RecordReader<T> reader;

        /** The iterator used by the current batch. */
        private RecordReader.RecordIterator<T> iterator;

        /** The current minimum kv. */
        private T kv;

        /** Mark whether the visit is complete. */
        private boolean endOfInput;

        /** The index of the first same key that wins. */
        private int firstSameKeyIndex;

        /** The state of the current node. */
        private State state;

        private LeafIterator(RecordReader<T> reader) {
            this.reader = reader;
            this.endOfInput = false;
            this.firstSameKeyIndex = -1;
            this.state = State.WINNER_WITH_NEW_KEY;
        }

        public T peek() {
            return kv;
        }

        public T pop() {
            this.state = State.WINNER_POPPED;
            return kv;
        }

        public void setFirstSameKeyIndex(int index) {
            if (firstSameKeyIndex == -1) {
                firstSameKeyIndex = index;
            }
        }

        /** Reads the next kv if any, otherwise returns null. */
        public void advanceIfAvailable() throws IOException {
            this.firstSameKeyIndex = -1;
            this.state = State.WINNER_WITH_NEW_KEY;
            if (iterator == null || (kv = iterator.next()) == null) {
                while (!endOfInput) {
                    if (iterator != null) {
                        iterator.releaseBatch();
                        iterator = null;
                    }

                    iterator = reader.readBatch();
                    if (iterator == null) {
                        endOfInput = true;
                        kv = null;
                        reader.close();
                    } else if ((kv = iterator.next()) != null) {
                        break;
                    }
                }
            }
        }

        @Override
        public void close() throws IOException {
            if (this.iterator != null) {
                this.iterator.releaseBatch();
                this.iterator = null;
            }
            this.reader.close();
        }
    }

    /** The state of the node in the loser tree. */
    private enum State {
        LOSER_WITH_NEW_KEY(false),
        LOSER_WITH_SAME_KEY(false),
        LOSER_POPPED(false),
        WINNER_WITH_NEW_KEY(true),
        WINNER_WITH_SAME_KEY(true),
        WINNER_POPPED(true);

        private final boolean winner;

        State(boolean winner) {
            this.winner = winner;
        }

        public boolean isWinner() {
            return winner;
        }
    }
}
