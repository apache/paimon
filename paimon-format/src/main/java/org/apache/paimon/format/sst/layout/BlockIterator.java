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

package org.apache.paimon.format.sst.layout;

import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.memory.MemorySliceInput;

import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/** An {@link Iterator} for a block. */
public abstract class BlockIterator implements Iterator<BlockEntry> {

    protected final MemorySliceInput data;

    private final int recordCount;
    private int recordPosition = 0;

    protected final Comparator<MemorySlice> comparator;

    public BlockIterator(
            MemorySliceInput data, int recordCount, Comparator<MemorySlice> comparator) {
        this.data = data;
        this.recordCount = recordCount;
        this.comparator = comparator;
    }

    @Override
    public boolean hasNext() {
        return data.isReadable() && recordPosition < recordCount;
    }

    @Override
    public BlockEntry next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        return readEntry();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    /**
     * Seek to the position of the target key. See {@link BlockIterator#seekTo(Object, Comparator,
     * Function) seekTo}.
     */
    public boolean seekTo(MemorySlice targetKey) {
        return seekTo(targetKey, comparator, BlockEntry::getKey);
    }

    /**
     * Seeks to the first entry with a value that is either equal to or greater than the specified
     * value. After this call, the next invocation of {@link BlockIterator#next()} will return that
     * entry (if it exists).
     *
     * <p>Note that the comparing value must be monotonically increasing across current block e.g.
     * key and some special values such as the {@code lastRecordPosition} of an {@code
     * IndexBlockEntry}.
     *
     * @param targetValue target value
     * @param valueComparator comparator to compare value
     * @param valueExtractor extractor to extract a compared value from an entry
     * @return true if found an equal record
     */
    public <T> boolean seekTo(
            T targetValue, Comparator<T> valueComparator, Function<BlockEntry, T> valueExtractor) {
        int left = 0;
        int right = recordCount - 1;
        int mid = recordCount;

        while (left <= right) {
            mid = left + (right - left) / 2;

            seekTo(mid);
            BlockEntry midEntry = readEntry();
            int compare = valueComparator.compare(valueExtractor.apply(midEntry), targetValue);

            if (compare == 0) {
                break;
            } else if (compare > 0) {
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }

        // left <= right means we found an equal key
        boolean equal = left <= right;
        int targetPos = equal ? mid : left;

        if (targetPos >= recordCount) {
            moveToEnd();
        } else {
            seekTo(targetPos);
        }

        return equal;
    }

    private void moveToEnd() {
        data.setPosition(data.getSlice().length());
        recordPosition = recordCount;
    }

    public int getRecordCount() {
        return recordCount;
    }

    public int getRecordPosition() {
        return recordPosition;
    }

    /**
     * Seek to the record position. This operation is quite lightweight, only setting underlying
     * data's position.
     *
     * @param recordPosition target record position
     */
    public void seekTo(int recordPosition) {
        this.recordPosition = recordPosition;
        innerSeekTo(recordPosition);
    }

    public abstract void innerSeekTo(int recordPosition);

    public abstract BlockIterator detach();

    private BlockEntry readEntry() {
        requireNonNull(data, "data is null");

        int keyLength;
        keyLength = data.readVarLenInt();
        MemorySlice key = data.readSlice(keyLength);

        int valueLength = data.readVarLenInt();
        MemorySlice value = data.readSlice(valueLength);

        recordPosition++;

        return new BlockEntry(key, value);
    }
}
