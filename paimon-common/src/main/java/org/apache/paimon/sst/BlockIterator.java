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

package org.apache.paimon.sst;

import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.memory.MemorySliceInput;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/** An {@link Iterator} for a block. */
public class BlockIterator implements Iterator<Map.Entry<MemorySlice, MemorySlice>> {

    private final BlockReader reader;
    private final MemorySliceInput input;
    private BlockEntry polled;

    public BlockIterator(BlockReader reader) {
        this.reader = reader;
        this.input = reader.blockInput();
    }

    @Override
    public boolean hasNext() {
        return polled != null || input.isReadable();
    }

    @Override
    public BlockEntry next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        if (polled != null) {
            BlockEntry result = polled;
            polled = null;
            return result;
        }

        return readEntry();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    public boolean seekTo(MemorySlice targetKey) {
        int left = 0;
        int right = reader.recordCount() - 1;

        while (left <= right) {
            int mid = left + (right - left) / 2;

            input.setPosition(reader.seekTo(mid));
            BlockEntry midEntry = readEntry();
            int compare = reader.comparator().compare(midEntry.getKey(), targetKey);

            if (compare == 0) {
                polled = midEntry;
                return true;
            } else if (compare > 0) {
                polled = midEntry;
                right = mid - 1;
            } else {
                polled = null;
                left = mid + 1;
            }
        }

        return false;
    }

    private BlockEntry readEntry() {
        int keyLength;
        keyLength = input.readVarLenInt();
        MemorySlice key = input.readSlice(keyLength);

        int valueLength = input.readVarLenInt();
        MemorySlice value = input.readSlice(valueLength);

        return new BlockEntry(key, value);
    }
}
