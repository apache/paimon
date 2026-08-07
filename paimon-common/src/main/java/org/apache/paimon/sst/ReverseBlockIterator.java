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

/** An {@link Iterator} which reads entries in a block from last to first. */
public class ReverseBlockIterator implements Iterator<Map.Entry<MemorySlice, MemorySlice>> {

    private final BlockReader reader;
    private final MemorySliceInput input;
    private int recordPosition;

    public ReverseBlockIterator(BlockReader reader) {
        this.reader = reader;
        this.input = reader.blockInput();
        this.recordPosition = reader.recordCount() - 1;
    }

    @Override
    public boolean hasNext() {
        return recordPosition >= 0;
    }

    @Override
    public BlockEntry next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        input.setPosition(reader.seekTo(recordPosition--));
        int keyLength = input.readVarLenInt();
        MemorySlice key = input.readSlice(keyLength);
        int valueLength = input.readVarLenInt();
        MemorySlice value = input.readSlice(valueLength);
        return new BlockEntry(key, value);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
