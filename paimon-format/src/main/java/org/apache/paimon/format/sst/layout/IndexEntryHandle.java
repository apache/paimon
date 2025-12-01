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
import org.apache.paimon.memory.MemorySliceOutput;

/**
 * Handle of each index entry. The entries of an IndexBlock is a set of special sorted key-value
 * pairs. Its layout is as below:
 *
 * <pre>
 *     +--------------------------------------------------------+
 *     |  Key  |          Last key of the Data Block            |
 *     +--------------------------------------------------------+
 *     | Value | Block Offset | Block Size |  Last Record Pos   |
 *     +--------------------------------------------------------+
 * </pre>
 */
public class IndexEntryHandle {
    private static final int MAX_ENCODED_LENGTH = 9 + 5 + 5;
    private final long offset;
    private final int size;

    /** The last record position of this index entry. */
    private final int lastRecordPosition;

    public IndexEntryHandle(long offset, int size, int lastRecordPosition) {
        this.offset = offset;
        this.size = size;
        this.lastRecordPosition = lastRecordPosition;
    }

    public int getLastRecordPosition() {
        return lastRecordPosition;
    }

    public int getSize() {
        return size;
    }

    public long getOffset() {
        return offset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IndexEntryHandle other = (IndexEntryHandle) o;
        return offset == other.offset
                && size == other.size
                && lastRecordPosition == other.lastRecordPosition;
    }

    public static IndexEntryHandle read(MemorySliceInput sliceInput) {
        return new IndexEntryHandle(
                sliceInput.readVarLenLong(),
                sliceInput.readVarLenInt(),
                sliceInput.readVarLenInt());
    }

    public static MemorySlice write(IndexEntryHandle indexEntryHandle) {
        MemorySliceOutput sliceOutput = new MemorySliceOutput(MAX_ENCODED_LENGTH);
        write(indexEntryHandle, sliceOutput);
        return sliceOutput.toSlice();
    }

    private static void write(IndexEntryHandle indexEntryHandle, MemorySliceOutput sliceOutput) {
        sliceOutput.writeVarLenLong(indexEntryHandle.offset);
        sliceOutput.writeVarLenInt(indexEntryHandle.size);
        sliceOutput.writeVarLenInt(indexEntryHandle.lastRecordPosition);
    }
}
