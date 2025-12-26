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

package org.apache.paimon.globalindex.btree;

import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.memory.MemorySliceInput;
import org.apache.paimon.memory.MemorySliceOutput;

/** Index Meta of each BTree index file. */
public class BTreeIndexMeta {

    private final byte[] firstKey;
    private final byte[] lastKey;
    private final boolean hasNulls;

    public BTreeIndexMeta(byte[] firstKey, byte[] lastKey, boolean hasNulls) {
        this.firstKey = firstKey;
        this.lastKey = lastKey;
        this.hasNulls = hasNulls;
    }

    public byte[] getFirstKey() {
        return firstKey;
    }

    public byte[] getLastKey() {
        return lastKey;
    }

    public boolean hasNulls() {
        return hasNulls;
    }

    public byte[] serialize() {
        MemorySliceOutput sliceOutput = new MemorySliceOutput(firstKey.length + lastKey.length + 8);
        sliceOutput.writeInt(firstKey.length);
        sliceOutput.writeBytes(firstKey);
        sliceOutput.writeInt(lastKey.length);
        sliceOutput.writeBytes(lastKey);
        sliceOutput.writeByte(hasNulls ? 1 : 0);
        return sliceOutput.toSlice().getHeapMemory();
    }

    public static BTreeIndexMeta deserialize(byte[] data) {
        MemorySliceInput sliceInput = MemorySlice.wrap(data).toInput();
        int firstKeyLength = sliceInput.readInt();
        byte[] firstKey = sliceInput.readSlice(firstKeyLength).copyBytes();
        int lastKeyLength = sliceInput.readInt();
        byte[] lastKey = sliceInput.readSlice(lastKeyLength).copyBytes();
        boolean hasNulls = sliceInput.readByte() == 1;
        return new BTreeIndexMeta(firstKey, lastKey, hasNulls);
    }
}
