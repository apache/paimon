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

import javax.annotation.Nullable;

/**
 * Index Meta of each BTree index file. The first key and last key of this meta could be null if the
 * entire btree index file only contains nulls.
 */
public class BTreeIndexMeta {

    private static final byte FORMAT_VERSION_WITH_NULL_FLAGS = 1;
    private static final byte FIRST_KEY_IS_NULL = 1;
    private static final byte LAST_KEY_IS_NULL = 1 << 1;

    @Nullable private final byte[] firstKey;
    @Nullable private final byte[] lastKey;
    private final boolean hasNulls;

    public BTreeIndexMeta(@Nullable byte[] firstKey, @Nullable byte[] lastKey, boolean hasNulls) {
        this.firstKey = firstKey;
        this.lastKey = lastKey;
        this.hasNulls = hasNulls;
    }

    @Nullable
    public byte[] getFirstKey() {
        return firstKey;
    }

    @Nullable
    public byte[] getLastKey() {
        return lastKey;
    }

    public boolean hasNulls() {
        return hasNulls;
    }

    public boolean onlyNulls() {
        return firstKey == null && lastKey == null;
    }

    private int memorySize() {
        return (firstKey == null ? 0 : firstKey.length)
                + (lastKey == null ? 0 : lastKey.length)
                + 11;
    }

    public byte[] serialize() {
        MemorySliceOutput sliceOutput = new MemorySliceOutput(memorySize());
        byte nullKeyFlags = 0;
        if (firstKey != null) {
            sliceOutput.writeInt(firstKey.length);
            sliceOutput.writeBytes(firstKey);
        } else {
            sliceOutput.writeInt(0);
            nullKeyFlags |= FIRST_KEY_IS_NULL;
        }
        if (lastKey != null) {
            sliceOutput.writeInt(lastKey.length);
            sliceOutput.writeBytes(lastKey);
        } else {
            sliceOutput.writeInt(0);
            nullKeyFlags |= LAST_KEY_IS_NULL;
        }
        sliceOutput.writeByte(hasNulls ? 1 : 0);
        sliceOutput.writeByte(FORMAT_VERSION_WITH_NULL_FLAGS);
        sliceOutput.writeByte(nullKeyFlags);
        return sliceOutput.toSlice().getHeapMemory();
    }

    public static BTreeIndexMeta deserialize(byte[] data) {
        MemorySliceInput sliceInput = MemorySlice.wrap(data).toInput();
        int firstKeyLength = sliceInput.readInt();
        byte[] firstKey = readKey(sliceInput, firstKeyLength);
        int lastKeyLength = sliceInput.readInt();
        byte[] lastKey = readKey(sliceInput, lastKeyLength);
        boolean hasNulls = sliceInput.readByte() == 1;

        if (sliceInput.available() >= 2) {
            int formatVersion = sliceInput.readByte();
            if (formatVersion == FORMAT_VERSION_WITH_NULL_FLAGS) {
                int nullKeyFlags = sliceInput.readByte();
                firstKey = (nullKeyFlags & FIRST_KEY_IS_NULL) != 0 ? null : firstKey;
                lastKey = (nullKeyFlags & LAST_KEY_IS_NULL) != 0 ? null : lastKey;
            }
        } else if (firstKeyLength == 0 && lastKeyLength == 0 && hasNulls) {
            // Compatibility with old metadata which used zero length to represent null keys.
            // A legacy all-null index file can be identified by both key lengths being zero with
            // null bitmap. When only one length is zero, the zero-length key is a valid serialized
            // key, for example an empty string.
            firstKey = null;
            lastKey = null;
        }
        return new BTreeIndexMeta(firstKey, lastKey, hasNulls);
    }

    private static byte[] readKey(MemorySliceInput sliceInput, int keyLength) {
        return sliceInput.readSlice(keyLength).copyBytes();
    }
}
