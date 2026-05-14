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
 *
 * <h3>Serialization format versions:</h3>
 *
 * <p>V0 (legacy): {@code [firstKeyLength(4)] [firstKey] [lastKeyLength(4)] [lastKey] [hasNulls(1)]}
 *
 * <p>V0 uses 0 to represent null key length, which cannot distinguish null from empty byte array
 * (e.g. empty string key). The last byte is hasNulls (0 or 1).
 *
 * <p>V1: {@code [firstKeyLength(4)] [firstKey] [lastKeyLength(4)] [lastKey] [hasNulls(1)]
 * [VERSION(1)]}
 *
 * <p>V1 uses -1 to represent null key length and 0 for empty byte array. A version byte is appended
 * at the end. The version value must be >= 2 to avoid conflict with V0's hasNulls byte (0 or 1), so
 * that the format can be detected by reading the last byte.
 */
public class BTreeIndexMeta {

    // Must be >= 2 to distinguish from V0's hasNulls byte (0 or 1).
    private static final byte VERSION = 2;

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
                + 10; // 4 (firstKeyLength) + 4 (lastKeyLength) + 1 (hasNulls) + 1 (version)
    }

    public byte[] serialize() {
        MemorySliceOutput sliceOutput = new MemorySliceOutput(memorySize());
        if (firstKey != null) {
            sliceOutput.writeInt(firstKey.length);
            sliceOutput.writeBytes(firstKey);
        } else {
            // -1 represents null in V1 format
            sliceOutput.writeInt(-1);
        }
        if (lastKey != null) {
            sliceOutput.writeInt(lastKey.length);
            sliceOutput.writeBytes(lastKey);
        } else {
            // -1 represents null in V1 format
            sliceOutput.writeInt(-1);
        }
        sliceOutput.writeByte(hasNulls ? 1 : 0);
        // Append version byte at the end. Must be >= 2 to distinguish from V0's hasNulls (0 or 1).
        sliceOutput.writeByte(VERSION);
        return sliceOutput.toSlice().getHeapMemory();
    }

    public static BTreeIndexMeta deserialize(byte[] data) {
        // Detect format version by reading the last byte.
        // V0 format ends with hasNulls (0 or 1), V1 format ends with VERSION (>= 2).
        byte lastByte = data[data.length - 1];

        if (lastByte == VERSION) {
            // V1 format: -1 means null, 0 means empty byte array, version byte at the end
            MemorySliceInput sliceInput = MemorySlice.wrap(data).toInput();
            int firstKeyLength = sliceInput.readInt();
            byte[] firstKey =
                    firstKeyLength < 0
                            ? null
                            : firstKeyLength == 0
                                    ? new byte[0]
                                    : sliceInput.readSlice(firstKeyLength).copyBytes();
            int lastKeyLength = sliceInput.readInt();
            byte[] lastKey =
                    lastKeyLength < 0
                            ? null
                            : lastKeyLength == 0
                                    ? new byte[0]
                                    : sliceInput.readSlice(lastKeyLength).copyBytes();
            boolean hasNulls = sliceInput.readByte() == 1;
            // Skip the version byte at the end, it has already been read.
            return new BTreeIndexMeta(firstKey, lastKey, hasNulls);
        } else {
            // V0 format (legacy): no version byte, 0 means null, positive means key length.
            // The last byte is hasNulls (0 or 1), not a version byte.
            MemorySliceInput sliceInput = MemorySlice.wrap(data).toInput();
            int firstKeyLength = sliceInput.readInt();
            byte[] firstKey =
                    firstKeyLength == 0 ? null : sliceInput.readSlice(firstKeyLength).copyBytes();
            int lastKeyLength = sliceInput.readInt();
            byte[] lastKey =
                    lastKeyLength == 0 ? null : sliceInput.readSlice(lastKeyLength).copyBytes();
            boolean hasNulls = sliceInput.readByte() == 1;
            return new BTreeIndexMeta(firstKey, lastKey, hasNulls);
        }
    }
}
