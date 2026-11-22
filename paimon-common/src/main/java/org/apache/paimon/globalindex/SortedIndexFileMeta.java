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

package org.apache.paimon.globalindex;

import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.memory.MemorySliceInput;
import org.apache.paimon.memory.MemorySliceOutput;

import javax.annotation.Nullable;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Manifest-level min/max metadata for one global index file. */
public class SortedIndexFileMeta {

    private static final byte FORMAT_VERSION_WITH_NULL_FLAGS = 1;
    private static final byte FIRST_KEY_IS_NULL = 1;
    private static final byte LAST_KEY_IS_NULL = 1 << 1;
    private static final byte HAS_BUILD_SCHEMA_ID = 1 << 2;

    @Nullable private final byte[] firstKey;
    @Nullable private final byte[] lastKey;
    private final boolean hasNulls;
    private final boolean ambiguousNullBounds;
    @Nullable private final Long buildSchemaId;

    public SortedIndexFileMeta(
            @Nullable byte[] firstKey, @Nullable byte[] lastKey, boolean hasNulls) {
        this(firstKey, lastKey, hasNulls, false, null);
    }

    private SortedIndexFileMeta(
            @Nullable byte[] firstKey,
            @Nullable byte[] lastKey,
            boolean hasNulls,
            boolean ambiguousNullBounds,
            @Nullable Long buildSchemaId) {
        this.ambiguousNullBounds = ambiguousNullBounds;
        this.firstKey = firstKey;
        this.lastKey = lastKey;
        this.hasNulls = hasNulls;
        this.buildSchemaId = buildSchemaId;
    }

    @Nullable
    public byte[] firstKey() {
        return firstKey;
    }

    @Nullable
    public byte[] getFirstKey() {
        return firstKey;
    }

    @Nullable
    public byte[] lastKey() {
        return lastKey;
    }

    @Nullable
    public byte[] getLastKey() {
        return lastKey;
    }

    public boolean hasNulls() {
        return hasNulls;
    }

    public boolean onlyNulls() {
        return !ambiguousNullBounds && firstKey() == null && lastKey() == null;
    }

    /** Legacy zero-length bounds cannot distinguish all-null files from empty keys plus nulls. */
    public boolean hasAmbiguousNullBounds() {
        return ambiguousNullBounds;
    }

    @Nullable
    public Long buildSchemaId() {
        return buildSchemaId;
    }

    /** Adds the table schema used to encode these keys. */
    public SortedIndexFileMeta withBuildSchemaId(long schemaId) {
        checkState(!ambiguousNullBounds, "Cannot add a build schema to ambiguous key bounds.");
        checkArgument(schemaId >= 0, "Build schema id must be non-negative.");
        return new SortedIndexFileMeta(firstKey, lastKey, hasNulls, ambiguousNullBounds, schemaId);
    }

    private int memorySize() {
        return (firstKey == null ? 0 : firstKey.length)
                + (lastKey == null ? 0 : lastKey.length)
                + (ambiguousNullBounds ? 9 : 11)
                + (buildSchemaId == null ? 0 : Long.BYTES);
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
        // Preserve unknown legacy bounds when metadata is serialized again.
        if (!ambiguousNullBounds) {
            sliceOutput.writeByte(FORMAT_VERSION_WITH_NULL_FLAGS);
            if (buildSchemaId != null) {
                nullKeyFlags |= HAS_BUILD_SCHEMA_ID;
            }
            sliceOutput.writeByte(nullKeyFlags);
            // Existing readers use only the two null flags and ignore the optional suffix.
            if (buildSchemaId != null) {
                sliceOutput.writeLong(buildSchemaId);
            }
        }
        return sliceOutput.toSlice().getHeapMemory();
    }

    public static SortedIndexFileMeta deserialize(byte[] data) {
        MemorySliceInput sliceInput = MemorySlice.wrap(data).toInput();
        int firstKeyLength = sliceInput.readInt();
        byte[] firstKey = readKey(sliceInput, firstKeyLength);
        int lastKeyLength = sliceInput.readInt();
        byte[] lastKey = readKey(sliceInput, lastKeyLength);
        boolean hasNulls = sliceInput.readByte() == 1;
        Long buildSchemaId = null;

        if (sliceInput.available() >= 2) {
            int formatVersion = sliceInput.readByte();
            if (formatVersion == FORMAT_VERSION_WITH_NULL_FLAGS) {
                int nullKeyFlags = sliceInput.readByte();
                firstKey = (nullKeyFlags & FIRST_KEY_IS_NULL) != 0 ? null : firstKey;
                lastKey = (nullKeyFlags & LAST_KEY_IS_NULL) != 0 ? null : lastKey;
                if ((nullKeyFlags & HAS_BUILD_SCHEMA_ID) != 0) {
                    buildSchemaId = sliceInput.readLong();
                }
            }
        } else if (firstKeyLength == 0 && lastKeyLength == 0 && hasNulls) {
            // These bytes also represent empty non-null keys together with nulls. Readers must
            // inspect the index file instead of treating this metadata as proof of all-null data.
            return new SortedIndexFileMeta(null, null, true, true, null);
        }
        return new SortedIndexFileMeta(firstKey, lastKey, hasNulls, false, buildSchemaId);
    }

    private static byte[] readKey(MemorySliceInput sliceInput, int keyLength) {
        return sliceInput.readSlice(keyLength).copyBytes();
    }
}
