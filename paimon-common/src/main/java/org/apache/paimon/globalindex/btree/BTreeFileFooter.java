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
import org.apache.paimon.sst.BlockHandle;
import org.apache.paimon.sst.BloomFilterHandle;

import javax.annotation.Nullable;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** The Footer for BTree file. */
public class BTreeFileFooter {

    public static final int MAGIC_NUMBER = 0x50425449;
    public static final int CURRENT_VERSION = 1;
    public static final int ENCODED_LENGTH = 52;

    private final int version;
    @Nullable private final BloomFilterHandle bloomFilterHandle;
    private final BlockHandle indexBlockHandle;
    @Nullable private final BlockHandle nullBitmapHandle;

    public BTreeFileFooter(
            @Nullable BloomFilterHandle bloomFilterHandle,
            BlockHandle indexBlockHandle,
            BlockHandle nullBitmapHandle) {
        this(CURRENT_VERSION, bloomFilterHandle, indexBlockHandle, nullBitmapHandle);
    }

    public BTreeFileFooter(
            int version,
            @Nullable BloomFilterHandle bloomFilterHandle,
            BlockHandle indexBlockHandle,
            BlockHandle nullBitmapHandle) {
        this.version = version;
        this.bloomFilterHandle = bloomFilterHandle;
        this.indexBlockHandle = indexBlockHandle;
        this.nullBitmapHandle = nullBitmapHandle;
    }

    public int getVersion() {
        return version;
    }

    @Nullable
    public BloomFilterHandle getBloomFilterHandle() {
        return bloomFilterHandle;
    }

    public BlockHandle getIndexBlockHandle() {
        return indexBlockHandle;
    }

    @Nullable
    public BlockHandle getNullBitmapHandle() {
        return nullBitmapHandle;
    }

    public static BTreeFileFooter readFooter(MemorySliceInput sliceInput) {
        // read version and verify magic number
        sliceInput.setPosition(ENCODED_LENGTH - 8);

        int version = sliceInput.readInt();
        int magicNumber = sliceInput.readInt();
        checkArgument(
                magicNumber == MAGIC_NUMBER, "File is not a btree index file (bad magic number)");

        sliceInput.setPosition(0);

        // read bloom filter and index handles
        @Nullable
        BloomFilterHandle bloomFilterHandle =
                new BloomFilterHandle(
                        sliceInput.readLong(), sliceInput.readInt(), sliceInput.readLong());
        if (bloomFilterHandle.offset() == 0
                && bloomFilterHandle.size() == 0
                && bloomFilterHandle.expectedEntries() == 0) {
            bloomFilterHandle = null;
        }
        BlockHandle indexBlockHandle = new BlockHandle(sliceInput.readLong(), sliceInput.readInt());

        @Nullable
        BlockHandle nullBitmapHandle = new BlockHandle(sliceInput.readLong(), sliceInput.readInt());
        if (nullBitmapHandle.offset() == 0 && nullBitmapHandle.size() == 0) {
            nullBitmapHandle = null;
        }

        return new BTreeFileFooter(version, bloomFilterHandle, indexBlockHandle, nullBitmapHandle);
    }

    public static MemorySlice writeFooter(BTreeFileFooter footer) {
        MemorySliceOutput output = new MemorySliceOutput(ENCODED_LENGTH);
        writeFooter(footer, output);
        return output.toSlice();
    }

    public static void writeFooter(BTreeFileFooter footer, MemorySliceOutput sliceOutput) {
        // write bloom filter and index handles
        if (footer.bloomFilterHandle == null) {
            sliceOutput.writeLong(0);
            sliceOutput.writeInt(0);
            sliceOutput.writeLong(0);
        } else {
            sliceOutput.writeLong(footer.bloomFilterHandle.offset());
            sliceOutput.writeInt(footer.bloomFilterHandle.size());
            sliceOutput.writeLong(footer.bloomFilterHandle.expectedEntries());
        }

        sliceOutput.writeLong(footer.indexBlockHandle.offset());
        sliceOutput.writeInt(footer.indexBlockHandle.size());

        if (footer.nullBitmapHandle == null) {
            sliceOutput.writeLong(0);
            sliceOutput.writeInt(0);
        } else {
            sliceOutput.writeLong(footer.nullBitmapHandle.offset());
            sliceOutput.writeInt(footer.nullBitmapHandle.size());
        }

        // write version and magic number
        sliceOutput.writeInt(footer.version);
        sliceOutput.writeInt(MAGIC_NUMBER);
    }
}
