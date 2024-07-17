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

package org.apache.paimon.lookup.sort;

import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.memory.MemorySliceInput;
import org.apache.paimon.memory.MemorySliceOutput;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.paimon.lookup.sort.SortLookupStoreWriter.MAGIC_NUMBER;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Footer for a sorted file. */
public class Footer {

    public static final int ENCODED_LENGTH = 36;

    @Nullable private final BloomFilterHandle bloomFilterHandle;
    private final BlockHandle indexBlockHandle;

    Footer(@Nullable BloomFilterHandle bloomFilterHandle, BlockHandle indexBlockHandle) {
        this.bloomFilterHandle = bloomFilterHandle;
        this.indexBlockHandle = indexBlockHandle;
    }

    @Nullable
    public BloomFilterHandle getBloomFilterHandle() {
        return bloomFilterHandle;
    }

    public BlockHandle getIndexBlockHandle() {
        return indexBlockHandle;
    }

    public static Footer readFooter(MemorySliceInput sliceInput) throws IOException {
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

        // skip padding
        sliceInput.setPosition(ENCODED_LENGTH - 4);

        // verify magic number
        int magicNumber = sliceInput.readInt();
        checkArgument(magicNumber == MAGIC_NUMBER, "File is not a table (bad magic number)");

        return new Footer(bloomFilterHandle, indexBlockHandle);
    }

    public static MemorySlice writeFooter(Footer footer) {
        MemorySliceOutput output = new MemorySliceOutput(ENCODED_LENGTH);
        writeFooter(footer, output);
        return output.toSlice();
    }

    public static void writeFooter(Footer footer, MemorySliceOutput sliceOutput) {
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

        // write magic number
        sliceOutput.writeInt(MAGIC_NUMBER);
    }
}
