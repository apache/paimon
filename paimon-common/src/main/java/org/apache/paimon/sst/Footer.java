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

import org.apache.paimon.compression.BlockCompressionType;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.memory.MemorySliceInput;
import org.apache.paimon.memory.MemorySliceOutput;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.paimon.sst.SstFileWriter.MAGIC_NUMBER;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * A fixed-length Footer for an SST file. The layout is as below:
 *
 * <pre>
 *     +---------------------------------------------------------------+
 *     |   FileInfo Offset : 8 bytes    |    FileInfo size : 4 bytes   |
 *     +---------------------------------------------------------------+
 *     | BF offset : 8 bytes | BF size : 4 bytes | BF entries : 8 bytes|
 *     +---------------------------------------------------------------+
 *     |  IndexBlock Offset : 8 bytes   |   IndexBlock size : 4 bytes  |
 *     +---------------------------------------------------------------+
 *     |   MetaBlock Offset : 8 bytes   |    MetaBlock size : 4 bytes  |
 *     +---------------------------------------------------------------+
 *     | RowCnt : 4 bytes | DataSize : 8 bytes | CompressType : 1 byte |
 *     +---------------------------------------------------------------+
 *     |       Version : 4 bytes        |     Magic Number : 4 bytes   |
 *     +---------------------------------------------------------------+
 * </pre>
 */
public class Footer {

    public static final int ENCODED_LENGTH = 77;
    private final BlockHandle indexBlockHandle;
    private final BlockHandle fileInfoHandle;
    private final int version;
    private final long totalUncompressedBytes;
    private final int rowCount;
    private final BlockCompressionType compressionType;
    @Nullable private final BloomFilterHandle bloomFilterHandle;
    @Nullable private final BlockHandle metaBlockHandle;

    Footer(
            int version,
            long totalUncompressedBytes,
            int rowCount,
            BlockCompressionType compressionType,
            BlockHandle indexBlockHandle,
            BlockHandle fileInfoHandle,
            @Nullable BloomFilterHandle bloomFilterHandle,
            @Nullable BlockHandle metaBlockHandle) {
        this.bloomFilterHandle = bloomFilterHandle;
        this.indexBlockHandle = indexBlockHandle;
        this.metaBlockHandle = metaBlockHandle;
        this.fileInfoHandle = fileInfoHandle;
        this.version = version;
        this.totalUncompressedBytes = totalUncompressedBytes;
        this.rowCount = rowCount;
        this.compressionType = compressionType;
    }

    @Nullable
    public BloomFilterHandle getBloomFilterHandle() {
        return bloomFilterHandle;
    }

    public BlockHandle getIndexBlockHandle() {
        return indexBlockHandle;
    }

    public int getVersion() {
        return version;
    }

    public BlockHandle getFileInfoHandle() {
        return fileInfoHandle;
    }

    @Nullable
    public BlockHandle getMetaBlockHandle() {
        return metaBlockHandle;
    }

    public int getRowCount() {
        return rowCount;
    }

    public long getTotalUncompressedBytes() {
        return totalUncompressedBytes;
    }

    public BlockCompressionType getCompressionType() {
        return compressionType;
    }

    public static Footer readFooter(MemorySliceInput sliceInput) throws IOException {
        // 1. read file info
        BlockHandle fileInfoHandle = new BlockHandle(sliceInput.readLong(), sliceInput.readInt());

        // 2. read optional bloom filter handle
        @Nullable
        BloomFilterHandle bloomFilterHandle =
                new BloomFilterHandle(
                        sliceInput.readLong(), sliceInput.readInt(), sliceInput.readLong());
        if (bloomFilterHandle.offset() == 0
                && bloomFilterHandle.size() == 0
                && bloomFilterHandle.expectedEntries() == 0) {
            bloomFilterHandle = null;
        }

        // 3. read index block handle
        BlockHandle indexBlockHandle = new BlockHandle(sliceInput.readLong(), sliceInput.readInt());

        // 4. read optional metadata handle
        BlockHandle metaBlockHandle = new BlockHandle(sliceInput.readLong(), sliceInput.readInt());
        if (metaBlockHandle.offset() == 0 && metaBlockHandle.size() == 0) {
            metaBlockHandle = null;
        }

        // skip padding
        sliceInput.setPosition(ENCODED_LENGTH - 21);

        int rowCount = sliceInput.readInt();
        long totalUncompressedBytes = sliceInput.readLong();
        BlockCompressionType compressionType =
                BlockCompressionType.getCompressionTypeByPersistentId(
                        sliceInput.readUnsignedByte());
        int version = sliceInput.readInt();

        // verify magic number
        int magicNumber = sliceInput.readInt();
        checkArgument(magicNumber == MAGIC_NUMBER, "File is not a table (bad magic number)");

        return new Footer(
                version,
                totalUncompressedBytes,
                rowCount,
                compressionType,
                indexBlockHandle,
                fileInfoHandle,
                bloomFilterHandle,
                metaBlockHandle);
    }

    public static MemorySlice writeFooter(Footer footer) {
        MemorySliceOutput output = new MemorySliceOutput(ENCODED_LENGTH);
        writeFooter(footer, output);
        return output.toSlice();
    }

    public static void writeFooter(Footer footer, MemorySliceOutput sliceOutput) {
        // 1. write file info
        sliceOutput.writeLong(footer.fileInfoHandle.offset());
        sliceOutput.writeInt(footer.fileInfoHandle.size());

        // 2. write optional bloom filter handle
        if (footer.bloomFilterHandle == null) {
            sliceOutput.writeLong(0);
            sliceOutput.writeInt(0);
            sliceOutput.writeLong(0);
        } else {
            sliceOutput.writeLong(footer.bloomFilterHandle.offset());
            sliceOutput.writeInt(footer.bloomFilterHandle.size());
            sliceOutput.writeLong(footer.bloomFilterHandle.expectedEntries());
        }

        // 3. write index block
        sliceOutput.writeLong(footer.indexBlockHandle.offset());
        sliceOutput.writeInt(footer.indexBlockHandle.size());

        // 4. write optional metadata handle
        if (footer.metaBlockHandle == null) {
            sliceOutput.writeLong(0);
            sliceOutput.writeInt(0);
        } else {
            sliceOutput.writeLong(footer.metaBlockHandle.offset());
            sliceOutput.writeInt(footer.metaBlockHandle.size());
        }

        // 5. write fixed-size statistics
        sliceOutput.writeInt(footer.rowCount);
        sliceOutput.writeLong(footer.totalUncompressedBytes);
        sliceOutput.writeByte(footer.compressionType.persistentId());

        // 6. write version and magic number
        sliceOutput.writeInt(footer.version);
        sliceOutput.writeInt(MAGIC_NUMBER);
    }

    @Override
    public String toString() {
        return "Footer{"
                + "indexBlockHandle="
                + indexBlockHandle
                + ", fileInfoHandle="
                + fileInfoHandle
                + ", version="
                + version
                + ", totalUncompressedBytes="
                + totalUncompressedBytes
                + ", rowCount="
                + rowCount
                + ", compressionType="
                + compressionType
                + ", bloomFilterHandle="
                + bloomFilterHandle
                + ", metaBlockHandle="
                + metaBlockHandle
                + '}';
    }
}
