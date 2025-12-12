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
import static org.apache.paimon.sst.SstFileWriter.VERSION;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Footer for a sorted file. The layout is as below:
 *
 * <pre>
 *     +---------------------------------------------------------------+
 *     |   File Info Offset : 8 bytes   |   File Info Size : 4 bytes   |
 *     +---------------------------------------------------------------+
 *     | BF offset : 8 bytes | BF size : 4 bytes | BF entries : 8 bytes|
 *     +---------------------------------------------------------------+
 *     |  Root Index Offset : 8 bytes   |   Root Index Size : 4 bytes  |
 *     +---------------------------------------------------------------+
 *     | Idx Num : 4 bytes | Idx Level : 1 byte |  Idx Size : 8 bytes  |
 *     +---------------------------------------------------------------+
 *     | Row Count : 4 bytes  |    Uncompressed Data Size: 8 bytes     |
 *     +---------------------------------------------------------------+
 *     | Compress : 1 byte | Version : 4 bytes |  Magic Num : 4 bytes  |
 *     +---------------------------------------------------------------+
 * </pre>
 */
public class Footer {

    public static final int ENCODED_LENGTH = 78;

    @Nullable private final BloomFilterHandle bloomFilterHandle;
    private final BlockHandle indexBlockHandle;
    private final BlockHandle fileInfoHandle;
    private final int indexEntryNum;
    private final byte indexLevel;
    private final long uncompressedIndexSize;
    private final int dataCount;
    private final long uncompressedDataSize;
    private final BlockCompressionType compressionType;

    Footer(
            BlockHandle fileInfoHandle,
            @Nullable BloomFilterHandle bloomFilterHandle,
            BlockHandle indexBlockHandle,
            int indexEntryNum,
            byte indexLevel,
            long uncompressedIndexSize,
            int dataCount,
            long uncompressedDataSize,
            BlockCompressionType compressionType) {
        this.fileInfoHandle = fileInfoHandle;
        this.bloomFilterHandle = bloomFilterHandle;
        this.indexBlockHandle = indexBlockHandle;
        this.indexEntryNum = indexEntryNum;
        this.indexLevel = indexLevel;
        this.uncompressedIndexSize = uncompressedIndexSize;
        this.dataCount = dataCount;
        this.uncompressedDataSize = uncompressedDataSize;
        this.compressionType = compressionType;
    }

    @Nullable
    public BloomFilterHandle getBloomFilterHandle() {
        return bloomFilterHandle;
    }

    public BlockHandle getIndexBlockHandle() {
        return indexBlockHandle;
    }

    public byte getIndexLevel() {
        return indexLevel;
    }

    public long getUncompressedIndexSize() {
        return uncompressedIndexSize;
    }

    public int getDataCount() {
        return dataCount;
    }

    public long getUncompressedDataSize() {
        return uncompressedDataSize;
    }

    public BlockCompressionType getCompressionType() {
        return compressionType;
    }

    public BlockHandle getFileInfoHandle() {
        return fileInfoHandle;
    }

    public int getIndexEntryNum() {
        return indexEntryNum;
    }

    public static Footer readFooter(MemorySliceInput sliceInput) throws IOException {
        // read fileInfo
        BlockHandle fileInfoHandle = new BlockHandle(sliceInput.readLong(), sliceInput.readInt());

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

        // read statistics
        int indexEntryNum = sliceInput.readInt();
        byte indexLevel = sliceInput.readByte();
        long uncompressedIndexSize = sliceInput.readLong();
        int dataCount = sliceInput.readInt();
        long uncompressedDataSize = sliceInput.readLong();

        // compression
        BlockCompressionType compressionType =
                BlockCompressionType.getCompressionTypeByPersistentId(sliceInput.readByte());

        // skip padding
        sliceInput.setPosition(ENCODED_LENGTH - 8);

        // verify magic number
        // todo: verify version
        int version = sliceInput.readInt();
        int magicNumber = sliceInput.readInt();
        checkArgument(magicNumber == MAGIC_NUMBER, "File is not a table (bad magic number)");

        return new Footer(
                fileInfoHandle,
                bloomFilterHandle,
                indexBlockHandle,
                indexEntryNum,
                indexLevel,
                uncompressedIndexSize,
                dataCount,
                uncompressedDataSize,
                compressionType);
    }

    public static MemorySlice writeFooter(Footer footer) {
        MemorySliceOutput output = new MemorySliceOutput(ENCODED_LENGTH);
        writeFooter(footer, output);
        return output.toSlice();
    }

    public static void writeFooter(Footer footer, MemorySliceOutput sliceOutput) {
        // write file info handle
        sliceOutput.writeLong(footer.fileInfoHandle.offset());
        sliceOutput.writeInt(footer.fileInfoHandle.size());

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

        // write root index
        sliceOutput.writeLong(footer.indexBlockHandle.offset());
        sliceOutput.writeInt(footer.indexBlockHandle.size());

        // statistics
        sliceOutput.writeInt(footer.indexEntryNum);
        sliceOutput.writeByte(footer.indexLevel);
        sliceOutput.writeLong(footer.uncompressedIndexSize);
        sliceOutput.writeInt(footer.dataCount);
        sliceOutput.writeLong(footer.uncompressedDataSize);

        // compressionType
        sliceOutput.writeByte(footer.compressionType.persistentId());

        // write version and magic number
        sliceOutput.writeInt(VERSION);
        sliceOutput.writeInt(MAGIC_NUMBER);
    }

    @Override
    public String toString() {
        return "Footer{"
                + "bloomFilterHandle="
                + bloomFilterHandle
                + ", indexBlockHandle="
                + indexBlockHandle
                + ", fileInfoHandle="
                + fileInfoHandle
                + ", indexEntryNum="
                + indexEntryNum
                + ", indexLevel="
                + indexLevel
                + ", uncompressedIndexSize="
                + uncompressedIndexSize
                + ", dataCount="
                + dataCount
                + ", uncompressedDataSize="
                + uncompressedDataSize
                + ", compressionType="
                + compressionType
                + '}';
    }
}
