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

package org.apache.paimon.io;

import org.apache.paimon.compression.BlockCompressionFactory;
import org.apache.paimon.compression.BlockDecompressor;
import org.apache.paimon.utils.MathUtils;

import java.io.IOException;
import java.io.RandomAccessFile;

/** A class to wrap compressed {@link RandomAccessFile}. */
public class CompressedPageFileInput implements PageFileInput {

    private final RandomAccessFile file;
    private final int pageSize;
    private final long uncompressedBytes;
    private final long[] pagePositions;

    private final BlockDecompressor decompressor;
    private final byte[] uncompressedBuffer;
    private final byte[] compressedBuffer;

    private final int pageSizeBits;
    private final int pageSizeMask;

    public CompressedPageFileInput(
            RandomAccessFile file,
            int pageSize,
            BlockCompressionFactory compressionFactory,
            long uncompressedBytes,
            long[] pagePositions) {
        this.file = file;
        this.pageSize = pageSize;
        this.uncompressedBytes = uncompressedBytes;
        this.pagePositions = pagePositions;

        this.uncompressedBuffer = new byte[pageSize];
        this.decompressor = compressionFactory.getDecompressor();
        this.compressedBuffer =
                new byte[compressionFactory.getCompressor().getMaxCompressedSize(pageSize)];

        this.pageSizeBits = MathUtils.log2strict(pageSize);
        this.pageSizeMask = pageSize - 1;
    }

    @Override
    public RandomAccessFile file() {
        return file;
    }

    @Override
    public long uncompressBytes() {
        return uncompressedBytes;
    }

    @Override
    public int pageSize() {
        return pageSize;
    }

    @Override
    public byte[] readPage(int pageIndex) throws IOException {
        long position = pagePositions[pageIndex];
        file.seek(position);
        int compressLength = file.readInt();
        file.readFully(compressedBuffer, 0, compressLength);

        int uncompressedLength =
                decompressor.decompress(compressedBuffer, 0, compressLength, uncompressedBuffer, 0);

        byte[] result = new byte[uncompressedLength];
        System.arraycopy(uncompressedBuffer, 0, result, 0, uncompressedLength);
        return result;
    }

    @Override
    public byte[] readPosition(long position, int length) throws IOException {
        int offset = (int) (position & this.pageSizeMask);
        int pageIndex = (int) (position >>> this.pageSizeBits);

        byte[] result = new byte[length];
        int n = 0;
        do {
            byte[] page = readPage(pageIndex);
            int currentLength = Math.min(page.length - offset, length - n);
            System.arraycopy(page, offset, result, n, currentLength);
            offset = 0;
            n += currentLength;
            pageIndex++;
        } while (n < length);
        return result;
    }

    @Override
    public void close() throws IOException {
        this.file.close();
    }
}
