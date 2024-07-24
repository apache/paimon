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

package org.apache.paimon.cache;

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;

/** cached input stream. */
public class CachedSeekableInputStream extends SeekableInputStream {

    public static final int BLOCK_SIZE = 1024 * 1024;
    public final SeekableInputStream inputStream;
    public final String fileName;
    public final long fileSize;
    private long offsetInStream;
    private final BlockCache<CacheKey, byte[]> blockCache;
    private final byte[] data = new byte[BLOCK_SIZE];

    public CachedSeekableInputStream(
            SeekableInputStream inputStream, Path path, long fileSize, BlockCacheConfig config) {
        this.inputStream = inputStream;
        this.blockCache = BlockCacheManager.getCache(config);
        this.fileName = path.getName();
        this.fileSize = fileSize;
    }

    @Override
    public void seek(long desired) throws IOException {
        inputStream.seek(desired);
        this.offsetInStream = desired;
    }

    @Override
    public long getPos() throws IOException {
        return offsetInStream;
    }

    @Override
    public int read() throws IOException {
        byte[] bytes = new byte[1];
        if (read(bytes, 0, 1) > 0) {
            return bytes[0];
        } else {
            return -1;
        }
    }

    @Override
    public int read(byte[] b, int destOffset, int len) throws IOException {
        final long endOff = Math.min(offsetInStream + len, fileSize);
        long startBlockId = offsetInStream / BLOCK_SIZE;
        long endBlockId = (endOff - 1) / BLOCK_SIZE;
        int totalLength = 0;
        int totalCachedLength = 0;
        for (long i = startBlockId; i <= endBlockId; i++) {
            final long startOffsetOfBlock = i * BLOCK_SIZE;
            final long endOffsetOfBlock = (i + 1) * BLOCK_SIZE;
            CacheKey cacheKey = new CacheKey(fileName, startOffsetOfBlock);
            Optional<byte[]> bytesOp = blockCache.read(cacheKey);
            if (bytesOp.isPresent()) {
                byte[] bytes = bytesOp.get();
                int validOffsetInBlock = (int) (this.offsetInStream - startOffsetOfBlock);
                int validLength =
                        (int)
                                Math.min(
                                        Math.min(endOffsetOfBlock, endOff) - offsetInStream,
                                        bytes.length);
                if (validLength > 0) {
                    System.arraycopy(bytes, validOffsetInBlock, b, destOffset, validLength);

                    totalLength += validLength;
                    totalCachedLength += validLength;
                    destOffset += validLength;
                    offsetInStream += validLength;
                }
            } else {
                // reduce seek call
                if (inputStream.getPos() != startOffsetOfBlock) {
                    inputStream.seek(startOffsetOfBlock);
                }
                int lengthRead = 0;
                int toRead = BLOCK_SIZE;
                int dataOff = 0;
                // ensure BLOCK_SIZE
                int length0;
                while (toRead > 0 && (length0 = inputStream.read(this.data, dataOff, toRead)) > 0) {
                    lengthRead += length0;
                    dataOff += lengthRead;
                    toRead = BLOCK_SIZE - lengthRead;
                }

                if (lengthRead > 0) {
                    int validOffsetInBlock = (int) (this.offsetInStream - startOffsetOfBlock);
                    int validLength =
                            (int)
                                    Math.min(
                                            Math.min(endOffsetOfBlock, endOff) - offsetInStream,
                                            lengthRead);

                    System.arraycopy(data, validOffsetInBlock, b, destOffset, validLength);

                    blockCache.write(
                            new CacheKey(fileName, startOffsetOfBlock),
                            Arrays.copyOf(this.data, lengthRead));

                    // should copy away
                    destOffset += validLength;
                    totalLength += validLength;
                    // align to block size
                    offsetInStream = startOffsetOfBlock + lengthRead;
                }

                // end of file
                if (lengthRead < BLOCK_SIZE) {
                    break;
                }
            }
        }
        return totalLength;
    }

    @Override
    public void close() throws IOException {
        inputStream.close();
    }
}
