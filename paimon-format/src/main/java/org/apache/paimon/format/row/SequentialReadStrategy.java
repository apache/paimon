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

package org.apache.paimon.format.row;

import org.apache.paimon.compression.ZstdBlockDecompressor;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.utils.IOUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.CompletableFuture;

/**
 * Reads blocks sequentially in a background thread.
 *
 * <p>One block is prefetched ahead so that IO and decompression can overlap.
 */
class SequentialReadStrategy implements ReadStrategy {

    private final SeekableInputStream inputStream;
    private final RowBlockIndex blockIndex;
    private final ZstdBlockDecompressor decompressor;
    private final int[] blocksToRead;
    private CompletableFuture<byte[]> nextFuture;
    private int nextSubmit;
    private int nextConsume;

    SequentialReadStrategy(
            SeekableInputStream inputStream, RowBlockIndex blockIndex, int[] blocksToRead) {
        this.inputStream = inputStream;
        this.blockIndex = blockIndex;
        this.decompressor = new ZstdBlockDecompressor();
        this.blocksToRead = blocksToRead;
        this.nextSubmit = 0;
        this.nextConsume = 0;
        submitNext();
    }

    @Override
    public byte[] nextBlock() throws IOException {
        if (nextConsume >= blocksToRead.length) {
            return null;
        }

        byte[] compressed;
        if (nextFuture != null) {
            compressed = BlockPrefetcher.awaitFuture(nextFuture);
            nextFuture = null;
        } else {
            compressed = readBlock(blocksToRead[nextConsume]);
        }
        nextConsume++;
        submitNext();

        int blockIdx = blocksToRead[nextConsume - 1];
        int uncompressedSize = (int) blockIndex.blockUncompressedSize(blockIdx);
        byte[] decompressed = new byte[uncompressedSize];
        decompressor.decompress(compressed, 0, compressed.length, decompressed, 0);
        return decompressed;
    }

    @Override
    public int currentBlockIdx() {
        if (nextConsume <= 0 || nextConsume > blocksToRead.length) {
            return -1;
        }
        return blocksToRead[nextConsume - 1];
    }

    @Override
    public void close() {
        if (nextFuture != null) {
            nextFuture.cancel(true);
            nextFuture = null;
        }
    }

    private void submitNext() {
        if (nextSubmit < blocksToRead.length) {
            int blockIdx = blocksToRead[nextSubmit++];
            nextFuture =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return readBlock(blockIdx);
                                } catch (IOException e) {
                                    throw new UncheckedIOException(e);
                                }
                            },
                            IO_POOL);
        }
    }

    private byte[] readBlock(int blockIdx) throws IOException {
        int compressedSize = (int) blockIndex.blockCompressedSize(blockIdx);
        byte[] buf = new byte[compressedSize];
        inputStream.seek(blockIndex.blockOffset(blockIdx));
        IOUtils.readFully(inputStream, buf);
        return buf;
    }
}
