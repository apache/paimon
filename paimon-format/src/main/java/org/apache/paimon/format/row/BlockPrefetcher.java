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

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Prefetches and decompresses blocks with IO coalescing and concurrent reading.
 *
 * <p>Adjacent blocks are merged into larger IO requests to reduce QPS. Multiple merged ranges are
 * prefetched concurrently using separate InputStreams.
 */
class BlockPrefetcher implements Closeable {

    private static final int HOLE_SIZE_LIMIT = 256 * 1024;
    private static final int RANGE_SIZE_LIMIT = 2 * 1024 * 1024;
    private static final int PREFETCH_COUNT = 4;

    private static final ExecutorService IO_POOL = Executors.newCachedThreadPool();

    private final InputStreamPool streamPool;
    private final RowBlockIndex blockIndex;
    private final ZstdBlockDecompressor decompressor;

    private final List<MergedRange> mergedRanges;
    private final Queue<CompletableFuture<byte[]>> prefetchQueue;
    private int nextRangeToSubmit;
    private int currentRangeIdx;
    private byte[] currentRangeData;
    private int currentBlockInRange;

    BlockPrefetcher(InputStreamPool streamPool, RowBlockIndex blockIndex, int[] blocksToRead) {
        this.streamPool = streamPool;
        this.blockIndex = blockIndex;
        this.decompressor = new ZstdBlockDecompressor();
        this.mergedRanges = coalesceRanges(blocksToRead, blockIndex);
        this.prefetchQueue = new ArrayDeque<>(PREFETCH_COUNT);
        this.nextRangeToSubmit = 0;
        this.currentRangeIdx = -1;
        this.currentBlockInRange = 0;

        fillPrefetch();
    }

    byte[] nextBlock() throws IOException {
        if (currentRangeIdx < 0
                || currentBlockInRange >= mergedRanges.get(currentRangeIdx).blockIndices.length) {
            advanceToNextRange();
        }
        if (currentRangeIdx >= mergedRanges.size()) {
            return null;
        }

        MergedRange range = mergedRanges.get(currentRangeIdx);
        int blockIdx = range.blockIndices[currentBlockInRange];
        int offsetInBuf = (int) (blockIndex.blockOffset(blockIdx) - range.offset);
        int compressedSize = (int) blockIndex.blockCompressedSize(blockIdx);
        int uncompressedSize = (int) blockIndex.blockUncompressedSize(blockIdx);

        byte[] decompressed = new byte[uncompressedSize];
        decompressor.decompress(currentRangeData, offsetInBuf, compressedSize, decompressed, 0);

        currentBlockInRange++;
        return decompressed;
    }

    int currentBlockIdx() {
        if (currentRangeIdx < 0 || currentRangeIdx >= mergedRanges.size()) {
            return -1;
        }
        MergedRange range = mergedRanges.get(currentRangeIdx);
        return range.blockIndices[currentBlockInRange - 1];
    }

    @Override
    public void close() throws IOException {
        for (CompletableFuture<byte[]> f : prefetchQueue) {
            f.cancel(true);
        }
        prefetchQueue.clear();
        streamPool.close();
    }

    private void advanceToNextRange() throws IOException {
        currentRangeIdx++;
        currentBlockInRange = 0;

        if (currentRangeIdx >= mergedRanges.size()) {
            currentRangeData = null;
            return;
        }

        CompletableFuture<byte[]> future = prefetchQueue.poll();
        if (future == null) {
            currentRangeData = readRange(mergedRanges.get(currentRangeIdx));
        } else {
            try {
                currentRangeData = future.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for prefetch", e);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof IOException) {
                    throw (IOException) cause;
                }
                throw new IOException("Prefetch failed", cause);
            }
        }
        fillPrefetch();
    }

    private void fillPrefetch() {
        while (prefetchQueue.size() < PREFETCH_COUNT && nextRangeToSubmit < mergedRanges.size()) {
            int rangeIdx = nextRangeToSubmit++;
            MergedRange range = mergedRanges.get(rangeIdx);
            prefetchQueue.add(
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return readRange(range);
                                } catch (IOException e) {
                                    throw new UncheckedIOException(e);
                                }
                            },
                            IO_POOL));
        }
    }

    private byte[] readRange(MergedRange range) throws IOException {
        byte[] buf = new byte[range.length];
        SeekableInputStream in = streamPool.borrow();
        try {
            in.seek(range.offset);
            IOUtils.readFully(in, buf);
        } finally {
            streamPool.returnStream(in);
        }
        return buf;
    }

    // ======================== Range Coalescing ========================

    static List<MergedRange> coalesceRanges(int[] blocksToRead, RowBlockIndex blockIndex) {
        List<MergedRange> result = new ArrayList<>();
        if (blocksToRead.length == 0) {
            return result;
        }

        int rangeStart = 0;
        long rangeOffset = blockIndex.blockOffset(blocksToRead[0]);
        long rangeEnd = rangeOffset + blockIndex.blockCompressedSize(blocksToRead[0]);

        for (int i = 1; i < blocksToRead.length; i++) {
            int blockIdx = blocksToRead[i];
            long blockOffset = blockIndex.blockOffset(blockIdx);
            long blockEnd = blockOffset + blockIndex.blockCompressedSize(blockIdx);
            long gap = blockOffset - rangeEnd;
            long newLength = blockEnd - rangeOffset;

            if (gap < HOLE_SIZE_LIMIT && newLength <= RANGE_SIZE_LIMIT) {
                rangeEnd = blockEnd;
            } else {
                result.add(buildRange(blocksToRead, rangeStart, i, rangeOffset, rangeEnd));
                rangeStart = i;
                rangeOffset = blockOffset;
                rangeEnd = blockEnd;
            }
        }
        result.add(
                buildRange(blocksToRead, rangeStart, blocksToRead.length, rangeOffset, rangeEnd));
        return result;
    }

    private static MergedRange buildRange(
            int[] blocksToRead, int from, int to, long rangeOffset, long rangeEnd) {
        int[] indices = new int[to - from];
        System.arraycopy(blocksToRead, from, indices, 0, indices.length);
        return new MergedRange(rangeOffset, (int) (rangeEnd - rangeOffset), indices);
    }

    // ======================== MergedRange ========================

    static class MergedRange {
        final long offset;
        final int length;
        final int[] blockIndices;

        MergedRange(long offset, int length, int[] blockIndices) {
            this.offset = offset;
            this.length = length;
            this.blockIndices = blockIndices;
        }
    }
}
