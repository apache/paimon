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

import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.utils.Preconditions;

import java.io.IOException;
import java.util.Iterator;

/**
 * An index writer to write leveled data index to SST Files.
 *
 * <p>Note that this class is NOT thread safe.
 */
public class IndexWriter {
    private final BlockWriteFunction blockWriteFunction;
    private final int maxIndexBlockSize;
    private final int minEntryNum;
    private final BlockWriter currentLeafIndexWriter;
    private BlockWriter lazyRootIndexWriter;
    private long uncompressedSize;
    private int entryNum;
    private byte[] lastKey;
    int levelNum = 1;
    boolean finished;

    public IndexWriter(
            BlockWriteFunction blockWriteFunction, int maxIndexBlockSize, int minEntryNum) {
        this.blockWriteFunction = blockWriteFunction;
        this.maxIndexBlockSize = maxIndexBlockSize;
        this.minEntryNum = minEntryNum;
        this.currentLeafIndexWriter = new BlockWriter(maxIndexBlockSize);
        this.finished = false;
    }

    public void addEntry(byte[] key, byte[] blockHandleBytes) throws IOException {
        checkFinished();

        currentLeafIndexWriter.add(key, blockHandleBytes);
        this.lastKey = key;

        if (currentLeafIndexWriter.memory() >= maxIndexBlockSize) {
            flush();
        }
    }

    private void flush() throws IOException {
        flush(lastKey, BlockType.LEAF_INDEX);
    }

    /**
     * Flush current leaf index writer to SST File, and then add an {@link BlockHandle} to current
     * root index writer.
     *
     * @param lastKey key of the index entry
     * @param blockType block type, could be either {@link BlockType#LEAF_INDEX} or {@link
     *     BlockType#INTERMEDIATE_INDEX}
     */
    private void flush(byte[] lastKey, BlockType blockType) throws IOException {
        if (currentLeafIndexWriter.size() == 0) {
            return;
        }

        // 1. flush current leaf block to file
        updateStats(currentLeafIndexWriter);
        BlockHandle indexBlockHandle =
                blockWriteFunction.writeBlock(currentLeafIndexWriter, blockType);

        currentLeafIndexWriter.reset();

        // 2. write index block handle to parent Block
        if (lazyRootIndexWriter == null) {
            lazyRootIndexWriter = new BlockWriter(maxIndexBlockSize);
        }
        lazyRootIndexWriter.add(
                lastKey, BlockHandle.writeBlockHandle(indexBlockHandle).copyBytes());
    }

    public BlockHandle finish() throws IOException {
        checkFinished();
        finished = true;

        // single-leveled
        if (lazyRootIndexWriter == null) {
            updateStats(currentLeafIndexWriter);
            return blockWriteFunction.writeBlock(currentLeafIndexWriter, BlockType.LEAF_INDEX);
        }

        // multiple-leveled
        levelNum++;
        flush();
        while (lazyRootIndexWriter.memory() >= maxIndexBlockSize) {
            spillRootIndex();
        }

        Preconditions.checkState(levelNum < Byte.MAX_VALUE, "To much levels to fit into byte.");

        // at last, write the root index
        updateStats(lazyRootIndexWriter);
        return blockWriteFunction.writeBlock(lazyRootIndexWriter, BlockType.ROOT_INDEX);
    }

    /**
     * Spill current root data index into SST File. This function will sequentially consume current
     * root index, generating a sort of intermediate index blocks and then replace current root
     * index with the new one. For efficiency, we reuse the {@code currentLeafIndexWriter} to write
     * intermediate index blocks.
     */
    private void spillRootIndex() throws IOException {
        Preconditions.checkNotNull(lazyRootIndexWriter);

        MemorySlice rootIndexSlice = lazyRootIndexWriter.finish();
        lazyRootIndexWriter.reset();
        Iterator<BlockEntry> reader =
                BlockIterator.SequentialBlockIterator.wrap(
                        new BlockReader(rootIndexSlice, null, BlockType.ROOT_INDEX).iterator());
        Preconditions.checkState(reader.hasNext());

        int entryNum = 0;
        byte[] key = null;
        while (reader.hasNext()) {
            BlockEntry entry = reader.next();
            key = entry.getKey().copyBytes();
            entryNum++;
            currentLeafIndexWriter.add(key, entry.getValue().copyBytes());

            if (entryNum >= minEntryNum && currentLeafIndexWriter.memory() >= maxIndexBlockSize) {
                flush(key, BlockType.INTERMEDIATE_INDEX);
                entryNum = 0;
            }
        }

        if (currentLeafIndexWriter.size() > 0) {
            flush(key, BlockType.INTERMEDIATE_INDEX);
        }

        levelNum++;
    }

    private void checkFinished() {
        if (finished) {
            throw new IllegalStateException("IndexWriter has already finished");
        }
    }

    private void updateStats(BlockWriter blockWriter) {
        uncompressedSize += blockWriter.memory();
        entryNum += blockWriter.size();
    }

    public byte getLevelNum() {
        return (byte) levelNum;
    }

    public int getEntryNum() {
        return entryNum;
    }

    public long getUncompressedSize() {
        return uncompressedSize;
    }

    /** The block writer function interface. */
    public interface BlockWriteFunction {
        BlockHandle writeBlock(BlockWriter blockWriter, BlockType blockType) throws IOException;
    }
}
