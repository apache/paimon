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
import org.apache.paimon.memory.MemorySliceOutput;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

/** Test class for {@link IndexIterator} and {@link IndexWriter}. */
public class IndexTest {
    private static final Logger LOG = LoggerFactory.getLogger(IndexTest.class);
    private static final Comparator<MemorySlice> COMPARATOR =
            Comparator.comparingInt(s -> s.readInt(0));

    @Test
    public void testEmptyFile() throws IOException {
        BlockStore blockStore = new BlockStore();
        BlockHandle rootHandle = writeIndex(blockStore, 0, 1000, 16, 1);

        BlockReader rootReader = blockStore.readBlock(rootHandle);
        IndexIterator indexReader = new IndexIterator(rootReader, blockStore, 1);
        Assertions.assertFalse(indexReader.hasNext());
    }

    @Test
    public void testSingleLevel() throws IOException {
        BlockStore blockStore = new BlockStore();
        BlockHandle rootHandle = writeIndex(blockStore, 1000, 1000 * 20, 16, 1);

        BlockReader rootReader = blockStore.readBlock(rootHandle);
        IndexIterator indexReader = new IndexIterator(rootReader, blockStore, 1);
        testIndexReader(indexReader, 999);
    }

    @Test
    public void testMultipleLevels() throws IOException {
        BlockStore blockStore = new BlockStore();

        // 1. two levels
        BlockHandle rootHandle = writeIndex(blockStore, 100000, 1000 * 20, 16, 2);
        BlockReader rootReader = blockStore.readBlock(rootHandle);
        IndexIterator indexReader = new IndexIterator(rootReader, blockStore, 2);
        testIndexReader(indexReader, 100000 - 1);

        // 2. three levels
        blockStore.reset();
        BlockHandle rootHandle1 = writeIndex(blockStore, 100000, 1000 * 2, 16, 3);
        BlockReader rootReader1 = blockStore.readBlock(rootHandle1);
        IndexIterator indexReader1 = new IndexIterator(rootReader1, blockStore, 3);
        testIndexReader(indexReader1, 100000 - 1);
    }

    private void testIndexReader(IndexIterator indexReader, int maxIndex) throws IOException {
        // 1. test random seek and scan
        Random random = new Random();
        MemorySliceOutput keyOut = new MemorySliceOutput(4);
        for (int i = 0; i < 1000; i++) {
            int startIndex = random.nextInt(maxIndex);
            keyOut.reset();
            keyOut.writeInt(startIndex);
            indexReader.seekTo(keyOut.toSlice());
            assertScan(indexReader, (startIndex + 1) / 2, maxIndex);
        }

        // 2. test seek to boundaries
        keyOut.reset();
        keyOut.writeInt(0);
        indexReader.seekTo(keyOut.toSlice());
        assertScan(indexReader, 0, maxIndex);

        keyOut.reset();
        keyOut.writeInt(maxIndex * 2);
        indexReader.seekTo(keyOut.toSlice());
        assertScan(indexReader, maxIndex, maxIndex);

        // 3. test seek out of boundaries
        keyOut.reset();
        keyOut.writeInt(-1);
        indexReader.seekTo(keyOut.toSlice());
        assertScan(indexReader, 0, maxIndex);

        keyOut.reset();
        keyOut.writeInt(maxIndex * 2 + 1);
        indexReader.seekTo(keyOut.toSlice());
        Assertions.assertFalse(indexReader.hasNext());
    }

    private void assertScan(Iterator<BlockEntry> iterator, int startIndex, int endIndex) {
        Assertions.assertTrue(iterator.hasNext());
        MemorySliceOutput keyOut = new MemorySliceOutput(4);
        int index = startIndex;
        while (iterator.hasNext()) {
            BlockEntry entry = iterator.next();
            keyOut.reset();
            keyOut.writeInt(index * 2);
            Assertions.assertEquals(0, COMPARATOR.compare(keyOut.toSlice(), entry.getKey()));
            Assertions.assertEquals(
                    generateBlockHandle(index),
                    BlockHandle.readBlockHandle(entry.getValue().toInput()));
            index++;
        }
        Assertions.assertEquals(
                endIndex,
                index - 1,
                String.format("Expected last entry value %d, but found %d", endIndex, index - 1));
    }

    private BlockHandle writeIndex(
            BlockStore blockStore,
            int entryNum,
            int maxBlockSize,
            int minBlockEntryNum,
            int expectedLevel)
            throws IOException {
        // each index entry is about 12 bytes
        IndexWriter writer = new IndexWriter(blockStore, maxBlockSize, minBlockEntryNum);
        MemorySliceOutput keyOut = new MemorySliceOutput(4);
        for (int i = 0; i < entryNum; i++) {
            keyOut.reset();
            keyOut.writeInt(i * 2);
            writer.addEntry(
                    keyOut.toSlice().copyBytes(),
                    BlockHandle.writeBlockHandle(generateBlockHandle(i)).copyBytes());
        }
        LOG.info(
                "Total index size {}, entry Num {}",
                writer.getUncompressedSize(),
                writer.getEntryNum());
        BlockHandle rootHandle = writer.finish();
        Assertions.assertEquals(expectedLevel, writer.getLevelNum());
        Assertions.assertTrue(writer.finished);
        return rootHandle;
    }

    private BlockHandle generateBlockHandle(int i) {
        return new BlockHandle(i, i);
    }

    /** Mock store for index blocks. */
    private static class BlockStore
            implements IndexIterator.BlockReadFunction, IndexWriter.BlockWriteFunction {
        private static final int SIZE = 1024;
        private final Map<BlockHandle, BlockType> typeMap = new HashMap<>();
        private final Map<BlockHandle, MemorySlice> dataMap = new HashMap<>();

        private long position = 0L;

        @Override
        public BlockReader readBlock(BlockHandle blockHandle) throws IOException {
            return new BlockReader(dataMap.get(blockHandle), COMPARATOR, typeMap.get(blockHandle));
        }

        @Override
        public BlockHandle writeBlock(BlockWriter blockWriter, BlockType blockType)
                throws IOException {
            BlockHandle mockBlockHandle = new BlockHandle(position, SIZE);
            position += SIZE;
            byte[] data = blockWriter.finish().copyBytes();
            blockWriter.reset();

            typeMap.put(mockBlockHandle, blockType);
            dataMap.put(mockBlockHandle, MemorySlice.wrap(data));
            return mockBlockHandle;
        }

        public void reset() {
            typeMap.clear();
            dataMap.clear();
            position = 0L;
        }
    }
}
