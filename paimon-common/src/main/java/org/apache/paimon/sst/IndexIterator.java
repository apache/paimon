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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An index iterator to iterate over all leaf indices.
 *
 * <p>Note that this class is NOT thread safe.
 */
public class IndexIterator implements Iterator<BlockEntry> {
    private static final Logger LOG = LoggerFactory.getLogger(IndexIterator.class);

    private final BlockReadFunction blockReadFunction;
    private final int level;

    /**
     * An array of block readers. The element at the lowest index is the root index reader, and the
     * element at the highest index is the leaf index reader. At most cases, there will be only one
     * or two readers.
     */
    private final BlockIterator[] readers;

    private boolean seekOutOfBounds;

    public IndexIterator(BlockReader rootReader, BlockReadFunction blockReadFunction, int level)
            throws IOException {
        this.blockReadFunction = blockReadFunction;
        this.level = level;
        this.readers = new BlockIterator[level];
        initializeReaders(rootReader);
    }

    public void initializeReaders(BlockReader rootReader) throws IOException {
        Preconditions.checkState(rootReader.getBlockType().isIndex());
        readers[0] = rootReader.iterator();
        for (int i = 0; i < level - 1; i++) {
            BlockIterator iterator = readers[i];

            Preconditions.checkState(iterator.hasNext(), "Found an empty index block, it's a bug.");
            BlockHandle blockHandle = readHandle(iterator);
            BlockReader childReader = blockReadFunction.readBlock(blockHandle);
            Preconditions.checkState(childReader.getBlockType().isIndex());

            readers[i + 1] = childReader.iterator();
        }
    }

    @Override
    public boolean hasNext() {
        if (seekOutOfBounds) {
            return false;
        }
        // return false only if all iterators are exhausted.
        for (int i = 0; i < level; i++) {
            if (readers[i].hasNext()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public BlockEntry next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        while (true) {
            if (readers[level - 1].hasNext()) {
                return readers[level - 1].next();
            }
            try {
                moveLeafIndex();
            } catch (IOException e) {
                LOG.error("Failed to move leaf index block.", e);
                throw new RuntimeException(e);
            }
        }
    }

    private void moveLeafIndex() throws IOException {
        // 1. search upward from leaf index for available iterator
        //    i >= 0 is guaranteed by this#hasNext() check
        int i = level - 1;
        while (!readers[i].hasNext()) {
            i--;
        }
        // 2. initialize downward from top-level index
        for (; i < level - 1; i++) {
            BlockHandle blockHandle = readHandle(readers[i]);
            readers[i + 1] = blockReadFunction.readBlock(blockHandle).iterator();
        }
    }

    /** Seek to the index entry whose key is exactly equal to or greater than the specified key. */
    public void seekTo(MemorySlice keySlice) throws IOException {
        seekOutOfBounds = false;
        for (int i = 0; i < level; i++) {
            readers[i].seekTo(keySlice);
            if (!readers[i].hasNext()) {
                seekOutOfBounds = true;
                break;
            }
            if (i < level - 1) {
                BlockHandle blockHandle = readHandle(readers[i]);
                readers[i + 1] = blockReadFunction.readBlock(blockHandle).iterator();
            }
        }
    }

    private BlockHandle readHandle(BlockIterator blockIterator) {
        BlockEntry entry = blockIterator.next();
        return BlockHandle.readBlockHandle(entry.getValue().toInput());
    }

    /** An interface for block read function. */
    public interface BlockReadFunction {
        BlockReader readBlock(BlockHandle blockHandle) throws IOException;
    }
}
