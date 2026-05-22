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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.NestedProjectedRow;
import org.apache.paimon.utils.RoaringBitmap32;

import javax.annotation.Nullable;

import java.io.IOException;

/** Reader for row-store format files. Reads block by block and returns row iterators. */
public class RowFormatReader implements FileRecordReader<InternalRow> {

    private final Path filePath;
    private final RowFileFooter footer;
    private final RowBlockIndex blockIndex;
    private final RowType rowType;
    @Nullable private final NestedProjectedRow projection;
    @Nullable private final RoaringBitmap32 selection;
    private final BlockPrefetcher prefetcher;

    RowFormatReader(
            SeekableInputStream inputStream,
            Path filePath,
            RowFileFooter footer,
            RowBlockIndex blockIndex,
            RowType rowType,
            @Nullable NestedProjectedRow projection,
            @Nullable RoaringBitmap32 selection) {
        this.filePath = filePath;
        this.footer = footer;
        this.blockIndex = blockIndex;
        this.rowType = rowType;
        this.projection = projection;
        this.selection = selection;
        this.prefetcher =
                new BlockPrefetcher(
                        inputStream,
                        blockIndex,
                        computeBlocksToRead(blockIndex, footer.totalRowCount, selection));
    }

    @Nullable
    @Override
    public FileRecordIterator<InternalRow> readBatch() throws IOException {
        byte[] decompressed = prefetcher.nextBlock();
        if (decompressed == null) {
            return null;
        }

        int blockIdx = prefetcher.currentBlockIdx();
        long blockStartRow = blockIndex.blockRowStart(blockIdx);
        RowBlockReader blockReader = new RowBlockReader(new BlockInput(decompressed), rowType);

        if (selection != null) {
            long blockEndRow = blockEndRow(blockIdx);
            int[] localIndices = computeSelectedLocalIndices(selection, blockStartRow, blockEndRow);
            return new RowFileRecordIterator(
                    filePath, blockReader, projection, blockStartRow, localIndices);
        } else {
            return new RowFileRecordIterator(filePath, blockReader, projection, blockStartRow);
        }
    }

    @Override
    public void close() throws IOException {
        prefetcher.close();
    }

    private long blockEndRow(int blockIdx) {
        if (blockIdx + 1 < blockIndex.blockCount()) {
            return blockIndex.blockRowStart(blockIdx + 1);
        }
        return footer.totalRowCount;
    }

    private static int[] computeBlocksToRead(
            RowBlockIndex blockIndex, long totalRowCount, @Nullable RoaringBitmap32 selection) {
        int blockCount = blockIndex.blockCount();
        if (selection == null) {
            int[] all = new int[blockCount];
            for (int i = 0; i < blockCount; i++) {
                all[i] = i;
            }
            return all;
        }

        int[] blocks = new int[blockCount];
        int count = 0;
        for (int i = 0; i < blockCount; i++) {
            long blockStart = blockIndex.blockRowStart(i);
            long blockEnd = (i + 1 < blockCount) ? blockIndex.blockRowStart(i + 1) : totalRowCount;
            if (selection.intersects(blockStart, blockEnd)) {
                blocks[count++] = i;
            }
        }

        int[] result = new int[count];
        System.arraycopy(blocks, 0, result, 0, count);
        return result;
    }

    private static int[] computeSelectedLocalIndices(
            RoaringBitmap32 selection, long blockStartRow, long blockEndRow) {
        int capacity = (int) (blockEndRow - blockStartRow);
        int[] indices = new int[capacity];
        int count = 0;

        long current = selection.nextValue((int) blockStartRow);
        while (current >= 0 && current < blockEndRow) {
            indices[count++] = (int) (current - blockStartRow);
            if (current == Integer.MAX_VALUE) {
                break;
            }
            current = selection.nextValue((int) current + 1);
        }

        if (count == capacity) {
            return indices;
        }
        int[] result = new int[count];
        System.arraycopy(indices, 0, result, 0, count);
        return result;
    }
}
