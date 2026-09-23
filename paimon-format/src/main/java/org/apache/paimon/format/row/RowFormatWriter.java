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

import org.apache.paimon.compression.ZstdBlockCompressor;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.LongArrayList;

import java.io.IOException;

/** Writer that produces row-store format files with block-level ZSTD compression. */
public class RowFormatWriter implements FormatWriter {

    private final PositionOutputStream out;
    private final ZstdBlockCompressor compressor;
    private final int blockSizeThreshold;
    private final RowBlockWriter blockWriter;

    private final LongArrayList blockCompressedSizes;
    private final LongArrayList blockUncompressedSizes;
    private final LongArrayList blockRowStarts;

    private long totalRowCount;

    public RowFormatWriter(
            PositionOutputStream out, RowType rowType, int blockSize, int zstdLevel) {
        this.out = out;
        this.compressor = new ZstdBlockCompressor(zstdLevel);
        this.blockSizeThreshold = blockSize;
        this.blockWriter = new RowBlockWriter(new BlockOutput(blockSize), rowType);
        this.blockCompressedSizes = new LongArrayList(128);
        this.blockUncompressedSizes = new LongArrayList(128);
        this.blockRowStarts = new LongArrayList(128);
    }

    @Override
    public void addElement(InternalRow element) throws IOException {
        blockWriter.writeRow(element);
        totalRowCount++;

        if (blockWriter.estimatedSize() >= blockSizeThreshold) {
            flushBlock();
        }
    }

    @Override
    public boolean reachTargetSize(boolean suggestedCheck, long targetSize) throws IOException {
        if (!suggestedCheck) {
            return false;
        }
        return out.getPos() >= targetSize;
    }

    @Override
    public void close() throws IOException {
        flushBlock();

        long indexOffset = out.getPos();
        RowBlockIndex index =
                new RowBlockIndex(
                        blockCompressedSizes.toArray(),
                        blockUncompressedSizes.toArray(),
                        blockRowStarts.toArray());
        index.writeTo(out);
        int indexLength = (int) (out.getPos() - indexOffset);

        RowFileFooter footer =
                new RowFileFooter(
                        totalRowCount, blockCompressedSizes.size(), indexOffset, indexLength);
        footer.writeTo(out);

        out.flush();
    }

    private void flushBlock() throws IOException {
        if (blockWriter.rowCount() == 0) {
            return;
        }

        blockRowStarts.add(totalRowCount - blockWriter.rowCount());

        byte[] uncompressed = blockWriter.finish();
        blockUncompressedSizes.add(uncompressed.length);

        int maxCompressedSize = compressor.getMaxCompressedSize(uncompressed.length);
        byte[] compressed = new byte[maxCompressedSize];
        int compressedLen =
                compressor.compress(uncompressed, 0, uncompressed.length, compressed, 0);

        out.write(compressed, 0, compressedLen);
        blockCompressedSizes.add(compressedLen);

        blockWriter.reset();
    }
}
