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

package org.apache.paimon.format.mosaic;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.mosaic.MosaicSpec.RowGroupMeta;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.types.RowType;

import com.github.luben.zstd.Zstd;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.format.mosaic.MosaicSpec.COMPRESSION_NONE;
import static org.apache.paimon.format.mosaic.MosaicSpec.COMPRESSION_ZSTD;
import static org.apache.paimon.format.mosaic.MosaicUtils.writeLong;
import static org.apache.paimon.format.mosaic.MosaicUtils.writeVarint;

/** Writer for the Mosaic file format with row group support. */
public class MosaicWriter implements FormatWriter {

    private final PositionOutputStream out;
    private final MosaicSchema schema;
    private final MosaicBucketWriter[] bucketWriters;
    private final int numBuckets;
    private final int zstdLevel;
    private final byte compressionByte;
    private final long rowGroupMaxSize;

    private final List<RowGroupMeta> rowGroupMetas;
    private byte[] compressBuffer;
    private int currentRowGroupRows;
    private long currentBufferedSize;
    private double compressionRatio;
    private boolean closed;

    public MosaicWriter(
            PositionOutputStream out,
            RowType rowType,
            int numBuckets,
            int zstdLevel,
            String compression,
            long rowGroupMaxSize,
            int maxDictTotalBytes,
            int maxDictEntries) {
        this.out = out;
        this.numBuckets = Math.min(numBuckets, rowType.getFieldCount());
        this.zstdLevel = zstdLevel;
        this.compressionByte = MosaicSpec.compressionToByte(compression);
        this.rowGroupMaxSize = rowGroupMaxSize;
        this.schema = MosaicSchema.create(rowType, this.numBuckets);
        this.bucketWriters = new MosaicBucketWriter[this.numBuckets];

        int[][] bucketMapping = schema.bucketToGlobalIndices();
        for (int b = 0; b < this.numBuckets; b++) {
            if (bucketMapping[b].length > 0) {
                bucketWriters[b] =
                        new MosaicBucketWriter(
                                rowType, bucketMapping[b], maxDictTotalBytes, maxDictEntries);
            }
        }

        this.rowGroupMetas = new ArrayList<>();
        this.compressBuffer = new byte[0];
        this.currentRowGroupRows = 0;
        this.currentBufferedSize = 0;
        this.compressionRatio = this.compressionByte == COMPRESSION_NONE ? 1.0 : 0.3;
        this.closed = false;
    }

    @Override
    public void addElement(InternalRow element) throws IOException {
        long size = 0;
        for (int i = 0; i < numBuckets; i++) {
            if (bucketWriters[i] != null) {
                size += bucketWriters[i].writeRow(element);
            }
        }
        currentRowGroupRows++;
        currentBufferedSize += size;

        if (currentBufferedSize >= rowGroupMaxSize) {
            flushRowGroup();
        }
    }

    @Override
    public boolean reachTargetSize(boolean suggestedCheck, long targetSize) throws IOException {
        long estimatedSize = out.getPos() + (long) (currentBufferedSize * compressionRatio);
        return estimatedSize >= targetSize;
    }

    private void flushRowGroup() throws IOException {
        if (currentRowGroupRows == 0) {
            return;
        }

        long[] bucketOffsets = new long[numBuckets];
        int[] compressedSizes = new int[numBuckets];
        int[] uncompressedSizes = new int[numBuckets];

        for (int b = 0; b < numBuckets; b++) {
            MosaicBucketWriter bucketWriter = bucketWriters[b];
            if (bucketWriter == null || bucketWriter.isEmpty()) {
                continue;
            }
            byte[] raw = bucketWriter.finish();
            compressedSizes[b] = writeCompressed(raw);
            uncompressedSizes[b] = raw.length;
            bucketOffsets[b] = out.getPos() - compressedSizes[b];
            bucketWriter.reset();
        }

        rowGroupMetas.add(
                new RowGroupMeta(
                        currentRowGroupRows, bucketOffsets, compressedSizes, uncompressedSizes));

        long totalCompressed = 0;
        long totalUncompressed = 0;
        for (int b = 0; b < numBuckets; b++) {
            totalCompressed += compressedSizes[b];
            totalUncompressed += uncompressedSizes[b];
        }
        if (totalUncompressed > 0) {
            compressionRatio = (double) totalCompressed / totalUncompressed;
        }

        currentRowGroupRows = 0;
        currentBufferedSize = 0;
    }

    private int writeCompressed(byte[] raw) throws IOException {
        switch (compressionByte) {
            case COMPRESSION_NONE:
                out.write(raw);
                return raw.length;
            case COMPRESSION_ZSTD:
                int bound = (int) Zstd.compressBound(raw.length);
                if (compressBuffer.length < bound) {
                    compressBuffer = new byte[bound];
                }
                int compLen = (int) Zstd.compress(compressBuffer, raw, zstdLevel);
                out.write(compressBuffer, 0, compLen);
                return compLen;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported compression: " + compressionByte);
        }
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;

        // Flush remaining rows as the last row group
        flushRowGroup();

        byte[] schemaRaw = schema.serialize();
        long schemaBlockOffset = out.getPos();
        switch (compressionByte) {
            case COMPRESSION_NONE:
                {
                    ByteBuffer lenBuf = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
                    lenBuf.putInt(schemaRaw.length);
                    out.write(lenBuf.array());
                    out.write(schemaRaw);
                    break;
                }
            case COMPRESSION_ZSTD:
                {
                    int schemaBound = (int) Zstd.compressBound(schemaRaw.length);
                    if (compressBuffer.length < schemaBound) {
                        compressBuffer = new byte[schemaBound];
                    }
                    long compLen = Zstd.compress(compressBuffer, schemaRaw, zstdLevel);
                    ByteBuffer lenBuf = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
                    lenBuf.putInt(schemaRaw.length);
                    out.write(lenBuf.array());
                    out.write(compressBuffer, 0, (int) compLen);
                    break;
                }
            default:
                throw new UnsupportedOperationException(
                        "Unsupported compression: " + compressionByte);
        }

        // Write row group index (varint encoded, only non-empty buckets)
        long indexOffset = out.getPos();
        int numRowGroups = rowGroupMetas.size();
        byte[] indexBuf = new byte[numRowGroups * (5 + numBuckets * 25)];
        int idxPos = 0;
        for (RowGroupMeta meta : rowGroupMetas) {
            idxPos = writeVarint(indexBuf, idxPos, meta.numRows);
            int nonEmpty = 0;
            for (int b = 0; b < numBuckets; b++) {
                if (meta.compressedSizes[b] > 0) {
                    nonEmpty++;
                }
            }
            idxPos = writeVarint(indexBuf, idxPos, nonEmpty);
            for (int b = 0; b < numBuckets; b++) {
                if (meta.compressedSizes[b] > 0) {
                    idxPos = writeVarint(indexBuf, idxPos, b);
                    idxPos = writeLong(indexBuf, idxPos, meta.bucketOffsets[b]);
                    idxPos = writeVarint(indexBuf, idxPos, meta.compressedSizes[b]);
                    idxPos = writeVarint(indexBuf, idxPos, meta.uncompressedSizes[b]);
                }
            }
        }
        out.write(indexBuf, 0, idxPos);

        // Write footer
        ByteBuffer footer = ByteBuffer.allocate(MosaicSpec.FOOTER_SIZE).order(ByteOrder.BIG_ENDIAN);
        footer.putLong(indexOffset);
        footer.putLong(schemaBlockOffset);
        footer.putInt(numBuckets);
        footer.putInt(numRowGroups);
        footer.put(compressionByte);
        footer.put(MosaicSpec.VERSION);
        footer.putShort((short) 0);
        footer.put(MosaicSpec.MAGIC);
        out.write(footer.array());

        out.flush();
    }
}
