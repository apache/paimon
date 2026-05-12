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

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IteratorResultIterator;
import org.apache.paimon.utils.IteratorWithException;

import com.github.luben.zstd.Zstd;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Set;

import static org.apache.paimon.format.mosaic.MosaicSpec.COMPRESSION_NONE;
import static org.apache.paimon.format.mosaic.MosaicSpec.COMPRESSION_ZSTD;
import static org.apache.paimon.format.mosaic.MosaicUtils.readLong;
import static org.apache.paimon.format.mosaic.MosaicUtils.readVarint;

/** Reader for the Mosaic file format with row group support. */
public class MosaicReader implements FileRecordReader<InternalRow> {

    private final Path filePath;
    private final SeekableInputStream inputStream;
    private final RowType projectedRowType;

    private MosaicSchema schema;
    private byte compression;
    private int[] sortedRequiredBuckets;
    private MosaicSpec.RowGroupMeta[] rowGroupMetas;
    private MosaicBucketReader[] bucketReaders;
    private int currentRowGroup;
    private byte[] compressedBuf;

    public MosaicReader(FileIO fileIO, Path filePath, long fileSize, RowType projectedRowType)
            throws IOException {
        this.filePath = filePath;
        this.inputStream = fileIO.newInputStream(filePath);
        this.projectedRowType = projectedRowType;
        this.currentRowGroup = 0;

        readFooterAndInit(fileSize);
    }

    private void readFooterAndInit(long fileSize) throws IOException {
        // Read footer (last 32 bytes)
        inputStream.seek(fileSize - MosaicSpec.FOOTER_SIZE);
        byte[] footerBytes = new byte[MosaicSpec.FOOTER_SIZE];
        readFully(footerBytes);

        ByteBuffer footer = ByteBuffer.wrap(footerBytes).order(ByteOrder.BIG_ENDIAN);
        long indexOffset = footer.getLong();
        long schemaBlockOffset = footer.getLong();
        int numBuckets = footer.getInt();
        int numRowGroups = footer.getInt();
        this.compression = footer.get();
        byte version = footer.get();
        footer.getShort(); // padding
        byte[] magic = new byte[4];
        footer.get(magic);

        if (magic[0] != 'M' || magic[1] != 'O' || magic[2] != 'S' || magic[3] != 'A') {
            throw new IOException("Invalid Mosaic file: bad magic bytes");
        }

        if (version != MosaicSpec.VERSION) {
            throw new IOException(
                    "Unsupported Mosaic file version: "
                            + version
                            + ", expected: "
                            + MosaicSpec.VERSION);
        }

        // Read schema block
        inputStream.seek(schemaBlockOffset);
        int schemaUncompressedSize = readInt();
        int schemaCompressedSize = (int) (indexOffset - schemaBlockOffset - 4);
        byte[] schemaCompressed = new byte[schemaCompressedSize];
        readFully(schemaCompressed);

        byte[] schemaRaw;
        switch (compression) {
            case COMPRESSION_NONE:
                schemaRaw = schemaCompressed;
                break;
            case COMPRESSION_ZSTD:
                schemaRaw = new byte[schemaUncompressedSize];
                Zstd.decompress(schemaRaw, schemaCompressed);
                break;
            default:
                throw new UnsupportedEncodingException("Unsupported compression: " + compression);
        }
        this.schema = MosaicSchema.deserialize(schemaRaw);

        // Determine which buckets we need
        Set<Integer> requiredBuckets = schema.getRequiredBuckets(projectedRowType);

        // Read row group index (varint encoded, only non-empty buckets)
        inputStream.seek(indexOffset);
        int indexSize = (int) (fileSize - MosaicSpec.FOOTER_SIZE - indexOffset);
        byte[] indexBytes = new byte[indexSize];
        readFully(indexBytes);
        int[] idxPos = {0};

        this.rowGroupMetas = new MosaicSpec.RowGroupMeta[numRowGroups];
        for (int rg = 0; rg < numRowGroups; rg++) {
            int numRows = readVarint(indexBytes, idxPos);
            int nonEmpty = readVarint(indexBytes, idxPos);

            long[] bucketOffsets = new long[numBuckets];
            int[] compressedSizes = new int[numBuckets];
            int[] uncompressedSizes = new int[numBuckets];

            for (int i = 0; i < nonEmpty; i++) {
                int bucketId = readVarint(indexBytes, idxPos);
                bucketOffsets[bucketId] = readLong(indexBytes, idxPos);
                compressedSizes[bucketId] = readVarint(indexBytes, idxPos);
                uncompressedSizes[bucketId] = readVarint(indexBytes, idxPos);
            }

            rowGroupMetas[rg] =
                    new MosaicSpec.RowGroupMeta(
                            numRows, bucketOffsets, compressedSizes, uncompressedSizes);
        }

        this.bucketReaders = new MosaicBucketReader[numBuckets];
        int count = 0;
        for (int b : requiredBuckets) {
            DataType[] bucketTypes = schema.getBucketColumnTypes(b);
            int[] projMapping = schema.getProjectionMapping(b, projectedRowType);
            if (projMapping != null) {
                bucketReaders[b] = new MosaicBucketReader(bucketTypes, projMapping);
                count++;
            }
        }
        this.sortedRequiredBuckets = new int[count];
        int idx = 0;
        for (int b : requiredBuckets) {
            if (bucketReaders[b] != null) {
                sortedRequiredBuckets[idx++] = b;
            }
        }
        this.compressedBuf = new byte[0];
    }

    @Nullable
    @Override
    public FileRecordIterator<InternalRow> readBatch() throws IOException {
        if (currentRowGroup >= rowGroupMetas.length) {
            return null;
        }

        MosaicSpec.RowGroupMeta meta = rowGroupMetas[currentRowGroup++];
        if (meta.numRows == 0) {
            return readBatch();
        }

        final MosaicBucketReader[] readers = this.bucketReaders;

        // Sort required buckets by file offset for sequential I/O
        int[] ordered = Arrays.copyOf(sortedRequiredBuckets, sortedRequiredBuckets.length);
        final long[] offsets = meta.bucketOffsets;
        // insertion sort — array is small (number of projected buckets)
        for (int i = 1; i < ordered.length; i++) {
            int key = ordered[i];
            long keyOff = offsets[key];
            int j = i - 1;
            while (j >= 0 && offsets[ordered[j]] > keyOff) {
                ordered[j + 1] = ordered[j];
                j--;
            }
            ordered[j + 1] = key;
        }

        int activeCount = 0;
        int[] activeBuckets = new int[ordered.length];

        for (int i = 0; i < ordered.length; i++) {
            int b = ordered[i];
            if (meta.compressedSizes[b] == 0) {
                continue;
            }

            int compSize = meta.compressedSizes[b];
            inputStream.seek(meta.bucketOffsets[b]);

            byte[] bucketData;
            switch (compression) {
                case COMPRESSION_NONE:
                    bucketData = new byte[compSize];
                    readFully(bucketData);
                    break;
                case COMPRESSION_ZSTD:
                    if (compressedBuf.length < compSize) {
                        compressedBuf = new byte[compSize];
                    }
                    readFully(compressedBuf, compSize);
                    int uncompSize = meta.uncompressedSizes[b];
                    bucketData = new byte[uncompSize];
                    Zstd.decompressByteArray(bucketData, 0, uncompSize, compressedBuf, 0, compSize);
                    break;
                default:
                    throw new UnsupportedEncodingException(
                            "Unsupported compression: " + compression);
            }

            readers[b].init(bucketData, meta.numRows);
            activeBuckets[activeCount++] = b;
        }

        final int totalRows = meta.numRows;
        final int[] active = Arrays.copyOf(activeBuckets, activeCount);
        final int projectedFieldCount = projectedRowType.getFieldCount();

        IteratorWithException<InternalRow, IOException> iter =
                new IteratorWithException<InternalRow, IOException>() {
                    int currentRow = 0;
                    final Object[] fields = new Object[projectedFieldCount];

                    @Override
                    public boolean hasNext() {
                        return currentRow < totalRows;
                    }

                    @Override
                    public InternalRow next() throws IOException {
                        Arrays.fill(fields, null);
                        for (int i = 0; i < active.length; i++) {
                            readers[active[i]].readRow(fields);
                        }
                        currentRow++;
                        return GenericRow.of(fields);
                    }
                };

        return new IteratorResultIterator(iter, null, filePath, 0);
    }

    @Override
    public void close() throws IOException {
        inputStream.close();
    }

    private void readFully(byte[] buf) throws IOException {
        readFully(buf, buf.length);
    }

    private void readFully(byte[] buf, int len) throws IOException {
        int offset = 0;
        while (offset < len) {
            int read = inputStream.read(buf, offset, len - offset);
            if (read < 0) {
                throw new IOException("Unexpected EOF");
            }
            offset += read;
        }
    }

    private int readInt() throws IOException {
        byte[] buf = new byte[4];
        readFully(buf);
        return ByteBuffer.wrap(buf).order(ByteOrder.BIG_ENDIAN).getInt();
    }
}
