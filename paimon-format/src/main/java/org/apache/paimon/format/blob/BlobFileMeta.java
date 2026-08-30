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

package org.apache.paimon.format.blob;

import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.memory.BytesUtils;
import org.apache.paimon.utils.DeltaVarintCompressor;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.RoaringBitmap32;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Iterator;

/** Metadata of blob file. */
public class BlobFileMeta {

    private static final int FILE_FOOTER_LENGTH = Integer.BYTES + Byte.BYTES;
    private static final int MIN_RECORD_LENGTH = Integer.BYTES + Long.BYTES + Integer.BYTES;

    private final long[] blobLengths;
    private final long[] blobOffsets;
    private final @Nullable int[] returnedPositions;

    public BlobFileMeta(SeekableInputStream in, long fileSize, @Nullable RoaringBitmap32 selection)
            throws IOException {
        if (fileSize < FILE_FOOTER_LENGTH) {
            throw new IOException(
                    String.format(
                            "Corrupt blob file: file size %s is smaller than footer size %s.",
                            fileSize, FILE_FOOTER_LENGTH));
        }

        in.seek(fileSize - FILE_FOOTER_LENGTH);
        byte[] header = new byte[FILE_FOOTER_LENGTH];
        IOUtils.readFully(in, header);
        byte version = header[Integer.BYTES];
        if (version != BlobFormatWriter.VERSION) {
            throw new IOException("Unsupported version: " + version);
        }
        int indexLength = BytesUtils.getInt(header, 0);
        long maximumIndexLength = fileSize - FILE_FOOTER_LENGTH;
        if (indexLength < 0 || indexLength > maximumIndexLength) {
            throw new IOException(
                    String.format(
                            "Corrupt blob file: invalid index length %s for file size %s.",
                            indexLength, fileSize));
        }

        long indexStart = maximumIndexLength - indexLength;
        in.seek(indexStart);
        byte[] indexBytes = new byte[indexLength];
        IOUtils.readFully(in, indexBytes);

        long[] blobLengths;
        try {
            blobLengths = DeltaVarintCompressor.decompress(indexBytes);
        } catch (RuntimeException e) {
            throw new IOException("Corrupt blob file: invalid index.", e);
        }
        long[] blobOffsets = new long[blobLengths.length];
        long offset = 0;
        for (int i = 0; i < blobLengths.length; i++) {
            long blobLength = blobLengths[i];
            if (blobLength == BlobFormatWriter.NULL_LENGTH
                    || blobLength == BlobFormatWriter.PLACE_HOLDER_LENGTH) {
                blobOffsets[i] = -1;
            } else {
                if (blobLength < MIN_RECORD_LENGTH) {
                    throw new IOException(
                            String.format(
                                    "Corrupt blob file: invalid record length %s at position %s.",
                                    blobLength, i));
                }
                if (blobLength > indexStart - offset) {
                    throw new IOException(
                            String.format(
                                    "Corrupt blob file: record length %s at position %s exceeds the data region.",
                                    blobLength, i));
                }
                blobOffsets[i] = offset;
                offset += blobLength;
            }
        }
        if (offset != indexStart) {
            throw new IOException(
                    String.format(
                            "Corrupt blob file: indexed records use %s bytes, but data region contains %s bytes.",
                            offset, indexStart));
        }

        this.blobLengths = blobLengths;
        this.blobOffsets = blobOffsets;
        this.returnedPositions = selectedPositions(selection, blobLengths.length);
    }

    private BlobFileMeta(
            long[] blobLengths, long[] blobOffsets, @Nullable int[] returnedPositions) {
        this.blobLengths = blobLengths;
        this.blobOffsets = blobOffsets;
        this.returnedPositions = returnedPositions;
    }

    public BlobFileMeta select(@Nullable RoaringBitmap32 selection) throws IOException {
        return selection == null
                ? this
                : new BlobFileMeta(
                        blobLengths, blobOffsets, selectedPositions(selection, blobLengths.length));
    }

    @Nullable
    private static int[] selectedPositions(@Nullable RoaringBitmap32 selection, int recordCount)
            throws IOException {
        if (selection == null) {
            return null;
        }
        long selectionCardinality = selection.getCardinality();
        if (selectionCardinality > recordCount) {
            throw new IOException(
                    String.format(
                            "Invalid blob selection: cardinality %s exceeds record count %s.",
                            selectionCardinality, recordCount));
        }
        int[] positions = new int[(int) selectionCardinality];
        Iterator<Integer> iterator = selection.iterator();
        for (int i = 0; i < positions.length; i++) {
            int position = iterator.next();
            if (position < 0 || position >= recordCount) {
                throw new IOException(
                        String.format(
                                "Invalid blob selection: position %s is outside record count %s.",
                                position, recordCount));
            }
            positions[i] = position;
        }
        return positions;
    }

    private int logicalPosition(int returnedPosition) {
        return returnedPositions == null ? returnedPosition : returnedPositions[returnedPosition];
    }

    public boolean isNull(int i) {
        return blobLengths[logicalPosition(i)] == BlobFormatWriter.NULL_LENGTH;
    }

    public boolean isPlaceHolder(int i) {
        return blobLengths[logicalPosition(i)] == BlobFormatWriter.PLACE_HOLDER_LENGTH;
    }

    public long blobLength(int i) {
        return blobLengths[logicalPosition(i)];
    }

    public long blobOffset(int i) {
        return blobOffsets[logicalPosition(i)];
    }

    public int returnedPosition(int i) {
        return returnedPositions == null ? i - 1 : returnedPositions[i - 1];
    }

    public int recordNumber() {
        return returnedPositions == null ? blobLengths.length : returnedPositions.length;
    }
}
