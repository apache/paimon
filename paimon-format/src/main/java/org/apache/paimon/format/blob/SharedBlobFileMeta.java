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

/** Metadata and logical-row mapping of a shared blob file. */
public class SharedBlobFileMeta {

    private static final int FILE_FOOTER_LENGTH = Integer.BYTES * 3 + Byte.BYTES;
    private static final int MIN_RECORD_LENGTH = Integer.BYTES + Long.BYTES + Integer.BYTES;

    private final long[] physicalBlobLengths;
    private final long[] physicalBlobOffsets;
    private final long[] rowReferences;
    private final @Nullable int[] returnedPositions;

    public SharedBlobFileMeta(
            SeekableInputStream in, long fileSize, @Nullable RoaringBitmap32 selection)
            throws IOException {
        if (fileSize < FILE_FOOTER_LENGTH) {
            throw corrupt(
                    "file size %s is smaller than footer size %s.", fileSize, FILE_FOOTER_LENGTH);
        }

        in.seek(fileSize - FILE_FOOTER_LENGTH);
        byte[] footer = new byte[FILE_FOOTER_LENGTH];
        IOUtils.readFully(in, footer);
        int physicalIndexLength = BytesUtils.getInt(footer, 0);
        int rowIndexLength = BytesUtils.getInt(footer, Integer.BYTES);
        int magic = BytesUtils.getInt(footer, Integer.BYTES * 2);
        byte version = footer[Integer.BYTES * 3];
        if (magic != SharedBlobFormatWriter.MAGIC_NUMBER) {
            throw corrupt("invalid footer magic %s.", magic);
        }
        if (version != SharedBlobFormatWriter.VERSION) {
            throw new IOException("Unsupported shared blob version: " + version);
        }

        long maximumIndexLength = fileSize - FILE_FOOTER_LENGTH;
        long totalIndexLength = (long) physicalIndexLength + rowIndexLength;
        if (physicalIndexLength < 0
                || rowIndexLength < 0
                || totalIndexLength > maximumIndexLength) {
            throw corrupt(
                    "invalid index lengths %s and %s for file size %s.",
                    physicalIndexLength, rowIndexLength, fileSize);
        }

        long physicalIndexStart = maximumIndexLength - totalIndexLength;
        long rowIndexStart = physicalIndexStart + physicalIndexLength;
        long[] physicalBlobLengths =
                readIndex(in, physicalIndexStart, physicalIndexLength, "physical blob");
        long[] rowReferences = readIndex(in, rowIndexStart, rowIndexLength, "row reference");

        long[] physicalBlobOffsets = new long[physicalBlobLengths.length];
        long offset = 0;
        for (int i = 0; i < physicalBlobLengths.length; i++) {
            long blobLength = physicalBlobLengths[i];
            if (blobLength < MIN_RECORD_LENGTH) {
                throw corrupt("invalid physical blob length %s at ordinal %s.", blobLength, i);
            }
            if (blobLength > physicalIndexStart - offset) {
                throw corrupt(
                        "physical blob length %s at ordinal %s exceeds the data region.",
                        blobLength, i);
            }
            physicalBlobOffsets[i] = offset;
            offset += blobLength;
        }
        if (offset != physicalIndexStart) {
            throw corrupt(
                    "indexed blobs use %s bytes, but data region contains %s bytes.",
                    offset, physicalIndexStart);
        }
        validateRowReferences(rowReferences, physicalBlobLengths.length);

        int[] returnedPositions = null;
        if (selection != null) {
            long selectionCardinality = selection.getCardinality();
            if (selectionCardinality > rowReferences.length) {
                throw new IOException(
                        String.format(
                                "Invalid shared blob selection: cardinality %s exceeds row count %s.",
                                selectionCardinality, rowReferences.length));
            }
            int cardinality = (int) selectionCardinality;
            returnedPositions = new int[cardinality];
            long[] selectedReferences = new long[cardinality];
            Iterator<Integer> iterator = selection.iterator();
            for (int i = 0; i < cardinality; i++) {
                int position = iterator.next();
                if (position < 0 || position >= rowReferences.length) {
                    throw new IOException(
                            String.format(
                                    "Invalid shared blob selection: position %s is outside row count %s.",
                                    position, rowReferences.length));
                }
                selectedReferences[i] = rowReferences[position];
                returnedPositions[i] = position;
            }
            rowReferences = selectedReferences;
        }

        this.physicalBlobLengths = physicalBlobLengths;
        this.physicalBlobOffsets = physicalBlobOffsets;
        this.rowReferences = rowReferences;
        this.returnedPositions = returnedPositions;
    }

    public boolean isNull(int row) {
        return rowReferences[row] == SharedBlobFormatWriter.NULL_REFERENCE;
    }

    public boolean isPlaceHolder(int row) {
        return rowReferences[row] == SharedBlobFormatWriter.PLACEHOLDER_REFERENCE;
    }

    public long blobLength(int row) {
        return physicalBlobLengths[physicalOrdinal(row)];
    }

    public long blobOffset(int row) {
        return physicalBlobOffsets[physicalOrdinal(row)];
    }

    public int returnedPosition(int currentPosition) {
        return returnedPositions == null
                ? currentPosition - 1
                : returnedPositions[currentPosition - 1];
    }

    public int recordNumber() {
        return rowReferences.length;
    }

    public int physicalBlobNumber() {
        return physicalBlobLengths.length;
    }

    private int physicalOrdinal(int row) {
        long reference = rowReferences[row];
        if (reference < 0 || reference > Integer.MAX_VALUE) {
            throw new IllegalStateException("Row " + row + " does not reference a physical blob.");
        }
        return (int) reference;
    }

    private static long[] readIndex(SeekableInputStream in, long start, int length, String name)
            throws IOException {
        in.seek(start);
        byte[] bytes = new byte[length];
        IOUtils.readFully(in, bytes);
        try {
            return DeltaVarintCompressor.decompress(bytes);
        } catch (RuntimeException e) {
            throw new IOException("Corrupt shared blob file: invalid " + name + " index.", e);
        }
    }

    private static void validateRowReferences(long[] references, int physicalBlobCount)
            throws IOException {
        for (int i = 0; i < references.length; i++) {
            long reference = references[i];
            if (reference == SharedBlobFormatWriter.NULL_REFERENCE
                    || reference == SharedBlobFormatWriter.PLACEHOLDER_REFERENCE) {
                continue;
            }
            if (reference < 0 || reference >= physicalBlobCount) {
                throw corrupt(
                        "row %s references physical blob %s, but physical blob count is %s.",
                        i, reference, physicalBlobCount);
            }
        }
    }

    private static IOException corrupt(String message, Object... args) {
        return new IOException("Corrupt shared blob file: " + String.format(message, args));
    }
}
