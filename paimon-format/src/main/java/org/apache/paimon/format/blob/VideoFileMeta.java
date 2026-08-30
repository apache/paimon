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
import java.util.Arrays;
import java.util.Iterator;

/** Embedded physical-video and logical-frame-run metadata of a {@code .video} file. */
public class VideoFileMeta {

    private final long[] physicalVideoLengths;
    private final long[] physicalVideoOffsets;
    private final long[] runEnds;
    private final long[] runReferences;
    private final long[] runFirstFrames;
    private final int rowCount;
    private final @Nullable int[] selectedPositions;

    public VideoFileMeta(SeekableInputStream in, long fileSize, @Nullable RoaringBitmap32 selection)
            throws IOException {
        if (fileSize < VideoFormatWriter.FILE_FOOTER_LENGTH) {
            throw corrupt(
                    "file size %s is smaller than footer size %s.",
                    fileSize, VideoFormatWriter.FILE_FOOTER_LENGTH);
        }

        long footerStart = fileSize - VideoFormatWriter.FILE_FOOTER_LENGTH;
        in.seek(footerStart);
        byte[] footer = new byte[VideoFormatWriter.FILE_FOOTER_LENGTH];
        IOUtils.readFully(in, footer);
        int physicalIndexLength = BytesUtils.getInt(footer, 0);
        int runLengthIndexLength = BytesUtils.getInt(footer, Integer.BYTES);
        int runReferenceIndexLength = BytesUtils.getInt(footer, Integer.BYTES * 2);
        int firstFrameIndexLength = BytesUtils.getInt(footer, Integer.BYTES * 3);
        int magic = BytesUtils.getInt(footer, Integer.BYTES * 4);
        byte version = footer[Integer.BYTES * 5];
        if (magic != VideoFormatWriter.MAGIC_NUMBER) {
            throw corrupt("invalid footer magic %s.", magic);
        }
        if (version != VideoFormatWriter.VERSION) {
            throw new IOException("Unsupported video format version: " + version);
        }

        int[] indexLengths = {
            physicalIndexLength,
            runLengthIndexLength,
            runReferenceIndexLength,
            firstFrameIndexLength
        };
        long totalIndexLength = 0;
        for (int length : indexLengths) {
            if (length < 0) {
                throw corrupt("negative index length %s.", length);
            }
            totalIndexLength += length;
        }
        if (totalIndexLength > footerStart) {
            throw corrupt("index length %s exceeds file size %s.", totalIndexLength, fileSize);
        }

        long indexStart = footerStart - totalIndexLength;
        long offset = indexStart;
        long[] physicalVideoLengths = readIndex(in, offset, physicalIndexLength, "physical video");
        offset += physicalIndexLength;
        long[] runLengths = readIndex(in, offset, runLengthIndexLength, "run length");
        offset += runLengthIndexLength;
        long[] runReferences = readIndex(in, offset, runReferenceIndexLength, "run reference");
        offset += runReferenceIndexLength;
        long[] runFirstFrames = readIndex(in, offset, firstFrameIndexLength, "run first-frame");

        long[] physicalVideoOffsets = new long[physicalVideoLengths.length];
        long payloadOffset = 0;
        for (int i = 0; i < physicalVideoLengths.length; i++) {
            long length = physicalVideoLengths[i];
            if (length <= 0 || length > indexStart - payloadOffset) {
                throw corrupt("invalid physical video length %s at ordinal %s.", length, i);
            }
            physicalVideoOffsets[i] = payloadOffset;
            payloadOffset += length;
        }
        if (payloadOffset != indexStart) {
            throw corrupt(
                    "indexed videos use %s bytes, but payload region contains %s bytes.",
                    payloadOffset, indexStart);
        }

        if (runLengths.length != runReferences.length
                || runLengths.length != runFirstFrames.length) {
            throw corrupt(
                    "run indexes have different counts: %s, %s, and %s.",
                    runLengths.length, runReferences.length, runFirstFrames.length);
        }
        long[] runEnds = new long[runLengths.length];
        long rows = 0;
        for (int i = 0; i < runLengths.length; i++) {
            long length = runLengths[i];
            if (length <= 0 || rows > Integer.MAX_VALUE - length) {
                throw corrupt("invalid run length %s at run %s.", length, i);
            }
            long reference = runReferences[i];
            if (reference != VideoFormatWriter.NULL_REFERENCE
                    && reference != VideoFormatWriter.PLACEHOLDER_REFERENCE
                    && (reference < 0 || reference >= physicalVideoLengths.length)) {
                throw corrupt(
                        "run %s references physical video %s, but physical video count is %s.",
                        i, reference, physicalVideoLengths.length);
            }
            if (reference >= 0 && runFirstFrames[i] < 0) {
                throw corrupt("run %s has negative first frame %s.", i, runFirstFrames[i]);
            }
            rows += length;
            runEnds[i] = rows;
        }

        this.physicalVideoLengths = physicalVideoLengths;
        this.physicalVideoOffsets = physicalVideoOffsets;
        this.runEnds = runEnds;
        this.runReferences = runReferences;
        this.runFirstFrames = runFirstFrames;
        this.rowCount = (int) rows;
        this.selectedPositions = selectedPositions(selection, rowCount);
    }

    private VideoFileMeta(
            long[] physicalVideoLengths,
            long[] physicalVideoOffsets,
            long[] runEnds,
            long[] runReferences,
            long[] runFirstFrames,
            int rowCount,
            @Nullable int[] selectedPositions) {
        this.physicalVideoLengths = physicalVideoLengths;
        this.physicalVideoOffsets = physicalVideoOffsets;
        this.runEnds = runEnds;
        this.runReferences = runReferences;
        this.runFirstFrames = runFirstFrames;
        this.rowCount = rowCount;
        this.selectedPositions = selectedPositions;
    }

    public VideoFileMeta select(@Nullable RoaringBitmap32 selection) throws IOException {
        return selection == null
                ? this
                : new VideoFileMeta(
                        physicalVideoLengths,
                        physicalVideoOffsets,
                        runEnds,
                        runReferences,
                        runFirstFrames,
                        rowCount,
                        selectedPositions(selection, rowCount));
    }

    @Nullable
    private static int[] selectedPositions(@Nullable RoaringBitmap32 selection, int rowCount)
            throws IOException {
        if (selection == null) {
            return null;
        }
        long cardinality = selection.getCardinality();
        if (cardinality > rowCount) {
            throw new IOException(
                    String.format(
                            "Invalid video selection: cardinality %s exceeds row count %s.",
                            cardinality, rowCount));
        }
        int[] positions = new int[(int) cardinality];
        Iterator<Integer> iterator = selection.iterator();
        for (int i = 0; i < positions.length; i++) {
            int position = iterator.next();
            if (position < 0 || position >= rowCount) {
                throw new IOException(
                        String.format(
                                "Invalid video selection: position %s is outside row count %s.",
                                position, rowCount));
            }
            positions[i] = position;
        }
        return positions;
    }

    public boolean isNull(int returnedRow) {
        return runReference(returnedRow) == VideoFormatWriter.NULL_REFERENCE;
    }

    public boolean isPlaceHolder(int returnedRow) {
        return runReference(returnedRow) == VideoFormatWriter.PLACEHOLDER_REFERENCE;
    }

    public long videoOffset(int returnedRow) {
        return physicalVideoOffsets[physicalOrdinal(returnedRow)];
    }

    public long videoLength(int returnedRow) {
        return physicalVideoLengths[physicalOrdinal(returnedRow)];
    }

    public long frameIndex(int returnedRow) {
        int row = logicalPosition(returnedRow);
        int run = run(row);
        long runStart = run == 0 ? 0 : runEnds[run - 1];
        return runFirstFrames[run] + row - runStart;
    }

    public int returnedPosition(int currentPosition) {
        return logicalPosition(currentPosition - 1);
    }

    public int recordNumber() {
        return selectedPositions == null ? rowCount : selectedPositions.length;
    }

    public int physicalVideoNumber() {
        return physicalVideoLengths.length;
    }

    public int runNumber() {
        return runEnds.length;
    }

    private int physicalOrdinal(int returnedRow) {
        long reference = runReference(returnedRow);
        if (reference < 0 || reference > Integer.MAX_VALUE) {
            throw new IllegalStateException(
                    "Row " + logicalPosition(returnedRow) + " does not reference a video.");
        }
        return (int) reference;
    }

    private long runReference(int returnedRow) {
        return runReferences[run(logicalPosition(returnedRow))];
    }

    private int logicalPosition(int returnedRow) {
        return selectedPositions == null ? returnedRow : selectedPositions[returnedRow];
    }

    private int run(int logicalRow) {
        int run = Arrays.binarySearch(runEnds, logicalRow + 1L);
        return run >= 0 ? run : -run - 1;
    }

    private static long[] readIndex(SeekableInputStream in, long start, int length, String name)
            throws IOException {
        in.seek(start);
        byte[] bytes = new byte[length];
        IOUtils.readFully(in, bytes);
        try {
            return DeltaVarintCompressor.decompress(bytes);
        } catch (RuntimeException e) {
            throw new IOException("Corrupt video file: invalid " + name + " index.", e);
        }
    }

    private static IOException corrupt(String message, Object... args) {
        return new IOException("Corrupt video file: " + String.format(message, args));
    }
}
