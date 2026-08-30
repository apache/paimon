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

import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.data.BlobFetchMetricReporter;
import org.apache.paimon.data.BlobPlaceholder;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.VideoFrameDescriptor;
import org.apache.paimon.format.FileAwareFormatWriter;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.DeltaVarintCompressor;
import org.apache.paimon.utils.LongArrayList;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.StreamUtils.intToLittleEndian;

/**
 * {@link FormatWriter} for a Paimon video pack.
 *
 * <p>The data region concatenates complete encoded-video payloads without per-payload wrappers.
 * Logical frame rows are represented by compact contiguous runs. A run points to one physical video
 * and stores its first frame; subsequent rows increment the frame ordinal by one.
 */
public class VideoFormatWriter implements FileAwareFormatWriter {

    public static final byte VERSION = 1;
    public static final int MAGIC_NUMBER = 0x4F454449; // "IDEO" in little endian
    public static final long NULL_REFERENCE = -1L;
    public static final long PLACEHOLDER_REFERENCE = -2L;
    public static final int FILE_FOOTER_LENGTH = Integer.BYTES * 5 + Byte.BYTES;

    private final PositionOutputStream out;
    private final RawVideoPayloadWriter payloadWriter;
    private final LongArrayList physicalVideoLengths;
    private final LongArrayList runLengths;
    private final LongArrayList runReferences;
    private final LongArrayList runFirstFrames;
    private final Map<BlobDescriptor, Integer> physicalVideos;

    private long currentRunLength;
    private long currentRunReference;
    private long currentRunFirstFrame;
    private long currentRunLastFrame;
    private boolean closed;

    public VideoFormatWriter(
            PositionOutputStream out,
            RowType type,
            boolean writeNullOnMissingFile,
            boolean writeNullOnFetchFailure,
            BlobFetchMetricReporter blobFetchMetricReporter,
            int copyBufferSize) {
        checkArgument(type.getFieldCount() == 1, "VideoFormatWriter only supports one field.");
        this.out = out;
        this.payloadWriter =
                new RawVideoPayloadWriter(
                        out,
                        type.getFieldNames().get(0),
                        writeNullOnMissingFile,
                        writeNullOnFetchFailure,
                        blobFetchMetricReporter,
                        copyBufferSize);
        this.physicalVideoLengths = new LongArrayList(16);
        this.runLengths = new LongArrayList(16);
        this.runReferences = new LongArrayList(16);
        this.runFirstFrames = new LongArrayList(16);
        this.physicalVideos = new HashMap<>();
    }

    @Override
    public void setFile(Path file) {
        payloadWriter.setFile(file);
    }

    @Override
    public boolean deleteFileUponAbort() {
        return true;
    }

    @Override
    public void addElement(InternalRow element) throws IOException {
        checkArgument(element.getFieldCount() == 1, "VideoFormatWriter only supports one field.");
        if (element.isNullAt(0)) {
            append(NULL_REFERENCE, 0);
            return;
        }

        Blob blob = element.getBlob(0);
        if (blob == BlobPlaceholder.INSTANCE) {
            append(PLACEHOLDER_REFERENCE, 0);
            return;
        }
        VideoFrameDescriptor frame = VideoFrameDescriptor.fromBlob(blob);
        checkArgument(
                frame != null,
                "Video fields require an exact BlobRef containing a VideoFrameDescriptor.");

        BlobDescriptor payload = frame.payloadDescriptor();
        Integer ordinal = physicalVideos.get(payload);
        if (ordinal == null) {
            long length = payloadWriter.write(element);
            if (length == BlobFormatWriter.NULL_LENGTH) {
                append(NULL_REFERENCE, 0);
                return;
            }
            ordinal = physicalVideoLengths.size();
            physicalVideoLengths.add(length);
            physicalVideos.put(payload, ordinal);
        }
        append(ordinal, frame.frameIndex());
    }

    @Override
    public boolean reachTargetSize(boolean suggestedCheck, long targetSize) throws IOException {
        return out.getPos() >= targetSize;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        flushRun();
        payloadWriter.close();

        byte[] physicalIndex = DeltaVarintCompressor.compressLongArrayList(physicalVideoLengths);
        byte[] runLengthIndex = DeltaVarintCompressor.compressLongArrayList(runLengths);
        byte[] runReferenceIndex = DeltaVarintCompressor.compressLongArrayList(runReferences);
        byte[] firstFrameIndex = DeltaVarintCompressor.compressLongArrayList(runFirstFrames);
        out.write(physicalIndex);
        out.write(runLengthIndex);
        out.write(runReferenceIndex);
        out.write(firstFrameIndex);
        out.write(intToLittleEndian(physicalIndex.length));
        out.write(intToLittleEndian(runLengthIndex.length));
        out.write(intToLittleEndian(runReferenceIndex.length));
        out.write(intToLittleEndian(firstFrameIndex.length));
        out.write(intToLittleEndian(MAGIC_NUMBER));
        out.write(VERSION);
        closed = true;
    }

    int physicalVideoCount() {
        return physicalVideoLengths.size();
    }

    int runCount() {
        return runLengths.size() + (currentRunLength == 0 ? 0 : 1);
    }

    private void append(long reference, long frameIndex) {
        if (canExtend(reference, frameIndex)) {
            currentRunLength++;
            currentRunLastFrame = frameIndex;
            return;
        }
        flushRun();
        currentRunReference = reference;
        currentRunFirstFrame = frameIndex;
        currentRunLastFrame = frameIndex;
        currentRunLength = 1;
    }

    private boolean canExtend(long reference, long frameIndex) {
        if (currentRunLength == 0 || currentRunReference != reference) {
            return false;
        }
        return reference < 0 || frameIndex == currentRunLastFrame + 1;
    }

    private void flushRun() {
        if (currentRunLength == 0) {
            return;
        }
        runLengths.add(currentRunLength);
        runReferences.add(currentRunReference);
        runFirstFrames.add(currentRunFirstFrame);
        currentRunLength = 0;
    }

    /** Copies raw video bytes without the ordinary BLOB record header and trailer. */
    private static class RawVideoPayloadWriter extends AbstractBlobElementWriter {

        private RawVideoPayloadWriter(
                PositionOutputStream out,
                String fieldName,
                boolean writeNullOnMissingFile,
                boolean writeNullOnFetchFailure,
                BlobFetchMetricReporter blobFetchMetricReporter,
                int copyBufferSize) {
            super(
                    out,
                    fieldName,
                    null,
                    writeNullOnMissingFile,
                    writeNullOnFetchFailure,
                    blobFetchMetricReporter,
                    copyBufferSize);
        }

        @Override
        public long write(InternalRow row) throws IOException {
            BlobFetchResult fetchResult = getBlob(() -> row.getBlob(0));
            if (fetchResult.fetchFailure()) {
                return BlobFormatWriter.NULL_LENGTH;
            }
            Blob blob = fetchResult.blob();
            BlobCopySource source = prepareBlobSource(blob);
            if (source == null) {
                return BlobFormatWriter.NULL_LENGTH;
            }
            BlobDescriptor written = writeBlobData(source);
            checkArgument(written.length() > 0, "Encoded video payload must not be empty.");
            recordSuccess(written.length());
            return written.length();
        }
    }
}
