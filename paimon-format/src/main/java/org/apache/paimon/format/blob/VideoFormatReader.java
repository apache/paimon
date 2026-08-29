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
import org.apache.paimon.data.BlobPlaceholder;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.VideoFrameDescriptor;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.utils.UriReader;

import javax.annotation.Nullable;

/** Reader that exposes each logical row as a descriptor for one frame in a packed video. */
public class VideoFormatReader implements FileRecordReader<InternalRow> {

    private final Path filePath;
    private final VideoFileMeta fileMeta;
    private final int fieldCount;
    private final int blobIndex;
    private final UriReader uriReader;
    private boolean returned;

    public VideoFormatReader(
            FileIO fileIO, Path filePath, VideoFileMeta fileMeta, int fieldCount, int blobIndex) {
        this.filePath = filePath;
        this.fileMeta = fileMeta;
        this.fieldCount = fieldCount;
        this.blobIndex = blobIndex;
        this.uriReader = UriReader.fromFile(fileIO);
    }

    @Nullable
    @Override
    public FileRecordIterator<InternalRow> readBatch() {
        if (returned) {
            return null;
        }
        returned = true;
        return new FileRecordIterator<InternalRow>() {

            private int currentPosition;

            @Override
            public long returnedPosition() {
                return fileMeta.returnedPosition(currentPosition);
            }

            @Override
            public Path filePath() {
                return filePath;
            }

            @Nullable
            @Override
            public InternalRow next() {
                if (currentPosition >= fileMeta.recordNumber()) {
                    return null;
                }

                Object field;
                if (fileMeta.isNull(currentPosition)) {
                    field = null;
                } else if (fileMeta.isPlaceHolder(currentPosition)) {
                    field = BlobPlaceholder.INSTANCE;
                } else {
                    VideoFrameDescriptor descriptor =
                            new VideoFrameDescriptor(
                                    filePath.toString(),
                                    fileMeta.videoOffset(currentPosition),
                                    fileMeta.videoLength(currentPosition),
                                    fileMeta.frameIndex(currentPosition));
                    field = Blob.fromDescriptor(uriReader, descriptor);
                }
                currentPosition++;
                GenericRow row = new GenericRow(fieldCount);
                row.setField(blobIndex, field);
                return row;
            }

            @Override
            public boolean skip() {
                if (currentPosition >= fileMeta.recordNumber()) {
                    return false;
                }
                currentPosition++;
                return true;
            }

            @Override
            public void releaseBatch() {}
        };
    }

    @Override
    public void close() {}
}
