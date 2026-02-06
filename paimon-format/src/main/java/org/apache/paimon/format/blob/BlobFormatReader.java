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
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.utils.IOUtils;

import javax.annotation.Nullable;

import java.io.IOException;

/** {@link FileRecordReader} for blob file. */
public class BlobFormatReader implements FileRecordReader<InternalRow> {

    private final FileIO fileIO;
    private final Path filePath;
    private final String filePathString;
    private final BlobFileMeta fileMeta;
    private final @Nullable SeekableInputStream in;

    private boolean returned;

    public BlobFormatReader(
            FileIO fileIO, Path filePath, BlobFileMeta fileMeta, @Nullable SeekableInputStream in) {
        this.fileIO = fileIO;
        this.filePath = filePath;
        this.filePathString = filePath.toString();
        this.fileMeta = fileMeta;
        this.in = in;
        this.returned = false;
    }

    @Nullable
    @Override
    public FileRecordIterator<InternalRow> readBatch() throws IOException {
        if (returned) {
            return null;
        }

        returned = true;
        return new FileRecordIterator<InternalRow>() {

            int currentPosition = 0;

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

                Blob blob;
                if (fileMeta.isNull(currentPosition)) {
                    blob = null;
                } else {
                    long offset = fileMeta.blobOffset(currentPosition) + 4;
                    long length = fileMeta.blobLength(currentPosition) - 16;
                    if (in != null) {
                        blob = Blob.fromData(readInlineBlob(in, offset, length));
                    } else {
                        blob = Blob.fromFile(fileIO, filePathString, offset, length);
                    }
                }
                currentPosition++;
                return GenericRow.of(blob);
            }

            @Override
            public void releaseBatch() {}
        };
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(in);
    }

    private static byte[] readInlineBlob(SeekableInputStream in, long position, long length) {
        byte[] blobData = new byte[(int) length];
        try {
            in.seek(position);
            IOUtils.readFully(in, blobData);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return blobData;
    }
}
