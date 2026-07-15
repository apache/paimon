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

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.DataType;

import javax.annotation.Nullable;

import java.io.IOException;

/** {@link FileRecordReader} for blob file. */
public class BlobFormatReader implements FileRecordReader<InternalRow> {

    private final Path filePath;
    private final BlobFileMeta fileMeta;
    private final int fieldCount;
    private final int blobIndex;
    private final BlobElementSerializer.Reader elementReader;

    private boolean returned;

    public BlobFormatReader(
            FileIO fileIO,
            Path filePath,
            BlobFileMeta fileMeta,
            @Nullable SeekableInputStream in,
            int fieldCount,
            int blobIndex,
            DataType blobFieldType,
            boolean blobAsDescriptor) {
        this(
                filePath,
                fileMeta,
                fieldCount,
                blobIndex,
                BlobElementSerializerFactory.create(blobFieldType)
                        .createReader(fileIO, filePath, in, blobAsDescriptor));
    }

    BlobFormatReader(
            Path filePath,
            BlobFileMeta fileMeta,
            int fieldCount,
            int blobIndex,
            BlobElementSerializer.Reader elementReader) {
        this.filePath = filePath;
        this.fileMeta = fileMeta;
        this.fieldCount = fieldCount;
        this.blobIndex = blobIndex;
        this.elementReader = elementReader;
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

                Object field;
                if (fileMeta.isNull(currentPosition)) {
                    field = null;
                } else if (fileMeta.isPlaceHolder(currentPosition)) {
                    field = elementReader.placeholder();
                } else {
                    long payloadPosition = fileMeta.blobOffset(currentPosition) + 4;
                    long payloadLength = fileMeta.blobLength(currentPosition) - 16;
                    field = elementReader.read(payloadPosition, payloadLength);
                }
                currentPosition++;
                GenericRow row = new GenericRow(fieldCount);
                row.setField(blobIndex, field);
                return row;
            }

            @Override
            public void releaseBatch() {}
        };
    }

    @Override
    public void close() throws IOException {
        elementReader.close();
    }
}
