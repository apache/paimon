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
import org.apache.paimon.data.BlobArrayPlaceholder;
import org.apache.paimon.data.BlobPlaceholder;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.utils.DeltaVarintCompressor;
import org.apache.paimon.utils.IOUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/** {@link FileRecordReader} for blob file. */
public class BlobFormatReader implements FileRecordReader<InternalRow> {

    private final FileIO fileIO;
    private final Path filePath;
    private final String filePathString;
    private final BlobFileMeta fileMeta;
    private final @Nullable SeekableInputStream in;
    private final int fieldCount;
    private final int blobIndex;
    private final DataType blobFieldType;
    private final Object blobPlaceholder;
    private final boolean blobAsDescriptor;

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
        this.fileIO = fileIO;
        this.filePath = filePath;
        this.filePathString = filePath.toString();
        this.fileMeta = fileMeta;
        this.in = in;
        this.fieldCount = fieldCount;
        this.blobIndex = blobIndex;
        this.blobFieldType = blobFieldType;
        this.blobPlaceholder =
                blobFieldType.getTypeRoot() == DataTypeRoot.ARRAY
                        ? BlobArrayPlaceholder.INSTANCE
                        : BlobPlaceholder.INSTANCE;
        this.blobAsDescriptor = blobAsDescriptor;
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
                    field = blobPlaceholder;
                } else {
                    long offset = fileMeta.blobOffset(currentPosition) + 4;
                    long length = fileMeta.blobLength(currentPosition) - 16;
                    if (blobFieldType.getTypeRoot() == DataTypeRoot.ARRAY) {
                        field = readBlobArray(in, offset, length);
                    } else {
                        field = readBlob(in, offset, length);
                    }
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

    private Blob readBlob(@Nullable SeekableInputStream in, long position, long length) {
        if (in != null && !blobAsDescriptor) {
            return Blob.fromData(readInlineBlob(in, position, length));
        }
        return Blob.fromFile(fileIO, filePathString, position, length);
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

    private GenericArray readBlobArray(SeekableInputStream in, long position, long length) {
        if (in == null) {
            throw new IllegalStateException("Input stream must be available for ARRAY<BLOB>.");
        }

        try {
            byte[] header = new byte[9];
            in.seek(position);
            IOUtils.readFully(in, header);
            ByteBuffer headerBuffer = littleEndianBuffer(header);
            int magic = headerBuffer.getInt();
            if (magic != BlobFormatWriter.ARRAY_MAGIC_NUMBER) {
                throw new IllegalArgumentException(
                        "Invalid ARRAY<BLOB> payload magic number: " + magic);
            }
            byte version = headerBuffer.get();
            if (version > BlobFormatWriter.ARRAY_VERSION) {
                throw new UnsupportedOperationException(
                        "Unsupported ARRAY<BLOB> payload version: " + version);
            }
            int elementCount = headerBuffer.getInt();

            long elementIndexLengthPosition = position + length - Integer.BYTES;
            byte[] indexLengthBytes = new byte[Integer.BYTES];
            in.seek(elementIndexLengthPosition);
            IOUtils.readFully(in, indexLengthBytes);
            int elementIndexLength = littleEndianBuffer(indexLengthBytes).getInt();

            byte[] elementIndexBytes = new byte[elementIndexLength];
            in.seek(elementIndexLengthPosition - elementIndexLength);
            IOUtils.readFully(in, elementIndexBytes);
            long[] elementLengths = DeltaVarintCompressor.decompress(elementIndexBytes);
            if (elementLengths.length != elementCount) {
                throw new IllegalArgumentException(
                        "ARRAY<BLOB> element count does not match element index length.");
            }

            Object[] blobs = new Object[elementCount];
            long elementOffset = position + header.length;
            for (int i = 0; i < elementCount; i++) {
                long elementLength = elementLengths[i];
                if (elementLength == BlobFormatWriter.ARRAY_NULL_ELEMENT_LENGTH) {
                    blobs[i] = null;
                } else if (blobAsDescriptor) {
                    blobs[i] = Blob.fromFile(fileIO, filePathString, elementOffset, elementLength);
                    elementOffset += elementLength;
                } else {
                    blobs[i] = Blob.fromData(readInlineBlob(in, elementOffset, elementLength));
                    elementOffset += elementLength;
                }
            }

            return new GenericArray(blobs);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static ByteBuffer littleEndianBuffer(byte[] bytes) {
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
    }
}
