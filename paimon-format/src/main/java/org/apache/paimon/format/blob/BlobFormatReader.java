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
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.data.BlobPlaceholder;
import org.apache.paimon.data.BlobRef;
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
import org.apache.paimon.utils.UriReader;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/** {@link FileRecordReader} for blob file. */
public class BlobFormatReader implements FileRecordReader<InternalRow> {

    private static final int ARRAY_HEADER_LENGTH = 9;
    private static final int ARRAY_INDEX_LENGTH_SIZE = Integer.BYTES;
    private static final int MIN_ARRAY_PAYLOAD_LENGTH =
            ARRAY_HEADER_LENGTH + ARRAY_INDEX_LENGTH_SIZE;

    // Shared by all BlobRefs this reader produces, so a BlobFormatWriter can reuse one source
    // stream.
    private final UriReader uriReader;
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
        this.uriReader = UriReader.fromFile(fileIO);
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
        return new BlobRef(uriReader, new BlobDescriptor(filePathString, position, length));
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
        if (position < 0
                || length < MIN_ARRAY_PAYLOAD_LENGTH
                || position > Long.MAX_VALUE - length) {
            throw new IllegalArgumentException(
                    "Invalid ARRAY<BLOB> payload position or length: " + position + ", " + length);
        }

        try {
            byte[] header = new byte[ARRAY_HEADER_LENGTH];
            in.seek(position);
            IOUtils.readFully(in, header);
            ByteBuffer headerBuffer = littleEndianBuffer(header);
            int magic = headerBuffer.getInt();
            if (magic != BlobFormatWriter.ARRAY_MAGIC_NUMBER) {
                throw new IllegalArgumentException(
                        "Invalid ARRAY<BLOB> payload magic number: " + magic);
            }
            byte version = headerBuffer.get();
            if (version != BlobFormatWriter.ARRAY_VERSION) {
                throw new UnsupportedOperationException(
                        "Unsupported ARRAY<BLOB> payload version: " + version);
            }
            int elementCount = headerBuffer.getInt();
            if (elementCount < 0) {
                throw new IllegalArgumentException(
                        "Invalid ARRAY<BLOB> element count: " + elementCount);
            }

            long payloadEnd = position + length;
            long elementDataStart = position + ARRAY_HEADER_LENGTH;
            long elementIndexLengthPosition = payloadEnd - ARRAY_INDEX_LENGTH_SIZE;
            byte[] indexLengthBytes = new byte[ARRAY_INDEX_LENGTH_SIZE];
            in.seek(elementIndexLengthPosition);
            IOUtils.readFully(in, indexLengthBytes);
            int elementIndexLength = littleEndianBuffer(indexLengthBytes).getInt();
            long maximumIndexLength = length - MIN_ARRAY_PAYLOAD_LENGTH;
            if (elementIndexLength < 0 || elementIndexLength > maximumIndexLength) {
                throw new IllegalArgumentException(
                        "Invalid ARRAY<BLOB> element index length: " + elementIndexLength);
            }
            if (elementCount > elementIndexLength) {
                throw new IllegalArgumentException(
                        "ARRAY<BLOB> element count exceeds element index length.");
            }

            byte[] elementIndexBytes = new byte[elementIndexLength];
            long elementIndexStart = elementIndexLengthPosition - elementIndexLength;
            in.seek(elementIndexStart);
            IOUtils.readFully(in, elementIndexBytes);
            long[] elementLengths;
            try {
                elementLengths = DeltaVarintCompressor.decompress(elementIndexBytes);
            } catch (RuntimeException e) {
                throw new IllegalArgumentException("Invalid ARRAY<BLOB> element index.", e);
            }
            if (elementLengths.length != elementCount) {
                throw new IllegalArgumentException(
                        "ARRAY<BLOB> element count does not match element index length.");
            }

            long elementDataLength = elementIndexStart - elementDataStart;
            long totalElementLength = 0;
            for (long elementLength : elementLengths) {
                if (elementLength == BlobFormatWriter.ARRAY_NULL_ELEMENT_LENGTH) {
                    continue;
                }
                if (elementLength < 0) {
                    throw new IllegalArgumentException(
                            "Invalid ARRAY<BLOB> element length: " + elementLength);
                }
                if (!blobAsDescriptor && elementLength > Integer.MAX_VALUE) {
                    throw new IllegalArgumentException(
                            "ARRAY<BLOB> inline element is too large: " + elementLength);
                }
                if (elementLength > elementDataLength - totalElementLength) {
                    throw new IllegalArgumentException(
                            "ARRAY<BLOB> element lengths exceed the payload data length.");
                }
                totalElementLength += elementLength;
            }
            if (totalElementLength != elementDataLength) {
                throw new IllegalArgumentException(
                        "ARRAY<BLOB> element lengths do not match the payload data length.");
            }

            Object[] blobs = new Object[elementCount];
            long elementOffset = elementDataStart;
            for (int i = 0; i < elementCount; i++) {
                long elementLength = elementLengths[i];
                if (elementLength == BlobFormatWriter.ARRAY_NULL_ELEMENT_LENGTH) {
                    blobs[i] = null;
                } else if (blobAsDescriptor) {
                    blobs[i] =
                            new BlobRef(
                                    uriReader,
                                    new BlobDescriptor(
                                            filePathString, elementOffset, elementLength));
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
