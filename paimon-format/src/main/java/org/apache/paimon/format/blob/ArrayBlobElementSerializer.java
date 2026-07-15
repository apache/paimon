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
import org.apache.paimon.data.BlobConsumer;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.data.BlobFetchMetricReporter;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.utils.DeltaVarintCompressor;
import org.apache.paimon.utils.IOUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.apache.paimon.utils.StreamUtils.intToLittleEndian;

/** Element serializer for an ARRAY&lt;BLOB&gt; field. */
final class ArrayBlobElementSerializer implements BlobElementSerializer {

    static final byte VERSION = 1;
    static final int MAGIC_NUMBER = 1094861634;
    static final byte[] MAGIC_NUMBER_BYTES = intToLittleEndian(MAGIC_NUMBER);
    static final long NULL_ELEMENT_LENGTH = -1L;

    private static final int HEADER_LENGTH = 9;
    private static final int INDEX_LENGTH_SIZE = Integer.BYTES;
    private static final int MIN_PAYLOAD_LENGTH = HEADER_LENGTH + INDEX_LENGTH_SIZE;

    @Override
    public BlobElementSerializer.Writer createWriter(
            PositionOutputStream out,
            String blobFieldName,
            @Nullable BlobConsumer writeConsumer,
            boolean writeNullOnMissingFile,
            boolean writeNullOnFetchFailure,
            BlobFetchMetricReporter blobFetchMetricReporter,
            int copyBufferSize) {
        return new Writer(
                out,
                blobFieldName,
                writeConsumer,
                writeNullOnMissingFile,
                writeNullOnFetchFailure,
                blobFetchMetricReporter,
                copyBufferSize);
    }

    @Override
    public BlobElementSerializer.Reader createReader(
            FileIO fileIO,
            Path filePath,
            @Nullable SeekableInputStream in,
            boolean blobAsDescriptor) {
        return new Reader(fileIO, filePath, in, blobAsDescriptor);
    }

    /** Writer for an ARRAY&lt;BLOB&gt; field. */
    static final class Writer extends AbstractBlobElementWriter {

        Writer(
                PositionOutputStream out,
                String blobFieldName,
                @Nullable BlobConsumer writeConsumer,
                boolean writeNullOnMissingFile,
                boolean writeNullOnFetchFailure,
                BlobFetchMetricReporter blobFetchMetricReporter,
                int copyBufferSize) {
            super(
                    out,
                    blobFieldName,
                    writeConsumer,
                    writeNullOnMissingFile,
                    writeNullOnFetchFailure,
                    blobFetchMetricReporter,
                    copyBufferSize);
        }

        @Override
        public long write(InternalRow row) throws IOException {
            if (row.isNullAt(0)) {
                return writeNullElement();
            }

            InternalArray array = row.getArray(0);
            if (array == BlobArrayPlaceholder.INSTANCE) {
                return writePlaceholderElement();
            }

            long recordPosition = startRecord();
            write(MAGIC_NUMBER_BYTES);
            write(new byte[] {VERSION});
            write(intToLittleEndian(array.size()));

            long[] elementLengths = new long[array.size()];
            boolean flush = false;
            for (int i = 0; i < array.size(); i++) {
                if (array.isNullAt(i)) {
                    elementLengths[i] = NULL_ELEMENT_LENGTH;
                    continue;
                }

                final int position = i;
                BlobFetchResult fetchResult = getBlob(() -> array.getBlob(position));
                Blob blob = fetchResult.blob();
                if (fetchResult.fetchFailure() || blob == null) {
                    elementLengths[i] = NULL_ELEMENT_LENGTH;
                    continue;
                }
                SeekableInputStream in = openBlobInputStream(blob);
                if (in == null) {
                    elementLengths[i] = NULL_ELEMENT_LENGTH;
                    continue;
                }
                BlobDescriptor descriptor = writeBlobData(in);
                elementLengths[i] = descriptor.length();
                flush |= accept(descriptor);
                recordSuccess(descriptor.length());
            }

            byte[] indexBytes = DeltaVarintCompressor.compress(elementLengths);
            write(indexBytes);
            write(intToLittleEndian(indexBytes.length));

            long recordLength = finishRecord(recordPosition);
            if (flush) {
                flush();
            }
            return recordLength;
        }
    }

    /** Reader for an ARRAY&lt;BLOB&gt; field. */
    static final class Reader extends AbstractBlobElementReader {

        Reader(
                FileIO fileIO,
                Path filePath,
                @Nullable SeekableInputStream in,
                boolean blobAsDescriptor) {
            super(fileIO, filePath, in, blobAsDescriptor);
        }

        @Override
        public Object read(long payloadPosition, long payloadLength) {
            SeekableInputStream in = inputStream();
            if (in == null) {
                throw new IllegalStateException("Input stream must be available for ARRAY<BLOB>.");
            }
            if (payloadPosition < 0
                    || payloadLength < MIN_PAYLOAD_LENGTH
                    || payloadPosition > Long.MAX_VALUE - payloadLength) {
                throw new IllegalArgumentException(
                        "Invalid ARRAY<BLOB> payload position or length: "
                                + payloadPosition
                                + ", "
                                + payloadLength);
            }

            try {
                byte[] header = new byte[HEADER_LENGTH];
                in.seek(payloadPosition);
                IOUtils.readFully(in, header);
                ByteBuffer headerBuffer = littleEndianBuffer(header);
                int magic = headerBuffer.getInt();
                if (magic != MAGIC_NUMBER) {
                    throw new IllegalArgumentException(
                            "Invalid ARRAY<BLOB> payload magic number: " + magic);
                }
                byte version = headerBuffer.get();
                if (version != VERSION) {
                    throw new UnsupportedOperationException(
                            "Unsupported ARRAY<BLOB> payload version: " + version);
                }
                int elementCount = headerBuffer.getInt();
                if (elementCount < 0) {
                    throw new IllegalArgumentException(
                            "Invalid ARRAY<BLOB> element count: " + elementCount);
                }

                long payloadEnd = payloadPosition + payloadLength;
                long elementDataStart = payloadPosition + HEADER_LENGTH;
                long indexLengthPosition = payloadEnd - INDEX_LENGTH_SIZE;
                byte[] indexLengthBytes = new byte[INDEX_LENGTH_SIZE];
                in.seek(indexLengthPosition);
                IOUtils.readFully(in, indexLengthBytes);
                int indexLength = littleEndianBuffer(indexLengthBytes).getInt();
                long maximumIndexLength = payloadLength - MIN_PAYLOAD_LENGTH;
                if (indexLength < 0 || indexLength > maximumIndexLength) {
                    throw new IllegalArgumentException(
                            "Invalid ARRAY<BLOB> element index length: " + indexLength);
                }
                if (elementCount > indexLength) {
                    throw new IllegalArgumentException(
                            "ARRAY<BLOB> element count exceeds element index length.");
                }

                byte[] indexBytes = new byte[indexLength];
                long indexStart = indexLengthPosition - indexLength;
                in.seek(indexStart);
                IOUtils.readFully(in, indexBytes);
                long[] elementLengths;
                try {
                    elementLengths = DeltaVarintCompressor.decompress(indexBytes);
                } catch (RuntimeException e) {
                    throw new IllegalArgumentException("Invalid ARRAY<BLOB> element index.", e);
                }
                if (elementLengths.length != elementCount) {
                    throw new IllegalArgumentException(
                            "ARRAY<BLOB> element count does not match element index length.");
                }

                long elementDataLength = indexStart - elementDataStart;
                long totalElementLength = 0;
                for (long elementLength : elementLengths) {
                    if (elementLength == NULL_ELEMENT_LENGTH) {
                        continue;
                    }
                    if (elementLength < 0) {
                        throw new IllegalArgumentException(
                                "Invalid ARRAY<BLOB> element length: " + elementLength);
                    }
                    if (!blobAsDescriptor() && elementLength > Integer.MAX_VALUE) {
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
                    if (elementLength == NULL_ELEMENT_LENGTH) {
                        blobs[i] = null;
                    } else {
                        blobs[i] = readBlob(elementOffset, elementLength);
                        elementOffset += elementLength;
                    }
                }
                return new GenericArray(blobs);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean requiresInputStream() {
            return true;
        }

        @Override
        public Object placeholder() {
            return BlobArrayPlaceholder.INSTANCE;
        }
    }

    private static ByteBuffer littleEndianBuffer(byte[] bytes) {
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
    }
}
