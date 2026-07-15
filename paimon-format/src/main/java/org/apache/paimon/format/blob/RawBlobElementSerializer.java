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
import org.apache.paimon.data.BlobConsumer;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.data.BlobFetchMetricReporter;
import org.apache.paimon.data.BlobPlaceholder;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;

import javax.annotation.Nullable;

import java.io.IOException;

/** Element serializer for a raw BLOB field. */
final class RawBlobElementSerializer implements BlobElementSerializer {

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
    public boolean requiresReadInputStream(boolean blobAsDescriptor) {
        return !blobAsDescriptor;
    }

    @Override
    public BlobElementSerializer.Reader createReader(
            FileIO fileIO,
            Path filePath,
            @Nullable SeekableInputStream in,
            boolean blobAsDescriptor) {
        return new Reader(fileIO, filePath, in, blobAsDescriptor);
    }

    /** Writer for a raw BLOB field. */
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
                recordPreCheckedMissingFileNull(row);
                return writeNullElement();
            }

            BlobFetchResult fetchResult = getBlob(() -> row.getBlob(0));
            if (fetchResult.fetchFailure()) {
                return writeNullElement();
            }
            Blob blob = fetchResult.blob();
            if (blob == BlobPlaceholder.INSTANCE) {
                return writePlaceholderElement();
            }

            SeekableInputStream in = openBlobInputStream(blob);
            if (in == null) {
                return writeNullElement();
            }

            long recordPosition = startRecord();
            BlobDescriptor descriptor = writeBlobData(in);
            long recordLength = finishRecord(recordPosition);
            if (accept(descriptor)) {
                flush();
            }
            recordSuccess(descriptor.length());
            return recordLength;
        }
    }

    /** Reader for a raw BLOB field. */
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
            return readBlob(payloadPosition, payloadLength);
        }

        @Override
        public Object placeholder() {
            return BlobPlaceholder.INSTANCE;
        }
    }
}
