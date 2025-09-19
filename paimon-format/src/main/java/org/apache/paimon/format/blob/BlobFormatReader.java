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
import org.apache.paimon.memory.BytesUtils;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.utils.DeltaVarintCompressor;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.RoaringBitmap32;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Iterator;

/** {@link FileRecordReader} for blob file. */
public class BlobFormatReader implements FileRecordReader<InternalRow> {

    private final FileIO fileIO;
    private final Path filePath;
    private final long[] blobLengths;
    private final long[] blobOffsets;

    private boolean returned;

    public BlobFormatReader(
            FileIO fileIO, Path filePath, long fileSize, @Nullable RoaringBitmap32 selection)
            throws IOException {
        this.fileIO = fileIO;
        this.filePath = filePath;
        this.returned = false;
        try (SeekableInputStream in = fileIO.newInputStream(filePath)) {
            in.seek(fileSize - 5);
            byte[] header = new byte[5];
            IOUtils.readFully(in, header);
            byte version = header[4];
            if (version != 1) {
                throw new IOException("Unsupported version: " + version);
            }
            int indexLength = BytesUtils.getInt(header, 0);

            in.seek(fileSize - 5 - indexLength);
            byte[] indexBytes = new byte[indexLength];
            IOUtils.readFully(in, indexBytes);

            long[] blobLengths = DeltaVarintCompressor.decompress(indexBytes);
            long[] blobOffsets = new long[blobLengths.length];
            long offset = 0;
            for (int i = 0; i < blobLengths.length; i++) {
                blobOffsets[i] = offset;
                offset += blobLengths[i];
            }

            if (selection != null) {
                int cardinality = (int) selection.getCardinality();
                long[] newLengths = new long[cardinality];
                long[] newOffsets = new long[cardinality];
                Iterator<Integer> iterator = selection.iterator();
                for (int i = 0; i < cardinality; i++) {
                    Integer next = iterator.next();
                    newLengths[i] = blobLengths[next];
                    newOffsets[i] = blobOffsets[next];
                }
                blobLengths = newLengths;
                blobOffsets = newOffsets;
            }

            this.blobLengths = blobLengths;
            this.blobOffsets = blobOffsets;
        }
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
                return currentPosition;
            }

            @Override
            public Path filePath() {
                return filePath;
            }

            @Nullable
            @Override
            public InternalRow next() {
                if (currentPosition >= blobLengths.length) {
                    return null;
                }

                BlobFileRef blobRef =
                        new BlobFileRef(
                                fileIO,
                                filePath,
                                blobLengths[currentPosition],
                                blobOffsets[currentPosition]);
                currentPosition++;
                return GenericRow.of(blobRef);
            }

            @Override
            public void releaseBatch() {}
        };
    }

    @Override
    public void close() throws IOException {}
}
