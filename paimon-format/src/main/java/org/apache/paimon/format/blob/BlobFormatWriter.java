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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileAwareFormatWriter;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.utils.DeltaVarintCompressor;
import org.apache.paimon.utils.LongArrayList;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.zip.CRC32;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.StreamUtils.intToLittleEndian;
import static org.apache.paimon.utils.StreamUtils.longToLittleEndian;

/** {@link FormatWriter} for blob file. */
public class BlobFormatWriter implements FileAwareFormatWriter {

    public static final byte VERSION = 1;
    public static final int MAGIC_NUMBER = 1481511375;
    public static final byte[] MAGIC_NUMBER_BYTES = intToLittleEndian(MAGIC_NUMBER);

    private final PositionOutputStream out;
    @Nullable private final BlobConsumer writeConsumer;
    private final CRC32 crc32;
    private final byte[] tmpBuffer;
    private final LongArrayList lengths;

    private String pathString;

    public BlobFormatWriter(PositionOutputStream out, @Nullable BlobConsumer writeConsumer) {
        this.out = out;
        this.writeConsumer = writeConsumer;
        this.crc32 = new CRC32();
        this.tmpBuffer = new byte[4096];
        this.lengths = new LongArrayList(16);
    }

    @Override
    public void setFile(Path file) {
        this.pathString = file.toString();
    }

    @Override
    public boolean deleteFileUponAbort() {
        return writeConsumer == null;
    }

    @Override
    public void addElement(InternalRow element) throws IOException {
        checkArgument(element.getFieldCount() == 1, "BlobFormatWriter only support one field.");
        checkArgument(!element.isNullAt(0), "BlobFormatWriter only support non-null blob.");
        Blob blob = element.getBlob(0);

        long previousPos = out.getPos();
        crc32.reset();

        write(MAGIC_NUMBER_BYTES);

        long blobPos = out.getPos();
        try (SeekableInputStream in = blob.newInputStream()) {
            int bytesRead = in.read(tmpBuffer);
            while (bytesRead >= 0) {
                write(tmpBuffer, bytesRead);
                bytesRead = in.read(tmpBuffer);
            }
        }

        long blobLength = out.getPos() - blobPos;
        long binLength = out.getPos() - previousPos + 12;
        lengths.add(binLength);
        byte[] lenBytes = longToLittleEndian(binLength);
        write(lenBytes);
        int crcValue = (int) crc32.getValue();
        out.write(intToLittleEndian(crcValue));

        if (writeConsumer != null) {
            BlobDescriptor descriptor = new BlobDescriptor(pathString, blobPos, blobLength);
            boolean flush = writeConsumer.accept(descriptor);
            if (flush) {
                out.flush();
            }
        }
    }

    private void write(byte[] bytes) throws IOException {
        write(bytes, bytes.length);
    }

    private void write(byte[] bytes, int length) throws IOException {
        crc32.update(bytes, 0, length);
        out.write(bytes, 0, length);
    }

    @Override
    public boolean reachTargetSize(boolean suggestedCheck, long targetSize) throws IOException {
        // check target size every record
        // Each blob is very large, so the cost of check is not high
        return out.getPos() >= targetSize;
    }

    @Override
    public void close() throws IOException {
        // index
        byte[] indexBytes = DeltaVarintCompressor.compress(lengths.toArray());
        out.write(indexBytes);
        // header
        out.write(intToLittleEndian(indexBytes.length));
        out.write(VERSION);
    }
}
