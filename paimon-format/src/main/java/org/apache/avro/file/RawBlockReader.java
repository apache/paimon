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

package org.apache.avro.file;

import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.utils.IOUtils;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.NoSuchElementException;

/** Package bridge exposing Avro's compressed blocks without reflection. */
public final class RawBlockReader extends DataFileStream<Void> {

    private final SeekableInputStream input;
    private final long headerLength;
    @Nullable private byte[] headerBytes;
    private long blockOffset;
    private long blockLength;
    private boolean pending;

    public RawBlockReader(SeekableInputStream input) throws IOException {
        super(input, new NoOpDatumReader<Void>());
        this.input = input;
        this.headerLength = position();
    }

    /** Returns a copy of the complete OCF header, reading and caching it on first access. */
    public byte[] headerBytes() throws IOException {
        if (headerBytes == null) {
            byte[] bytes = new byte[Math.toIntExact(headerLength)];
            long resumePosition = input.getPos();
            try {
                input.seek(0);
                IOUtils.readFully(input, bytes);
            } finally {
                // Preserve the position past any bytes already buffered by the Avro decoder.
                input.seek(resumePosition);
            }
            headerBytes = bytes;
        }
        return headerBytes.clone();
    }

    /**
     * Returns the physical block offset; read immediately after {@link #nextRawBlock(RawBlock)}.
     */
    public long blockOffset() {
        return blockOffset;
    }

    /** Returns the last-read block's encoded length, including its header and sync marker. */
    public long blockLength() {
        return blockLength;
    }

    private long position() throws IOException {
        // This is the same read-ahead adjustment used by DataFileReader.blockFinished().
        return input.getPos() - vin.inputStream().available();
    }

    public boolean hasNextRawBlock() throws IOException {
        if (!pending) {
            blockOffset = position();
            pending = super.hasNextBlock();
        }
        return pending;
    }

    public RawBlock nextRawBlock(RawBlock reuse) throws IOException {
        if (!hasNextRawBlock()) {
            throw new NoSuchElementException();
        }
        DataBlock raw = super.nextRawBlock(reuse == null ? null : reuse.dataBlock());
        blockLength = position() - blockOffset;
        pending = false;
        return reuse == null
                ? new RawBlock(raw, resolveCodec(), getSchema())
                : reuse.replace(raw, resolveCodec(), getSchema());
    }

    private static final class NoOpDatumReader<D> implements DatumReader<D> {

        @Override
        public void setSchema(Schema schema) {}

        @Override
        public D read(D reuse, Decoder decoder) {
            throw new UnsupportedOperationException();
        }
    }
}
