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

import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;

import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.NoSuchElementException;

/** Package bridge exposing Avro's compressed blocks without reflection. */
public final class RawBlockReader extends DataFileStream<Void> {

    private final CountingInput input;
    private final byte[] headerBytes;
    private long blockOffset;
    private long blockLength;
    private boolean pending;

    public RawBlockReader(InputStream input) throws IOException {
        this(new CountingInput(input));
    }

    private RawBlockReader(CountingInput input) throws IOException {
        super(input, new NoOpDatumReader<Void>());
        this.input = input;
        this.headerBytes = Arrays.copyOf(input.prefix.toByteArray(), Math.toIntExact(position()));
        input.prefix = null;
    }

    /** Returns a copy of the complete OCF header, including schema, codec and sync marker. */
    public byte[] headerBytes() {
        return headerBytes.clone();
    }

    /**
     * Returns the block offset relative to the initial input position; read immediately after
     * {@link #nextRawBlock(RawBlock)}.
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
        return input.position - vin.inputStream().available();
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

    private static final class CountingInput extends FilterInputStream {
        private long position;
        private ByteArrayOutputStream prefix = new ByteArrayOutputStream();

        private CountingInput(InputStream input) {
            super(input);
        }

        @Override
        public int read() throws IOException {
            int value = in.read();
            if (value >= 0) {
                position++;
                if (prefix != null) {
                    prefix.write(value);
                }
            }
            return value;
        }

        @Override
        public int read(byte[] bytes, int offset, int length) throws IOException {
            int n = in.read(bytes, offset, length);
            if (n > 0) {
                position += n;
                if (prefix != null) {
                    prefix.write(bytes, offset, n);
                }
            }
            return n;
        }
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
