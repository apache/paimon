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

package org.apache.paimon.format.avro;

import org.apache.paimon.utils.IOUtils;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Reader which exposes decompressed blocks from an Avro object container file.
 *
 * <p>This reader owns the input stream and closes it when construction fails or {@link #close()} is
 * called.
 */
public final class AvroBlockReader implements Closeable {

    private final DataFileStream<Object> reader;

    private long currentBlockRecordCount = -1;

    public AvroBlockReader(InputStream input) throws IOException {
        try {
            this.reader = new DataFileStream<>(input, new GenericDatumReader<>());
        } catch (IOException | RuntimeException | Error e) {
            IOUtils.closeQuietly(input);
            throw e;
        }
    }

    /** Returns the writer schema stored in the Avro file header. */
    public Schema schema() {
        return reader.getSchema();
    }

    /** Returns whether another block is available. */
    public boolean hasNextBlock() throws IOException {
        return replaceAvroRuntimeException(reader::hasNext);
    }

    /**
     * Returns a decompressed copy of the next block.
     *
     * <p>The returned array is owned by the caller and remains valid after this reader advances or
     * closes.
     */
    public byte[] nextBlock() throws IOException {
        ByteBuffer block = replaceAvroRuntimeException(reader::nextBlock);
        currentBlockRecordCount = reader.getBlockCount();

        ByteBuffer copy = block.duplicate();
        byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return bytes;
    }

    /** Returns the record count of the last block returned by {@link #nextBlock()}. */
    public long currentBlockRecordCount() {
        if (currentBlockRecordCount < 0) {
            throw new IllegalStateException("No block has been read.");
        }
        return currentBlockRecordCount;
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    private static <T> T replaceAvroRuntimeException(IOSupplier<T> supplier) throws IOException {
        try {
            return supplier.get();
        } catch (AvroRuntimeException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw e;
        }
    }

    @FunctionalInterface
    private interface IOSupplier<T> {

        T get() throws IOException;
    }
}
