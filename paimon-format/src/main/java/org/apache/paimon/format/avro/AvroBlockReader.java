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

import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IOUtils;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.file.RawBlock;
import org.apache.avro.file.RawBlockReader;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Reader which exposes compressed and decompressed blocks from an Avro object container file.
 *
 * <p>This reader owns the input stream and closes it when construction fails or {@link #close()} is
 * called.
 */
public final class AvroBlockReader implements Closeable {

    private final RawBlockReader reader;

    private @Nullable AvroRawBlock borrowedRawBlock;
    private @Nullable ByteBuffer decompressionBuffer;
    private long currentBlockRecordCount = -1;

    public AvroBlockReader(InputStream input) throws IOException {
        try {
            this.reader = new RawBlockReader(input);
        } catch (IOException | RuntimeException | Error e) {
            IOUtils.closeQuietly(input);
            throw e;
        }
    }

    Schema schema() {
        return reader.getSchema();
    }

    /** Creates a record decoder from the writer schema stored in the Avro file header. */
    public AvroRecordDecoder createRecordDecoder() {
        return new AvroRecordDecoder(reader.getSchema());
    }

    /** Returns whether blocks can be copied directly to the given Avro format. */
    public boolean supportsRawBlockCopy(AvroFileFormat fileFormat, RowType rowType) {
        return fileFormat.supportsRawBlockCopy(rowType, reader.getSchema());
    }

    /** Returns whether another block is available. */
    public boolean hasNextBlock() throws IOException {
        return replaceAvroRuntimeException(reader::hasNextRawBlock);
    }

    /**
     * Returns a decompressed copy of the next block.
     *
     * <p>The returned array is owned by the caller and remains valid after this reader advances or
     * closes.
     */
    public byte[] nextBlock() throws IOException {
        BorrowedBlock block = nextBorrowedBlock();
        byte[] bytes = new byte[block.length];
        System.arraycopy(block.bytes, block.offset, bytes, 0, block.length);
        return bytes;
    }

    /**
     * Returns a borrowed view of the next decompressed block.
     *
     * <p>The returned bytes are owned by this reader and may be overwritten by the next call to
     * {@link #hasNextBlock()}, {@link #nextBlock()}, or this method, or when this reader is closed.
     */
    public BorrowedBlock nextBorrowedBlock() throws IOException {
        ByteBuffer block = nextBorrowedRawBlock().decompress(decompressionBuffer);
        decompressionBuffer = block;
        return new BorrowedBlock(
                block.array(),
                block.arrayOffset() + block.position(),
                block.remaining(),
                currentBlockRecordCount);
    }

    /**
     * Returns a borrowed view of the next compressed block.
     *
     * <p>The returned holder and its storage are owned by this reader and reused by the next call
     * to this method. Consume the block before advancing this reader.
     */
    public AvroRawBlock nextBorrowedRawBlock() throws IOException {
        RawBlock block =
                replaceAvroRuntimeException(
                        () ->
                                reader.nextRawBlock(
                                        borrowedRawBlock == null
                                                ? null
                                                : borrowedRawBlock.rawBlock()));
        borrowedRawBlock =
                borrowedRawBlock == null
                        ? new AvroRawBlock(block)
                        : borrowedRawBlock.replace(block);
        currentBlockRecordCount = borrowedRawBlock.recordCount();
        return borrowedRawBlock;
    }

    /** Returns the record count of the last block returned by a block-reading method. */
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

    /** Borrowed decompressed block contents and its record count. */
    public static final class BorrowedBlock {

        private final byte[] bytes;
        private final int offset;
        private final int length;
        private final long recordCount;

        private BorrowedBlock(byte[] bytes, int offset, int length, long recordCount) {
            this.bytes = bytes;
            this.offset = offset;
            this.length = length;
            this.recordCount = recordCount;
        }

        public byte[] bytes() {
            return bytes;
        }

        public int offset() {
            return offset;
        }

        public int length() {
            return length;
        }

        public long recordCount() {
            return recordCount;
        }
    }

    @FunctionalInterface
    private interface IOSupplier<T> {

        T get() throws IOException;
    }
}
