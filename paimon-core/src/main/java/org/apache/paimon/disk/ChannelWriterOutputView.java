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

package org.apache.paimon.disk;

import org.apache.paimon.compression.BlockCompressionFactory;
import org.apache.paimon.compression.BlockCompressor;
import org.apache.paimon.data.AbstractPagedOutputView;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.memory.Buffer;
import org.apache.paimon.memory.MemorySegment;

import java.io.Closeable;
import java.io.IOException;

/**
 * A {@link DataOutputView} that is backed by a {@link FileIOChannel}, making it effectively a data
 * output stream. The view will compress its data before writing it in blocks to the underlying
 * channel.
 */
public final class ChannelWriterOutputView extends AbstractPagedOutputView implements Closeable {

    private final MemorySegment compressedBuffer;
    private final BlockCompressor compressor;
    private final BufferFileWriter writer;

    private int blockCount;

    private long numBytes;
    private long numCompressedBytes;
    private long writeBytes;

    public ChannelWriterOutputView(
            BufferFileWriter writer,
            BlockCompressionFactory compressionCodecFactory,
            int compressionBlockSize) {
        super(MemorySegment.wrap(new byte[compressionBlockSize]), compressionBlockSize);

        compressor = compressionCodecFactory.getCompressor();
        compressedBuffer =
                MemorySegment.wrap(new byte[compressor.getMaxCompressedSize(compressionBlockSize)]);
        this.writer = writer;
    }

    public FileIOChannel getChannel() {
        return writer;
    }

    @Override
    public void close() throws IOException {
        if (!writer.isClosed()) {
            int currentPositionInSegment = getCurrentPositionInSegment();
            writeCompressed(currentSegment, currentPositionInSegment);
            clear();
            this.writeBytes = writer.getSize();
            this.writer.close();
        }
    }

    public void closeAndDelete() throws IOException {
        try {
            close();
        } finally {
            writer.deleteChannel();
        }
    }

    @Override
    protected MemorySegment nextSegment(MemorySegment current, int positionInCurrent)
            throws IOException {
        writeCompressed(current, positionInCurrent);
        return current;
    }

    private void writeCompressed(MemorySegment current, int size) throws IOException {
        int compressedLen =
                compressor.compress(current.getArray(), 0, size, compressedBuffer.getArray(), 0);
        writer.writeBlock(Buffer.create(compressedBuffer, compressedLen));
        blockCount++;
        numBytes += size;
        numCompressedBytes += compressedLen;
    }

    public long getNumBytes() {
        return numBytes;
    }

    public long getNumCompressedBytes() {
        return numCompressedBytes;
    }

    public long getWriteBytes() {
        return writeBytes;
    }

    public int getBlockCount() {
        return blockCount;
    }
}
