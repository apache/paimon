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
import org.apache.paimon.compression.BlockDecompressor;
import org.apache.paimon.data.AbstractPagedInputView;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.serializer.BinaryRowSerializer;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.memory.Buffer;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.utils.MutableObjectIterator;

import java.io.EOFException;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * A {@link DataInputView} that is backed by a {@link BufferFileReader}, making it effectively a
 * data input stream. The view reads it data in blocks from the underlying channel and decompress it
 * before returning to caller. The view can only read data that has been written by {@link
 * ChannelWriterOutputView}, due to block formatting.
 */
public class ChannelReaderInputView extends AbstractPagedInputView {

    private final BlockDecompressor decompressor;
    private final BufferFileReader reader;
    private final MemorySegment uncompressedBuffer;

    private final MemorySegment compressedBuffer;

    private int numBlocksRemaining;
    private int currentSegmentLimit;

    public ChannelReaderInputView(
            FileIOChannel.ID id,
            IOManager ioManager,
            BlockCompressionFactory compressionCodecFactory,
            int compressionBlockSize,
            int numBlocks)
            throws IOException {
        this.numBlocksRemaining = numBlocks;
        this.reader = ioManager.createBufferFileReader(id);
        uncompressedBuffer = MemorySegment.wrap(new byte[compressionBlockSize]);
        decompressor = compressionCodecFactory.getDecompressor();
        compressedBuffer =
                MemorySegment.wrap(
                        new byte
                                [compressionCodecFactory
                                        .getCompressor()
                                        .getMaxCompressedSize(compressionBlockSize)]);
    }

    @Override
    protected MemorySegment nextSegment(MemorySegment current) throws IOException {
        // check for end-of-stream
        if (this.numBlocksRemaining <= 0) {
            this.reader.close();
            throw new EOFException();
        }

        Buffer buffer = Buffer.create(compressedBuffer);
        reader.readInto(buffer);
        this.currentSegmentLimit =
                decompressor.decompress(
                        buffer.getMemorySegment().getArray(),
                        0,
                        buffer.getSize(),
                        uncompressedBuffer.getArray(),
                        0);
        this.numBlocksRemaining--;
        return uncompressedBuffer;
    }

    @Override
    protected int getLimitForSegment(MemorySegment segment) {
        return currentSegmentLimit;
    }

    public List<MemorySegment> close() throws IOException {
        reader.close();
        return Collections.emptyList();
    }

    public FileIOChannel getChannel() {
        return reader;
    }

    public MutableObjectIterator<BinaryRow> createBinaryRowIterator(
            BinaryRowSerializer serializer) {
        return new BinaryRowChannelInputViewIterator(serializer);
    }

    private class BinaryRowChannelInputViewIterator implements MutableObjectIterator<BinaryRow> {

        protected final BinaryRowSerializer serializer;

        public BinaryRowChannelInputViewIterator(BinaryRowSerializer serializer) {
            this.serializer = serializer;
        }

        @Override
        public BinaryRow next(BinaryRow reuse) throws IOException {
            try {
                return this.serializer.deserializeFromPages(reuse, ChannelReaderInputView.this);
            } catch (EOFException e) {
                close();
                return null;
            }
        }

        @Override
        public BinaryRow next() throws IOException {
            throw new UnsupportedOperationException(
                    "This method is disabled due to performance issue!");
        }
    }
}
