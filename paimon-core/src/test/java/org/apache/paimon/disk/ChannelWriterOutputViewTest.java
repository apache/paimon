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
import org.apache.paimon.compression.BlockCompressionType;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.serializer.BinaryRowSerializer;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.memory.Buffer;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.sort.BinaryExternalSortBuffer;
import org.apache.paimon.sort.BinaryInMemorySortBuffer;
import org.apache.paimon.sort.IntNormalizedKeyComputer;
import org.apache.paimon.sort.IntRecordComparator;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ChannelWriterOutputView}. */
public class ChannelWriterOutputViewTest {

    private static final int BLOCK_SIZE = 64 * 1024;

    @TempDir Path tempDir;

    @Test
    public void testEmptyOutput() throws Exception {
        BlockCompressionFactory compressionFactory =
                BlockCompressionFactory.create(BlockCompressionType.LZ4);
        try (IOManager ioManager = IOManager.create(tempDir.toString())) {
            FileIOChannel.ID channel = ioManager.createChannel();
            ChannelWriterOutputView output =
                    FileChannelUtil.createOutputView(
                            ioManager, channel, compressionFactory, BLOCK_SIZE);
            output.close();

            ChannelReaderInputView input =
                    new ChannelReaderInputView(
                            channel,
                            ioManager,
                            compressionFactory,
                            BLOCK_SIZE,
                            output.getBlockCount());
            try {
                assertThat(
                                new ChannelReaderInputViewIterator(
                                                input, null, new BinaryRowSerializer(1))
                                        .next())
                        .isNull();
                assertThat(output.getBlockCount()).isZero();
            } finally {
                input.getChannel().closeAndDelete();
            }
        }
    }

    @Test
    public void testCloseClosesChannelWhenFinalFlushFails() throws Exception {
        IOException failure = new IOException("spill write failed");
        try (IOManager ioManager = IOManager.create(tempDir.toString())) {
            BufferFileWriter writer = failingWriter(ioManager.createChannel(), failure, null);
            ChannelWriterOutputView output = output(writer);
            try {
                output.writeInt(1);
                assertThatThrownBy(output::close).isSameAs(failure);
                assertThat(writer.isClosed()).isTrue();
            } finally {
                writer.getNioFileChannel().close();
            }
        }
    }

    @Test
    public void testCloseClosesChannelWhenReadingSizeFails() throws Exception {
        IOException failure = new IOException("channel size failed");
        try (IOManager ioManager = IOManager.create(tempDir.toString())) {
            BufferFileWriter writer =
                    new BufferFileWriterImpl(ioManager.createChannel()) {
                        @Override
                        public long getSize() throws IOException {
                            throw failure;
                        }
                    };
            ChannelWriterOutputView output = output(writer);
            try {
                assertThatThrownBy(output::close).isSameAs(failure);
                assertThat(writer.isClosed()).isTrue();
            } finally {
                writer.getNioFileChannel().close();
            }
        }
    }

    @Test
    public void testNonEmptyOutput() throws Exception {
        try (IOManager ioManager = IOManager.create(tempDir.toString())) {
            FileIOChannel.ID channel = ioManager.createChannel();
            ChannelWriterOutputView output = output(ioManager.createBufferFileWriter(channel));
            output.writeInt(123);
            output.close();
            output.close();
            assertThat(output.getNumBytes()).isEqualTo(4);
            assertThat(output.getBlockCount()).isEqualTo(1);
            assertThat(output.getWriteBytes()).isEqualTo(channel.getPathFile().length());
            assertThat(output.getNumCompressedBytes()).isPositive();

            ChannelReaderInputView input =
                    new ChannelReaderInputView(
                            channel,
                            ioManager,
                            BlockCompressionFactory.create(BlockCompressionType.LZ4),
                            BLOCK_SIZE,
                            output.getBlockCount());
            try {
                assertThat(input.readInt()).isEqualTo(123);
            } finally {
                input.getChannel().closeAndDelete();
            }
        }
    }

    @Test
    public void testClosePreservesFlushFailureWhenChannelCloseFails() throws Exception {
        IOException failure = new IOException("spill write failed");
        IOException closeFailure = new IOException("channel close failed");
        try (IOManager ioManager = IOManager.create(tempDir.toString())) {
            BufferFileWriter writer =
                    failingWriter(ioManager.createChannel(), failure, closeFailure);
            ChannelWriterOutputView output = output(writer);
            try {
                output.writeInt(1);
                assertThatThrownBy(output::close).isSameAs(failure);
                assertThat(writer.isClosed()).isTrue();
                assertThat(failure.getSuppressed()).containsExactly(closeFailure);
            } finally {
                writer.getNioFileChannel().close();
            }
        }
    }

    @Test
    public void testClosePreservesSharedFlushAndCloseFailure() throws Exception {
        IOException failure = new IOException("spill write and close failed");
        try (IOManager ioManager = IOManager.create(tempDir.toString())) {
            BufferFileWriter writer = failingWriter(ioManager.createChannel(), failure, failure);
            ChannelWriterOutputView output = output(writer);
            try {
                output.writeInt(1);
                assertThatThrownBy(output::close).isSameAs(failure);
                assertThat(writer.isClosed()).isTrue();
                assertThat(failure.getSuppressed()).isEmpty();
            } finally {
                writer.getNioFileChannel().close();
            }
        }
    }

    @Test
    public void testCloseAndDeleteDeletesChannelWhenFinalFlushFails() throws Exception {
        IOException failure = new IOException("spill write failed");
        try (IOManager ioManager = IOManager.create(tempDir.toString())) {
            FileIOChannel.ID channel = ioManager.createChannel();
            BufferFileWriter writer = failingWriter(channel, failure, null);
            ChannelWriterOutputView output = output(writer);
            try {
                output.writeInt(1);
                assertThatThrownBy(output::closeAndDelete).isSameAs(failure);
                assertThat(writer.isClosed()).isTrue();
                assertThat(channel.getPathFile()).doesNotExist();
            } finally {
                writer.getNioFileChannel().close();
            }
        }
    }

    @Test
    public void testExternalSortClosesFailedSpillChannel() throws Exception {
        IOException failure = new IOException("spill write failed");
        AtomicReference<BufferFileWriter> createdWriter = new AtomicReference<>();
        try (IOManager ioManager =
                new IOManagerImpl(tempDir.toString()) {
                    @Override
                    public BufferFileWriter createBufferFileWriter(FileIOChannel.ID channel)
                            throws IOException {
                        BufferFileWriter writer = failingWriter(channel, failure, null);
                        createdWriter.set(writer);
                        return writer;
                    }
                }) {
            HeapMemorySegmentPool pool =
                    new HeapMemorySegmentPool(1024 * 1024, MemorySegmentPool.DEFAULT_PAGE_SIZE);
            int freePages = pool.freePages();
            BinaryInMemorySortBuffer inMemory =
                    BinaryInMemorySortBuffer.createBuffer(
                            IntNormalizedKeyComputer.INSTANCE,
                            new InternalRowSerializer(DataTypes.INT()),
                            IntRecordComparator.INSTANCE,
                            pool);
            BinaryExternalSortBuffer sorter =
                    new BinaryExternalSortBuffer(
                            new BinaryRowSerializer(1),
                            IntRecordComparator.INSTANCE,
                            pool.pageSize(),
                            inMemory,
                            ioManager,
                            128,
                            new CompressOptions("lz4", 1),
                            MemorySize.MAX_VALUE);
            try {
                assertThat(sorter.write(GenericRow.of(1))).isTrue();
                assertThatThrownBy(sorter::flushMemory).isSameAs(failure);
                BufferFileWriter writer = createdWriter.get();
                assertThat(writer).isNotNull();
                assertThat(writer.isClosed()).isTrue();
                assertThat(writer.getChannelID().getPathFile()).doesNotExist();
            } finally {
                if (createdWriter.get() != null) {
                    createdWriter.get().getNioFileChannel().close();
                }
                sorter.clear();
            }
            assertThat(pool.freePages()).isEqualTo(freePages);
        }
    }

    private ChannelWriterOutputView output(BufferFileWriter writer) {
        return new ChannelWriterOutputView(
                writer, BlockCompressionFactory.create(BlockCompressionType.LZ4), BLOCK_SIZE);
    }

    private BufferFileWriter failingWriter(
            FileIOChannel.ID channel, IOException failure, IOException closeFailure)
            throws IOException {
        return new BufferFileWriterImpl(channel) {
            @Override
            public void writeBlock(Buffer buffer) throws IOException {
                throw failure;
            }

            @Override
            public void close() throws IOException {
                super.close();
                if (closeFailure != null) {
                    throw closeFailure;
                }
            }
        };
    }
}
