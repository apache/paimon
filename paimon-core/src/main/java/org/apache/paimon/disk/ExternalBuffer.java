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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.compression.BlockCompressionFactory;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.AbstractRowDataSerializer;
import org.apache.paimon.data.serializer.BinaryRowSerializer;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.utils.MutableObjectIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkState;

/** An external buffer for storing rows, it will spill the data to disk when the memory is full. */
public class ExternalBuffer implements RowBuffer {

    private static final Logger LOG = LoggerFactory.getLogger(ExternalBuffer.class);

    private final IOManager ioManager;
    private final MemorySegmentPool pool;
    private final BinaryRowSerializer binaryRowSerializer;
    private final InMemoryBuffer inMemoryBuffer;
    private final MemorySize maxDiskSize;
    private final BlockCompressionFactory compactionFactory;

    // The size of each segment
    private final int segmentSize;

    private final List<ChannelWithMeta> spilledChannelIDs;
    private int numRows;

    private boolean addCompleted;

    ExternalBuffer(
            IOManager ioManager,
            MemorySegmentPool pool,
            AbstractRowDataSerializer<?> serializer,
            MemorySize maxDiskSize,
            String compression) {
        this.ioManager = ioManager;
        this.pool = pool;
        this.maxDiskSize = maxDiskSize;

        this.compactionFactory = BlockCompressionFactory.create(compression);

        this.binaryRowSerializer =
                serializer instanceof BinaryRowSerializer
                        ? (BinaryRowSerializer) serializer.duplicate()
                        : new BinaryRowSerializer(serializer.getArity());

        this.segmentSize = pool.pageSize();

        this.spilledChannelIDs = new ArrayList<>();

        this.numRows = 0;

        this.addCompleted = false;

        //noinspection unchecked
        this.inMemoryBuffer =
                new InMemoryBuffer(pool, (AbstractRowDataSerializer<InternalRow>) serializer);
    }

    @Override
    public void reset() {
        clearChannels();
        inMemoryBuffer.reset();
        numRows = 0;
        addCompleted = false;
    }

    @Override
    public boolean flushMemory() throws IOException {
        boolean isFull = getDiskUsage() >= maxDiskSize.getBytes();
        if (isFull) {
            return false;
        } else {
            spill();
            return true;
        }
    }

    private long getDiskUsage() {
        long bytes = 0;

        for (ChannelWithMeta spillChannelID : spilledChannelIDs) {
            bytes += spillChannelID.getNumBytes();
        }
        return bytes;
    }

    @Override
    public boolean put(InternalRow row) throws IOException {
        checkState(!addCompleted, "This buffer has add completed.");
        if (!inMemoryBuffer.put(row)) {
            // Check if record is too big.
            if (inMemoryBuffer.getCurrentDataBufferOffset() == 0) {
                throwTooBigException(row);
            }
            spill();
            if (!inMemoryBuffer.put(row)) {
                throwTooBigException(row);
            }
        }

        numRows++;
        return true;
    }

    @Override
    public void complete() {
        addCompleted = true;
    }

    @Override
    public RowBufferIterator newIterator() {
        checkState(addCompleted, "This buffer has not add completed.");
        return new BufferIterator();
    }

    private void throwTooBigException(InternalRow row) throws IOException {
        int rowSize = inMemoryBuffer.getSerializer().toBinaryRow(row).toBytes().length;
        throw new IOException(
                "Record is too big, it can't be added to a empty InMemoryBuffer! "
                        + "Record size: "
                        + rowSize
                        + ", Buffer: "
                        + memorySize());
    }

    private void spill() throws IOException {
        FileIOChannel.ID channel = ioManager.createChannel();

        ChannelWriterOutputView channelWriterOutputView =
                new ChannelWriterOutputView(
                        ioManager.createBufferFileWriter(channel), compactionFactory, segmentSize);
        int numRecordBuffers = inMemoryBuffer.getNumRecordBuffers();
        ArrayList<MemorySegment> segments = inMemoryBuffer.getRecordBufferSegments();
        try {
            // spill in memory buffer in zero-copy.
            for (int i = 0; i < numRecordBuffers; i++) {
                MemorySegment segment = segments.get(i);
                int bufferSize =
                        i == numRecordBuffers - 1
                                ? inMemoryBuffer.getNumBytesInLastBuffer()
                                : segment.size();
                channelWriterOutputView.write(segment, 0, bufferSize);
            }
            LOG.info(
                    "here spill the reset buffer data with {} records {} bytes",
                    inMemoryBuffer.size(),
                    channelWriterOutputView.getNumBytes());
            channelWriterOutputView.close();
        } catch (IOException e) {
            channelWriterOutputView.closeAndDelete();
            throw e;
        }

        spilledChannelIDs.add(
                new ChannelWithMeta(
                        channel,
                        inMemoryBuffer.getNumRecordBuffers(),
                        inMemoryBuffer.getNumBytesInLastBuffer(),
                        channelWriterOutputView.getNumBytes()));

        inMemoryBuffer.reset();
    }

    @Override
    public int size() {
        return numRows;
    }

    @Override
    public long memoryOccupancy() {
        return inMemoryBuffer.memoryOccupancy();
    }

    private int memorySize() {
        return pool.freePages() * segmentSize;
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void clearChannels() {
        for (ChannelWithMeta meta : spilledChannelIDs) {
            final File f = new File(meta.getChannel().getPath());
            if (f.exists()) {
                f.delete();
            }
        }
        spilledChannelIDs.clear();
    }

    /** Iterator of external buffer. */
    public class BufferIterator implements RowBufferIterator {

        private MutableObjectIterator<BinaryRow> currentIterator;
        private final BinaryRow reuse = binaryRowSerializer.createInstance();

        private int currentChannelID = -1;
        private BinaryRow row;
        private boolean closed;
        private ChannelReaderInputView channelReader;

        private BufferIterator() {
            this.closed = false;
        }

        private void checkValidity() {
            if (closed) {
                throw new RuntimeException("This iterator is closed!");
            }
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }

            try {
                closeCurrentFileReader();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            closed = true;
        }

        @Override
        public boolean advanceNext() {
            checkValidity();

            try {
                // get from curr iterator or new iterator.
                while (true) {
                    if (currentIterator != null && (row = currentIterator.next(reuse)) != null) {
                        return true;
                    } else {
                        if (!nextIterator()) {
                            return false;
                        }
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private boolean nextIterator() throws IOException {
            if (currentChannelID == Integer.MAX_VALUE || numRows == 0) {
                return false;
            } else if (currentChannelID < spilledChannelIDs.size() - 1) {
                nextSpilledIterator();
            } else {
                newMemoryIterator();
            }
            return true;
        }

        @Override
        public BinaryRow getRow() {
            return row;
        }

        private void closeCurrentFileReader() throws IOException {
            if (channelReader != null) {
                channelReader.close();
                channelReader = null;
            }
        }

        private void nextSpilledIterator() throws IOException {
            ChannelWithMeta channel = spilledChannelIDs.get(currentChannelID + 1);
            currentChannelID++;

            // close current reader first.
            closeCurrentFileReader();

            // new reader.
            this.channelReader =
                    new ChannelReaderInputView(
                            channel.getChannel(),
                            ioManager,
                            compactionFactory,
                            segmentSize,
                            channel.getBlockCount());

            this.currentIterator = channelReader.createBinaryRowIterator(binaryRowSerializer);
        }

        private void newMemoryIterator() {
            this.currentChannelID = Integer.MAX_VALUE;
            this.currentIterator = inMemoryBuffer.newIterator();
        }
    }

    @VisibleForTesting
    public List<ChannelWithMeta> getSpillChannels() {
        return spilledChannelIDs;
    }
}
