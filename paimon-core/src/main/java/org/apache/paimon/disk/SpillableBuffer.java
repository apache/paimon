/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.disk;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.RandomAccessInputView;
import org.apache.paimon.data.SimpleCollectingOutputView;
import org.apache.paimon.data.serializer.AbstractRowDataSerializer;
import org.apache.paimon.data.serializer.BinaryRowSerializer;
import org.apache.paimon.memory.Buffer;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.utils.MutableObjectIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkState;

/** An external buffer for storing rows, it will spill the data to disk when the memory is full. */
public class SpillableBuffer {

    private static final Logger LOG = LoggerFactory.getLogger(SpillableBuffer.class);

    private final IOManager ioManager;
    private final MemorySegmentPool pool;
    private final BinaryRowSerializer binaryRowSerializer;
    private final InMemoryBuffer inMemoryBuffer;

    // The size of each segment
    private final int segmentSize;
    private final boolean spillable;

    private final List<ChannelWithMeta> spilledChannelIDs;
    private int numRows;

    private boolean addCompleted;

    public SpillableBuffer(
            IOManager ioManager,
            MemorySegmentPool pool,
            AbstractRowDataSerializer<?> serializer,
            boolean spillable) {
        this.ioManager = ioManager;
        this.pool = pool;

        this.binaryRowSerializer =
                serializer instanceof BinaryRowSerializer
                        ? (BinaryRowSerializer) serializer.duplicate()
                        : new BinaryRowSerializer(serializer.getArity());

        this.segmentSize = pool.pageSize();

        this.spillable = spillable;
        this.spilledChannelIDs = new ArrayList<>();

        this.numRows = 0;

        this.addCompleted = false;

        //noinspection unchecked
        this.inMemoryBuffer =
                new InMemoryBuffer((AbstractRowDataSerializer<InternalRow>) serializer);
    }

    public void reset() {
        clearChannels();
        inMemoryBuffer.reset();
        numRows = 0;
        addCompleted = false;
    }

    public boolean add(InternalRow row) throws IOException {
        checkState(!addCompleted, "This buffer has add completed.");
        if (!inMemoryBuffer.write(row)) {
            // Check if record is too big.
            if (inMemoryBuffer.getCurrentDataBufferOffset() == 0) {
                throwTooBigException(row);
            }
            if (!spillable) {
                return false;
            }
            spill();
            if (!inMemoryBuffer.write(row)) {
                throwTooBigException(row);
            }
        }

        numRows++;
        return true;
    }

    public void complete() {
        addCompleted = true;
    }

    public BufferIterator newIterator() {
        checkState(addCompleted, "This buffer has not add completed.");
        return new BufferIterator();
    }

    private void throwTooBigException(InternalRow row) throws IOException {
        int rowSize = inMemoryBuffer.serializer.toBinaryRow(row).toBytes().length;
        throw new IOException(
                "Record is too big, it can't be added to a empty InMemoryBuffer! "
                        + "Record size: "
                        + rowSize
                        + ", Buffer: "
                        + memorySize());
    }

    private void spill() throws IOException {
        FileIOChannel.ID channel = ioManager.createChannel();

        BufferFileWriter writer = ioManager.createBufferFileWriter(channel);
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
                writer.writeBlock(Buffer.create(segment, bufferSize));
            }
            LOG.info(
                    "here spill the reset buffer data with {} records {} bytes",
                    inMemoryBuffer.numRecords,
                    writer.getSize());
            writer.close();
        } catch (IOException e) {
            writer.closeAndDelete();
            throw e;
        }

        spilledChannelIDs.add(
                new ChannelWithMeta(
                        channel,
                        inMemoryBuffer.getNumRecordBuffers(),
                        inMemoryBuffer.getNumBytesInLastBuffer()));

        inMemoryBuffer.reset();
    }

    public int size() {
        return numRows;
    }

    public long memoryOccupancy() {
        return inMemoryBuffer.currentDataBufferOffset;
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
    public class BufferIterator implements Closeable {

        private MutableObjectIterator<BinaryRow> currentIterator;
        private final BinaryRow reuse = binaryRowSerializer.createInstance();

        private int currentChannelID = -1;
        private BinaryRow row;
        private boolean closed;
        private BufferFileReaderInputView channelReader;

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
                    new BufferFileReaderInputView(channel.getChannel(), ioManager, segmentSize);
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

    private class InMemoryBuffer {

        private final AbstractRowDataSerializer<InternalRow> serializer;
        private final ArrayList<MemorySegment> recordBufferSegments;
        private final SimpleCollectingOutputView recordCollector;

        private long currentDataBufferOffset;
        private int numBytesInLastBuffer;
        private int numRecords = 0;

        private InMemoryBuffer(AbstractRowDataSerializer<InternalRow> serializer) {
            // serializer has states, so we must duplicate
            this.serializer = (AbstractRowDataSerializer<InternalRow>) serializer.duplicate();
            this.recordBufferSegments = new ArrayList<>();
            this.recordCollector =
                    new SimpleCollectingOutputView(this.recordBufferSegments, pool, segmentSize);
        }

        private void reset() {
            this.currentDataBufferOffset = 0;
            this.numRecords = 0;
            returnToSegmentPool();
            this.recordCollector.reset();
        }

        private void returnToSegmentPool() {
            pool.returnAll(this.recordBufferSegments);
            this.recordBufferSegments.clear();
        }

        public boolean write(InternalRow row) throws IOException {
            try {
                this.serializer.serializeToPages(row, this.recordCollector);
                currentDataBufferOffset = this.recordCollector.getCurrentOffset();
                numBytesInLastBuffer = this.recordCollector.getCurrentPositionInSegment();
                numRecords++;
                return true;
            } catch (EOFException e) {
                return false;
            }
        }

        private ArrayList<MemorySegment> getRecordBufferSegments() {
            return recordBufferSegments;
        }

        private long getCurrentDataBufferOffset() {
            return currentDataBufferOffset;
        }

        private int getNumRecordBuffers() {
            int result = (int) (currentDataBufferOffset / segmentSize);
            long mod = currentDataBufferOffset % segmentSize;
            if (mod != 0) {
                result += 1;
            }
            return result;
        }

        private int getNumBytesInLastBuffer() {
            return numBytesInLastBuffer;
        }

        private InMemoryBufferIterator newIterator() {
            RandomAccessInputView recordBuffer =
                    new RandomAccessInputView(
                            this.recordBufferSegments, segmentSize, numBytesInLastBuffer);
            return new InMemoryBufferIterator(recordBuffer, serializer);
        }
    }

    private static class InMemoryBufferIterator
            implements MutableObjectIterator<BinaryRow>, Closeable {

        private final RandomAccessInputView recordBuffer;
        private final AbstractRowDataSerializer<InternalRow> serializer;

        private InMemoryBufferIterator(
                RandomAccessInputView recordBuffer,
                AbstractRowDataSerializer<InternalRow> serializer) {
            this.recordBuffer = recordBuffer;
            this.serializer = serializer;
        }

        @Override
        public BinaryRow next(BinaryRow reuse) throws IOException {
            try {
                return (BinaryRow) serializer.mapFromPages(reuse, recordBuffer);
            } catch (EOFException e) {
                return null;
            }
        }

        @Override
        public BinaryRow next() throws IOException {
            throw new RuntimeException("Not support!");
        }

        @Override
        public void close() {}
    }
}
