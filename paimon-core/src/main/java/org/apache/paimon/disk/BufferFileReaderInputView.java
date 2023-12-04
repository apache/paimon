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

import org.apache.paimon.data.AbstractPagedInputView;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.serializer.BinaryRowSerializer;
import org.apache.paimon.memory.Buffer;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.utils.MutableObjectIterator;

import java.io.EOFException;
import java.io.IOException;

/** An {@link AbstractPagedInputView} which reads blocks from channel without compression. */
public class BufferFileReaderInputView extends AbstractPagedInputView {

    private final BufferFileReader reader;
    private final MemorySegment segment;

    private int currentSegmentLimit;

    public BufferFileReaderInputView(FileIOChannel.ID id, IOManager ioManager, int segmentSize)
            throws IOException {
        this.reader = ioManager.createBufferFileReader(id);
        this.segment = MemorySegment.wrap(new byte[segmentSize]);
    }

    @Override
    protected MemorySegment nextSegment(MemorySegment current) throws IOException {
        if (reader.hasReachedEndOfFile()) {
            throw new EOFException();
        }

        Buffer buffer = Buffer.create(segment);
        reader.readInto(buffer);
        this.currentSegmentLimit = buffer.getSize();
        return segment;
    }

    @Override
    protected int getLimitForSegment(MemorySegment segment) {
        return currentSegmentLimit;
    }

    public void close() throws IOException {
        reader.close();
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

        private final BinaryRow reuse;

        public BinaryRowChannelInputViewIterator(BinaryRowSerializer serializer) {
            this.serializer = serializer;
            this.reuse = new BinaryRow(serializer.getArity());
        }

        @Override
        public BinaryRow next() throws IOException {
            try {
                return this.serializer.deserializeFromPages(reuse, BufferFileReaderInputView.this);
            } catch (EOFException e) {
                close();
                return null;
            }
        }
    }
}
