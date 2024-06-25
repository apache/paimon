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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.AbstractRowDataSerializer;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.options.MemorySize;

import java.io.Closeable;
import java.io.IOException;

/** Cache buffer for {@link InternalRow}. */
public interface RowBuffer {

    boolean put(InternalRow row) throws IOException;

    int size();

    long memoryOccupancy();

    void complete();

    void reset();

    boolean flushMemory() throws IOException;

    RowBufferIterator newIterator();

    /** Iterator to fetch record from buffer. */
    interface RowBufferIterator extends Closeable {

        boolean advanceNext();

        BinaryRow getRow();

        void close();
    }

    static RowBuffer getBuffer(
            IOManager ioManager,
            MemorySegmentPool memoryPool,
            AbstractRowDataSerializer<InternalRow> serializer,
            boolean spillable,
            MemorySize maxDiskSize,
            String compression) {
        if (spillable) {
            return new ExternalBuffer(ioManager, memoryPool, serializer, maxDiskSize, compression);
        } else {
            return new InMemoryBuffer(memoryPool, serializer);
        }
    }
}
