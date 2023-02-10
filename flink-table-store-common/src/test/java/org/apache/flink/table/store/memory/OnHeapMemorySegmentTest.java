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

package org.apache.flink.table.store.memory;

import org.apache.flink.table.store.testutils.junit.parameterized.ParameterizedTestExtension;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

/** Tests for the {@link MemorySegment} in on-heap mode. */
@ExtendWith(ParameterizedTestExtension.class)
public class OnHeapMemorySegmentTest extends MemorySegmentTestBase {

    public OnHeapMemorySegmentTest(int pageSize) {
        super(pageSize);
    }

    @Override
    MemorySegment createSegment(int size) {
        return MemorySegment.allocateHeapMemory(size);
    }

    @TestTemplate
    public void testHeapSegmentSpecifics() {
        final byte[] buffer = new byte[411];
        MemorySegment seg = MemorySegment.wrap(buffer);

        assertFalse(seg.isOffHeap());
        assertEquals(buffer.length, seg.size());
        assertSame(buffer, seg.getArray());

        ByteBuffer buf1 = seg.wrap(1, 2);
        ByteBuffer buf2 = seg.wrap(3, 4);

        assertNotSame(buf1, buf2);
        assertEquals(1, buf1.position());
        assertEquals(3, buf1.limit());
        assertEquals(3, buf2.position());
        assertEquals(7, buf2.limit());
    }

    @TestTemplate
    public void testReadOnlyByteBufferPut() {
        final byte[] buffer = new byte[100];
        MemorySegment seg = MemorySegment.wrap(buffer);

        String content = "hello world";
        ByteBuffer bb = ByteBuffer.allocate(20);
        bb.put(content.getBytes());
        bb.rewind();

        int offset = 10;
        int numBytes = 5;

        ByteBuffer readOnlyBuf = bb.asReadOnlyBuffer();
        assertFalse(readOnlyBuf.isDirect());
        assertFalse(readOnlyBuf.hasArray());

        seg.put(offset, readOnlyBuf, numBytes);

        // verify the area before the written region.
        for (int i = 0; i < offset; i++) {
            assertEquals(0, buffer[i]);
        }

        // verify the region that is written.
        assertEquals("hello", new String(buffer, offset, numBytes));

        // verify the area after the written region.
        for (int i = offset + numBytes; i < buffer.length; i++) {
            assertEquals(0, buffer[i]);
        }
    }
}
