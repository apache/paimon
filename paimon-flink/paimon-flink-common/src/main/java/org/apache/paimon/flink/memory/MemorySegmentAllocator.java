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

package org.apache.paimon.flink.memory;

import org.apache.paimon.memory.MemorySegment;

import org.apache.flink.runtime.memory.MemoryManager;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Allocate memory segment from memory manager in flink for paimon. */
public class MemorySegmentAllocator {
    private final Object owner;
    private final MemoryManager memoryManager;
    private final List<org.apache.flink.core.memory.MemorySegment> allocatedSegments;
    private final List<org.apache.flink.core.memory.MemorySegment> segments;
    private final Field offHeapBufferField;

    public MemorySegmentAllocator(Object owner, MemoryManager memoryManager) {
        this.owner = owner;
        this.memoryManager = memoryManager;
        this.allocatedSegments = new ArrayList<>();
        this.segments = new ArrayList<>(1);
        try {
            this.offHeapBufferField =
                    org.apache.flink.core.memory.MemorySegment.class.getDeclaredField(
                            "offHeapBuffer");
            this.offHeapBufferField.setAccessible(true);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    /** Allocates a set of memory segments for memory pool. */
    public MemorySegment allocate() {
        segments.clear();
        try {
            memoryManager.allocatePages(owner, segments, 1);
            org.apache.flink.core.memory.MemorySegment segment = segments.remove(0);
            checkNotNull(segment, "Allocate null segment from memory manager for paimon.");
            checkArgument(segment.isOffHeap(), "Segment is not off heap from memory manager.");
            allocatedSegments.add(segment);
            // TODO Use getOffHeapBuffer in MemorySegment after
            // https://issues.apache.org/jira/browse/FLINK-32213
            return MemorySegment.wrapOffHeapMemory((ByteBuffer) offHeapBufferField.get(segment));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /* Release the segments allocated by the allocator if the task is closed. */
    public void release() {
        memoryManager.release(allocatedSegments);
    }
}
