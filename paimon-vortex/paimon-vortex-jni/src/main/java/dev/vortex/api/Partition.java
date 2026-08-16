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

package dev.vortex.api;

import dev.vortex.jni.NativePartition;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;

/** A partition from a Vortex scan, consumable once as an Arrow stream. */
public final class Partition implements AutoCloseable {

    private final Session session;
    private long pointer;
    private boolean consumed;

    private Partition(Session session, long pointer) {
        this.session = session;
        this.pointer = pointer;
    }

    static Partition fromPointer(Session session, long pointer) {
        return new Partition(session, pointer);
    }

    public ArrowReader scanArrow(BufferAllocator allocator) {
        if (consumed) {
            throw new IllegalStateException("partition already consumed");
        }
        consumed = true;
        ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
        try {
            NativePartition.scanArrow(
                    session.nativePointer(), pointer, stream.memoryAddress());
        } catch (RuntimeException e) {
            stream.close();
            throw e;
        }
        return Data.importArrayStream(allocator, stream);
    }

    @Override
    public void close() {
        // Only free unconsumed partitions. scanArrow() transfers the native
        // partition's ownership to the ArrowArrayStream; the stream's release
        // callback frees it when the ArrowReader is closed.
        if (pointer != 0 && !consumed) {
            NativePartition.free(pointer);
        }
        pointer = 0;
    }
}
