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
import dev.vortex.jni.NativeScan;

import java.util.Iterator;
import java.util.NoSuchElementException;

/** An iterator over partitions from a Vortex scan. */
public final class Scan implements Iterator<Partition>, AutoCloseable {

    private final Session session;
    private long pointer;
    private long nextPartitionPointer;
    private boolean primed;

    private Scan(Session session, long pointer) {
        this.session = session;
        this.pointer = pointer;
    }

    static Scan fromPointer(Session session, long pointer) {
        return new Scan(session, pointer);
    }

    @Override
    public boolean hasNext() {
        if (primed) {
            return nextPartitionPointer != 0;
        }
        nextPartitionPointer = NativeScan.nextPartition(pointer);
        primed = true;
        return nextPartitionPointer != 0;
    }

    @Override
    public Partition next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        long ptr = nextPartitionPointer;
        nextPartitionPointer = 0;
        primed = false;
        return Partition.fromPointer(session, ptr);
    }

    @Override
    public void close() {
        if (nextPartitionPointer != 0) {
            NativePartition.free(nextPartitionPointer);
            nextPartitionPointer = 0;
        }
        if (pointer != 0) {
            NativeScan.free(pointer);
            pointer = 0;
        }
    }
}
