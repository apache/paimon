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

package org.apache.paimon.memory;

import org.apache.paimon.annotation.Public;

import java.nio.ByteBuffer;

/**
 * A buffer with size.
 *
 * @since 0.4.0
 */
@Public
public class Buffer {

    private final MemorySegment segment;

    private int size;

    public Buffer(MemorySegment segment, int size) {
        this.segment = segment;
        this.size = size;
    }

    public static Buffer create(MemorySegment segment) {
        return create(segment, 0);
    }

    public static Buffer create(MemorySegment segment, int size) {
        return new Buffer(segment, size);
    }

    public MemorySegment getMemorySegment() {
        return segment;
    }

    public int getSize() {
        return size;
    }

    public int getMaxCapacity() {
        return segment.size();
    }

    public ByteBuffer getNioBuffer(int index, int length) {
        return segment.wrap(index, length).slice();
    }

    public void setSize(int size) {
        this.size = size;
    }
}
