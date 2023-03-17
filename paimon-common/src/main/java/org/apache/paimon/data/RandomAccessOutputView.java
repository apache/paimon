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

package org.apache.paimon.data;

import org.apache.paimon.memory.MemorySegment;

import java.io.EOFException;

/** A {@link AbstractPagedOutputView} with memory segments. */
public class RandomAccessOutputView extends AbstractPagedOutputView {

    private final MemorySegment[] segments;

    private int currentSegmentIndex;

    public RandomAccessOutputView(MemorySegment[] segments, int segmentSize) {
        super(segments[0], segmentSize);

        if ((segmentSize & (segmentSize - 1)) != 0) {
            throw new IllegalArgumentException("Segment size must be a power of 2!");
        }

        this.segments = segments;
    }

    @Override
    protected MemorySegment nextSegment(MemorySegment current, int positionInCurrent)
            throws EOFException {
        if (++this.currentSegmentIndex < this.segments.length) {
            return this.segments[this.currentSegmentIndex];
        } else {
            throw new EOFException();
        }
    }
}
