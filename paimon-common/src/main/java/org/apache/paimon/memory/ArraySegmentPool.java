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

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/** A {@link MemorySegmentPool} for allocated segments. */
public class ArraySegmentPool implements MemorySegmentPool {

    private final Queue<MemorySegment> segments;
    private final int pageSize;

    public ArraySegmentPool(List<MemorySegment> segments) {
        this.segments = new LinkedList<>(segments);
        this.pageSize = segments.get(0).size();
    }

    @Override
    public int pageSize() {
        return pageSize;
    }

    @Override
    public void returnAll(List<MemorySegment> memory) {
        segments.addAll(memory);
    }

    @Override
    public int freePages() {
        return segments.size();
    }

    @Override
    public MemorySegment nextSegment() {
        return segments.poll();
    }
}
