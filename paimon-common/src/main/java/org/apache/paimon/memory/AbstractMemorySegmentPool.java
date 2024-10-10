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

/** Abstract memory segment pool. */
public abstract class AbstractMemorySegmentPool implements MemorySegmentPool {
    private final LinkedList<MemorySegment> segments;
    private final int maxPages;
    protected final int pageSize;

    private int numPage;

    public AbstractMemorySegmentPool(long maxMemory, int pageSize) {
        this.segments = new LinkedList<>();
        this.maxPages = (int) (maxMemory / pageSize);
        this.pageSize = pageSize;
        this.numPage = 0;
    }

    @Override
    public synchronized MemorySegment nextSegment() {
        if (!this.segments.isEmpty()) {
            return this.segments.poll();
        } else if (numPage < maxPages) {
            numPage++;
            return allocateMemory();
        }

        return null;
    }

    protected abstract MemorySegment allocateMemory();

    @Override
    public int pageSize() {
        return pageSize;
    }

    @Override
    public synchronized void returnAll(List<MemorySegment> memory) {
        segments.addAll(memory);
    }

    @Override
    public int freePages() {
        return segments.size() + maxPages - numPage;
    }
}
