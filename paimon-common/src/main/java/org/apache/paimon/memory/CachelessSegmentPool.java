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

import java.util.List;

/** A {@link MemorySegmentPool} without cache. */
public class CachelessSegmentPool implements MemorySegmentPool {

    private final int maxPages;
    private final int pageSize;

    private int numPage;

    public CachelessSegmentPool(long maxMemory, int pageSize) {
        this.maxPages = (int) (maxMemory / pageSize);
        this.pageSize = pageSize;
        this.numPage = 0;
    }

    @Override
    public MemorySegment nextSegment() {
        if (numPage < maxPages) {
            numPage++;
            return MemorySegment.allocateHeapMemory(pageSize);
        }

        return null;
    }

    @Override
    public int pageSize() {
        return pageSize;
    }

    @Override
    public void returnAll(List<MemorySegment> memory) {
        numPage -= memory.size();
    }

    @Override
    public int freePages() {
        return maxPages - numPage;
    }
}
