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

/**
 * A factory which creates {@link MemorySegmentPool} from {@link MemoryOwner}. The returned memory
 * pool will try to preempt memory when there is no memory left.
 */
public class MemoryPoolFactory {

    private final MemorySegmentPool innerPool;
    private final int totalPages;
    private final Iterable<MemoryOwner> owners;

    public MemoryPoolFactory(MemorySegmentPool innerPool, Iterable<MemoryOwner> owners) {
        this.innerPool = innerPool;
        this.totalPages = innerPool.freePages();
        this.owners = owners;
    }

    public void notifyNewOwner(MemoryOwner owner) {
        owner.setMemoryPool(createSubPool(owner));
    }

    MemorySegmentPool createSubPool(MemoryOwner owner) {
        return new OwnerMemoryPool(owner);
    }

    private void preemptMemory(MemoryOwner owner) {
        long maxMemory = -1;
        MemoryOwner max = null;
        for (MemoryOwner other : owners) {
            // Don't preempt yourself! Write and flush at the same time, which may lead to
            // inconsistent state
            if (other != owner && other.memoryOccupancy() > maxMemory) {
                maxMemory = other.memoryOccupancy();
                max = other;
            }
        }

        if (max != null) {
            try {
                max.flushMemory();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private class OwnerMemoryPool implements MemorySegmentPool {

        private final MemoryOwner owner;

        private int allocatedPages = 0;

        public OwnerMemoryPool(MemoryOwner owner) {
            this.owner = owner;
        }

        @Override
        public int pageSize() {
            return innerPool.pageSize();
        }

        @Override
        public void returnAll(List<MemorySegment> memory) {
            allocatedPages -= memory.size();
            innerPool.returnAll(memory);
        }

        @Override
        public int freePages() {
            return totalPages - allocatedPages;
        }

        @Override
        public MemorySegment nextSegment() {
            MemorySegment segment = innerPool.nextSegment();
            if (segment == null) {
                preemptMemory(owner);
                segment = innerPool.nextSegment();
            }
            if (segment != null) {
                allocatedPages++;
            }
            return segment;
        }
    }
}
