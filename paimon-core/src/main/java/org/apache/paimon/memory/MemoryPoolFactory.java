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

import org.apache.paimon.annotation.VisibleForTesting;

import org.apache.paimon.shade.guava30.com.google.common.collect.Iterators;

import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * A factory which creates {@link MemorySegmentPool} from {@link MemoryOwner}. The returned memory
 * pool will try to preempt memory when there is no memory left.
 */
public class MemoryPoolFactory {

    private final MemorySegmentPool innerPool;
    private final int totalPages;

    private Iterable<MemoryOwner> owners;

    private final long totalBufferSize;
    private long bufferPreemptCount;

    public MemoryPoolFactory(MemorySegmentPool innerPool) {
        this.innerPool = innerPool;
        this.totalPages = innerPool.freePages();
        this.totalBufferSize = (long) totalPages * innerPool.pageSize();
    }

    public MemoryPoolFactory addOwners(Iterable<MemoryOwner> newOwners) {
        if (this.owners == null) {
            this.owners = newOwners;
        } else {
            Iterable<MemoryOwner> currentOwners = this.owners;
            this.owners = () -> Iterators.concat(currentOwners.iterator(), newOwners.iterator());
        }
        return this;
    }

    public void notifyNewOwner(MemoryOwner owner) {
        checkNotNull(owners);
        owner.setMemoryPool(createSubPool(owner));
    }

    @VisibleForTesting
    public Iterable<MemoryOwner> memoryOwners() {
        return owners;
    }

    MemorySegmentPool createSubPool(MemoryOwner owner) {
        return new OwnerMemoryPool(owner);
    }

    private void preemptMemory(MemoryOwner owner) {
        long maxMemory = 0;
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
                ++bufferPreemptCount;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public long bufferPreemptCount() {
        return bufferPreemptCount;
    }

    public long usedBufferSize() {
        long usedBufferSize = 0L;
        if (owners != null) {
            for (MemoryOwner owner : owners) {
                usedBufferSize += owner.memoryOccupancy();
            }
        }
        return usedBufferSize;
    }

    public long totalBufferSize() {
        return totalBufferSize;
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
