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

package org.apache.flink.table.store.file.utils;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.table.runtime.util.MemorySegmentPool;

import java.util.List;

/**
 * A factory which creates {@link MemorySegmentPool} from {@link OwnerT}. The returned memory pool
 * will try to preempt memory when there is no memory left.
 */
public class MemoryPoolFactory<OwnerT> {

    private final MemorySegmentPool innerPool;
    private final PreemptRunner<OwnerT> preemptRunner;

    public MemoryPoolFactory(MemorySegmentPool innerPool, PreemptRunner<OwnerT> preemptRunner) {
        this.innerPool = innerPool;
        this.preemptRunner = preemptRunner;
    }

    public MemorySegmentPool create(OwnerT owner) {
        return new OwnerMemoryPool(owner);
    }

    private class OwnerMemoryPool implements MemorySegmentPool {

        private final OwnerT owner;

        public OwnerMemoryPool(OwnerT owner) {
            this.owner = owner;
        }

        @Override
        public int pageSize() {
            return innerPool.pageSize();
        }

        @Override
        public void returnAll(List<MemorySegment> memory) {
            innerPool.returnAll(memory);
        }

        @Override
        public int freePages() {
            return innerPool.freePages();
        }

        @Override
        public MemorySegment nextSegment() {
            MemorySegment segment = innerPool.nextSegment();
            if (segment == null) {
                preemptRunner.preemptMemory(owner);
                return innerPool.nextSegment();
            }
            return segment;
        }
    }

    /** A runner to preempt memory. */
    public interface PreemptRunner<T> {
        void preemptMemory(T owner);
    }
}
