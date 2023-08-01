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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link MemoryPoolFactory}. */
public class MemoryPoolFactoryTest {

    @Test
    public void testFreePages() {
        MemoryPoolFactory factory =
                new MemoryPoolFactory(new HeapMemorySegmentPool(1024 * 10, 1024))
                        .addOwners(new ArrayList<>());
        MemorySegmentPool pool1 = factory.createSubPool(new TestMemoryOwner());
        MemorySegmentPool pool2 = factory.createSubPool(new TestMemoryOwner());
        assertThat(pool1.nextSegment()).isNotNull();
        assertThat(pool2.nextSegment()).isNotNull();
        assertThat(pool2.nextSegment()).isNotNull();

        assertThat(pool1.freePages()).isEqualTo(9);
        assertThat(pool2.freePages()).isEqualTo(8);
    }

    @Test
    public void testAddOwners() {
        MemoryPoolFactory factory =
                new MemoryPoolFactory(new HeapMemorySegmentPool(1024 * 10, 1024));

        List<List<MemoryOwner>> ownerList = new ArrayList<>();
        Random random = ThreadLocalRandom.current();

        // prepare empty owners lists
        int numList = random.nextInt(5) + 1;
        for (int i = 0; i < numList; i++) {
            List<MemoryOwner> owners = new ArrayList<>();
            factory.addOwners(owners);
            ownerList.add(owners);
        }
        assertThat(factory.memoryOwners()).isEmpty();

        // add owners
        List<MemoryOwner> allAddedOwners = new ArrayList<>();
        for (int i = 0; i < numList; i++) {
            List<MemoryOwner> owners = ownerList.get(i);
            int numOwners = random.nextInt(10);
            for (int j = 0; j < numOwners; j++) {
                MemoryOwner owner = new TestMemoryOwner();
                owners.add(owner);
                allAddedOwners.add(owner);
            }
        }
        assertThat(factory.memoryOwners()).containsExactlyInAnyOrderElementsOf(allAddedOwners);
    }

    private static class TestMemoryOwner implements MemoryOwner {
        @Override
        public void setMemoryPool(MemorySegmentPool memoryPool) {}

        @Override
        public long memoryOccupancy() {
            return 0;
        }

        @Override
        public void flushMemory() {}
    }
}
