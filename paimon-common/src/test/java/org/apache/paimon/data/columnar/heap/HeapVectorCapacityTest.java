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

package org.apache.paimon.data.columnar.heap;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for heap vector capacity reuse. */
class HeapVectorCapacityTest {

    @Test
    void testHugeVectorCapacityIsReleasedOnReset() {
        int threshold = 1 << 20;
        HeapIntVector vector = new HeapIntVector(4);

        vector.reserve(threshold);
        assertThat(vector.getCapacity()).isGreaterThan(threshold);
        assertThat(vector.vector.length).isEqualTo(vector.getCapacity());

        vector.addElementsAppended(1);
        vector.reset();

        assertThat(vector.getCapacity()).isEqualTo(4);
        assertThat(vector.vector).hasSize(4);
    }

    @Test
    void testHugeVectorCapacityIsRetainedWhenUsageIsNotTooSmall() {
        int threshold = 1 << 20;
        HeapIntVector vector = new HeapIntVector(4);

        vector.reserve(threshold);
        int expandedCapacity = vector.getCapacity();
        // Use ceiling division (shrink ratio = 4)
        // so capacity is not strictly greater than usage * 4.
        vector.addElementsAppended((expandedCapacity + 3) / 4);
        vector.reset();

        assertThat(vector.getCapacity()).isEqualTo(expandedCapacity);
        assertThat(vector.vector).hasSize(expandedCapacity);
    }
}
