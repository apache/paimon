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

package org.apache.paimon.data.columnar.writable;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link AbstractWritableVector} capacity growth. */
class AbstractWritableVectorTest {

    @Test
    void testSmallVectorDoublesRequiredCapacity() {
        TestVector vector = new TestVector(4);

        vector.reserve(5);

        assertThat(vector.getCapacity()).isEqualTo(10);
        assertThat(vector.reservedCapacity).isEqualTo(10);
    }

    @Test
    void testHugeVectorUsesConservativeGrowth() {
        int threshold = AbstractWritableVector.DEFAULT_HUGE_VECTOR_THRESHOLD;
        TestVector vector = new TestVector(threshold - 1);

        vector.reserve(threshold);

        assertThat(vector.getCapacity())
                .isEqualTo(
                        (int)
                                (threshold
                                        * AbstractWritableVector
                                                .DEFAULT_HUGE_VECTOR_RESERVE_RATIO));
    }

    @Test
    void testHugeVectorCapacityResetsToInitialCapacityWhenUsageIsSmall() {
        int threshold = AbstractWritableVector.DEFAULT_HUGE_VECTOR_THRESHOLD;
        TestVector vector = new TestVector(4);

        vector.reserve(threshold);
        vector.addElementsAppended(1);
        vector.reset();

        assertThat(vector.getCapacity()).isEqualTo(4);
        assertThat(vector.getElementsAppended()).isZero();
    }

    @Test
    void testHugeVectorCapacityIsRetainedWhenUsageIsNotTooSmall() {
        int threshold = AbstractWritableVector.DEFAULT_HUGE_VECTOR_THRESHOLD;
        TestVector vector = new TestVector(4);

        vector.reserve(threshold);
        int expandedCapacity = vector.getCapacity();
        // Use ceiling division so capacity is not strictly greater than usage * 4.
        int retainedUsage =
                (expandedCapacity + AbstractWritableVector.DEFAULT_HUGE_VECTOR_SHRINK_RATIO - 1)
                        / AbstractWritableVector.DEFAULT_HUGE_VECTOR_SHRINK_RATIO;
        vector.addElementsAppended(retainedUsage);
        vector.reset();

        assertThat(vector.getCapacity()).isEqualTo(expandedCapacity);
        assertThat(vector.getElementsAppended()).isZero();
    }

    @Test
    void testNormalExpandedCapacityIsRetainedAfterReset() {
        TestVector vector = new TestVector(4);

        vector.reserve(5);
        vector.reset();

        assertThat(vector.getCapacity()).isEqualTo(10);
    }

    @Test
    void testReserveRejectsCapacityBeyondMaxRoundedArrayLength() {
        TestVector vector = new TestVector(4);

        assertThatThrownBy(() -> vector.reserve(Integer.MAX_VALUE))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Cannot allocate " + Integer.MAX_VALUE + " elements");
    }

    private static class TestVector extends AbstractWritableVector {

        private int reservedCapacity;

        private TestVector(int capacity) {
            super(capacity);
            this.reservedCapacity = capacity;
        }

        @Override
        public void setNullAt(int rowId) {}

        @Override
        public void setNulls(int rowId, int count) {}

        @Override
        public void fillWithNulls() {}

        @Override
        public WritableIntVector reserveDictionaryIds(int capacity) {
            return null;
        }

        @Override
        public WritableIntVector getDictionaryIds() {
            return null;
        }

        @Override
        public boolean isNullAt(int i) {
            return false;
        }

        @Override
        protected void reserveInternal(int newCapacity) {
            this.reservedCapacity = newCapacity;
        }
    }
}
