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

/** Tests for {@link HeapBytesVector#putByteArray}, focusing on reserveBytes() overflow safety. */
class HeapBytesVectorReserveBytesTest {

    @Test
    void testNormalGrowthDoublesCapacity() {
        HeapBytesVector vector = new HeapBytesVector(4);
        int initialBufferSize = vector.buffer.length;

        // Write enough data to trigger growth
        byte[] data = new byte[initialBufferSize + 1];
        vector.putByteArray(0, data, 0, data.length);

        // Buffer should have doubled
        assertThat(vector.buffer.length).isEqualTo((initialBufferSize + 1) * 2);
    }

    @Test
    void testPutByteArrayStoresDataCorrectly() {
        HeapBytesVector vector = new HeapBytesVector(4);

        byte[] data1 = new byte[] {1, 2, 3};
        byte[] data2 = new byte[] {4, 5, 6, 7};
        vector.putByteArray(0, data1, 0, data1.length);
        vector.putByteArray(1, data2, 0, data2.length);

        HeapBytesVector.Bytes bytes0 = vector.getBytes(0);
        assertThat(bytes0.len).isEqualTo(3);
        assertThat(vector.buffer[bytes0.offset]).isEqualTo((byte) 1);
        assertThat(vector.buffer[bytes0.offset + 2]).isEqualTo((byte) 3);

        HeapBytesVector.Bytes bytes1 = vector.getBytes(1);
        assertThat(bytes1.len).isEqualTo(4);
        assertThat(vector.buffer[bytes1.offset]).isEqualTo((byte) 4);
    }

    @Test
    void testLargeCapacityDoesNotOverflow() {
        HeapBytesVector vector = new HeapBytesVector(2);

        // Simulate a scenario where the required capacity is large but still within
        // MAX_ARRAY_SIZE. We can't actually allocate Integer.MAX_VALUE bytes in a test,
        // but we can verify the logic by checking that a moderately large allocation works.
        int largeSize = 64 * 1024 * 1024; // 64MB
        byte[] largeData = new byte[largeSize];
        vector.putByteArray(0, largeData, 0, largeData.length);

        assertThat(vector.buffer.length).isGreaterThanOrEqualTo(largeSize);
        assertThat(vector.getBytes(0).len).isEqualTo(largeSize);
    }

    @Test
    void testResetClearsBytesAppended() {
        HeapBytesVector vector = new HeapBytesVector(4);

        byte[] data = new byte[] {1, 2, 3};
        vector.putByteArray(0, data, 0, data.length);

        vector.reset();

        // After reset, we should be able to write again from the beginning
        byte[] data2 = new byte[] {10, 20};
        vector.putByteArray(0, data2, 0, data2.length);

        HeapBytesVector.Bytes bytes = vector.getBytes(0);
        assertThat(bytes.offset).isEqualTo(0);
        assertThat(bytes.len).isEqualTo(2);
        assertThat(vector.buffer[0]).isEqualTo((byte) 10);
    }
}
