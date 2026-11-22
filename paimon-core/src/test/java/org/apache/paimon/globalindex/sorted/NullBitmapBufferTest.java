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

package org.apache.paimon.globalindex.sorted;

import org.apache.paimon.utils.RoaringNavigableMap64;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NullBitmapBufferTest {

    @Test
    void testBoundedRoundTripAndReuse() throws IOException {
        NullBitmapBuffer buffer = new NullBitmapBuffer();
        long base = 1L << 40;
        for (int i = 0; i < NullBitmapBuffer.MAX_ROWS; i++) {
            buffer.add(base + i * 2L);
        }
        assertThat(buffer.isFull()).isTrue();
        assertThat(buffer.firstRowId()).isEqualTo(base);
        assertThatThrownBy(() -> buffer.add(0)).isInstanceOf(IllegalStateException.class);
        byte[] bytes = buffer.drain();
        assertThat(bytes.length).isLessThan(NullBitmapBuffer.MAX_ROWS * Long.BYTES);
        RoaringNavigableMap64 restored = new RoaringNavigableMap64();
        restored.deserialize(bytes);
        assertThat(restored.getLongCardinality()).isEqualTo(NullBitmapBuffer.MAX_ROWS);
        for (int i = 0; i < NullBitmapBuffer.MAX_ROWS; i++) {
            assertThat(restored.contains(base + i * 2L)).isTrue();
        }
        assertThat(buffer.isEmpty()).isTrue();
        buffer.add(Long.MAX_VALUE);
        restored = new RoaringNavigableMap64();
        restored.deserialize(buffer.drain());
        assertThat(restored.getLongCardinality()).isEqualTo(1);
        assertThat(restored.contains(Long.MAX_VALUE)).isTrue();
    }

    @Test
    void testWidelySeparatedIdsHaveBoundedPayload() throws IOException {
        NullBitmapBuffer buffer = new NullBitmapBuffer();
        for (long i = 0; i < NullBitmapBuffer.MAX_ROWS; i++) {
            buffer.add((i << 40) + i);
        }
        byte[] bytes = buffer.drain();
        assertThat(bytes.length).isLessThan(NullBitmapBuffer.MAX_ROWS * 32);
        RoaringNavigableMap64 restored = new RoaringNavigableMap64();
        restored.deserialize(bytes);
        assertThat(restored.getLongCardinality()).isEqualTo(NullBitmapBuffer.MAX_ROWS);
    }

    @Test
    void testEmptyAndNegativeIds() {
        NullBitmapBuffer buffer = new NullBitmapBuffer();
        assertThatThrownBy(buffer::drain).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(buffer::firstRowId).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> buffer.add(-1)).isInstanceOf(IllegalArgumentException.class);
    }
}
