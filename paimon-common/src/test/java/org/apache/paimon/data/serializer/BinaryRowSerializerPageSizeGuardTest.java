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

package org.apache.paimon.data.serializer;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.RandomAccessInputView;
import org.apache.paimon.data.SimpleCollectingOutputView;
import org.apache.paimon.memory.CachelessSegmentPool;
import org.apache.paimon.memory.MemorySegment;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the page-size guard in {@link BinaryRowSerializer#serializeToPages}. When a row's
 * fixed-length part is larger than the page size, serialization must fail fast with a clear error
 * instead of silently spanning segments and corrupting field access (which previously surfaced as a
 * cryptic {@link NegativeArraySizeException}).
 */
class BinaryRowSerializerPageSizeGuardTest {

    // 200 INT columns -> fixed-length part 1632 bytes + 4 length bytes = 1636 bytes.
    private static final int NUM_FIELDS = 200;

    @Test
    void testThrowsWhenFixedPartExceedsPageSize() {
        BinaryRowSerializer serializer = new BinaryRowSerializer(NUM_FIELDS);
        int fixedPartLength = serializer.getSerializedRowFixedPartLength();
        // Page size (power of two) smaller than the fixed-length part.
        int pageSize = 1024;
        assertThat(fixedPartLength).isGreaterThan(pageSize);

        SimpleCollectingOutputView out =
                new SimpleCollectingOutputView(
                        new CachelessSegmentPool(pageSize, pageSize), pageSize);

        BinaryRow row = newIntRow(NUM_FIELDS);
        assertThatThrownBy(() -> serializer.serializeToPages(row, out))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("page size")
                .hasMessageContaining(Integer.toString(fixedPartLength))
                .hasMessageContaining(Integer.toString(pageSize))
                .hasMessageContaining(Integer.toString(NUM_FIELDS));
    }

    @Test
    void testSerializeSucceedsWhenPageLargeEnough() throws Exception {
        BinaryRowSerializer serializer = new BinaryRowSerializer(NUM_FIELDS);
        int pageSize = 4096;
        assertThat(serializer.getSerializedRowFixedPartLength()).isLessThanOrEqualTo(pageSize);

        SimpleCollectingOutputView out =
                new SimpleCollectingOutputView(
                        new CachelessSegmentPool((long) pageSize * 4, pageSize), pageSize);

        BinaryRow row = newIntRow(NUM_FIELDS);
        serializer.serializeToPages(row, out);

        // Round-trip the row back from the written pages.
        ArrayList<MemorySegment> segments = out.fullSegments();
        RandomAccessInputView in = new RandomAccessInputView(segments, pageSize);
        BinaryRow deserialized = serializer.deserializeFromPages(serializer.createInstance(), in);
        for (int i = 0; i < NUM_FIELDS; i++) {
            assertThat(deserialized.getInt(i)).isEqualTo(i);
        }
    }

    private static BinaryRow newIntRow(int numFields) {
        BinaryRow row = new BinaryRow(numFields);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        for (int i = 0; i < numFields; i++) {
            writer.writeInt(i, i);
        }
        writer.complete();
        return row;
    }
}
