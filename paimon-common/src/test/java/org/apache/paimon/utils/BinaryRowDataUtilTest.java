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

package org.apache.paimon.utils;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link BinaryRowDataUtil}. */
public class BinaryRowDataUtilTest {

    @Test
    void testByteBufferEqualsHeap() {
        byte[] content = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};

        assertThat(
                        BinaryRowDataUtil.byteBufferEquals(
                                ByteBuffer.wrap(content), ByteBuffer.wrap(content.clone())))
                .isTrue();

        byte[] different = content.clone();
        different[different.length - 1] = (byte) (different[different.length - 1] + 1);
        assertThat(
                        BinaryRowDataUtil.byteBufferEquals(
                                ByteBuffer.wrap(content), ByteBuffer.wrap(different)))
                .isFalse();

        assertThat(
                        BinaryRowDataUtil.byteBufferEquals(
                                ByteBuffer.wrap(content), ByteBuffer.wrap(content, 0, 10)))
                .isFalse();

        // Lengths that are not a multiple of 8 exercise the tail loop.
        assertThat(
                        BinaryRowDataUtil.byteBufferEquals(
                                ByteBuffer.wrap(content, 0, 5), ByteBuffer.wrap(content, 0, 5)))
                .isTrue();
    }

    @Test
    void testByteBufferEqualsComparesRemainingRegion() {
        // Views over the same backing array: only [position, limit) participates.
        byte[] content = new byte[] {9, 1, 2, 3, 4, 5, 6, 7, 8, 9, 9};
        assertThat(
                        BinaryRowDataUtil.byteBufferEquals(
                                ByteBuffer.wrap(content, 1, 9), ByteBuffer.wrap(content, 1, 9)))
                .isTrue();
        assertThat(
                        BinaryRowDataUtil.byteBufferEquals(
                                ByteBuffer.wrap(content, 1, 9), ByteBuffer.wrap(content, 0, 9)))
                .isFalse();
    }

    @Test
    void testByteBufferEqualsDoesNotModifyPositions() {
        byte[] content = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        ByteBuffer a = ByteBuffer.wrap(content, 2, 6);
        ByteBuffer b = ByteBuffer.wrap(content, 2, 6);

        assertThat(BinaryRowDataUtil.byteBufferEquals(a, b)).isTrue();

        assertThat(a.position()).isEqualTo(2);
        assertThat(a.limit()).isEqualTo(8);
        assertThat(b.position()).isEqualTo(2);
        assertThat(b.limit()).isEqualTo(8);
    }

    @Test
    void testByteBufferEqualsDirect() {
        // Direct buffers have no backing array: the getLong fallback must handle them,
        // including unaligned regions.
        byte[] content = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};
        ByteBuffer direct = ByteBuffer.allocateDirect(16);
        direct.put(content);
        direct.position(0).limit(content.length);

        assertThat(BinaryRowDataUtil.byteBufferEquals(direct, ByteBuffer.wrap(content))).isTrue();
        assertThat(BinaryRowDataUtil.byteBufferEquals(ByteBuffer.wrap(content), direct)).isTrue();

        // Offsets that are not 8-byte aligned, on both sides.
        ByteBuffer directShifted = ByteBuffer.allocateDirect(16);
        directShifted.put(new byte[] {0});
        directShifted.put(content);
        directShifted.position(1).limit(1 + content.length);
        assertThat(BinaryRowDataUtil.byteBufferEquals(directShifted, direct)).isTrue();

        // Same unaligned view, one byte different.
        ByteBuffer directDifferent = ByteBuffer.allocateDirect(16);
        directDifferent.put(new byte[] {0});
        directDifferent.put(content);
        directDifferent.put(1, (byte) (directDifferent.get(1) + 1));
        directDifferent.position(1).limit(1 + content.length);
        assertThat(BinaryRowDataUtil.byteBufferEquals(directShifted, directDifferent)).isFalse();

        // Direct vs heap, byte order is irrelevant for a byte-wise comparison.
        assertThat(BinaryRowDataUtil.byteBufferEquals(direct, ByteBuffer.wrap(content))).isTrue();
        assertThat(BinaryRowDataUtil.byteBufferEquals(ByteBuffer.wrap(content), direct)).isTrue();

        // Differ only in the last byte
        directDifferent = ByteBuffer.allocateDirect(16);
        directDifferent.put(content);
        directDifferent.put(content.length - 1, (byte) 12);
        directDifferent.position(0).limit(content.length);

        assertThat(BinaryRowDataUtil.byteBufferEquals(direct, directDifferent)).isFalse();
        assertThat(BinaryRowDataUtil.byteBufferEquals(directDifferent, direct)).isFalse();

        // Same tail difference on unaligned (shifted) direct views.
        directShifted = ByteBuffer.allocateDirect(16);
        directShifted.put(new byte[] {0});
        directShifted.put(content);
        directShifted.position(1).limit(1 + content.length);

        ByteBuffer directShiftedDifferent = ByteBuffer.allocateDirect(16);
        directShiftedDifferent.put(new byte[] {0});
        directShiftedDifferent.put(content);
        directShiftedDifferent.put(content.length, (byte) 12);
        directShiftedDifferent.position(1).limit(1 + content.length);

        assertThat(BinaryRowDataUtil.byteBufferEquals(directShifted, directShiftedDifferent))
                .isFalse();
    }

    @Test
    void testByteBufferEqualsLittleEndianOrder() {
        // The comparison is byte-wise, so byte order must not affect the result.
        byte[] content = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};
        ByteBuffer littleEndian = ByteBuffer.wrap(content).order(ByteOrder.LITTLE_ENDIAN);
        ByteBuffer bigEndian = ByteBuffer.wrap(content).order(ByteOrder.BIG_ENDIAN);
        assertThat(BinaryRowDataUtil.byteBufferEquals(littleEndian, bigEndian)).isTrue();
    }
}
