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

package org.apache.paimon.geo;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import static org.apache.paimon.geo.WkbTestUtils.collection;
import static org.apache.paimon.geo.WkbTestUtils.line;
import static org.apache.paimon.geo.WkbTestUtils.point;
import static org.apache.paimon.geo.WkbTestUtils.polygon;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Contracts for planar WKB envelopes and conservative boundary handling. */
class BoundingBoxTest {

    @Test
    void testAllGeometryTypesAndMixedByteOrders() {
        assertBounds(point(-3, 7), -3, 7, -3, 7);
        assertBounds(point(8, -2, ByteOrder.BIG_ENDIAN), 8, -2, 8, -2);
        assertBounds(line(1, 8, -3, 2, 5, -4), -3, -4, 5, 8);
        byte[] polygon = polygon(new double[] {-4, -2, 3, -2, 3, 5, -4, -2});
        assertBounds(polygon, -4, -2, 3, 5);
        assertBounds(collection(4, point(-3, 7), point(8, -2, ByteOrder.BIG_ENDIAN)), -3, -2, 8, 7);
        assertBounds(collection(5, line(-3, 7, 8, -2), line()), -3, -2, 8, 7);
        assertBounds(collection(6, polygon, polygon()), -4, -2, 3, 5);
        assertBounds(collection(7, point(-3, 7), collection(7, polygon)), -4, -2, 3, 7);
    }

    @Test
    void testEmptyGeometriesDoNotExpandBounds() {
        for (byte[] empty :
                new byte[][] {
                    point(Double.NaN, Double.NaN),
                    line(),
                    polygon(),
                    collection(4),
                    collection(5),
                    collection(6),
                    collection(7)
                }) {
            BoundingBox bounds = BoundingBox.fromWkb(empty);
            assertThat(bounds.isEmpty()).isTrue();
            assertThat(bounds.intersects(BoundingBox.of(-10, -10, 10, 10))).isFalse();
            assertBounds(collection(7, empty, point(2, 4)), 2, 4, 2, 4);
        }
    }

    @Test
    void testClosedAndDegenerateBounds() {
        BoundingBox bounds = BoundingBox.of(0, 0, 1, 1);
        assertThat(bounds.intersects(BoundingBox.of(1, 1, 2, 2))).isTrue();
        assertThat(bounds.intersects(BoundingBox.of(1, 0, 1, 0))).isTrue();
        assertThat(bounds.intersects(BoundingBox.of(Math.nextUp(1d), 0, 2, 1))).isFalse();
        assertThatThrownBy(() -> BoundingBox.of(1, 0, 0, 1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testRejectsTruncatedMalformedAndUnsupportedWkb() {
        byte[] point = point(1, 2);
        for (int length = 0; length < point.length; length++) {
            byte[] truncated = Arrays.copyOf(point, length);
            assertThatThrownBy(() -> BoundingBox.fromWkb(truncated))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> BoundingBox.fromWkb(Arrays.copyOf(point, point.length + 1)))
                .hasMessageContaining("Trailing");
        byte[] invalidOrder = point.clone();
        invalidOrder[0] = 2;
        assertThatThrownBy(() -> BoundingBox.fromWkb(invalidOrder))
                .hasMessageContaining("byte order");
        for (int type : new int[] {0, 8, 1001, 2001, 3001, 0x20000001, 0x80000001}) {
            byte[] unsupported = point.clone();
            ByteBuffer.wrap(unsupported).order(ByteOrder.LITTLE_ENDIAN).putInt(1, type);
            assertThatThrownBy(() -> BoundingBox.fromWkb(unsupported))
                    .hasMessageContaining("2D OGC");
        }
        assertThatThrownBy(() -> BoundingBox.fromWkb(collection(4, line())))
                .hasMessageContaining("member");
        byte[] invalidCount = line();
        ByteBuffer.wrap(invalidCount).order(ByteOrder.LITTLE_ENDIAN).putInt(5, Integer.MAX_VALUE);
        assertThatThrownBy(() -> BoundingBox.fromWkb(invalidCount))
                .hasMessageContaining("element count");
    }

    @Test
    void testRejectsNonFiniteCoordinatesAndExcessiveNesting() {
        for (byte[] invalid :
                new byte[][] {
                    point(Double.NaN, 1),
                    point(1, Double.POSITIVE_INFINITY),
                    line(0, 0, Double.NaN, Double.NaN)
                }) {
            assertThatThrownBy(() -> BoundingBox.fromWkb(invalid)).hasMessageContaining("finite");
        }
        byte[] nested = point(1, 2);
        for (int i = 0; i < 66; i++) {
            nested = collection(7, nested);
        }
        final byte[] deeplyNested = nested;
        assertThatThrownBy(() -> BoundingBox.fromWkb(deeplyNested)).hasMessageContaining("nesting");
        assertBounds(
                line(-Double.MAX_VALUE, -Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE),
                -Double.MAX_VALUE,
                -Double.MAX_VALUE,
                Double.MAX_VALUE,
                Double.MAX_VALUE);
    }

    private static void assertBounds(
            byte[] wkb, double minX, double minY, double maxX, double maxY) {
        BoundingBox bounds = BoundingBox.fromWkb(wkb);
        assertThat(bounds.minX()).isEqualTo(minX);
        assertThat(bounds.minY()).isEqualTo(minY);
        assertThat(bounds.maxX()).isEqualTo(maxX);
        assertThat(bounds.maxY()).isEqualTo(maxY);
    }
}
