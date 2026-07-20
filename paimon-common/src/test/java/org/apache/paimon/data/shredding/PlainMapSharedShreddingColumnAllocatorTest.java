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

package org.apache.paimon.data.shredding;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PlainMapSharedShreddingColumnAllocator}. */
class PlainMapSharedShreddingColumnAllocatorTest {

    @Test
    void testBasicAllocation() {
        PlainMapSharedShreddingColumnAllocator allocator =
                new PlainMapSharedShreddingColumnAllocator(3);

        MapSharedShreddingColumnAllocator.RowAllocation allocation =
                allocator.allocateRow(Arrays.asList(10, 20));

        assertThat(allocation.colToField()).containsExactly(10, 20, -1);
        assertThat(allocation.overflowFields()).isEmpty();
    }

    @Test
    void testExactlyKFields() {
        PlainMapSharedShreddingColumnAllocator allocator =
                new PlainMapSharedShreddingColumnAllocator(3);

        MapSharedShreddingColumnAllocator.RowAllocation allocation =
                allocator.allocateRow(Arrays.asList(0, 1, 2));

        assertThat(allocation.colToField()).containsExactly(0, 1, 2);
        assertThat(allocation.overflowFields()).isEmpty();
    }

    @Test
    void testOverflowWhenExceedK() {
        PlainMapSharedShreddingColumnAllocator allocator =
                new PlainMapSharedShreddingColumnAllocator(2);

        MapSharedShreddingColumnAllocator.RowAllocation allocation =
                allocator.allocateRow(Arrays.asList(10, 20, 30, 40));

        assertThat(allocation.colToField()).containsExactly(10, 20);
        assertThat(allocation.overflowFields()).containsExactly(30, 40);
        assertThatThrownBy(() -> allocation.overflowFields().add(50))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testEmptyRow() {
        PlainMapSharedShreddingColumnAllocator allocator =
                new PlainMapSharedShreddingColumnAllocator(3);

        MapSharedShreddingColumnAllocator.RowAllocation allocation =
                allocator.allocateRow(Arrays.asList());

        assertThat(allocation.colToField()).containsExactly(-1, -1, -1);
        assertThat(allocation.overflowFields()).isEmpty();
    }

    @Test
    void testMaxRowWidthTracked() {
        PlainMapSharedShreddingColumnAllocator allocator =
                new PlainMapSharedShreddingColumnAllocator(3);

        allocator.allocateRow(Arrays.asList(1, 2));
        allocator.allocateRow(Arrays.asList(1, 2, 3, 4, 5));
        allocator.allocateRow(Arrays.asList(1));

        assertThat(allocator.maxRowWidth()).isEqualTo(5);
    }

    @Test
    void testFieldToColumnsAccumulated() {
        PlainMapSharedShreddingColumnAllocator allocator =
                new PlainMapSharedShreddingColumnAllocator(3);

        allocator.allocateRow(Arrays.asList(10, 20, 30));
        allocator.allocateRow(Arrays.asList(20, 40));

        Map<Integer, List<Integer>> fieldToColumns = allocator.fieldToColumns();
        assertThat(fieldToColumns.get(10)).containsExactly(0);
        assertThat(fieldToColumns.get(20)).containsExactly(0, 1);
        assertThat(fieldToColumns.get(30)).containsExactly(2);
        assertThat(fieldToColumns.get(40)).containsExactly(1);
        assertThatThrownBy(() -> fieldToColumns.put(50, Arrays.asList(2)))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> fieldToColumns.get(20).add(2))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testOverflowFieldSetAccumulated() {
        PlainMapSharedShreddingColumnAllocator allocator =
                new PlainMapSharedShreddingColumnAllocator(2);

        allocator.allocateRow(Arrays.asList(1, 2, 3));
        allocator.allocateRow(Arrays.asList(4, 5, 6, 7));

        assertThat(allocator.overflowFieldSet()).isEqualTo(new TreeSet<>(Arrays.asList(3, 6, 7)));
        assertThatThrownBy(() -> allocator.overflowFieldSet().add(8))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testSingleColumnAllocator() {
        PlainMapSharedShreddingColumnAllocator allocator =
                new PlainMapSharedShreddingColumnAllocator(1);

        MapSharedShreddingColumnAllocator.RowAllocation allocation =
                allocator.allocateRow(Arrays.asList(10, 20, 30));

        assertThat(allocation.colToField()).containsExactly(10);
        assertThat(allocation.overflowFields()).containsExactly(20, 30);
        assertThat(allocator.numColumns()).isEqualTo(1);
    }

    @Test
    void testUsesInputOrder() {
        PlainMapSharedShreddingColumnAllocator allocator =
                new PlainMapSharedShreddingColumnAllocator(3);

        MapSharedShreddingColumnAllocator.RowAllocation row0 =
                allocator.allocateRow(Arrays.asList(2, 0, 1));
        assertThat(row0.colToField()).containsExactly(2, 0, 1);
        assertThat(row0.overflowFields()).isEmpty();

        MapSharedShreddingColumnAllocator.RowAllocation row1 =
                allocator.allocateRow(Arrays.asList(4, 3, 5, 6));
        assertThat(row1.colToField()).containsExactly(4, 3, 5);
        assertThat(row1.overflowFields()).containsExactly(6);

        assertThat(allocator.fieldToColumns().get(0)).containsExactly(1);
        assertThat(allocator.fieldToColumns().get(1)).containsExactly(2);
        assertThat(allocator.fieldToColumns().get(2)).containsExactly(0);
        assertThat(allocator.fieldToColumns().get(3)).containsExactly(1);
        assertThat(allocator.fieldToColumns().get(4)).containsExactly(0);
        assertThat(allocator.fieldToColumns().get(5)).containsExactly(2);
        assertThat(allocator.overflowFieldSet()).containsExactly(6);
    }
}
