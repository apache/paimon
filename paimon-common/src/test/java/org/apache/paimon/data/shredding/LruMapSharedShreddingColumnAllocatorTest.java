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
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link LruMapSharedShreddingColumnAllocator}. */
class LruMapSharedShreddingColumnAllocatorTest {

    @Test
    void testAllocatesWithHitRetainEvictAndOverflow() {
        LruMapSharedShreddingColumnAllocator allocator =
                new LruMapSharedShreddingColumnAllocator(3);

        MapSharedShreddingColumnAllocator.RowAllocation row0 =
                allocator.allocateRow(Arrays.asList(0, 1, 2));
        assertThat(row0.colToField()).containsExactly(0, 1, 2);
        assertThat(row0.overflowFields()).isEmpty();

        MapSharedShreddingColumnAllocator.RowAllocation row1 =
                allocator.allocateRow(Arrays.asList(0, 1));
        assertThat(row1.colToField()).containsExactly(0, 1, -1);
        assertThat(row1.overflowFields()).isEmpty();

        MapSharedShreddingColumnAllocator.RowAllocation row2 =
                allocator.allocateRow(Arrays.asList(3, 4, 5));
        assertThat(row2.colToField()).containsExactly(4, 5, 3);
        assertThat(row2.overflowFields()).isEmpty();

        MapSharedShreddingColumnAllocator.RowAllocation row3 =
                allocator.allocateRow(Arrays.asList(0, 3, 4, 5));
        assertThat(row3.colToField()).containsExactly(4, 5, 3);
        assertThat(row3.overflowFields()).containsExactly(0);

        assertThat(allocator.maxRowWidth()).isEqualTo(4);
        assertThat(allocator.fieldToColumns().get(0)).containsExactly(0);
        assertThat(allocator.fieldToColumns().get(1)).containsExactly(1);
        assertThat(allocator.fieldToColumns().get(2)).containsExactly(2);
        assertThat(allocator.fieldToColumns().get(3)).containsExactly(2);
        assertThat(allocator.fieldToColumns().get(4)).containsExactly(0);
        assertThat(allocator.fieldToColumns().get(5)).containsExactly(1);
        assertThat(allocator.overflowFieldSet()).containsExactly(0);
    }

    @Test
    void testHandlesEmptyRows() {
        LruMapSharedShreddingColumnAllocator allocator =
                new LruMapSharedShreddingColumnAllocator(2);

        MapSharedShreddingColumnAllocator.RowAllocation emptyRow =
                allocator.allocateRow(Collections.emptyList());
        assertThat(emptyRow.colToField()).containsExactly(-1, -1);
        assertThat(emptyRow.overflowFields()).isEmpty();
        assertThat(allocator.maxRowWidth()).isZero();

        MapSharedShreddingColumnAllocator.RowAllocation row =
                allocator.allocateRow(Collections.singletonList(7));
        assertThat(row.colToField()).containsExactly(7, -1);
        assertThat(row.overflowFields()).isEmpty();
        assertThat(allocator.maxRowWidth()).isEqualTo(1);
    }
}
