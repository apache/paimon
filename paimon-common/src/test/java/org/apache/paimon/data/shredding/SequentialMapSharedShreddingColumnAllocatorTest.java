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

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SequentialMapSharedShreddingColumnAllocator}. */
class SequentialMapSharedShreddingColumnAllocatorTest {

    @Test
    void testSortsAndUsesLeadingColumns() {
        SequentialMapSharedShreddingColumnAllocator allocator =
                new SequentialMapSharedShreddingColumnAllocator(3);

        MapSharedShreddingColumnAllocator.RowAllocation row0 =
                allocator.allocateRow(Arrays.asList(1, 2));
        assertThat(row0.colToField()).containsExactly(1, 2, -1);
        assertThat(row0.overflowFields()).isEmpty();

        MapSharedShreddingColumnAllocator.RowAllocation row1 =
                allocator.allocateRow(Arrays.asList(2, 3));
        assertThat(row1.colToField()).containsExactly(2, 3, -1);
        assertThat(row1.overflowFields()).isEmpty();

        MapSharedShreddingColumnAllocator.RowAllocation row2 =
                allocator.allocateRow(Arrays.asList(7, 4, 6, 5));
        assertThat(row2.colToField()).containsExactly(4, 5, 6);
        assertThat(row2.overflowFields()).containsExactly(7);

        assertThat(allocator.fieldToColumns().get(1)).containsExactly(0);
        assertThat(allocator.fieldToColumns().get(2)).containsExactly(0, 1);
        assertThat(allocator.fieldToColumns().get(3)).containsExactly(1);
        assertThat(allocator.fieldToColumns().get(4)).containsExactly(0);
        assertThat(allocator.fieldToColumns().get(5)).containsExactly(1);
        assertThat(allocator.fieldToColumns().get(6)).containsExactly(2);
        assertThat(allocator.overflowFieldSet()).containsExactly(7);
    }
}
