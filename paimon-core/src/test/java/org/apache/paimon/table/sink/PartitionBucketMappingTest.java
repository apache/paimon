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

package org.apache.paimon.table.sink;

import org.apache.paimon.data.BinaryRow;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PartitionBucketMapping}. */
public class PartitionBucketMappingTest {

    @Test
    public void testDefaultBucketCount() {
        PartitionBucketMapping mapping = new PartitionBucketMapping(16);

        // Any partition should resolve to the default
        assertThat(mapping.resolveNumBuckets(BinaryRow.EMPTY_ROW)).isEqualTo(16);
        assertThat(mapping.resolveNumBuckets(partition(1))).isEqualTo(16);
        assertThat(mapping.resolveNumBuckets(partition(42))).isEqualTo(16);
    }

    @Test
    public void testExplicitPartitionMapping() {
        BinaryRow partA = partition(1);
        BinaryRow partB = partition(2);
        BinaryRow partC = partition(3);

        Map<BinaryRow, Integer> partitionMap = new HashMap<>();
        partitionMap.put(partA, 32);
        partitionMap.put(partB, 64);

        PartitionBucketMapping mapping = new PartitionBucketMapping(16, partitionMap);

        // Mapped partitions return their specific bucket counts
        assertThat(mapping.resolveNumBuckets(partA)).isEqualTo(32);
        assertThat(mapping.resolveNumBuckets(partB)).isEqualTo(64);

        // Unmapped partition falls back to the default
        assertThat(mapping.resolveNumBuckets(partC)).isEqualTo(16);
    }

    private static BinaryRow partition(int value) {
        return BinaryRow.singleColumn(value);
    }
}
