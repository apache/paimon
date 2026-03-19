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

package org.apache.paimon.globalindex;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link GlobalIndexBuilderUtils#adjustRowsPerShard}. */
public class GlobalIndexBuilderUtilsTest {

    @Test
    void testAdjustRowsPerShardNoAdjustmentNeeded() {
        // 1000 rows, 100 per shard = 10 shards, maxShard = 20 -> no adjustment
        long result = GlobalIndexBuilderUtils.adjustRowsPerShard(100, 1000, 20);
        assertThat(result).isEqualTo(100);
    }

    @Test
    void testAdjustRowsPerShardExactMatch() {
        // 1000 rows, 100 per shard = 10 shards, maxShard = 10 -> no adjustment
        long result = GlobalIndexBuilderUtils.adjustRowsPerShard(100, 1000, 10);
        assertThat(result).isEqualTo(100);
    }

    @Test
    void testAdjustRowsPerShardExceedsMaxShard() {
        // 1000 rows, 100 per shard = 10 shards, maxShard = 3 -> adjust to ceil(1000/3) = 334
        long result = GlobalIndexBuilderUtils.adjustRowsPerShard(100, 1000, 3);
        assertThat(result).isEqualTo(334);
        // Verify: ceil(1000/334) = 3 shards
        assertThat((1000 + result - 1) / result).isEqualTo(3);
    }

    @Test
    void testAdjustRowsPerShardMaxShardOne() {
        // 1000 rows, 100 per shard = 10 shards, maxShard = 1 -> all in one shard
        long result = GlobalIndexBuilderUtils.adjustRowsPerShard(100, 1000, 1);
        assertThat(result).isEqualTo(1000);
        assertThat((1000 + result - 1) / result).isEqualTo(1);
    }

    @Test
    void testAdjustRowsPerShardEvenDivision() {
        // 1000 rows, 100 per shard = 10 shards, maxShard = 5 -> adjust to 200
        long result = GlobalIndexBuilderUtils.adjustRowsPerShard(100, 1000, 5);
        assertThat(result).isEqualTo(200);
        assertThat((1000 + result - 1) / result).isEqualTo(5);
    }

    @Test
    void testAdjustRowsPerShardLargeRowCount() {
        // 10M rows, 100K per shard = 100 shards, maxShard = 10 -> adjust to 1M
        long result = GlobalIndexBuilderUtils.adjustRowsPerShard(100000, 10000000, 10);
        assertThat(result).isEqualTo(1000000);
        assertThat((10000000 + result - 1) / result).isEqualTo(10);
    }

    @Test
    void testAdjustRowsPerShardTotalRowsLessThanRowsPerShard() {
        // 50 rows, 100 per shard = 1 shard, maxShard = 3 -> no adjustment
        long result = GlobalIndexBuilderUtils.adjustRowsPerShard(100, 50, 3);
        assertThat(result).isEqualTo(100);
    }
}
