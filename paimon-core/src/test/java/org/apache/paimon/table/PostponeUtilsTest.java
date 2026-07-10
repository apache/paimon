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

package org.apache.paimon.table;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PostponeUtils}. */
public class PostponeUtilsTest {

    @Test
    public void testComputeBucketNumByRowCount() {
        assertThat(PostponeUtils.computeBucketNumByRowCount(0, 100)).isEqualTo(1);
        assertThat(PostponeUtils.computeBucketNumByRowCount(1, 100)).isEqualTo(1);
        assertThat(PostponeUtils.computeBucketNumByRowCount(100, 100)).isEqualTo(1);
        assertThat(PostponeUtils.computeBucketNumByRowCount(101, 100)).isEqualTo(2);
        assertThat(PostponeUtils.computeBucketNumByRowCount(999, 200)).isEqualTo(5);
        assertThat(PostponeUtils.computeBucketNumByRowCount(1000, 200)).isEqualTo(5);
    }

    @Test
    public void testComputeBucketNumByRowCountRejectsInvalidTarget() {
        assertThatThrownBy(() -> PostponeUtils.computeBucketNumByRowCount(100, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Option 'postpone.target-row-num-per-bucket' must be greater than 0.");
    }

    @Test
    public void testComputeBucketNumByRowCountRejectsOverflow() {
        assertThatThrownBy(() -> PostponeUtils.computeBucketNumByRowCount(Long.MAX_VALUE, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exceeds the maximum integer value")
                .hasMessageContaining("Consider increasing 'postpone.target-row-num-per-bucket'");
    }

    @Test
    public void testDetermineBucketNum() {
        Map<BinaryRow, Integer> knownNumBuckets = new HashMap<>();
        Map<BinaryRow, Long> postponeRowCounts = new HashMap<>();

        BinaryRow knownPartition = partition(1);
        BinaryRow targetPartition = partition(2);
        BinaryRow defaultPartition = partition(3);

        knownNumBuckets.put(knownPartition, 4);
        postponeRowCounts.put(knownPartition, 1000L);
        postponeRowCounts.put(targetPartition, 450L);

        assertThat(
                        PostponeUtils.determineBucketNum(
                                knownPartition, knownNumBuckets, 200L, postponeRowCounts, 1))
                .isEqualTo(4);
        assertThat(
                        PostponeUtils.determineBucketNum(
                                targetPartition, knownNumBuckets, 200L, postponeRowCounts, 1))
                .isEqualTo(3);
        assertThat(
                        PostponeUtils.determineBucketNum(
                                defaultPartition,
                                knownNumBuckets,
                                (Long) null,
                                postponeRowCounts,
                                7))
                .isEqualTo(7);
    }

    private static BinaryRow partition(int value) {
        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeInt(0, value);
        writer.complete();
        return row;
    }
}
