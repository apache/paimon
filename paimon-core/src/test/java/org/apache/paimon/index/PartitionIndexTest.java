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

package org.apache.paimon.index;

import org.apache.paimon.utils.Int2ShortHashMap;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PartitionIndex}. */
class PartitionIndexTest {

    @ParameterizedTest
    @ValueSource(ints = {-1, 32768})
    void testMaximumBucketCount(int maxBuckets) {
        PartitionIndex index =
                new PartitionIndex(new Int2ShortHashMap(), new HashMap<>(), Long.MAX_VALUE);

        int hash = 12345;
        assertThat(index.assign(hash, bucket -> bucket == Short.MAX_VALUE, maxBuckets))
                .isEqualTo(Short.MAX_VALUE);
        assertThat(index.assign(hash, bucket -> false, maxBuckets)).isEqualTo(Short.MAX_VALUE);
    }

    @Test
    void testRejectBucketCountAboveShortRange() {
        PartitionIndex index =
                new PartitionIndex(new Int2ShortHashMap(), new HashMap<>(), Long.MAX_VALUE);

        for (int i = 0; i < 2; i++) {
            assertThatThrownBy(() -> index.assign(12345, bucket -> bucket >= 32768, 40000))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(
                            "'dynamic-bucket.max-buckets' must be -1 or between 1 and 32768, but was 40000.");
        }
    }

    @Test
    void testRejectExistingBucketAboveShortRange() {
        HashMap<Integer, Long> buckets = new HashMap<>();
        buckets.put(32768, 0L);
        PartitionIndex index = new PartitionIndex(new Int2ShortHashMap(), buckets, Long.MAX_VALUE);

        assertThatThrownBy(() -> index.assign(12345, bucket -> false, 32768))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Dynamic bucket id must be between 0 and 32767, but was 32768.");
    }
}
