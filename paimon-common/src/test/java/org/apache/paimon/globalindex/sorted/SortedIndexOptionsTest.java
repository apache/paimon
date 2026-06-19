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

package org.apache.paimon.globalindex.sorted;

import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SortedIndexOptions}. */
class SortedIndexOptionsTest {

    @Test
    void testDefaultRecordsPerRange() {
        assertThat(SortedIndexOptions.SORTED_INDEX_RECORDS_PER_RANGE.defaultValue())
                .isEqualTo(10_000_000L);
    }

    @Test
    void testBTreeBuildOptionFallbacks() {
        Options options = new Options();
        options.setString("btree-index.records-per-range", "100");
        options.setString("btree-index.build.max-parallelism", "8");

        assertThat(options.get(SortedIndexOptions.SORTED_INDEX_RECORDS_PER_RANGE)).isEqualTo(100L);
        assertThat(options.get(SortedIndexOptions.SORTED_INDEX_BUILD_MAX_PARALLELISM))
                .isEqualTo(8);
    }
}
