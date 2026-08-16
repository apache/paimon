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

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link MapSharedShreddingFieldDict}. */
class MapSharedShreddingFieldDictTest {

    @Test
    void testAssignMonotonicallyIncreasingIds() {
        MapSharedShreddingFieldDict dict = new MapSharedShreddingFieldDict();

        assertThat(dict.getOrAssign("cpu_usage")).isEqualTo(0);
        assertThat(dict.getOrAssign("mem_load")).isEqualTo(1);
        assertThat(dict.getOrAssign("disk_io")).isEqualTo(2);
        assertThat(dict.size()).isEqualTo(3);
    }

    @Test
    void testLookupReturnsExistingId() {
        MapSharedShreddingFieldDict dict = new MapSharedShreddingFieldDict();

        int first = dict.getOrAssign("cpu_usage");

        assertThat(dict.getOrAssign("cpu_usage")).isEqualTo(first);
        assertThat(dict.size()).isEqualTo(1);
    }

    @Test
    void testGetNameToId() {
        MapSharedShreddingFieldDict dict = new MapSharedShreddingFieldDict();

        dict.getOrAssign("b_field");
        dict.getOrAssign("a_field");

        Map<String, Integer> nameToId = dict.nameToId();
        assertThat(nameToId.keySet()).containsExactly("a_field", "b_field");
        assertThat(nameToId.get("b_field")).isEqualTo(0);
        assertThat(nameToId.get("a_field")).isEqualTo(1);
        assertThatThrownBy(() -> nameToId.put("c_field", 2))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testEmptyDict() {
        MapSharedShreddingFieldDict dict = new MapSharedShreddingFieldDict();

        assertThat(dict.size()).isZero();
        assertThat(dict.nameToId()).isEmpty();
    }
}
