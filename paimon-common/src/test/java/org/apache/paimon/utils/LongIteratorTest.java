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

package org.apache.paimon.utils;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class LongIteratorTest {

    @Test
    public void testRange() {
        LongIterator iterator = LongIterator.fromRange(5, 10);
        List<Long> list = new ArrayList<>();
        while (iterator.hasNext()) {
            list.add(iterator.next());
        }
        assertThat(list).containsExactlyInAnyOrder(5L, 6L, 7L, 8L, 9L);
    }

    @Test
    public void testFromArray() {
        long[] array = new long[] {5L, 6L, 7L, 8L, 9L};
        LongIterator iterator = LongIterator.fromArray(array);
        List<Long> list = new ArrayList<>();
        while (iterator.hasNext()) {
            list.add(iterator.next());
        }
        assertThat(list).containsExactlyInAnyOrder(5L, 6L, 7L, 8L, 9L);
    }
}
