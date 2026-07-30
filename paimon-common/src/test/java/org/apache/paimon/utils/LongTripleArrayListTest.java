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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link LongTripleArrayList}. */
class LongTripleArrayListTest {

    @Test
    void testAddAndAccess() {
        LongTripleArrayList triples = new LongTripleArrayList(2);
        triples.add(1L, 2L, 3L);
        triples.add(4L, 5L, 6L);

        assertThat(triples.size()).isEqualTo(2);
        assertThat(triples.usedLongCount()).isEqualTo(6);
        assertThat(triples.retainedLongCount()).isEqualTo(6);
        assertThat(triples.first(0)).isEqualTo(1L);
        assertThat(triples.second(0)).isEqualTo(2L);
        assertThat(triples.third(0)).isEqualTo(3L);
        assertThat(triples.first(1)).isEqualTo(4L);
        assertThat(triples.second(1)).isEqualTo(5L);
        assertThat(triples.third(1)).isEqualTo(6L);
    }

    @Test
    void testGrowAndSwap() {
        LongTripleArrayList triples = new LongTripleArrayList();
        for (int i = 0; i < 100; i++) {
            triples.add(i, i + 1L, i + 2L);
        }

        triples.swap(0, 99);
        triples.swap(50, 50);

        assertThat(triples.first(0)).isEqualTo(99L);
        assertThat(triples.second(0)).isEqualTo(100L);
        assertThat(triples.third(0)).isEqualTo(101L);
        assertThat(triples.first(99)).isZero();
        assertThat(triples.second(99)).isEqualTo(1L);
        assertThat(triples.third(99)).isEqualTo(2L);
        assertThat(triples.first(50)).isEqualTo(50L);
        assertThat(triples.retainedLongCount()).isGreaterThanOrEqualTo(300);
    }

    @Test
    void testClearAndRelease() {
        LongTripleArrayList triples = new LongTripleArrayList(2);
        triples.add(1L, 2L, 3L);

        triples.clear();
        assertThat(triples.size()).isZero();
        assertThat(triples.retainedLongCount()).isEqualTo(6);

        triples.add(4L, 5L, 6L);
        triples.release();
        assertThat(triples.size()).isZero();
        assertThat(triples.retainedLongCount()).isZero();

        triples.add(7L, 8L, 9L);
        assertThat(triples.first(0)).isEqualTo(7L);
    }

    @Test
    void testRejectsInvalidAccess() {
        assertThatThrownBy(() -> new LongTripleArrayList(-1))
                .isInstanceOf(IllegalArgumentException.class);

        LongTripleArrayList triples = new LongTripleArrayList();
        assertThatThrownBy(() -> triples.first(0)).isInstanceOf(IllegalArgumentException.class);
        triples.add(1L, 2L, 3L);
        assertThatThrownBy(() -> triples.second(-1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> triples.swap(0, 1)).isInstanceOf(IllegalArgumentException.class);
    }
}
