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

import org.apache.paimon.data.SingleSegments;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.options.MemorySize;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link SegmentsCache}. */
public class SegmentsCacheTest {

    @Test
    public void testDefaultsSoftValuesEnabledAndNoTtl() {
        SegmentsCache<String> cache =
                new SegmentsCache<>(1024, MemorySize.ofKibiBytes(64), Long.MAX_VALUE);
        assertThat(cache.softValues()).isTrue();
        assertThat(cache.ttl()).isNull();
    }

    @Test
    public void testGettersReflectConstructorArgs() {
        Duration ttl = Duration.ofMinutes(5);
        SegmentsCache<String> cache =
                new SegmentsCache<>(1024, MemorySize.ofKibiBytes(64), 100L, ttl, false);
        assertThat(cache.softValues()).isFalse();
        assertThat(cache.ttl()).isEqualTo(ttl);
        assertThat(cache.pageSize()).isEqualTo(1024);
        assertThat(cache.maxElementSize()).isEqualTo(100L);
        assertThat(cache.maxMemorySize()).isEqualTo(MemorySize.ofKibiBytes(64));
    }

    @Test
    public void testCreateReturnsNullWhenMemoryZero() {
        assertThat(SegmentsCache.create(1024, MemorySize.ofBytes(0), Long.MAX_VALUE, null, false))
                .isNull();
    }

    @Test
    public void testCreatePassesThroughTtlAndSoftValues() {
        Duration ttl = Duration.ofMinutes(7);
        SegmentsCache<String> cache =
                SegmentsCache.create(2048, MemorySize.ofKibiBytes(64), 100L, ttl, false);
        assertThat(cache).isNotNull();
        assertThat(cache.ttl()).isEqualTo(ttl);
        assertThat(cache.softValues()).isFalse();
        assertThat(cache.pageSize()).isEqualTo(2048);
    }

    @Test
    public void testCreateDefaultOverloadHasNoTtlAndSoftValues() {
        SegmentsCache<String> cache =
                SegmentsCache.create(1024, MemorySize.ofKibiBytes(64), Long.MAX_VALUE);
        assertThat(cache).isNotNull();
        assertThat(cache.ttl()).isNull();
        assertThat(cache.softValues()).isTrue();
    }

    @Test
    public void testStrongRefsAreBoundedByWeight() {
        // With soft values disabled the cache holds strong references, so the only thing keeping
        // it bounded is weight-based (SIZE) eviction. Insert far more than the budget allows and
        // assert the retained footprint stays within the configured maximum.
        MemorySize budget = MemorySize.ofKibiBytes(8);
        SegmentsCache<String> cache =
                new SegmentsCache<>(1024, budget, Long.MAX_VALUE, null, false);
        for (int i = 0; i < 100; i++) {
            cache.put("k" + i, new SingleSegments(MemorySegment.allocateHeapMemory(1024), 1024));
        }
        assertThat(cache.totalCacheBytes()).isLessThanOrEqualTo(budget.getBytes());
        assertThat(cache.estimatedSize()).isLessThan(100);
    }
}
