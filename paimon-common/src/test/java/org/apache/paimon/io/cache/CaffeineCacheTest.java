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

package org.apache.paimon.io.cache;

import org.apache.paimon.fs.Path;
import org.apache.paimon.memory.MemorySlice;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the Caffeine adapter's non-loading lookup contract. */
class CaffeineCacheTest {

    @Test
    void testPresentLookupRefreshesAccessExpiration() {
        AtomicLong time = new AtomicLong();
        Cache cache =
                new CaffeineCache(
                        Caffeine.newBuilder()
                                .expireAfterAccess(10, TimeUnit.SECONDS)
                                .ticker(time::get)
                                .executor(Runnable::run)
                                .build());
        CacheKey key = CacheKey.forPosition(new Path("file"), 0, 2, false);
        Cache.CacheValue value =
                new Cache.CacheValue(MemorySlice.wrap(new byte[] {1, 2}), ignored -> {});
        cache.put(key, value);

        time.set(TimeUnit.SECONDS.toNanos(9));
        assertThat(cache.getIfPresent(key)).isSameAs(value);
        time.set(TimeUnit.SECONDS.toNanos(18));
        assertThat(cache.getIfPresent(key)).isSameAs(value);
        time.set(TimeUnit.SECONDS.toNanos(29));
        assertThat(cache.getIfPresent(key)).isNull();
    }
}
