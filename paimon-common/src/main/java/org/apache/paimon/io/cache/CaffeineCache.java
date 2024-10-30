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

import javax.annotation.Nullable;

import java.util.Map;
import java.util.function.Function;

/** Caffeine cache implementation. */
public class CaffeineCache implements Cache {
    private final org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache<
                    CacheKey, CacheValue>
            cache;

    public CaffeineCache(
            org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache<
                            CacheKey, CacheValue>
                    cache) {
        this.cache = cache;
    }

    @Nullable
    @Override
    public CacheValue get(CacheKey key, Function<CacheKey, CacheValue> supplier) {
        return this.cache.get(key, supplier);
    }

    @Override
    public void put(CacheKey key, CacheValue value) {
        this.cache.put(key, value);
    }

    @Override
    public void invalidate(CacheKey key) {
        this.cache.invalidate(key);
    }

    @Override
    public void invalidateAll() {
        this.cache.invalidateAll();
    }

    @Override
    public Map<CacheKey, CacheValue> asMap() {
        return this.cache.asMap();
    }
}
