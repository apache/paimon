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

import org.apache.paimon.shade.guava30.com.google.common.cache.Cache;

import javax.annotation.Nullable;

import java.util.Map;

/** Guava cache implementation. */
public class GuavaCache implements InternalCache {
    private final Cache<CacheKey, CacheValue> cache;

    public GuavaCache(
            org.apache.paimon.shade.guava30.com.google.common.cache.Cache<CacheKey, CacheValue>
                    cache) {
        this.cache = cache;
    }

    @Nullable
    @Override
    public CacheValue get(CacheKey key) {
        return cache.getIfPresent(key);
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
