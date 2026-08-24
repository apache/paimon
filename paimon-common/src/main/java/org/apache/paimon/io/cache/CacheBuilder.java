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

import org.apache.paimon.options.MemorySize;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.RemovalCause;

/** Builder for a Caffeine cache. */
public class CacheBuilder {
    private MemorySize memorySize;

    CacheBuilder maximumWeight(MemorySize memorySize) {
        this.memorySize = memorySize;
        return this;
    }

    public static CacheBuilder newBuilder() {
        return new CacheBuilder();
    }

    public Cache build() {
        return new CaffeineCache(
                Caffeine.newBuilder()
                        .weigher(CacheBuilder::weigh)
                        .maximumWeight(memorySize.getBytes())
                        .removalListener(this::onRemoval)
                        .executor(Runnable::run)
                        .build());
    }

    private void onRemoval(CacheKey key, Cache.CacheValue value, RemovalCause cause) {
        if (value != null) {
            value.callback.onRemoval(key);
        }
    }

    private static int weigh(CacheKey cacheKey, Cache.CacheValue cacheValue) {
        return cacheValue.segment.size();
    }
}
