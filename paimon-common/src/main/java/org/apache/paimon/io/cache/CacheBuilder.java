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
import org.apache.paimon.shade.guava30.com.google.common.cache.RemovalNotification;

/** Cache builder builds cache from cache type. */
public abstract class CacheBuilder {
    protected MemorySize memorySize;

    CacheBuilder maximumWeight(MemorySize memorySize) {
        this.memorySize = memorySize;
        return this;
    }

    public abstract Cache build();

    public static CacheBuilder newBuilder(Cache.CacheType type) {
        switch (type) {
            case CAFFEINE:
                return new CaffeineCacheBuilder();
            case GUAVA:
                return new GuavaCacheBuilder();
            default:
                throw new UnsupportedOperationException("Unsupported CacheType: " + type);
        }
    }

    static class CaffeineCacheBuilder extends CacheBuilder {
        @Override
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
    }

    static class GuavaCacheBuilder extends CacheBuilder {
        @Override
        public Cache build() {
            return new GuavaCache(
                    org.apache.paimon.shade.guava30.com.google.common.cache.CacheBuilder
                            .newBuilder()
                            .weigher(CacheBuilder::weigh)
                            .concurrencyLevel(1)
                            .maximumWeight(memorySize.getBytes())
                            .removalListener(this::onRemoval)
                            .build());
        }

        private void onRemoval(RemovalNotification<CacheKey, Cache.CacheValue> notification) {
            if (notification.getValue() != null) {
                notification.getValue().callback.onRemoval(notification.getKey());
            }
        }
    }

    private static int weigh(CacheKey cacheKey, Cache.CacheValue cacheValue) {
        return cacheValue.segment.size();
    }
}
