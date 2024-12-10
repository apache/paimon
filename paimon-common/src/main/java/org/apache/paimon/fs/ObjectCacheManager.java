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

package org.apache.paimon.fs;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;

import java.time.Duration;
import java.util.function.Function;

/**
 * Sample Object Cache Manager .
 *
 * @param <K>
 * @param <V>
 */
public class ObjectCacheManager<K, V> {
    private final Cache<K, V> cache;

    private ObjectCacheManager(Duration timeout, int maxSize) {
        this.cache = Caffeine.newBuilder().maximumSize(maxSize).expireAfterWrite(timeout).build();
    }

    public static <K, V> ObjectCacheManager<K, V> newObjectCacheManager(
            Duration timeout, int maxSize) {
        return new ObjectCacheManager<>(timeout, maxSize);
    }

    public ObjectCacheManager<K, V> put(K k, V v) {
        this.cache.put(k, v);
        return this;
    }

    public V get(K k, Function<? super K, ? extends V> creator) {
        return this.cache.get(k, creator);
    }

    public V getIfPresent(K k) {
        return this.cache.getIfPresent(k);
    }
}
