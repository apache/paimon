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

import org.apache.paimon.memory.MemorySlice;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.function.Function;

/** Cache interface in Paimon. */
public interface Cache {

    /** Looks up an entry without loading it, recording an access according to the cache policy. */
    @Nullable
    CacheValue getIfPresent(CacheKey key);

    @Nullable
    CacheValue get(CacheKey key, Function<CacheKey, CacheValue> supplier);

    void put(CacheKey key, CacheValue value);

    boolean contains(CacheKey key);

    void invalidate(CacheKey key);

    void invalidateAll();

    Map<CacheKey, CacheValue> asMap();

    /** Value for cache. */
    class CacheValue {

        final MemorySlice slice;
        final CacheCallback callback;

        CacheValue(MemorySlice slice, CacheCallback callback) {
            this.slice = slice;
            this.callback = callback;
        }
    }
}
