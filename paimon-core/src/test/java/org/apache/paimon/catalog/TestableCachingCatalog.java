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

package org.apache.paimon.catalog;

import org.apache.paimon.options.MemorySize;
import org.apache.paimon.table.Table;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Ticker;

import java.time.Duration;
import java.util.Optional;

/**
 * A wrapper around CachingCatalog that provides accessor methods to test the underlying cache,
 * without making those fields public in the CachingCatalog itself.
 */
public class TestableCachingCatalog extends CachingCatalog {

    private final Duration cacheExpirationInterval;

    public TestableCachingCatalog(Catalog catalog, Duration expirationInterval, Ticker ticker) {
        super(catalog, expirationInterval, true, MemorySize.ZERO, Long.MAX_VALUE, ticker);
        this.cacheExpirationInterval = expirationInterval;
    }

    public Cache<Identifier, Table> cache() {
        // cleanUp must be called as tests apply assertions directly on the underlying map, but
        // metadata
        // table map entries are cleaned up asynchronously.
        tableCache.cleanUp();
        return tableCache;
    }

    public Optional<Duration> ageOf(Identifier identifier) {
        return tableCache.policy().expireAfterAccess().get().ageOf(identifier);
    }

    public Optional<Duration> remainingAgeFor(Identifier identifier) {
        return ageOf(identifier).map(cacheExpirationInterval::minus);
    }
}
