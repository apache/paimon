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

package org.apache.paimon.jdbc;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.options.Options;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.apache.paimon.jdbc.JdbcCatalogOptions.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS;
import static org.apache.paimon.options.CatalogOptions.CLIENT_POOL_SIZE;
import static org.apache.paimon.options.CatalogOptions.URI;

/**
 * A cache that shares {@link JdbcClientPool} instances across multiple catalog instances in the
 * same JVM. This prevents each Flink operator from creating its own connection pool when using the
 * JDBC catalog.
 *
 * <p>The cache is keyed by JDBC URI and catalog key. Pools are evicted after a configurable
 * inactivity duration.
 */
public class CachedJdbcClientPool {

    private static volatile Cache<Key, JdbcClientPool> clientPoolCache;

    private final Key key;
    private final int poolSize;
    private final String dbUrl;
    private final Map<String, String> props;
    private final long evictionInterval;

    public CachedJdbcClientPool(Options options, Map<String, String> props) {
        this.dbUrl = options.get(URI);
        this.poolSize = options.get(CLIENT_POOL_SIZE);
        this.evictionInterval = options.get(CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS);
        this.props = props;
        this.key = Key.of(dbUrl, options.get(JdbcCatalogOptions.CATALOG_KEY));
        init();
    }

    private synchronized void init() {
        if (clientPoolCache == null) {
            clientPoolCache =
                    Caffeine.newBuilder()
                            .expireAfterAccess(evictionInterval, TimeUnit.MILLISECONDS)
                            .removalListener(
                                    (ignored, value, cause) -> {
                                        if (value != null) {
                                            ((JdbcClientPool) value).close();
                                        }
                                    })
                            .build();
        }
    }

    /** Returns the shared {@link JdbcClientPool} for this cache key, creating one if needed. */
    public JdbcClientPool get() {
        return clientPoolCache.get(key, k -> new JdbcClientPool(poolSize, dbUrl, props));
    }

    @VisibleForTesting
    static Cache<Key, JdbcClientPool> clientPoolCache() {
        return clientPoolCache;
    }

    @VisibleForTesting
    static synchronized void resetCache() {
        if (clientPoolCache != null) {
            clientPoolCache.invalidateAll();
            clientPoolCache = null;
        }
    }

    static class Key {
        private final String uri;
        private final String catalogKey;

        private Key(String uri, String catalogKey) {
            this.uri = uri;
            this.catalogKey = catalogKey;
        }

        static Key of(String uri, String catalogKey) {
            return new Key(uri, catalogKey);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Key that = (Key) o;
            return Objects.equals(uri, that.uri) && Objects.equals(catalogKey, that.catalogKey);
        }

        @Override
        public int hashCode() {
            return Objects.hash(uri, catalogKey);
        }
    }
}
