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

import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.apache.paimon.options.CatalogOptions.CLIENT_POOL_SIZE;
import static org.apache.paimon.options.CatalogOptions.URI;

/**
 * A cache that shares {@link JdbcClientPool} instances across multiple catalog instances in the
 * same JVM. This prevents each Flink operator from creating its own connection pool when using the
 * JDBC catalog.
 *
 * <p>The cache is keyed by JDBC URI, catalog key, pool size, and JDBC connection properties
 * (credentials, driver settings). Pools live for the lifetime of the JVM and are closed via a
 * shutdown hook.
 */
public class CachedJdbcClientPool {

    private static final ConcurrentMap<Key, JdbcClientPool> CLIENT_POOLS =
            new ConcurrentHashMap<>();

    static {
        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(
                                () -> {
                                    for (JdbcClientPool pool : CLIENT_POOLS.values()) {
                                        pool.close();
                                    }
                                    CLIENT_POOLS.clear();
                                },
                                "jdbc-client-pool-shutdown"));
    }

    private final Key key;
    private final int poolSize;
    private final String dbUrl;
    private final Map<String, String> props;

    public CachedJdbcClientPool(Options options, Map<String, String> props) {
        this.dbUrl = options.get(URI);
        this.poolSize = options.get(CLIENT_POOL_SIZE);
        this.props = props;
        Properties jdbcProps =
                JdbcUtils.extractJdbcConfiguration(props, JdbcCatalog.PROPERTY_PREFIX);
        this.key = Key.of(dbUrl, options.get(JdbcCatalogOptions.CATALOG_KEY), poolSize, jdbcProps);
    }

    /** Returns the shared {@link JdbcClientPool} for this cache key, creating one if needed. */
    public JdbcClientPool get() {
        return CLIENT_POOLS.computeIfAbsent(key, k -> new JdbcClientPool(poolSize, dbUrl, props));
    }

    @VisibleForTesting
    static ConcurrentMap<Key, JdbcClientPool> clientPools() {
        return CLIENT_POOLS;
    }

    @VisibleForTesting
    static void resetCache() {
        for (JdbcClientPool pool : CLIENT_POOLS.values()) {
            pool.close();
        }
        CLIENT_POOLS.clear();
    }

    static class Key {
        private final String uri;
        private final String catalogKey;
        private final int poolSize;
        private final Map<String, String> jdbcProperties;

        private Key(String uri, String catalogKey, int poolSize, Properties jdbcProps) {
            this.uri = uri;
            this.catalogKey = catalogKey;
            this.poolSize = poolSize;
            TreeMap<String, String> sorted = new TreeMap<>();
            for (String name : jdbcProps.stringPropertyNames()) {
                sorted.put(name, jdbcProps.getProperty(name));
            }
            this.jdbcProperties = sorted;
        }

        static Key of(String uri, String catalogKey, int poolSize, Properties jdbcProps) {
            return new Key(uri, catalogKey, poolSize, jdbcProps);
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
            return poolSize == that.poolSize
                    && Objects.equals(uri, that.uri)
                    && Objects.equals(catalogKey, that.catalogKey)
                    && Objects.equals(jdbcProperties, that.jdbcProperties);
        }

        @Override
        public int hashCode() {
            return Objects.hash(uri, catalogKey, poolSize, jdbcProperties);
        }
    }
}
