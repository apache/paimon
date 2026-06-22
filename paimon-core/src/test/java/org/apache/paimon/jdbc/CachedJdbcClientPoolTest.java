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

import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CachedJdbcClientPool}. */
public class CachedJdbcClientPoolTest {

    @AfterEach
    void tearDown() {
        CachedJdbcClientPool.resetCache();
    }

    @Test
    void testSameKeyReturnsSamePool() {
        String uri = sqliteUri();
        Options options = createOptions(uri, "my-catalog");

        CachedJdbcClientPool cache1 = new CachedJdbcClientPool(options, options.toMap());
        CachedJdbcClientPool cache2 = new CachedJdbcClientPool(options, options.toMap());

        assertThat(cache1.get()).isSameAs(cache2.get());
    }

    @Test
    void testDifferentUriReturnsDifferentPool() {
        Options options1 = createOptions(sqliteUri(), "my-catalog");
        Options options2 = createOptions(sqliteUri(), "my-catalog");

        CachedJdbcClientPool cache1 = new CachedJdbcClientPool(options1, options1.toMap());
        CachedJdbcClientPool cache2 = new CachedJdbcClientPool(options2, options2.toMap());

        assertThat(cache1.get()).isNotSameAs(cache2.get());
    }

    @Test
    void testDifferentCatalogKeyReturnsDifferentPool() {
        String uri = sqliteUri();
        Options options1 = createOptions(uri, "catalog-a");
        Options options2 = createOptions(uri, "catalog-b");

        CachedJdbcClientPool cache1 = new CachedJdbcClientPool(options1, options1.toMap());
        CachedJdbcClientPool cache2 = new CachedJdbcClientPool(options2, options2.toMap());

        assertThat(cache1.get()).isNotSameAs(cache2.get());
    }

    @Test
    void testPoolIsUsable() throws SQLException, InterruptedException {
        Options options = createOptions(sqliteUri(), "test-catalog");
        CachedJdbcClientPool cache = new CachedJdbcClientPool(options, options.toMap());

        JdbcClientPool pool = cache.get();
        Boolean result = pool.run(conn -> !conn.isClosed());

        assertThat(result).isTrue();
    }

    @Test
    void testMultipleCatalogInstancesSharePool() {
        String uri = sqliteUri();
        Options options = createOptions(uri, "shared-catalog");

        JdbcCatalog catalog1 =
                new JdbcCatalog(
                        new org.apache.paimon.fs.local.LocalFileIO(),
                        "shared-catalog",
                        org.apache.paimon.catalog.CatalogContext.create(options),
                        "/tmp/warehouse1");
        JdbcCatalog catalog2 =
                new JdbcCatalog(
                        new org.apache.paimon.fs.local.LocalFileIO(),
                        "shared-catalog",
                        org.apache.paimon.catalog.CatalogContext.create(options),
                        "/tmp/warehouse2");

        assertThat(catalog1.getConnections()).isSameAs(catalog2.getConnections());
    }

    @Test
    void testResetCacheClearsAllPools() {
        Options options = createOptions(sqliteUri(), "test-catalog");
        CachedJdbcClientPool cache = new CachedJdbcClientPool(options, options.toMap());
        JdbcClientPool pool = cache.get();

        assertThat(pool).isNotNull();
        assertThat(CachedJdbcClientPool.clientPools()).isNotEmpty();

        CachedJdbcClientPool.resetCache();

        assertThat(CachedJdbcClientPool.clientPools()).isEmpty();
    }

    private static Options createOptions(String uri, String catalogKey) {
        Options options = new Options();
        options.set(CatalogOptions.URI, uri);
        options.set(JdbcCatalogOptions.CATALOG_KEY, catalogKey);
        options.set(CatalogOptions.CLIENT_POOL_SIZE, 2);
        return options;
    }

    private static String sqliteUri() {
        return "jdbc:sqlite:file::memory:?ic" + UUID.randomUUID().toString().replace("-", "");
    }
}
