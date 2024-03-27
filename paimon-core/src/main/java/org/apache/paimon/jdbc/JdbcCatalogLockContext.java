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

import org.apache.paimon.catalog.CatalogLockContext;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.StringUtils;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Scheduler;
import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/** Jdbc lock context. */
public class JdbcCatalogLockContext implements CatalogLockContext {

    private static final String CONF_KEY_PREFIX = "lockConfigKey:";

    // Cache access connections for non Jdbc catalogs.
    protected static Cache<String, JdbcClientPool> clientPoolCache;
    private long evictionInterval;
    private String key;

    private JdbcClientPool connections;
    private final String catalogKey;
    private final Options options;
    private final String metastore;

    public JdbcCatalogLockContext(Options options) {
        this.options = options;
        this.catalogKey = options.get(JdbcCatalogOptions.CATALOG_KEY);
        this.metastore = options.get(CatalogOptions.METASTORE);
        if (!metastore.equals(JdbcCatalogFactory.IDENTIFIER)) {
            this.evictionInterval =
                    options.get(CatalogOptions.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS);
            this.key = extractKey();
            init();
        }
    }

    /** Use all connection parameters of jdbc to form a unique connection. */
    private String extractKey() {
        Map<String, String> props =
                JdbcUtils.extractJdbcConfigurationToMap(
                        options.toMap(), JdbcCatalog.PROPERTY_PREFIX);
        List<String> elements = Lists.newArrayList();
        props.forEach(
                (key, value) -> {
                    elements.add(value);
                });
        return CONF_KEY_PREFIX
                .concat(catalogKey)
                .concat(":")
                .concat(StringUtils.join(elements, "."));
    }

    JdbcCatalogLockContext(JdbcClientPool connections, Options options) {
        this(options);
        this.connections = connections;
    }

    protected synchronized void init() {
        if (clientPoolCache == null) {
            clientPoolCache =
                    Caffeine.newBuilder()
                            .expireAfterAccess(evictionInterval, TimeUnit.MILLISECONDS)
                            .removalListener(
                                    (ignored, value, cause) -> ((JdbcClientPool) value).close())
                            .scheduler(
                                    Scheduler.forScheduledExecutorService(
                                            new ScheduledThreadPoolExecutor(
                                                    1,
                                                    new ThreadFactory() {
                                                        final ThreadFactory defaultFactory =
                                                                Executors.defaultThreadFactory();

                                                        @Override
                                                        public Thread newThread(Runnable r) {
                                                            Thread thread =
                                                                    defaultFactory.newThread(r);
                                                            thread.setDaemon(true);
                                                            return thread;
                                                        }
                                                    })))
                            .build();
        }
    }

    @Override
    public Options options() {
        return options;
    }

    public String catalogKey() {
        return catalogKey;
    }

    public JdbcClientPool connections() {
        // Cache connection information is required for non jdbc catalogs.
        if (!metastore.equals(JdbcCatalogFactory.IDENTIFIER)) {
            return clientPoolCache.get(
                    key,
                    k -> {
                        JdbcClientPool connections =
                                new JdbcClientPool(
                                        options.get(CatalogOptions.CLIENT_POOL_SIZE),
                                        options.get(CatalogOptions.URI.key()),
                                        options.toMap());
                        try {
                            JdbcUtils.createDistributedLockTable(connections, options);
                        } catch (SQLException e) {
                            throw new RuntimeException(
                                    "Cannot initialize JDBC distributed lock.", e);
                        } catch (InterruptedException e) {
                            throw new RuntimeException("Interrupted in call to initialize", e);
                        }
                        return connections;
                    });
        } else {
            // The use of lock in the jdbc catalog is maintained by the jdbc catalog itself
            return connections;
        }
    }
}
