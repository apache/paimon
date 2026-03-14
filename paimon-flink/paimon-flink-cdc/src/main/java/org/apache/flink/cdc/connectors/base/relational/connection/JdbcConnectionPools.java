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

package org.apache.flink.cdc.connectors.base.relational.connection;

import com.zaxxer.hikari.HikariDataSource;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Copied from <a
 * href="https://github.com/apache/flink-cdc/blob/release-3.5.0/flink-cdc-connect/flink-cdc-source-connectors/flink-cdc-base/src/main/java/org/apache/flink/cdc/connectors/base/relational/connection/JdbcConnectionPools.java">Flink
 * CDC 3.5.0 resemblance</a>. Modified method {@link JdbcConnectionPools#clear()} at line 92 ~ 94.
 */
public class JdbcConnectionPools implements ConnectionPools<HikariDataSource, JdbcSourceConfig> {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcConnectionPools.class);

    private static JdbcConnectionPools instance;
    private final Map<ConnectionPoolId, HikariDataSource> pools = new HashMap<>();
    private static final Map<String, JdbcConnectionPoolFactory> POOL_FACTORY_MAP = new HashMap<>();

    private JdbcConnectionPools() {}

    public static synchronized JdbcConnectionPools getInstance(
            JdbcConnectionPoolFactory jdbcConnectionPoolFactory) {
        if (instance == null) {
            instance = new JdbcConnectionPools();
        }
        POOL_FACTORY_MAP.put(
                jdbcConnectionPoolFactory.getClass().getName(), jdbcConnectionPoolFactory);
        return instance;
    }

    @Override
    public HikariDataSource getOrCreateConnectionPool(
            ConnectionPoolId poolId, JdbcSourceConfig sourceConfig) {
        synchronized (pools) {
            if (!pools.containsKey(poolId)) {
                LOG.info("Create and register connection pool {}", poolId);
                JdbcConnectionPoolFactory jdbcConnectionPoolFactory =
                        POOL_FACTORY_MAP.get(poolId.getDataSourcePoolFactoryIdentifier());
                if (jdbcConnectionPoolFactory == null) {
                    throw new FlinkRuntimeException(
                            String.format(
                                    "Pool factory identifier is required for connection pool, but unknown pool factory identifier %s found.",
                                    poolId.getDataSourcePoolFactoryIdentifier()));
                }
                pools.put(poolId, jdbcConnectionPoolFactory.createPooledDataSource(sourceConfig));
            }
            return pools.get(poolId);
        }
    }

    /** this method is only supported for test. */
    @VisibleForTesting
    public String getJdbcUrl(
            JdbcSourceConfig sourceConfig, String dataSourcePoolFactoryIdentifier) {
        JdbcConnectionPoolFactory jdbcConnectionPoolFactory =
                POOL_FACTORY_MAP.get(dataSourcePoolFactoryIdentifier);
        if (jdbcConnectionPoolFactory == null) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Pool factory identifier is required for connection pools, but unknown pool factory identifier %s found.",
                            dataSourcePoolFactoryIdentifier));
        }
        return jdbcConnectionPoolFactory.getJdbcUrl(sourceConfig);
    }

    public void clear() throws IOException {
        // See org.apache.flink.cdc.connectors.mysql.source.connection.JdbcConnectionPools#clear.
    }
}
