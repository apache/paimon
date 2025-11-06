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

package org.apache.flink.cdc.connectors.mysql.source.connection;

import com.zaxxer.hikari.HikariDataSource;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Copied from <a
 * href="https://github.com/apache/flink-cdc/blob/release-3.5.0/flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mysql-cdc/src/main/java/org/apache/flink/cdc/connectors/mysql/source/connection/JdbcConnectionPools.java">Flink
 * CDC 3.5.0 resemblance</a>. Modified method {@link JdbcConnectionPools#clear()} at line 60 ~ 66.
 */
public class JdbcConnectionPools implements ConnectionPools {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcConnectionPools.class);

    private static final JdbcConnectionPools INSTANCE = new JdbcConnectionPools();
    private final Map<ConnectionPoolId, HikariDataSource> pools = new HashMap<>();

    private JdbcConnectionPools() {}

    public static JdbcConnectionPools getInstance() {
        return INSTANCE;
    }

    @Override
    public HikariDataSource getOrCreateConnectionPool(
            ConnectionPoolId poolId, MySqlSourceConfig sourceConfig) {
        synchronized (pools) {
            if (!pools.containsKey(poolId)) {
                LOG.info("Create and register connection pool {}", poolId);
                pools.put(poolId, PooledDataSourceFactory.createPooledDataSource(sourceConfig));
            }
            return pools.get(poolId);
        }
    }

    public void clear() throws IOException {
        // Intentionally no-op.
        //
        // Flink CDC 3.2+ automatically clears connection pools to avoid connection leakage.
        // However, this might accidentally affect two Paimon Action jobs running in one single mini
        // cluster. We overwrite this class to get the same behaviors in CDC 3.1.1.
    }
}
