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

import org.apache.paimon.client.ClientPool;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Client pool for jdbc. */
public class JdbcClientPool
        extends ClientPool.ClientPoolImpl<Connection, DruidDataSource, SQLException> {

    private static final Pattern PROTOCOL_PATTERN = Pattern.compile("jdbc:([^:]+):(.*)");

    private final String protocol;

    private volatile DruidDataSource dataSource;

    public JdbcClientPool(int poolSize, String dbUrl, Map<String, String> props) {
        super(initPoolSupplier(poolSize, dbUrl, props));
        Matcher matcher = PROTOCOL_PATTERN.matcher(dbUrl);
        if (matcher.matches()) {
            this.protocol = matcher.group(1);
        } else {
            throw new RuntimeException("Invalid Jdbc url: " + dbUrl);
        }
    }

    public String getProtocol() {
        return protocol;
    }

    private static Supplier<DruidDataSource> initPoolSupplier(
            int poolSize, String dbUrl, Map<String, String> props) {
        return () -> {
            try {
                Properties dbProps =
                        JdbcUtils.extractJdbcConfiguration(props, JdbcCatalog.PROPERTY_PREFIX);
                dbProps.setProperty(DruidDataSourceFactory.PROP_URL, dbUrl);
                dbProps.setProperty(
                        DruidDataSourceFactory.PROP_MAXACTIVE, String.valueOf(poolSize));
                return (DruidDataSource) DruidDataSourceFactory.createDataSource(dbProps);
            } catch (Exception e) {
                throw new RuntimeException("Failed to create datasource", e);
            }
        };
    }

    @Override
    protected void initPool(Supplier<DruidDataSource> supplier) {
        dataSource = supplier.get();
    }

    @Override
    protected Connection getClient(long timeout, TimeUnit unit) throws SQLException {
        if (this.dataSource == null) {
            throw new IllegalStateException("Cannot get a client from a closed pool");
        }
        return dataSource.getConnection(unit.toMillis(timeout));
    }

    @Override
    protected void recycleClient(Connection client) throws SQLException {
        client.close();
    }

    @Override
    protected void closePool() {
        DruidDataSource dataSource = this.dataSource;
        this.dataSource = null;
        if (dataSource != null) {
            dataSource.close();
        }
    }
}
