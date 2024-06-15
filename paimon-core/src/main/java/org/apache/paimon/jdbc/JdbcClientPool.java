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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Client pool for jdbc. */
public class JdbcClientPool extends ClientPool.ClientPoolImpl<Connection, SQLException> {

    private static final Pattern PROTOCOL_PATTERN = Pattern.compile("jdbc:([^:]+):(.*)");
    private final String dbUrl;
    private final Map<String, String> properties;
    private final String protocol;

    public JdbcClientPool(int poolSize, String dbUrl, Map<String, String> props) {
        super(poolSize, SQLNonTransientConnectionException.class, true);
        properties = props;
        this.dbUrl = dbUrl;
        Matcher matcher = PROTOCOL_PATTERN.matcher(dbUrl);
        if (matcher.matches()) {
            this.protocol = matcher.group(1);
        } else {
            throw new RuntimeException("Invalid Jdbc url: " + dbUrl);
        }
    }

    @Override
    protected Connection newClient() {
        try {
            Properties dbProps =
                    JdbcUtils.extractJdbcConfiguration(properties, JdbcCatalog.PROPERTY_PREFIX);
            return DriverManager.getConnection(dbUrl, dbProps);
        } catch (SQLException e) {
            throw new RuntimeException(String.format("Failed to connect: %s", dbUrl), e);
        }
    }

    @Override
    protected Connection reconnect(Connection client) {
        close(client);
        return newClient();
    }

    public String getProtocol() {
        return protocol;
    }

    @Override
    protected void close(Connection client) {
        try {
            client.close();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to close connection", e);
        }
    }
}
