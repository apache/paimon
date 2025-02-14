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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.Maps;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link JdbcClientPool} */
public class JdbcClientPoolTest {
    public JdbcClientPool connections;

    @BeforeEach
    public void setUp() {
        connections = initConnections(Maps.newHashMap());
    }

    private JdbcClientPool initConnections(HashMap<String, String> props) {
        props.put("jdbc.autoReconnect", "true");
        props.put("jdbc.testOnBorrow", "true");
        props.put("jdbc.validationQuery", "SELECT 1");

        String testUrl = "jdbc:mysql://127.0.0.1:33306/mysql?user=root&password=123456";
        return new JdbcClientPool(10, testUrl, props);
    }

    @Test
    public void testGetClient() throws SQLException, InterruptedException {
        Connection client = connections.getClient(10, TimeUnit.SECONDS);
        String lockId = "jdbc.testDb.testTable";
        assertThat(JdbcUtils.acquire(connections, lockId, 1000)).isTrue();
    }
}
