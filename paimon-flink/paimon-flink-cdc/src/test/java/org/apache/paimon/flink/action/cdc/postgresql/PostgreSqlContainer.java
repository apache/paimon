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

package org.apache.paimon.flink.action.cdc.postgresql;

import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.HashSet;
import java.util.Set;

/** PostgreSQL Container. */
public class PostgreSqlContainer extends PostgreSQLContainer {

    public static final String IMAGE_NAME = "postgres";
    public static final Integer POSTGRESQL_PORT = 5432;
    private static final String SETUP_SQL_PARAM_NAME = "SETUP_SQL";

    private static final String POSTGRESQL_CONF = "postgresql.conf";

    private String databaseName = "test_db";
    private String username = "postgres";
    private String password = "postgres";

    public PostgreSqlContainer(PostgreSqlVersion postgreSqlVersion) {
        super(DockerImageName.parse(IMAGE_NAME + ":" + postgreSqlVersion.getVersion()));
        addExposedPort(POSTGRESQL_PORT);
    }

    @Override
    protected Set<Integer> getLivenessCheckPorts() {
        return new HashSet<>(getMappedPort(POSTGRESQL_PORT));
    }

    @Override
    protected void configure() {
        // Disable Postgres driver use of java.util.logging to reduce noise at startup time
        withUrlParam("loggerLevel", "OFF");
        addEnv("POSTGRES_DB", databaseName);
        addEnv("POSTGRES_USER", username);
        addEnv("POSTGRES_PASSWORD", password);
    }

    @Override
    public String getDriverClassName() {
        return "org.postgresql.Driver";
    }

    @Override
    public String getJdbcUrl() {
        String additionalUrlParams = constructUrlParameters("?", "&");
        return ("jdbc:postgresql://"
                + getHost()
                + ":"
                + getMappedPort(POSTGRESQL_PORT)
                + "/"
                + databaseName
                + additionalUrlParams);
    }

    public int getDatabasePort() {
        return getMappedPort(POSTGRESQL_PORT);
    }

    @Override
    public String getDatabaseName() {
        return databaseName;
    }

    @Override
    public String getUsername() {
        return username;
    }

    @Override
    public String getPassword() {
        return password;
    }

    public PostgreSqlContainer withPostgresConf(String confPath) {
        parameters.put(POSTGRESQL_CONF, confPath);
        return this;
    }

    @Override
    public PostgreSqlContainer withDatabaseName(final String databaseName) {
        this.databaseName = databaseName;
        return this;
    }

    @Override
    public PostgreSqlContainer withUsername(final String username) {
        this.username = username;
        return this;
    }

    @Override
    public PostgreSqlContainer withPassword(final String password) {
        this.password = password;
        return this;
    }

    @Override
    protected void waitUntilContainerStarted() {
        getWaitStrategy().waitUntilReady(this);
    }
}
