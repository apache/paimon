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

package org.apache.paimon.flink.procedure;

import org.apache.paimon.flink.CatalogITCaseBase;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.rest.RESTCatalogInternalOptions;
import org.apache.paimon.rest.RESTCatalogOptions;
import org.apache.paimon.rest.RESTCatalogServer;
import org.apache.paimon.rest.RESTFileIOTestLoader;
import org.apache.paimon.rest.RESTTestFileIO;
import org.apache.paimon.rest.RESTTokenFileIO;
import org.apache.paimon.rest.auth.AuthProvider;
import org.apache.paimon.rest.auth.AuthProviderEnum;
import org.apache.paimon.rest.auth.BearTokenAuthProvider;
import org.apache.paimon.rest.responses.ConfigResponse;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** IT Case for {@link AlterViewDialectProcedure}. */
public class AlterViewDialectITCase extends CatalogITCaseBase {
    private static final String DATABASE_NAME = "mydb";
    private static final String TABLE_NAME = "t1";

    private RESTCatalogServer restCatalogServer;
    private String serverUrl;
    private String dataPath;
    private String warehouse;
    @TempDir java.nio.file.Path tempFile;

    @BeforeEach
    @Override
    public void before() throws IOException {
        String initToken = "init_token";
        dataPath = tempFile.toUri().toString();
        warehouse = UUID.randomUUID().toString();
        ConfigResponse config =
                new ConfigResponse(
                        ImmutableMap.of(
                                RESTCatalogInternalOptions.PREFIX.key(),
                                "paimon",
                                RESTTokenFileIO.DATA_TOKEN_ENABLED.key(),
                                "true",
                                CatalogOptions.WAREHOUSE.key(),
                                warehouse),
                        ImmutableMap.of());
        AuthProvider authProvider = new BearTokenAuthProvider(initToken);
        restCatalogServer = new RESTCatalogServer(dataPath, authProvider, config, warehouse);
        restCatalogServer.start();
        serverUrl = restCatalogServer.getUrl();
        super.before();
        sql(String.format("CREATE DATABASE %s", DATABASE_NAME));
        sql(String.format("CREATE TABLE %s.%s (a STRING, b DOUBLE)", DATABASE_NAME, TABLE_NAME));
    }

    @AfterEach()
    public void after() throws IOException {
        sql(String.format("DROP TABLE  %s.%s", DATABASE_NAME, TABLE_NAME));
        sql(String.format("DROP DATABASE %s", DATABASE_NAME));
        restCatalogServer.shutdown();
    }

    @Test
    public void testAlterView() throws Exception {

        sql(
                String.format(
                        "INSERT INTO %s.%s VALUES ('1', 11), ('2', 22)",
                        DATABASE_NAME, TABLE_NAME));
        String viewName = "view_test";
        String query =
                String.format("SELECT * FROM `%s`.`%s` WHERE `b` > 1", DATABASE_NAME, TABLE_NAME);
        sql(String.format("CREATE VIEW %s.%s AS %s", DATABASE_NAME, viewName, query));
        String newQuery =
                String.format("SELECT * FROM `%s`.`%s` WHERE `b` > 2", DATABASE_NAME, TABLE_NAME);
        sql(
                String.format(
                        "CALL sys.alter_view_dialect(`view` => '%s.%s', `action` => 'update', `query` => '%s')",
                        DATABASE_NAME, viewName, newQuery));
        List<Row> result = sql(String.format("SHOW CREATE VIEW %s.%s", DATABASE_NAME, viewName));
        assertThat(result.toString()).contains(newQuery);
        sql(
                String.format(
                        "CALL sys.alter_view_dialect(`view` => '%s.%s', `action` => 'drop')",
                        DATABASE_NAME, viewName));
        result = sql(String.format("SHOW CREATE VIEW %s.%s", DATABASE_NAME, viewName));
        assertThat(result.toString()).contains("`b` > 1");
        sql(
                String.format(
                        "CALL sys.alter_view_dialect(`view` => '%s.%s', `action` => 'add', `query` => '%s')",
                        DATABASE_NAME, viewName, newQuery));
        result = sql(String.format("SHOW CREATE VIEW %s.%s", DATABASE_NAME, viewName));
        assertThat(result.toString()).contains(newQuery);
    }

    @Override
    protected Map<String, String> catalogOptions() {
        String initToken = "init_token";
        Map<String, String> options = new HashMap<>();
        options.put("metastore", "rest");
        options.put(CatalogOptions.WAREHOUSE.key(), warehouse);
        options.put(RESTCatalogOptions.URI.key(), serverUrl);
        options.put(RESTCatalogOptions.TOKEN.key(), initToken);
        options.put(RESTCatalogOptions.TOKEN_PROVIDER.key(), AuthProviderEnum.BEAR.identifier());
        options.put(RESTTokenFileIO.DATA_TOKEN_ENABLED.key(), "true");
        options.put(
                RESTTestFileIO.DATA_PATH_CONF_KEY,
                dataPath.replaceFirst("file", RESTFileIOTestLoader.SCHEME));
        return options;
    }

    @Override
    protected String getTempDirPath() {
        return this.dataPath;
    }

    @Override
    protected boolean supportDefineWarehouse() {
        return false;
    }
}
