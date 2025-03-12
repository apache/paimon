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

package org.apache.paimon.flink;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.rest.RESTCatalogInternalOptions;
import org.apache.paimon.rest.RESTCatalogOptions;
import org.apache.paimon.rest.RESTCatalogServer;
import org.apache.paimon.rest.RESTFileIOTestLoader;
import org.apache.paimon.rest.RESTTestFileIO;
import org.apache.paimon.rest.RESTToken;
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
import static org.junit.jupiter.api.Assertions.assertThrows;

/** ITCase for REST catalog. */
class RESTCatalogITCase extends CatalogITCaseBase {

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
    void testCreateTable() {
        List<Row> result = sql(String.format("SHOW CREATE TABLE %s.%s", DATABASE_NAME, TABLE_NAME));
        assertThat(result.toString())
                .contains(
                        String.format(
                                "CREATE TABLE `PAIMON`.`%s`.`%s` (\n"
                                        + "  `a` VARCHAR(2147483647),\n"
                                        + "  `b` DOUBLE",
                                DATABASE_NAME, TABLE_NAME));
    }

    @Test
    void testAlterTable() {
        sql(String.format("ALTER TABLE %s.%s ADD e INT AFTER b", DATABASE_NAME, TABLE_NAME));
        sql(String.format("ALTER TABLE %s.%s DROP b", DATABASE_NAME, TABLE_NAME));
        sql(String.format("ALTER TABLE %s.%s RENAME a TO a1", DATABASE_NAME, TABLE_NAME));
        sql(String.format("ALTER TABLE %s.%s MODIFY e DOUBLE", DATABASE_NAME, TABLE_NAME));
        List<Row> result = sql(String.format("SHOW CREATE TABLE %s.%s", DATABASE_NAME, TABLE_NAME));
        assertThat(result.toString())
                .contains(
                        String.format(
                                "CREATE TABLE `PAIMON`.`%s`.`%s` (\n"
                                        + "  `a1` VARCHAR(2147483647),\n"
                                        + "  `e` DOUBLE",
                                DATABASE_NAME, TABLE_NAME));
    }

    @Test
    public void testWriteAndRead() {
        batchSql(
                String.format(
                        "INSERT INTO %s.%s VALUES ('1', 11), ('2', 22)",
                        DATABASE_NAME, TABLE_NAME));
        assertThat(batchSql(String.format("SELECT * FROM %s.%s", DATABASE_NAME, TABLE_NAME)))
                .containsExactlyInAnyOrder(Row.of("1", 11.0D), Row.of("2", 22.0D));
    }

    @Test
    public void testExpiredDataToken() {
        Identifier identifier = Identifier.create(DATABASE_NAME, TABLE_NAME);
        RESTToken expiredDataToken =
                new RESTToken(
                        ImmutableMap.of(
                                "akId", "akId-expire", "akSecret", UUID.randomUUID().toString()),
                        System.currentTimeMillis() - 100_000);
        restCatalogServer.setDataToken(identifier, expiredDataToken);
        assertThrows(
                RuntimeException.class,
                () ->
                        batchSql(
                                String.format(
                                        "INSERT INTO %s.%s VALUES ('1', 11), ('2', 22)",
                                        DATABASE_NAME, TABLE_NAME)));
        // update token and retry
        RESTToken dataToken =
                new RESTToken(
                        ImmutableMap.of("akId", "akId", "akSecret", UUID.randomUUID().toString()),
                        System.currentTimeMillis() + 100_000);
        restCatalogServer.setDataToken(identifier, dataToken);
        batchSql(
                String.format(
                        "INSERT INTO %s.%s VALUES ('1', 11), ('2', 22)",
                        DATABASE_NAME, TABLE_NAME));
        assertThat(batchSql(String.format("SELECT * FROM %s.%s", DATABASE_NAME, TABLE_NAME)))
                .containsExactlyInAnyOrder(Row.of("1", 11.0D), Row.of("2", 22.0D));
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
