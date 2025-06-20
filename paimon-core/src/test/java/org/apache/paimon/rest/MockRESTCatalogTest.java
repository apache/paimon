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

package org.apache.paimon.rest;

import org.apache.paimon.PagedList;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.auth.AuthProvider;
import org.apache.paimon.rest.auth.AuthProviderEnum;
import org.apache.paimon.rest.auth.BearTokenAuthProvider;
import org.apache.paimon.rest.auth.DLFAuthProvider;
import org.apache.paimon.rest.auth.DLFTokenLoader;
import org.apache.paimon.rest.auth.DLFTokenLoaderFactory;
import org.apache.paimon.rest.auth.RESTAuthParameter;
import org.apache.paimon.rest.exceptions.NotAuthorizedException;
import org.apache.paimon.rest.responses.ConfigResponse;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Test REST Catalog on Mocked REST server. */
class MockRESTCatalogTest extends RESTCatalogTest {

    private RESTCatalogServer restCatalogServer;
    private final String serverDefineHeaderName = "test-header";
    private final String serverDefineHeaderValue = "test-value";
    private String dataPath;
    private AuthProvider authProvider;
    private Map<String, String> authMap;

    @BeforeEach
    @Override
    public void setUp() throws Exception {
        super.setUp();
        dataPath = warehouse;
        String initToken = "init_token";
        this.authProvider = new BearTokenAuthProvider(initToken);
        this.authMap =
                ImmutableMap.of(
                        RESTCatalogOptions.TOKEN.key(),
                        initToken,
                        RESTCatalogOptions.TOKEN_PROVIDER.key(),
                        AuthProviderEnum.BEAR.identifier());
        this.restCatalog = initCatalog(false);
        this.catalog = restCatalog;

        // test retry commit
        RESTCatalogServer.commitSuccessThrowException = true;
    }

    @AfterEach
    public void tearDown() throws Exception {
        restCatalogServer.shutdown();
    }

    @Test
    void testAuthFail() {
        Options options = new Options();
        options.set(RESTCatalogOptions.URI, restCatalogServer.getUrl());
        options.set(RESTCatalogOptions.TOKEN, "aaaaa");
        options.set(RESTCatalogOptions.TOKEN_PROVIDER, AuthProviderEnum.BEAR.identifier());
        options.set(CatalogOptions.METASTORE, RESTCatalogFactory.IDENTIFIER);
        assertThatThrownBy(() -> new RESTCatalog(CatalogContext.create(options)))
                .isInstanceOf(NotAuthorizedException.class);
    }

    @Test
    void testDlfStSTokenAuth() throws Exception {
        String akId = "akId" + UUID.randomUUID();
        String akSecret = "akSecret" + UUID.randomUUID();
        String securityToken = "securityToken" + UUID.randomUUID();
        String region = "cn-hangzhou";
        this.authProvider = DLFAuthProvider.fromAccessKey(akId, akSecret, securityToken, region);
        this.authMap =
                ImmutableMap.of(
                        RESTCatalogOptions.TOKEN_PROVIDER.key(), AuthProviderEnum.DLF.identifier(),
                        RESTCatalogOptions.DLF_REGION.key(), region,
                        RESTCatalogOptions.DLF_ACCESS_KEY_ID.key(), akId,
                        RESTCatalogOptions.DLF_ACCESS_KEY_SECRET.key(), akSecret,
                        RESTCatalogOptions.DLF_SECURITY_TOKEN.key(), securityToken);
        RESTCatalog restCatalog = initCatalog(false);
        testDlfAuth(restCatalog);
    }

    @Test
    void testDlfStSTokenPathAuth() throws Exception {
        String region = "cn-hangzhou";
        String tokenPath = dataPath + UUID.randomUUID();
        generateTokenAndWriteToFile(tokenPath);
        DLFTokenLoader tokenLoader =
                DLFTokenLoaderFactory.createDLFTokenLoader(
                        "local_file",
                        new Options(
                                ImmutableMap.of(
                                        RESTCatalogOptions.DLF_TOKEN_PATH.key(), tokenPath)));
        this.authProvider = DLFAuthProvider.fromTokenLoader(tokenLoader, region);
        this.authMap =
                ImmutableMap.of(
                        RESTCatalogOptions.TOKEN_PROVIDER.key(), AuthProviderEnum.DLF.identifier(),
                        RESTCatalogOptions.DLF_REGION.key(), region,
                        RESTCatalogOptions.DLF_TOKEN_PATH.key(), tokenPath);
        RESTCatalog restCatalog = initCatalog(false);
        testDlfAuth(restCatalog);
        File file = new File(tokenPath);
        file.delete();
    }

    @Test
    void testHeader() {
        Map<String, String> parameters = new HashMap<>();
        parameters.put("k1", "v1");
        parameters.put("k2", "v2");
        RESTAuthParameter restAuthParameter =
                new RESTAuthParameter("/path", parameters, "method", "data");
        Map<String, String> headers = restCatalog.api().authFunction().apply(restAuthParameter);
        assertEquals(
                headers.get(BearTokenAuthProvider.AUTHORIZATION_HEADER_KEY), "Bearer init_token");
        assertEquals(headers.get(serverDefineHeaderName), serverDefineHeaderValue);
    }

    private void testDlfAuth(RESTCatalog restCatalog) throws Exception {
        String databaseName = "db1";
        restCatalog.createDatabase(databaseName, true);
        String[] tableNames = {"dt=20230101", "dt=20230102", "dt=20230103"};
        for (String tableName : tableNames) {
            restCatalog.createTable(
                    Identifier.create(databaseName, tableName), DEFAULT_TABLE_SCHEMA, false);
        }
        PagedList<String> listTablesPaged =
                restCatalog.listTablesPaged(databaseName, 1, "dt=20230101", null);
        PagedList<String> listTablesPaged2 =
                restCatalog.listTablesPaged(
                        databaseName, 1, listTablesPaged.getNextPageToken(), null);
        assertEquals(listTablesPaged.getElements().get(0), "dt=20230102");
        assertEquals(listTablesPaged2.getElements().get(0), "dt=20230103");
    }

    @Override
    protected Catalog newRestCatalogWithDataToken() throws IOException {
        return initCatalog(true);
    }

    @Override
    protected void revokeTablePermission(Identifier identifier) {
        restCatalogServer.addNoPermissionTable(identifier);
    }

    @Override
    protected void authTableColumns(Identifier identifier, List<String> columns) {
        restCatalogServer.addTableColumnAuth(identifier, columns);
    }

    @Override
    protected void revokeDatabasePermission(String database) {
        restCatalogServer.addNoPermissionDatabase(database);
    }

    @Override
    protected RESTToken getDataTokenFromRestServer(Identifier identifier) {
        return restCatalogServer.getDataToken(identifier);
    }

    @Override
    protected void setDataTokenToRestServerForMock(
            Identifier identifier, RESTToken expiredDataToken) {
        restCatalogServer.setDataToken(identifier, expiredDataToken);
    }

    @Override
    protected void resetDataTokenOnRestServer(Identifier identifier) {
        restCatalogServer.removeDataToken(identifier);
    }

    @Override
    protected void updateSnapshotOnRestServer(
            Identifier identifier,
            Snapshot snapshot,
            long recordCount,
            long fileSizeInBytes,
            long fileCount,
            long lastFileCreationTime) {
        restCatalogServer.setTableSnapshot(
                identifier,
                snapshot,
                recordCount,
                fileSizeInBytes,
                fileCount,
                lastFileCreationTime);
    }

    private RESTCatalog initCatalog(boolean enableDataToken) throws IOException {
        String restWarehouse = UUID.randomUUID().toString();
        this.config =
                new ConfigResponse(
                        ImmutableMap.of(
                                RESTCatalogInternalOptions.PREFIX.key(),
                                "paimon",
                                "header." + serverDefineHeaderName,
                                serverDefineHeaderValue,
                                RESTTokenFileIO.DATA_TOKEN_ENABLED.key(),
                                enableDataToken + "",
                                CatalogOptions.WAREHOUSE.key(),
                                restWarehouse),
                        ImmutableMap.of());
        restCatalogServer =
                new RESTCatalogServer(dataPath, this.authProvider, this.config, restWarehouse);
        restCatalogServer.start();
        for (Map.Entry<String, String> entry : this.authMap.entrySet()) {
            options.set(entry.getKey(), entry.getValue());
        }
        options.set(CatalogOptions.WAREHOUSE.key(), restWarehouse);
        options.set(RESTCatalogOptions.URI, restCatalogServer.getUrl());
        String path =
                enableDataToken
                        ? dataPath.replaceFirst("file", RESTFileIOTestLoader.SCHEME)
                        : dataPath;
        options.set(RESTTestFileIO.DATA_PATH_CONF_KEY, path);
        return new RESTCatalog(CatalogContext.create(options));
    }
}
