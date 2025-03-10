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

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.auth.AuthProviderEnum;
import org.apache.paimon.rest.auth.BearTokenAuthProvider;
import org.apache.paimon.rest.auth.RESTAuthParameter;
import org.apache.paimon.rest.exceptions.NotAuthorizedException;
import org.apache.paimon.rest.responses.ConfigResponse;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Test for REST Catalog on Mocked REST server. */
public class RESTCatalogTest extends RESTCatalogTestBase {

    private RESTCatalogServer restCatalogServer;
    protected String initToken = "init_token";
    protected String dataPath;

    protected String serverDefineHeaderName = "test-header";
    protected String serverDefineHeaderValue = "test-value";

    @BeforeEach
    @Override
    public void setUp() throws Exception {
        super.setUp();
        dataPath = warehouse;
        String restWarehouse = UUID.randomUUID().toString();
        this.config =
                new ConfigResponse(
                        ImmutableMap.of(
                                RESTCatalogInternalOptions.PREFIX.key(),
                                "paimon",
                                "header." + serverDefineHeaderName,
                                serverDefineHeaderValue,
                                CatalogOptions.WAREHOUSE.key(),
                                restWarehouse),
                        ImmutableMap.of());
        restCatalogServer = new RESTCatalogServer(dataPath, initToken, this.config, restWarehouse);
        restCatalogServer.start();
        options.set(CatalogOptions.WAREHOUSE.key(), restWarehouse);
        options.set(RESTCatalogOptions.URI, restCatalogServer.getUrl());
        options.set(RESTCatalogOptions.TOKEN, initToken);
        options.set(RESTCatalogOptions.TOKEN_PROVIDER, AuthProviderEnum.BEAR.identifier());
        this.restCatalog = new RESTCatalog(CatalogContext.create(options));
        this.catalog = restCatalog;
    }

    @AfterEach
    public void tearDown() throws Exception {
        restCatalogServer.shutdown();
    }

    @Test
    void testAuthFail() {
        Options options = new Options();
        options.set(RESTCatalogOptions.URI, getRestCatalogURL());
        options.set(RESTCatalogOptions.TOKEN, "aaaaa");
        options.set(RESTCatalogOptions.TOKEN_PROVIDER, AuthProviderEnum.BEAR.identifier());
        options.set(CatalogOptions.METASTORE, RESTCatalogFactory.IDENTIFIER);
        assertThatThrownBy(() -> new RESTCatalog(CatalogContext.create(options)))
                .isInstanceOf(NotAuthorizedException.class);
    }

    @Test
    void testHeader() {
        Map<String, String> parameters = new HashMap<>();
        parameters.put("k1", "v1");
        parameters.put("k2", "v2");
        RESTAuthParameter restAuthParameter =
                new RESTAuthParameter("host", "/path", parameters, "method", "data");
        Map<String, String> headers = restCatalog.headers(restAuthParameter);
        assertEquals(
                headers.get(BearTokenAuthProvider.AUTHORIZATION_HEADER_KEY), "Bearer init_token");
        assertEquals(headers.get(serverDefineHeaderName), serverDefineHeaderValue);
    }

    protected String getRestCatalogURL() {
        return restCatalogServer.getUrl();
    }

    protected void setupNoPermissionTable(Identifier identifier) {
        restCatalogServer.addNoPermissionTable(identifier);
    }

    protected void setupNoPermissionDatabase(String database) {
        restCatalogServer.addNoPermissionDatabase(database);
    }

    protected RESTToken getDataTokenFromRestServer(Identifier identifier) {
        return restCatalogServer.getDataToken(identifier);
    }

    protected void setDataTokenToRestServerForMock(
            Identifier identifier, RESTToken expiredDataToken) {
        restCatalogServer.setDataToken(identifier, expiredDataToken);
    }

    protected void cleanDataTokenOnRestServer(Identifier identifier) {
        restCatalogServer.removeDataToken(identifier);
    }

    protected void mockSnapshotOnRestServer(Identifier identifier, Snapshot snapshot) {
        restCatalogServer.setTableSnapshot(identifier, snapshot);
    }

    protected Catalog initCatalogWithDataToken() {
        options.set(RESTCatalogOptions.DATA_TOKEN_ENABLED, true);
        options.set(
                RESTTestFileIO.DATA_PATH_CONF_KEY,
                dataPath.replaceFirst("file", RESTFileIOTestLoader.SCHEME));
        return new RESTCatalog(CatalogContext.create(options));
    }
}
