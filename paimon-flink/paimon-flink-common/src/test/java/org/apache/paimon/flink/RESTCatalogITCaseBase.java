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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/** Base ITCase for REST catalog. */
public abstract class RESTCatalogITCaseBase extends CatalogITCaseBase {

    protected static final String DATABASE_NAME = "mydb";
    protected static final String TABLE_NAME = "t1";
    protected static final String INIT_TOKEN = "init_token";

    protected RESTCatalogServer restCatalogServer;

    private String serverUrl;
    protected String dataPath;
    protected String warehouse;
    @TempDir java.nio.file.Path tempFile;

    @BeforeEach
    @Override
    public void before() throws IOException {
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
        AuthProvider authProvider = new BearTokenAuthProvider(INIT_TOKEN);
        restCatalogServer = new RESTCatalogServer(dataPath, authProvider, config, warehouse);
        restCatalogServer.start();
        serverUrl = restCatalogServer.getUrl();
        super.before();
        sql(String.format("CREATE DATABASE %s", DATABASE_NAME));
        sql(String.format("CREATE TABLE %s.%s (a STRING, b DOUBLE)", DATABASE_NAME, TABLE_NAME));
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
