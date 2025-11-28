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

package org.apache.paimon.vfs.hadoop;

import org.apache.paimon.catalog.CatalogHadoopContext;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.rest.RESTCatalog;
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

/** Test for {@link PaimonVirtualFileSystem} with Mock Rest Server. */
public class MockRestVirtualFileSystemTest extends VirtualFileSystemTest {
    protected RESTCatalogServer restCatalogServer;
    private final String serverDefineHeaderName = "test-header";
    private final String serverDefineHeaderValue = "test-value";
    protected String dataPath;
    protected AuthProvider authProvider;
    protected Map<String, String> authMap;
    protected String initToken = "init_token";
    protected String restWarehouse;

    @BeforeEach
    @Override
    public void setUp() throws Exception {
        super.setUp();
        dataPath = warehouse;
        this.authProvider = new BearTokenAuthProvider(initToken);
        this.authMap =
                ImmutableMap.of(
                        RESTCatalogOptions.TOKEN.key(),
                        initToken,
                        RESTCatalogOptions.TOKEN_PROVIDER.key(),
                        AuthProviderEnum.BEAR.identifier());
        this.restWarehouse = UUID.randomUUID().toString();
        this.catalog = initCatalog(false);

        // test retry commit
        RESTCatalogServer.commitSuccessThrowException = true;

        initFs();
    }

    @AfterEach
    public void tearDown() throws Exception {
        restCatalogServer.shutdown();
    }

    private RESTCatalog initCatalog(boolean enableDataToken) throws IOException {
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
        return new RESTCatalog(CatalogHadoopContext.create(options));
    }

    protected void initFs() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.pvfs.uri", restCatalogServer.getUrl());
        conf.set("fs.pvfs.token.provider", AuthProviderEnum.BEAR.identifier());
        conf.set("fs.pvfs.token", initToken);
        this.vfs = new PaimonVirtualFileSystem();
        this.vfsRoot = new Path("pvfs://" + restWarehouse + "/");
        this.vfs.initialize(vfsRoot.toUri(), conf);

        Assert.assertTrue(((PaimonVirtualFileSystem) vfs).isCacheEnabled());
    }
}
