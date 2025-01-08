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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.Assert.assertThrows;

/** RESTCatalog test with mock server. */
public class RESTCatalogMockServerTest {
    MockRESTCatalogServer mockRESTCatalogServer;
    private String serverUrl;
    protected String warehouse;
    protected Catalog catalog;

    @TempDir java.nio.file.Path tempFile;

    @BeforeEach
    public void setUp() throws Exception {
        warehouse = tempFile.toUri().toString();
        String initToken = "init_token";
        mockRESTCatalogServer = new MockRESTCatalogServer(warehouse, initToken);
        mockRESTCatalogServer.start();
        serverUrl = mockRESTCatalogServer.getUrl();
        Options options = new Options();
        options.set(RESTCatalogOptions.URI, serverUrl);
        options.set(RESTCatalogOptions.TOKEN, initToken);
        options.set(RESTCatalogOptions.THREAD_POOL_SIZE, 1);
        this.catalog = new RESTCatalog(CatalogContext.create(options));
    }

    @AfterEach
    public void tearDown() throws Exception {
        mockRESTCatalogServer.shutdown();
    }

    @Test
    public void testInitFailWhenDefineWarehouse() {
        Options options = new Options();
        options.set(CatalogOptions.WAREHOUSE, warehouse);
        assertThrows(
                IllegalArgumentException.class,
                () -> new RESTCatalog(CatalogContext.create(options)));
    }
}
