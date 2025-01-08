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

import org.apache.paimon.rest.MockRESTCatalogServer;
import org.apache.paimon.rest.RESTCatalogOptions;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for REST catalog. */
public class RESTCatalogITCase extends CatalogITCaseBase {

    MockRESTCatalogServer mockRESTCatalogServer;
    private String serverUrl;
    protected String warehouse;
    @TempDir java.nio.file.Path tempFile;

    @BeforeEach
    public void before() throws IOException {
        String initToken = "init_token";
        warehouse = tempFile.toUri().toString();
        mockRESTCatalogServer = new MockRESTCatalogServer(warehouse, initToken);
        mockRESTCatalogServer.start();
        serverUrl = mockRESTCatalogServer.getUrl();
        super.before();
    }

    @AfterEach()
    public void after() throws IOException {
        mockRESTCatalogServer.shutdown();
    }

    @Test
    public void testCreateTable() {
        sql("CREATE DATABASE mydb");
        sql("CREATE TABLE mydb.T1 (a INT, b INT)");
        String result = sql("DESCRIBE mydb.T1").toString();
        sql("DROP TABLE mydb.T1");
        sql("DROP DATABASE mydb");
        assertThat(result)
                .isEqualTo(
                        "[+I[a, INT, true, null, null, null], +I[b, INT, true, null, null, null]]");
    }

    @Override
    protected Map<String, String> catalogOptions() {
        String initToken = "init_token";
        Map<String, String> options = new HashMap<>();
        options.put("metastore", "rest");
        options.put(RESTCatalogOptions.URI.key(), serverUrl);
        options.put(RESTCatalogOptions.TOKEN.key(), initToken);
        options.put(RESTCatalogOptions.THREAD_POOL_SIZE.key(), "" + 1);
        return options;
    }

    @Override
    protected String getTempDirPath() {
        return this.warehouse;
    }

    @Override
    protected boolean supportDefineWarehouse() {
        return false;
    }
}
