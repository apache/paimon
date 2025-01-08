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

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.MockRESTCatalogServer;
import org.apache.paimon.rest.RESTCatalogFactory;
import org.apache.paimon.rest.RESTCatalogOptions;
import org.apache.paimon.rest.exceptions.NotAuthorizedException;

import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.paimon.flink.FlinkCatalogOptions.LOG_SYSTEM_AUTO_REGISTER;
import static org.apache.paimon.flink.FlinkCatalogTestUtil.createTable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

/** Test for {@link FlinkCatalog} when catalog type is RESTCatalog. */
public class FlinkRESTCatalogTest {
    private final ObjectPath path1 = new ObjectPath("db1", "t1");
    private final ObjectPath nonExistDbPath = ObjectPath.fromString("non.exist");
    private MockRESTCatalogServer mockRESTCatalogServer;
    private String serverUrl;
    private String warehouse;
    private Catalog catalog;

    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void beforeEach() throws IOException {
        warehouse = new File(temporaryFolder.newFolder(), UUID.randomUUID().toString()).toString();
        String initToken = "init_token";
        mockRESTCatalogServer = new MockRESTCatalogServer(warehouse, initToken);
        mockRESTCatalogServer.start();
        serverUrl = mockRESTCatalogServer.getUrl();
        Options options = new Options();
        options.set(RESTCatalogOptions.URI, serverUrl);
        options.set(RESTCatalogOptions.TOKEN, initToken);
        options.set(RESTCatalogOptions.THREAD_POOL_SIZE, 1);
        options.set(LOG_SYSTEM_AUTO_REGISTER, true);
        options.set(CatalogOptions.METASTORE, RESTCatalogFactory.IDENTIFIER);
        catalog =
                FlinkCatalogFactory.createCatalog(
                        "test-catalog",
                        CatalogContext.create(options),
                        FlinkCatalogTest.class.getClassLoader());
    }

    @After
    public void tearDown() throws IOException {
        mockRESTCatalogServer.shutdown();
    }

    @Test
    public void testAuthFail() {
        Options options = new Options();
        options.set(RESTCatalogOptions.URI, serverUrl);
        options.set(RESTCatalogOptions.TOKEN, "aaaaa");
        options.set(RESTCatalogOptions.THREAD_POOL_SIZE, 1);
        options.set(LOG_SYSTEM_AUTO_REGISTER, true);
        options.set(CatalogOptions.METASTORE, RESTCatalogFactory.IDENTIFIER);
        assertThatThrownBy(
                        () ->
                                FlinkCatalogFactory.createCatalog(
                                        "test-catalog",
                                        CatalogContext.create(options),
                                        FlinkCatalogTest.class.getClassLoader()))
                .isInstanceOf(NotAuthorizedException.class);
    }

    @Test
    public void testListDatabases() {
        List<String> result = catalog.listDatabases();
        assertEquals(1, result.size());
    }

    @Test
    public void testGetDatabase() throws Exception {
        catalog.createDatabase(path1.getDatabaseName(), null, false);
        CatalogDatabase database = catalog.getDatabase(path1.getDatabaseName());
        assertThat(database.getProperties()).isEmpty();
        assertThat(database.getDescription()).isEmpty();
        assertThatThrownBy(() -> catalog.getDatabase(nonExistDbPath.getDatabaseName()))
                .isInstanceOf(DatabaseNotExistException.class)
                .hasMessageContaining("Database non does not exist in Catalog test-catalog.");
    }

    @Test
    public void testCreateTable_Streaming() throws Exception {
        catalog.createDatabase(path1.getDatabaseName(), null, false);
        Map<String, String> options = new HashMap<>();
        options.put("is_streaming", String.valueOf(true));
        CatalogTable table = createTable(options);
        catalog.createTable(path1, table, false);
        CatalogTable tableFromServer = (CatalogTable) catalog.getTable(path1);
        checkOptions(options, tableFromServer.getOptions());
        assertEquals(tableFromServer.getTableKind(), table.getTableKind());
        assertEquals(tableFromServer.getUnresolvedSchema(), table.getUnresolvedSchema());
    }

    private void checkOptions(Map<String, String> expected, Map<String, String> actual) {
        List<String> ignoreKeys = ImmutableList.of(FlinkCatalogOptions.REGISTER_TIMEOUT.key());
        for (Map.Entry<String, String> entry : expected.entrySet()) {
            String key = entry.getKey();
            if (!ignoreKeys.contains(key)) {
                assertEquals(actual.get(key), actual.get(key));
            }
        }
    }
}
