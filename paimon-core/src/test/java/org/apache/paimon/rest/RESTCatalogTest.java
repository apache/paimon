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
import org.apache.paimon.catalog.CatalogTestBase;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.rest.auth.AuthProviderEnum;
import org.apache.paimon.rest.auth.BearTokenAuthProvider;
import org.apache.paimon.rest.auth.RESTAuthParameter;
import org.apache.paimon.rest.exceptions.NotAuthorizedException;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;
import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;
import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.apache.paimon.CoreOptions.METASTORE_PARTITIONED_TABLE;
import static org.apache.paimon.utils.SnapshotManagerTest.createSnapshotWithMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Test for REST Catalog. */
class RESTCatalogTest extends CatalogTestBase {

    private RESTCatalogServer restCatalogServer;
    private String initToken = "init_token";

    @BeforeEach
    @Override
    public void setUp() throws Exception {
        super.setUp();
        restCatalogServer = new RESTCatalogServer(warehouse, initToken);
        restCatalogServer.start();
        Options options = new Options();
        options.set(RESTCatalogOptions.URI, restCatalogServer.getUrl());
        options.set(RESTCatalogOptions.TOKEN, initToken);
        options.set(RESTCatalogOptions.TOKEN_PROVIDER, AuthProviderEnum.BEAR.identifier());
        this.catalog = new RESTCatalog(CatalogContext.create(options));
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
    void testHeader() {
        RESTCatalog restCatalog = (RESTCatalog) catalog;
        Map<String, String> parameters = new HashMap<>();
        parameters.put("k1", "v1");
        parameters.put("k2", "v2");
        RESTAuthParameter restAuthParameter =
                new RESTAuthParameter("host", "/path", parameters, "method", "data");
        Map<String, String> headers = restCatalog.headers(restAuthParameter);
        assertEquals(
                headers.get(BearTokenAuthProvider.AUTHORIZATION_HEADER_KEY), "Bearer init_token");
        assertEquals(headers.get("test-header"), "test-value");
    }

    @Test
    void testListPartitionsWhenMetastorePartitionedIsTrue() throws Exception {
        Identifier identifier = Identifier.create("test_db", "test_table");
        createTable(
                identifier,
                ImmutableMap.of(METASTORE_PARTITIONED_TABLE.key(), "" + true),
                Lists.newArrayList("col1"));
        List<Partition> result = catalog.listPartitions(identifier);
        assertEquals(0, result.size());
    }

    @Test
    void testListPartitionsFromFile() throws Exception {
        Identifier identifier = Identifier.create("test_db", "test_table");
        createTable(identifier, Maps.newHashMap(), Lists.newArrayList("col1"));
        List<Partition> result = catalog.listPartitions(identifier);
        assertEquals(0, result.size());
    }

    @Test
    void testRefreshFileIO() throws Exception {
        this.catalog = initDataTokenCatalog();
        List<Identifier> identifiers =
                Lists.newArrayList(
                        Identifier.create("test_db_a", "test_table_a"),
                        Identifier.create("test_db_b", "test_table_b"),
                        Identifier.create("test_db_c", "test_table_c"));
        for (Identifier identifier : identifiers) {
            createTable(identifier, Maps.newHashMap(), Lists.newArrayList("col1"));
            FileStoreTable fileStoreTable = (FileStoreTable) catalog.getTable(identifier);
            assertEquals(true, fileStoreTable.fileIO().exists(fileStoreTable.location()));

            RESTTokenFileIO fileIO = (RESTTokenFileIO) fileStoreTable.fileIO();
            RESTToken fileDataToken = fileIO.validToken();
            RESTToken serverDataToken =
                    restCatalogServer.dataTokenStore.get(identifier.getFullName());
            assertEquals(serverDataToken, fileDataToken);
        }
    }

    @Test
    void testRefreshFileIOWhenExpired() throws Exception {
        this.catalog = initDataTokenCatalog();
        Identifier identifier =
                Identifier.create("test_data_token", "table_for_testing_date_token");
        RESTToken expiredDataToken =
                new RESTToken(
                        ImmutableMap.of("akId", "akId", "akSecret", UUID.randomUUID().toString()),
                        System.currentTimeMillis());
        restCatalogServer.setDataToken(identifier, expiredDataToken);
        createTable(identifier, Maps.newHashMap(), Lists.newArrayList("col1"));
        FileStoreTable fileStoreTable = (FileStoreTable) catalog.getTable(identifier);
        RESTTokenFileIO fileIO = (RESTTokenFileIO) fileStoreTable.fileIO();
        RESTToken fileDataToken = fileIO.validToken();
        assertEquals(expiredDataToken, fileDataToken);
        RESTToken newDataToken =
                new RESTToken(
                        ImmutableMap.of("akId", "akId", "akSecret", UUID.randomUUID().toString()),
                        System.currentTimeMillis() + 100_000);
        restCatalogServer.setDataToken(identifier, newDataToken);
        RESTToken nextFileDataToken = fileIO.validToken();
        assertEquals(newDataToken, nextFileDataToken);
        assertEquals(true, nextFileDataToken.expireAtMillis() - fileDataToken.expireAtMillis() > 0);
    }

    @Test
    void testSnapshotFromREST() throws Catalog.TableNotExistException {
        Options options = new Options();
        options.set(RESTCatalogOptions.URI, restCatalogServer.getUrl());
        options.set(RESTCatalogOptions.TOKEN, initToken);
        options.set(RESTCatalogOptions.TOKEN_PROVIDER, AuthProviderEnum.BEAR.identifier());
        RESTCatalog catalog = new RESTCatalog(CatalogContext.create(options));
        Identifier hasSnapshotTable = Identifier.create("test_db_a", "my_snapshot_table");
        long id = 10086;
        long millis = System.currentTimeMillis();
        restCatalogServer.setTableSnapshot(hasSnapshotTable, createSnapshotWithMillis(id, millis));
        Optional<Snapshot> snapshot = catalog.loadSnapshot(hasSnapshotTable);
        assertThat(snapshot).isPresent();
        assertThat(snapshot.get().id()).isEqualTo(id);
        assertThat(snapshot.get().timeMillis()).isEqualTo(millis);

        snapshot = catalog.loadSnapshot(Identifier.create("test_db_a", "unknown"));
        assertThat(snapshot).isEmpty();
    }

    @Test
    void testBranches() throws Exception {
        String databaseName = "testBranchTable";
        catalog.dropDatabase(databaseName, true, true);
        catalog.createDatabase(databaseName, true);
        Identifier identifier = Identifier.create(databaseName, "table");
        catalog.createTable(
                identifier, Schema.newBuilder().column("col", DataTypes.INT()).build(), true);

        RESTCatalog restCatalog = (RESTCatalog) catalog;
        restCatalog.createBranch(identifier, "my_branch", null);
        assertThat(restCatalog.listBranches(identifier)).containsOnly("my_branch");
        restCatalog.dropBranch(identifier, "my_branch");
        assertThat(restCatalog.listBranches(identifier)).isEmpty();
    }

    @Override
    protected boolean supportsFormatTable() {
        return true;
    }

    @Override
    protected boolean supportPartitions() {
        return true;
    }

    @Override
    protected boolean supportsView() {
        return true;
    }

    @Override
    protected boolean supportsAlterDatabase() {
        return true;
    }

    // TODO implement this
    @Override
    @Test
    public void testTableUUID() {}

    private void createTable(
            Identifier identifier, Map<String, String> options, List<String> partitionKeys)
            throws Exception {
        catalog.createDatabase(identifier.getDatabaseName(), false);
        catalog.createTable(
                identifier,
                new Schema(
                        Lists.newArrayList(new DataField(0, "col1", DataTypes.INT())),
                        partitionKeys,
                        Collections.emptyList(),
                        options,
                        ""),
                true);
    }

    private Catalog initDataTokenCatalog() {
        Options options = new Options();
        options.set(RESTCatalogOptions.URI, restCatalogServer.getUrl());
        options.set(RESTCatalogOptions.TOKEN, initToken);
        options.set(RESTCatalogOptions.DATA_TOKEN_ENABLED, true);
        options.set(RESTCatalogOptions.TOKEN_PROVIDER, AuthProviderEnum.BEAR.identifier());
        return new RESTCatalog(CatalogContext.create(options));
    }
}
