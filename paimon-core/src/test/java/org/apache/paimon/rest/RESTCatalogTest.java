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

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogTestBase;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.Partition;
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
import java.util.List;
import java.util.Map;

import static org.apache.paimon.CoreOptions.METASTORE_PARTITIONED_TABLE;
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
        options.set(RESTCatalogOptions.THREAD_POOL_SIZE, 1);
        this.catalog = new RESTCatalog(CatalogContext.create(options));
    }

    @AfterEach
    public void tearDown() throws Exception {
        restCatalogServer.shutdown();
    }

    @Test
    void testInitFailWhenDefineWarehouse() {
        Options options = new Options();
        options.set(CatalogOptions.WAREHOUSE, warehouse);
        assertThatThrownBy(() -> new RESTCatalog(CatalogContext.create(options)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testAuthFail() {
        Options options = new Options();
        options.set(RESTCatalogOptions.URI, restCatalogServer.getUrl());
        options.set(RESTCatalogOptions.TOKEN, "aaaaa");
        options.set(RESTCatalogOptions.THREAD_POOL_SIZE, 1);
        options.set(CatalogOptions.METASTORE, RESTCatalogFactory.IDENTIFIER);
        assertThatThrownBy(() -> new RESTCatalog(CatalogContext.create(options)))
                .isInstanceOf(NotAuthorizedException.class);
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
        Options options = new Options();
        options.set(RESTCatalogOptions.URI, restCatalogServer.getUrl());
        options.set(RESTCatalogOptions.TOKEN, initToken);
        options.set(RESTCatalogOptions.THREAD_POOL_SIZE, 1);
        options.set(RESTCatalogOptions.FILE_IO_REFRESH_CREDENTIAL_ENABLE, true);
        this.catalog = new RESTCatalog(CatalogContext.create(options));
        List<Identifier> identifiers =
                Lists.newArrayList(
                        Identifier.create("test_db_a", "test_table_a"),
                        Identifier.create("test_db_b", "test_table_b"),
                        Identifier.create("test_db_c", "test_table_c"));
        for (Identifier identifier : identifiers) {
            createTable(identifier, Maps.newHashMap(), Lists.newArrayList("col1"));
            FileStoreTable fileStoreTable = (FileStoreTable) catalog.getTable(identifier);
            assertEquals(true, fileStoreTable.fileIO().exists(fileStoreTable.location()));
        }
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

    // TODO implement this
    @Override
    @Test
    public void testTableUUID() {}
}
