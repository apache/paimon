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

package org.apache.paimon.table.system;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.RESTCatalogInternalOptions;
import org.apache.paimon.rest.RESTCatalogOptions;
import org.apache.paimon.rest.RESTCatalogServer;
import org.apache.paimon.rest.RESTTestFileIO;
import org.apache.paimon.rest.auth.AuthProviderEnum;
import org.apache.paimon.rest.auth.BearTokenAuthProvider;
import org.apache.paimon.rest.responses.ConfigResponse;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.paimon.catalog.Identifier.SYSTEM_TABLE_SPLITTER;
import static org.assertj.core.api.Assertions.assertThat;

/** Test PartitionsTable with REST catalog. */
class RestPartitionsTableTest extends PartitionsTableTest {

    private static final String TABLE_NAME = "MyTable";

    private RESTCatalogServer restCatalogServer;

    @BeforeEach
    @Override
    public void before() throws Exception {
        super.beforeEach();

        String dataPath = warehouse.toString();
        String restWarehouse = UUID.randomUUID().toString();
        String initToken = "init_token";
        BearTokenAuthProvider authProvider = new BearTokenAuthProvider(initToken);

        Map<String, String> defaultConf =
                new HashMap<>(
                        ImmutableMap.of(
                                RESTCatalogInternalOptions.PREFIX.key(),
                                "paimon",
                                CatalogOptions.WAREHOUSE.key(),
                                restWarehouse));
        ConfigResponse config = new ConfigResponse(defaultConf, ImmutableMap.of());
        restCatalogServer = new RESTCatalogServer(dataPath, authProvider, config, restWarehouse);
        restCatalogServer.start();

        Options options = new Options();
        options.set(RESTCatalogOptions.URI, restCatalogServer.getUrl());
        options.set(RESTCatalogOptions.TOKEN, initToken);
        options.set(RESTCatalogOptions.TOKEN_PROVIDER, AuthProviderEnum.BEAR.identifier());
        options.set(CatalogOptions.WAREHOUSE.key(), restWarehouse);
        options.set(CatalogOptions.METASTORE.key(), "rest");
        options.set(RESTTestFileIO.DATA_PATH_CONF_KEY, dataPath);

        catalog = CatalogFactory.createCatalog(CatalogContext.create(options));

        catalog.createDatabase(database, true);

        Schema schema =
                Schema.newBuilder()
                        .column("pk", DataTypes.INT())
                        .column("pt", DataTypes.INT())
                        .column("col1", DataTypes.INT())
                        .partitionKeys("pt")
                        .primaryKey("pk", "pt")
                        .option(CoreOptions.CHANGELOG_PRODUCER.key(), "input")
                        .option("bucket", "1")
                        .build();

        Identifier tableId = identifier(TABLE_NAME);
        catalog.createTable(tableId, schema, true);
        table = (FileStoreTable) catalog.getTable(tableId);

        Identifier filesTableId =
                identifier(TABLE_NAME + SYSTEM_TABLE_SPLITTER + PartitionsTable.PARTITIONS);
        partitionsTable = (PartitionsTable) catalog.getTable(filesTableId);

        write(table, GenericRow.of(1, 1, 1), GenericRow.of(1, 3, 5));

        write(table, GenericRow.of(1, 1, 3), GenericRow.of(1, 2, 4));
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (restCatalogServer != null) {
            restCatalogServer.shutdown();
        }
    }

    @Test
    void testPartitionAuditFieldsWithRestCatalog() throws Exception {
        // Read fields: partition(0), created_by(5), updated_by(6), options(7), created_at(8),
        // last_access_time(9)
        // After projection, indices are: 0=partition, 1=created_by, 2=updated_by, 3=options,
        // 4=created_at, 5=last_access_time
        List<InternalRow> result = read(partitionsTable, new int[] {0, 5, 6, 7, 8, 9});
        assertThat(result).isNotEmpty();

        for (InternalRow row : result) {
            // Index 1: created_by (from original index 5)
            assertThat(row.isNullAt(1)).isFalse();
            assertThat(row.getString(1).toString()).isEqualTo("created");
            // Index 2: updated_by (from original index 6)
            assertThat(row.isNullAt(2)).isFalse();
            assertThat(row.getString(2).toString()).isEqualTo("updated");
            // Index 3: options (from original index 7) - may be null
            // Index 4: created_at (from original index 8)
            assertThat(row.isNullAt(4)).isFalse();
            // Index 5: last_access_time (from original index 9)
            assertThat(row.isNullAt(5)).isFalse();
            // Check options if not null
            if (!row.isNullAt(3)) {
                String optionsJson = row.getString(3).toString();
                assertThat(optionsJson).isNotEmpty();
            }
        }
    }
}
