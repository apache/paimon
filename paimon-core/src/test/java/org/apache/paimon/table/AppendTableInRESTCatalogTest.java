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

package org.apache.paimon.table;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.RESTCatalog;
import org.apache.paimon.rest.RESTCatalogInternalOptions;
import org.apache.paimon.rest.RESTCatalogOptions;
import org.apache.paimon.rest.RESTCatalogServer;
import org.apache.paimon.rest.auth.AuthProvider;
import org.apache.paimon.rest.auth.AuthProviderEnum;
import org.apache.paimon.rest.auth.BearTokenAuthProvider;
import org.apache.paimon.rest.responses.ConfigResponse;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.RowType;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.BeforeEach;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

import static org.apache.paimon.CoreOptions.BUCKET;
import static org.apache.paimon.CoreOptions.BUCKET_KEY;

/** test table in rest catalog. */
public class AppendTableInRESTCatalogTest extends SimpleTableTestBase {
    protected CatalogEnvironment catalogEnvironment;
    protected Catalog catalog;
    protected RESTCatalog restCatalog;

    @BeforeEach
    public void before() throws Exception {
        super.before();
        String dataPath = tablePath.toString();
        String restWarehouse = UUID.randomUUID().toString();
        String initToken = UUID.randomUUID().toString();
        ConfigResponse config =
                new ConfigResponse(
                        ImmutableMap.of(
                                RESTCatalogInternalOptions.PREFIX.key(),
                                "paimon",
                                CatalogOptions.WAREHOUSE.key(),
                                restWarehouse),
                        ImmutableMap.of());
        AuthProvider authProvider = new BearTokenAuthProvider(initToken);
        RESTCatalogServer restCatalogServer =
                new RESTCatalogServer(dataPath, authProvider, config, restWarehouse);
        restCatalogServer.start();
        Options options = new Options();
        options.set(CatalogOptions.WAREHOUSE.key(), restWarehouse);
        options.set(RESTCatalogOptions.URI, restCatalogServer.getUrl());
        options.set(RESTCatalogOptions.TOKEN, initToken);
        options.set(RESTCatalogOptions.TOKEN_PROVIDER, AuthProviderEnum.BEAR.identifier());
        restCatalog = new RESTCatalog(CatalogContext.create(options));
        catalog = new RESTCatalog(CatalogContext.create(options));
        catalog.createDatabase(identifier.getDatabaseName(), true);
        catalogEnvironment =
                new CatalogEnvironment(
                        identifier, null, restCatalog.catalogLoader(), null, null, true);
    }

    @BeforeEach
    public void after() throws Exception {
        super.after();
        catalog.dropTable(identifier, true);
    }

    @Override
    protected FileStoreTable createFileStoreTable(Consumer<Options> configure, RowType rowType)
            throws Exception {
        Options conf = new Options();
        configure.accept(conf);
        if (!conf.contains(BUCKET_KEY) && conf.get(BUCKET) != -1) {
            conf.set(BUCKET_KEY, "a");
        }
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new SchemaManager(LocalFileIO.create(), tablePath),
                        new Schema(
                                rowType.getFields(),
                                Collections.singletonList("pt"),
                                Collections.emptyList(),
                                conf.toMap(),
                                ""));
        if (catalog.listTables(identifier.getDatabaseName()).contains(identifier.getTableName())) {
            List<SchemaChange> schemaChangeList = new ArrayList<>();
            for (Map.Entry<String, String> entry : conf.toMap().entrySet()) {
                schemaChangeList.add(SchemaChange.setOption(entry.getKey(), entry.getValue()));
            }
            catalog.alterTable(identifier, schemaChangeList, true);
        } else {
            catalog.createTable(identifier, tableSchema.toSchema(), false);
        }
        return (FileStoreTable) catalog.getTable(identifier);
    }

    @Override
    protected boolean supportDefinePath() {
        return false;
    }
}
