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

package org.apache.paimon.iceberg;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.RESTCatalog;
import org.apache.paimon.rest.RESTCatalogFactory;
import org.apache.paimon.rest.RESTCatalogOptions;
import org.apache.paimon.rest.RESTTokenFileIO;
import org.apache.paimon.rest.auth.AuthProviderEnum;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

import static org.apache.paimon.iceberg.IcebergCommitCallback.catalogDatabasePath;

public class IcebergRESTMetadataCommitter implements IcebergMetadataCommitter {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergRESTMetadataCommitter.class);

    private final RESTCatalog restCatalog;
    private final FileStoreTable table;
    private final Identifier identifier;
    private final String icebergDatabase;
    private final String icebergTable;

    public IcebergRESTMetadataCommitter(FileStoreTable table) {
        this.table = table;
        this.identifier =
                Preconditions.checkNotNull(
                        table.catalogEnvironment().identifier(),
                        "If you want to sync Paimon Iceberg compatible metadata to REST catalog, "
                                + "you must use a Paimon table created from a Paimon catalog, "
                                + "instead of a temporary table.");
        Preconditions.checkArgument(
                identifier.getBranchName() == null,
                "Paimon Iceberg compatibility currently does not support branches.");

        // TODO this is hardcoded for an IT test but should not be
        String initToken = "init_token";

        Options tableOptions = new Options(table.options());

        Options catalogOptions = new Options();
        catalogOptions.set("metastore", "rest");
        catalogOptions.set(CatalogOptions.WAREHOUSE.key(), tableOptions.get(IcebergOptions.REST_WAREHOUSE));
        catalogOptions.set(RESTCatalogOptions.URI.key(), tableOptions.get(IcebergOptions.URI));
        catalogOptions.set(RESTCatalogOptions.TOKEN.key(), initToken);
        catalogOptions.set(
                RESTCatalogOptions.TOKEN_PROVIDER.key(), AuthProviderEnum.BEAR.identifier());
        catalogOptions.set(RESTTokenFileIO.DATA_TOKEN_ENABLED.key(), "true");
        /*
        catalogOptions.put(
                RESTTestFileIO.DATA_PATH_CONF_KEY,
                dataPath.replaceFirst("file", RESTFileIOTestLoader.SCHEME));
         */

        this.restCatalog = new RESTCatalog(CatalogContext.create(catalogOptions));

        String icebergDatabase = tableOptions.get(IcebergOptions.METASTORE_DATABASE);
        String icebergTable = tableOptions.get(IcebergOptions.METASTORE_TABLE);
        this.icebergDatabase =
                icebergDatabase != null && !icebergDatabase.isEmpty()
                        ? icebergDatabase
                        : this.identifier.getDatabaseName();
        this.icebergTable =
                icebergTable != null && !icebergTable.isEmpty()
                        ? icebergTable
                        : this.identifier.getTableName();
    }

    @Override
    public void commitMetadata(Path newMetadataPath, @Nullable Path baseMetadataPath) {
        try {
            commitMetadataImpl(newMetadataPath, baseMetadataPath);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void commitMetadataImpl(Path newMetadataPath, @Nullable Path baseMetadataPath)
            throws Exception {
        boolean ignoreIfAlreadyExists = true;

        // Create database
        Map<String, String> databaseProperties = new HashMap<>();
        databaseProperties.put("LocationUri", catalogDatabasePath(table).toString());
        restCatalog.createDatabase(icebergDatabase, ignoreIfAlreadyExists, databaseProperties);

        // TODO change table name to icebergTable
        restCatalog.createTable(identifier, table.schema().toSchema(), ignoreIfAlreadyExists);

        /*
          TODO set Iceberg table parameters metadata_location and previous_metadata_location and options
        */
    }
}
