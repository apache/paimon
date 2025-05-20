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

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.Path;
import org.apache.paimon.iceberg.metadata.IcebergSchema;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

public class IcebergRESTMetadataCommitter implements IcebergMetadataCommitter {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergRESTMetadataCommitter.class);

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

        Options tableOptions = new Options(table.options());

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

    @Override
    public void commitMetadataREST(
            IcebergSchema schema,
            Path newMetadataPath,
            @org.jetbrains.annotations.Nullable Path baseMetadataPath) {}

    private void commitMetadataImpl(Path newMetadataPath, @Nullable Path baseMetadataPath)
            throws Exception {
        boolean ignoreIfAlreadyExists = true;

        // Create database
        /*
        Map<String, String> databaseProperties = new HashMap<>();
        databaseProperties.put("LocationUri", catalogDatabasePath(table).toString());
        restCatalog.createDatabase(icebergDatabase, ignoreIfAlreadyExists, databaseProperties);

        // TODO change table name to icebergTable
        restCatalog.createTable(identifier, table.schema().toSchema(), ignoreIfAlreadyExists);
         */
    }
}
