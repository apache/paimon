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
import org.apache.paimon.iceberg.metadata.IcebergMetadata;
import org.apache.paimon.iceberg.metadata.IcebergPartitionSpec;
import org.apache.paimon.iceberg.metadata.IcebergSchema;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.IcebergSnapshotRefType;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableCommit;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTCatalog;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * commit Iceberg metadata to Iceberg's rest catalog, so the table can be visited by Iceberg's rest
 * catalog.
 */
public class IcebergRestMetadataCommitter implements IcebergMetadataCommitter {

    private static final String REST_CATALOG_NAME = "rest-catalog";

    private final RESTCatalog restCatalog;
    private final String icebergDatabaseName;
    private final String icebergTableName;

    private final TableIdentifier icebergTableIdentifier;

    public IcebergRestMetadataCommitter(FileStoreTable table) {
        Options options = new Options(table.options());

        Identifier identifier = Preconditions.checkNotNull(table.catalogEnvironment().identifier());
        String icebergDatabase = options.get(IcebergOptions.METASTORE_DATABASE);
        String icebergTable = options.get(IcebergOptions.METASTORE_TABLE);
        this.icebergDatabaseName =
                icebergDatabase != null && !icebergDatabase.isEmpty()
                        ? icebergDatabase
                        : identifier.getDatabaseName();
        this.icebergTableName =
                icebergTable != null && !icebergTable.isEmpty()
                        ? icebergTable
                        : identifier.getTableName();
        this.icebergTableIdentifier =
                TableIdentifier.of(Namespace.of(icebergDatabaseName), icebergTableName);

        Map<String, String> restConfigs = IcebergOptions.icebergRestConfig(options.toMap());

        try {
            this.restCatalog = initRestCatalog(restConfigs);
        } catch (Exception e) {
            throw new RuntimeException("Fail to initialize iceberg rest catalog.", e);
        }

        // just for test
        //        restCatalog = initRestCatalog(table.options().get("iceberg.rest.uri"));
    }

    public void commitMetadata(Path newMetadataPath, @Nullable Path baseMetadataPath) {
        // do nothing
    }

    public void commitMetadata(
            IcebergMetadata newIcebergMetadata, @Nullable IcebergMetadata baseIcebergMetadata) {
        try {
            commitMetadataImpl(newIcebergMetadata, baseIcebergMetadata);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void commitMetadataImpl(
            IcebergMetadata newIcebergMetadata, @Nullable IcebergMetadata baseIcebergMetadata)
            throws Exception {

        TableMetadata icebergNewMetadata =
                TableMetadataParser.fromJson(newIcebergMetadata.toJson());

        // create database if not exist
        if (!databaseExists()) {
            createDatabase();
        }

        Table icebergTable;
        TableMetadata icebergBaseMetadata = null;
        // create table if not exist
        if (tableExists()) {
            icebergTable = getTable();
            icebergBaseMetadata = ((BaseTable) icebergTable).operations().current();
            if (!hasBaseMetadata(icebergBaseMetadata, icebergNewMetadata)) {
                restCatalog.dropTable(icebergTableIdentifier);
                icebergBaseMetadata = null;
                icebergTable = createTable(icebergNewMetadata);
            }

        } else {
            icebergTable = createTable(icebergNewMetadata);
        }
        /* 1. invoke AppendFiles and DeleteFiles to create manifest files by rest catalog
         * 2. invoke AddSnapshot and RemoveSnapshot to iceberg table
         */

        // commit changes
        // if no base, drop table first and create a new table and commit

        Snapshot newSnapshot = icebergNewMetadata.currentSnapshot();
        List<MetadataUpdate> updates = new ArrayList<>();
        updates.add(new MetadataUpdate.AddSnapshot(newSnapshot));
        updates.add(
                new MetadataUpdate.SetSnapshotRef(
                        SnapshotRef.MAIN_BRANCH,
                        newSnapshot.snapshotId(),
                        IcebergSnapshotRefType.branchType(),
                        null,
                        null,
                        null));
        IcebergTableCommit tableCommit =
                new IcebergTableCommit(
                        icebergTableIdentifier,
                        ImmutableList.of(
                                new UpdateRequirement.AssertTableUUID(
                                        icebergTable.uuid().toString())),
                        updates);
        restCatalog.commitTransaction(tableCommit);

        System.out.println("rest catalog commit!");

        // if base, commit
    }

    private RESTCatalog initRestCatalog(Map<String, String> restConfigs) {
        RESTCatalog catalog = new RESTCatalog();
        catalog.setConf(new Configuration());
        catalog.initialize(REST_CATALOG_NAME, restConfigs);

        return catalog;
    }

    private RESTCatalog initRestCatalog(String uri) {
        // just for test
        Configuration conf = new Configuration();
        SessionCatalog.SessionContext context =
                new SessionCatalog.SessionContext(
                        UUID.randomUUID().toString(),
                        "user",
                        ImmutableMap.of("credential", "user:12345"),
                        ImmutableMap.of());

        RESTCatalog catalog =
                new RESTCatalog(
                        context,
                        (config) ->
                                HTTPClient.builder(config)
                                        .uri(config.get(CatalogProperties.URI))
                                        .build());
        catalog.setConf(conf);
        Map<String, String> properties =
                ImmutableMap.of(
                        CatalogProperties.URI,
                        uri,
                        CatalogProperties.FILE_IO_IMPL,
                        "org.apache.iceberg.hadoop.HadoopFileIO",
                        "credential",
                        "catalog:12345");
        catalog.initialize(
                "prod", ImmutableMap.<String, String>builder().putAll(properties).build());
        return catalog;
    }

    // -------------------------------------------------------------------------------------
    // reset catalog invoke
    // -------------------------------------------------------------------------------------

    private boolean databaseExists() {
        return restCatalog.namespaceExists(Namespace.of(icebergDatabaseName));
    }

    private boolean tableExists() {
        return restCatalog.tableExists(
                TableIdentifier.of(Namespace.of(icebergDatabaseName), icebergTableName));
    }

    private void createDatabase() {
        restCatalog.createNamespace(Namespace.of(icebergDatabaseName));
    }

    private Table createTable(TableMetadata tableMetadata) {
        TableIdentifier tableIdentifier =
                TableIdentifier.of(Namespace.of(icebergDatabaseName), icebergTableName);

        return restCatalog.createTable(
                tableIdentifier, tableMetadata.schema(), tableMetadata.spec());
    }

    private Table getTable() {
        return restCatalog.loadTable(
                TableIdentifier.of(Namespace.of(icebergDatabaseName), icebergTableName));
    }

    private void dropTable() {
        restCatalog.dropTable(
                TableIdentifier.of(Namespace.of(icebergDatabaseName), icebergTableName));
    }

    // -------------------------------------------------------------------------------------
    // Utils
    // -------------------------------------------------------------------------------------

    private static Schema icebergSchemaToSchema(IcebergSchema icebergSchema) {
        String schemaJson = JsonSerdeUtil.toJson(icebergSchema);
        return SchemaParser.fromJson(schemaJson);
    }

    private static PartitionSpec icebergPartitionSpecToPartitionSpec(
            Schema schema, IcebergPartitionSpec icebergPartitionSpec) {
        String specJson = JsonSerdeUtil.toJson(icebergPartitionSpec);
        return PartitionSpecParser.fromJson(schema, specJson);
    }

    private static boolean hasBaseMetadata(
            TableMetadata icebergBaseMetadata, TableMetadata icebergNewMetadata) {
        // if iceberg table has been created, check whether the table metadata is the base of the
        // new table metadata
        if (icebergBaseMetadata.lastSequenceNumber()
                == icebergNewMetadata.lastSequenceNumber() - 1) {
            return true;
        }
        return false;
    }

    private class IcebergTableCommit implements TableCommit {
        private final TableIdentifier identifier;

        private final List<UpdateRequirement> requirements;

        private final List<MetadataUpdate> updates;

        IcebergTableCommit(
                TableIdentifier identifier,
                List<UpdateRequirement> requirements,
                List<MetadataUpdate> updates) {
            this.identifier = identifier;
            this.requirements = requirements;
            this.updates = updates;
        }

        @Override
        public TableIdentifier identifier() {
            return identifier;
        }

        @Override
        public List<UpdateRequirement> requirements() {
            return requirements;
        }

        @Override
        public List<MetadataUpdate> updates() {
            return updates;
        }
    }

    private class SchemaCache {
        Map<Long, Schema> schemas = new HashMap<>();

        private Schema get(long schemaId, List<IcebergSchema> icebergSchemas) {
            return schemas.computeIfAbsent(
                    schemaId, id -> icebergSchemaToSchema(icebergSchemas.get(id.intValue())));
        }
    }
}
