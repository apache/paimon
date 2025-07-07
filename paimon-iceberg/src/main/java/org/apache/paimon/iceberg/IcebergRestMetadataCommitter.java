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
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iceberg.CatalogUtil.ICEBERG_CATALOG_TYPE;
import static org.apache.iceberg.TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED;
import static org.apache.iceberg.TableProperties.METADATA_PREVIOUS_VERSIONS_MAX;

/**
 * commit Iceberg metadata to Iceberg's rest catalog, so the table can be visited by Iceberg's rest
 * catalog.
 */
public class IcebergRestMetadataCommitter implements IcebergMetadataCommitter {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergRestMetadataCommitter.class);

    private static final String REST_CATALOG_NAME = "rest-catalog";

    private final RESTCatalog restCatalog;
    private final String icebergDatabaseName;
    private final TableIdentifier icebergTableIdentifier;
    private final IcebergOptions icebergOptions;

    private Table icebergTable;

    public IcebergRestMetadataCommitter(FileStoreTable table) {
        Options options = new Options(table.options());
        icebergOptions = new IcebergOptions(options);

        Identifier identifier = Preconditions.checkNotNull(table.catalogEnvironment().identifier());
        String icebergDatabase = options.get(IcebergOptions.METASTORE_DATABASE);
        String icebergTable = options.get(IcebergOptions.METASTORE_TABLE);
        this.icebergDatabaseName =
                icebergDatabase != null && !icebergDatabase.isEmpty()
                        ? icebergDatabase
                        : identifier.getDatabaseName();
        String icebergTableName =
                icebergTable != null && !icebergTable.isEmpty()
                        ? icebergTable
                        : identifier.getTableName();
        this.icebergTableIdentifier =
                TableIdentifier.of(Namespace.of(icebergDatabaseName), icebergTableName);

        Map<String, String> restConfigs = icebergOptions.icebergRestConfig();

        try {
            Configuration hadoopConf = new Configuration();
            hadoopConf.setClassLoader(IcebergRestMetadataCommitter.class.getClassLoader());

            this.restCatalog = initRestCatalog(restConfigs, hadoopConf);
        } catch (Exception e) {
            throw new RuntimeException("Fail to initialize iceberg rest catalog.", e);
        }
    }

    @Override
    public String identifier() {
        return "rest";
    }

    @Override
    public void commitMetadata(Path newMetadataPath, @Nullable Path baseMetadataPath) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commitMetadata(
            IcebergMetadata newIcebergMetadata,
            Path newMetadataPath,
            @Nullable IcebergMetadata baseIcebergMetadata) {
        try {
            commitMetadataImpl(newIcebergMetadata, newMetadataPath, baseIcebergMetadata);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void commitMetadataImpl(
            IcebergMetadata newIcebergMetadata,
            Path newMetadataPath,
            @Nullable IcebergMetadata baseIcebergMetadata) {

        TableMetadata newMetadata = TableMetadataParser.fromJson(newIcebergMetadata.toJson());

        // create database if not exist
        if (!databaseExists()) {
            createDatabase();
        }

        try {
            if (!tableExists()) {
                LOG.info("Table {} does not exist, register it.", icebergTableIdentifier);
                icebergTable = registerTable(newMetadataPath);
            } else {
                icebergTable = getTable();

                TableMetadata metadata = ((BaseTable) icebergTable).operations().current();
                boolean withBase = checkBase(metadata, newMetadata, baseIcebergMetadata);
                if (withBase) {
                    LOG.info("update the table with base metadata.");
                    TableMetadata.Builder updatdeBuilder =
                            updatesForCorrectBase(metadata, newMetadata);
                    TableMetadata updatedForCommit = updatdeBuilder.build();

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("updates:{}", updatesToString(updatedForCommit.changes()));
                    }

                    ((BaseTable) icebergTable)
                            .operations()
                            .commit(
                                    ((BaseTable) icebergTable).operations().current(),
                                    updatedForCommit);
                } else {
                    LOG.info(
                            "the base metadata is incorrect. currentSnapshotId for base metadata: {}, for new metadata:{}. "
                                    + "we'll recreate the iceberg table.",
                            metadata.currentSnapshot().snapshotId(),
                            newMetadata.currentSnapshot().snapshotId());
                    icebergTable = reRegisterTable(newMetadataPath);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    "Fail to commit metadata to rest catalog, table is " + icebergTableIdentifier,
                    e);
        }
    }

    private TableMetadata.Builder updatesForCorrectBase(
            TableMetadata base, TableMetadata newMetadata) {
        TableMetadata.Builder updateBuilder = TableMetadata.buildFrom(base);

        int schemaId = icebergTable.schema().schemaId();

        // add new schema if needed
        Preconditions.checkArgument(
                newMetadata.currentSchemaId() >= schemaId,
                "the new metadata has correct base, but the schemaId(%s) in iceberg table "
                        + "is greater than currentSchemaId(%s) in new metadata.",
                schemaId,
                newMetadata.currentSchemaId());
        if (newMetadata.currentSchemaId() != schemaId) {
            addAndSetCurrentSchema(
                    Collections.singletonList(newMetadata.schema()),
                    newMetadata.currentSchemaId(),
                    updateBuilder);
        }

        // add snapshot
        addNewSnapshot(newMetadata.currentSnapshot(), updateBuilder);

        // remove snapshots not in new metadata
        Set<Long> snapshotIdsToRemove = new HashSet<>();
        icebergTable
                .snapshots()
                .forEach(snapshot -> snapshotIdsToRemove.add(snapshot.snapshotId()));
        Set<Long> snapshotIdsInNewMetadata =
                newMetadata.snapshots().stream()
                        .map(Snapshot::snapshotId)
                        .collect(Collectors.toSet());
        snapshotIdsToRemove.removeAll(snapshotIdsInNewMetadata);
        removeSnapshots(snapshotIdsToRemove, updateBuilder);

        return updateBuilder;
    }

    private RESTCatalog initRestCatalog(Map<String, String> restConfigs, Configuration conf) {
        restConfigs.put(ICEBERG_CATALOG_TYPE, "rest");
        Catalog catalog = CatalogUtil.buildIcebergCatalog(REST_CATALOG_NAME, restConfigs, conf);
        return (RESTCatalog) catalog;
    }

    // -------------------------------------------------------------------------------------
    // rest catalog invoke
    // -------------------------------------------------------------------------------------

    private boolean databaseExists() {
        return restCatalog.namespaceExists(Namespace.of(icebergDatabaseName));
    }

    private boolean tableExists() {
        return restCatalog.tableExists(icebergTableIdentifier);
    }

    private void createDatabase() {
        restCatalog.createNamespace(Namespace.of(icebergDatabaseName));
    }

    private Table registerTable(Path newMetadataPath) {
        return restCatalog.registerTable(icebergTableIdentifier, newMetadataPath.toString());
    }

    private Table getTable() {
        return restCatalog.loadTable(icebergTableIdentifier);
    }

    private void dropTable() {
        // set purge to false, because we don't need to delete the data files
        restCatalog.dropTable(icebergTableIdentifier, false);
    }

    private Table reRegisterTable(Path newMetadataPath) {
        try {
            dropTable();
            return registerTable(newMetadataPath);
        } catch (Exception e) {
            throw new RuntimeException("Fail to re-register iceberg table.", e);
        }
    }

    // -------------------------------------------------------------------------------------
    // metadata updates
    // -------------------------------------------------------------------------------------

    // add a new snapshot and point it as current snapshot
    private void addNewSnapshot(Snapshot newSnapshot, TableMetadata.Builder update) {
        update.setBranchSnapshot(newSnapshot, SnapshotRef.MAIN_BRANCH);
    }

    // remove snapshots recorded in table metadata
    private void removeSnapshots(Set<Long> snapshotIds, TableMetadata.Builder update) {
        update.removeSnapshots(snapshotIds);
    }

    private void addAndSetCurrentSchema(
            List<Schema> schemas, int currentSchemaId, TableMetadata.Builder update) {
        for (Schema schema : schemas) {
            update.addSchema(schema);
        }
        update.setCurrentSchema(currentSchemaId);

        // update properties
        Map<String, String> properties = new HashMap<>();
        properties.put(
                METADATA_PREVIOUS_VERSIONS_MAX,
                String.valueOf(icebergOptions.previousVersionsMax()));
        properties.put(
                METADATA_DELETE_AFTER_COMMIT_ENABLED,
                String.valueOf(icebergOptions.deleteAfterCommitEnabled()));
        update.setProperties(properties);
    }

    // -------------------------------------------------------------------------------------
    // Utils
    // -------------------------------------------------------------------------------------

    /**
     * @param currentMetadata the current metadata used by iceberg table
     * @param newMetadata the new metadata to be committed
     * @param baseIcebergMetadata the base metadata previously written by paimon
     * @return whether the iceberg table has base metadata
     */
    private static boolean checkBase(
            TableMetadata currentMetadata,
            TableMetadata newMetadata,
            @Nullable IcebergMetadata baseIcebergMetadata) {
        // take the base metadata from IcebergCommitCallback as the first reference
        if (baseIcebergMetadata == null) {
            LOG.info(
                    "new metadata without base metadata cause base metadata from upstream is null.");
            return false;
        }

        // if the iceberg table is existed, check whether the current metadata of the table is the
        // base of the new table metadata, we use current snapshot id to check
        return currentMetadata.currentSnapshot().snapshotId()
                == newMetadata.currentSnapshot().snapshotId() - 1;
    }

    private static String updateToString(MetadataUpdate update) {
        if (update instanceof MetadataUpdate.AddSnapshot) {
            return String.format(
                    "AddSnapshot(%s)",
                    ((MetadataUpdate.AddSnapshot) update).snapshot().snapshotId());
        } else if (update instanceof MetadataUpdate.RemoveSnapshot) {
            return String.format(
                    "RemoveSnapshot(%s)", ((MetadataUpdate.RemoveSnapshot) update).snapshotId());
        } else if (update instanceof MetadataUpdate.SetSnapshotRef) {
            return String.format(
                    "SetSnapshotRef(%s, %s, %s)",
                    ((MetadataUpdate.SetSnapshotRef) update).name(),
                    ((MetadataUpdate.SetSnapshotRef) update).type(),
                    ((MetadataUpdate.SetSnapshotRef) update).snapshotId());
        } else if (update instanceof MetadataUpdate.AddSchema) {
            return String.format(
                    "AddSchema(%s)", ((MetadataUpdate.AddSchema) update).schema().schemaId());
        } else if (update instanceof MetadataUpdate.SetCurrentSchema) {
            return String.format(
                    "SetCurrentSchema(%s)", ((MetadataUpdate.SetCurrentSchema) update).schemaId());
        } else if (update instanceof MetadataUpdate.SetProperties) {
            return String.format(
                    "SetProperties(%s)", ((MetadataUpdate.SetProperties) update).updated());
        } else {
            return update.toString();
        }
    }

    private static String updatesToString(List<MetadataUpdate> updates) {
        return updates.stream()
                .map(IcebergRestMetadataCommitter::updateToString)
                .collect(Collectors.joining(", "));
    }
}
