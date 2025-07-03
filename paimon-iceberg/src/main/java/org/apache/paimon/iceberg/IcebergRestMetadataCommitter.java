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
import org.apache.paimon.iceberg.metadata.IcebergSchema;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.IcebergSnapshotRefType;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableCommit;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.rest.RESTCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
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

    // we should use schema-0 in paimon to create iceberg table
    private final Schema schema0;

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

        schema0 =
                SchemaParser.fromJson(
                        IcebergSchema.create(table.schemaManager().schema(0)).toJson());

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
            IcebergMetadata newIcebergMetadata, @Nullable IcebergMetadata baseIcebergMetadata) {
        try {
            commitMetadataImpl(newIcebergMetadata, baseIcebergMetadata);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void commitMetadataImpl(
            IcebergMetadata newIcebergMetadata, @Nullable IcebergMetadata baseIcebergMetadata) {

        TableMetadata newMetadata = TableMetadataParser.fromJson(newIcebergMetadata.toJson());

        // updates to be committed
        List<MetadataUpdate> updates;

        // create database if not exist
        if (!databaseExists()) {
            createDatabase();
        }

        try {
            if (!tableExists()) {
                LOG.info("Table {} does not exist, create it.", icebergTableIdentifier);
                icebergTable = createTable(newMetadata);
                updates = updatesForCorrectBase(newMetadata, true);
            } else {
                icebergTable = getTable();

                TableMetadata metadata = ((BaseTable) icebergTable).operations().current();
                boolean withBase = checkBase(metadata, newMetadata, baseIcebergMetadata);
                if (metadata.lastSequenceNumber() == 0 && metadata.currentSnapshot() == null) {
                    // treat the iceberg table as a newly created table
                    LOG.info(
                            "lastSequenceNumber is 0 and currentSnapshotId is -1 for the existed iceberg table, "
                                    + "we'll treat it as a new table.");
                    updates = updatesForCorrectBase(newMetadata, true);
                } else if (withBase) {
                    LOG.info("create updates with base metadata.");
                    updates = updatesForCorrectBase(newMetadata, false);
                } else {
                    LOG.info(
                            "create updates without base metadata. currentSnapshotId for base metadata: {}, for new metadata:{}",
                            metadata.currentSnapshot().snapshotId(),
                            newMetadata.currentSnapshot().snapshotId());
                    updates = updatesForIncorrectBase(newMetadata);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    "Fail to create table or get table: " + icebergTableIdentifier, e);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("updates:{}", updatesToString(updates));
        }

        IcebergTableCommit tableCommit =
                new IcebergTableCommit(
                        icebergTableIdentifier,
                        ImmutableList.of(
                                new UpdateRequirement.AssertTableUUID(
                                        icebergTable.uuid().toString())),
                        updates);
        restCatalog.commitTransaction(tableCommit);
    }

    private List<MetadataUpdate> updatesForCorrectBase(
            TableMetadata newMetadata, boolean isNewTable) {
        List<MetadataUpdate> updates = new ArrayList<>();

        int schemaId = icebergTable.schema().schemaId();
        if (isNewTable) {
            Preconditions.checkArgument(
                    schemaId == 0,
                    "the schema id for newly created iceberg table should be 0, but is %s",
                    schemaId);
            // add schemas which schema id is greater than current schema id in iceberg table
            if (newMetadata.currentSchemaId() > schemaId) {
                addAndSetCurrentSchema(
                        updates,
                        newMetadata.schemas().stream()
                                .filter(schema -> schema.schemaId() > schemaId)
                                .collect(Collectors.toList()),
                        newMetadata.currentSchemaId());
            }
            // add snapshot
            addNewSnapshot(newMetadata.currentSnapshot(), updates);
        } else {
            // add new schema if needed
            Preconditions.checkArgument(
                    newMetadata.currentSchemaId() >= schemaId,
                    "the new metadata has correct base, but the schemaId(%s) in iceberg table "
                            + "is greater than currentSchemaId(%s) in new metadata.",
                    schemaId,
                    newMetadata.currentSchemaId());
            if (newMetadata.currentSchemaId() != schemaId) {
                addAndSetCurrentSchema(
                        updates,
                        Collections.singletonList(newMetadata.schema()),
                        newMetadata.currentSchemaId());
            }

            // add snapshot
            addNewSnapshot(newMetadata.currentSnapshot(), updates);

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
            removeSnapshots(snapshotIdsToRemove, updates);
        }

        return updates;
    }

    // way1: recreate  iceberg table, and commit new snapshots
    // side effects: unclear whether the data files will be deleted
    private List<MetadataUpdate> updatesForIncorrectBase(TableMetadata newMetadata) {
        List<MetadataUpdate> updates = new ArrayList<>();
        LOG.info("the base metadata is incorrect, we'll recreate the iceberg table.");
        icebergTable = recreateTable(newMetadata);
        updates.addAll(updatesForCorrectBase(newMetadata, true));

        return updates;
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

    private Table createTable(TableMetadata tableMetadata) {
        Map<String, String> properties = new HashMap<>();
        properties.put(
                METADATA_PREVIOUS_VERSIONS_MAX,
                String.valueOf(icebergOptions.previousVersionsMax()));
        properties.put(
                METADATA_DELETE_AFTER_COMMIT_ENABLED,
                String.valueOf(icebergOptions.deleteAfterCommitEnabled()));

        return restCatalog.createTable(
                icebergTableIdentifier, schema0, tableMetadata.spec(), properties);
    }

    private Table getTable() {
        return restCatalog.loadTable(icebergTableIdentifier);
    }

    private void dropTable() {
        // set purge to false, because we don't need to delete the data files
        restCatalog.dropTable(icebergTableIdentifier, false);
    }

    private Table recreateTable(TableMetadata tableMetadata) {
        try {
            dropTable();
            return createTable(tableMetadata);
        } catch (Exception e) {
            throw new RuntimeException("Fail to recreate iceberg table.", e);
        }
    }

    // -------------------------------------------------------------------------------------
    // metadata updates
    // -------------------------------------------------------------------------------------

    // add a new snapshot and point it as current snapshot
    private void addNewSnapshot(Snapshot newSnapshot, List<MetadataUpdate> updates) {
        updates.add(new MetadataUpdate.AddSnapshot(newSnapshot));
        updates.add(
                new MetadataUpdate.SetSnapshotRef(
                        SnapshotRef.MAIN_BRANCH,
                        newSnapshot.snapshotId(),
                        IcebergSnapshotRefType.branchType(),
                        null,
                        null,
                        null));
    }

    // remove snapshots recorded in table metadata
    private void removeSnapshots(Set<Long> snapshotIds, List<MetadataUpdate> updates) {
        for (Long snapshotId : snapshotIds) {
            updates.add(new MetadataUpdate.RemoveSnapshot(snapshotId));
        }
    }

    // reset current snapshot id to -1, this won't reset the last-sequence-number
    private void resetSnapshot(List<MetadataUpdate> updates) {
        updates.add(new MetadataUpdate.RemoveSnapshotRef(SnapshotRef.MAIN_BRANCH));
    }

    private void addAndSetCurrentSchema(
            List<MetadataUpdate> updates, List<Schema> schemas, int currentSchemaId) {
        for (Schema schema : schemas) {
            updates.add(new MetadataUpdate.AddSchema(schema));
        }
        updates.add(new MetadataUpdate.SetCurrentSchema(currentSchemaId));

        // update properties
        Map<String, String> properties = new HashMap<>();
        properties.put(
                METADATA_PREVIOUS_VERSIONS_MAX,
                String.valueOf(icebergOptions.previousVersionsMax()));
        properties.put(
                METADATA_DELETE_AFTER_COMMIT_ENABLED,
                String.valueOf(icebergOptions.deleteAfterCommitEnabled()));
        updates.add(new MetadataUpdate.SetProperties(properties));
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
            return "Unknown updates";
        }
    }

    private static String updatesToString(List<MetadataUpdate> updates) {
        return updates.stream()
                .map(IcebergRestMetadataCommitter::updateToString)
                .collect(Collectors.joining(", "));
    }

    private static class IcebergTableCommit implements TableCommit {
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
}
