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
            this.restCatalog = initRestCatalog(restConfigs);
        } catch (Exception e) {
            throw new RuntimeException("Fail to initialize iceberg rest catalog.", e);
        }
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

        TableMetadata newMetadata = TableMetadataParser.fromJson(newIcebergMetadata.toJson());

        // updates to be committed
        List<MetadataUpdate> updates;

        // create database if not exist
        if (!databaseExists()) {
            createDatabase();
        }

        try {
            if (!tableExists()) {
                icebergTable = createTable(newMetadata);
                updates = updatesForCorrectBase(newMetadata, true);
            } else {
                icebergTable = getTable();

                TableMetadata metadata = ((BaseTable) icebergTable).operations().current();
                boolean withBase = checkBase(metadata, newMetadata, baseIcebergMetadata);
                if (metadata.lastSequenceNumber() == 0
                        && metadata.currentSnapshot().snapshotId() == -1) {
                    updates = updatesForCorrectBase(newMetadata, true);
                } else if (withBase) {
                    updates = updatesForCorrectBase(newMetadata, false);
                } else {
                    updates = updatesForIncorrectBase(newMetadata);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    "Fail to create table or get table: " + icebergTableIdentifier, e);
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
        icebergTable = recreateTable(newMetadata);
        updates.addAll(updatesForCorrectBase(newMetadata, true));

        return updates;
    }

    // way2: remove all snapshots in iceberg table, and reset the currentSnapshotId
    // side effects; the lastSequenceNumber won't be reset. If the paimon table was recreated,
    // it will commit snapshots which snapshotId is less than the lastSequenceNumber
    private List<MetadataUpdate> updatesForIncorrectBase2(TableMetadata newMetadata) {
        List<MetadataUpdate> updates = new ArrayList<>();

        // reset currentSnapshotId to -1
        resetSnapshot(updates);

        // remove all snapshots
        Set<Long> snapshotIdsToRemove = new HashSet<>();
        icebergTable
                .snapshots()
                .forEach(snapshot -> snapshotIdsToRemove.add(snapshot.snapshotId()));
        removeSnapshots(snapshotIdsToRemove, updates);

        // add new snapshot
        addNewSnapshot(newMetadata.currentSnapshot(), updates);

        return updates;
    }

    private RESTCatalog initRestCatalog(Map<String, String> restConfigs) {
        RESTCatalog catalog = new RESTCatalog();
        catalog.setConf(new Configuration());
        catalog.initialize(REST_CATALOG_NAME, restConfigs);

        return catalog;
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
     * @param baseIcebergMetadataFromPath the base metadata previously written by paimon
     * @return whether the iceberg table has base metadata
     */
    private static boolean checkBase(
            TableMetadata currentMetadata,
            TableMetadata newMetadata,
            @Nullable IcebergMetadata baseIcebergMetadataFromPath) {
        // take the base metadata from IcebergCommitCallback as the first reference
        if (baseIcebergMetadataFromPath == null) {
            LOG.info(
                    "new metadata without base metadata cause base metadata from upstream is null.");
            return false;
        }

        // if the iceberg table is existed, check whether the current metadata of the table is the
        // base of the new table metadata, we use current snapshot id to check
        boolean hasBase =
                currentMetadata.currentSnapshot().snapshotId()
                        == newMetadata.currentSnapshot().snapshotId() - 1;
        if (newMetadata.currentSnapshot().snapshotId()
                <= currentMetadata.currentSnapshot().snapshotId()) {
            LOG.warn(
                    "snapshot id for new snapshot is less or equal than the current snapshot, this is abnormal. "
                            + "Most possible cause is that the paimon table has been recreated "
                            + "while the iceberg table in iceberg is still the old one. You can drop the iceberg table manually, "
                            + "otherwise the lastSequenceNumber may be incorrect.");
        }

        if (hasBase) {
            LOG.info("new metadata with base metadata.");
        } else {
            LOG.info(
                    "new metadata without base metadata. currentSnapshotId for base metadata: {}, for new metadata:{}",
                    currentMetadata.currentSnapshot().snapshotId(),
                    newMetadata.currentSnapshot().snapshotId());
        }
        return hasBase;
    }

    /** doc. */
    public static class IcebergTableCommit implements TableCommit {
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
