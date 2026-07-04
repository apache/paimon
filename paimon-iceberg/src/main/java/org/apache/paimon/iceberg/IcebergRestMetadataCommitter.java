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
import org.apache.paimon.iceberg.metadata.IcebergSnapshot;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
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
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
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
            IcebergMetadata newIcebergMetadata, @Nullable IcebergMetadata baseIcebergMetadata) {
        try {
            commitMetadataImpl(newIcebergMetadata, baseIcebergMetadata);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void commitMetadataImpl(
            IcebergMetadata newIcebergMetadata, @Nullable IcebergMetadata baseIcebergMetadata) {

        newIcebergMetadata = adjustMetadataForRest(newIcebergMetadata);
        TableMetadata newMetadata = TableMetadataParser.fromJson(newIcebergMetadata.toJson());

        // updates to be committed
        TableMetadata.Builder updateBuilder;

        // create database if not exist
        if (!databaseExists()) {
            createDatabase();
        }

        try {
            if (!tableExists()) {
                LOG.info("Table {} does not exist, create it.", icebergTableIdentifier);
                icebergTable = createTable(newMetadata);
                updateBuilder =
                        updatesForCorrectBase(
                                ((BaseTable) icebergTable).operations().current(),
                                newMetadata,
                                true);
            } else {
                icebergTable = getTable();

                TableMetadata metadata = ((BaseTable) icebergTable).operations().current();

                if (metadata.currentSnapshot() == null) {
                    // Table exists in the REST catalog but has no snapshots yet. This happens
                    // when a previous createTable() or recreateTable() succeeded but the
                    // subsequent commit() failed (e.g. network error, REST server timeout).
                    // Treat it as a new table — populate schemas, partition spec, and the
                    // current snapshot from scratch WITHOUT dropping and recreating the table.
                    // Routing through updatesForIncorrectBase would call recreateTable(), which
                    // on a repeated commit failure would create an infinite drop+create loop.
                    LOG.info(
                            "Iceberg table {} exists but has no snapshots, treating as new table.",
                            icebergTableIdentifier);
                    updateBuilder = updatesForCorrectBase(metadata, newMetadata, true);
                } else {
                    boolean withBase = checkBase(metadata, newMetadata, baseIcebergMetadata);
                    if (withBase) {
                        LOG.info("create updates with base metadata.");
                        updateBuilder = updatesForCorrectBase(metadata, newMetadata, false);
                    } else {
                        LOG.info(
                                "create updates without base metadata. currentSnapshotId for base metadata: {}, for new metadata:{}",
                                metadata.currentSnapshot().snapshotId(),
                                newMetadata.currentSnapshot() != null
                                        ? newMetadata.currentSnapshot().snapshotId()
                                        : "No snapshot");
                        updateBuilder = updatesForIncorrectBase(newMetadata);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    "Fail to create table or get table: " + icebergTableIdentifier, e);
        }

        TableMetadata updatedForCommit = updateBuilder.build();

        if (LOG.isDebugEnabled()) {
            LOG.debug("updates:{}", updatesToString(updatedForCommit.changes()));
        }

        try {
            ((BaseTable) icebergTable)
                    .operations()
                    .commit(((BaseTable) icebergTable).operations().current(), updatedForCommit);
        } catch (Exception e) {
            throw new RuntimeException("Fail to commit metadata to rest catalog.", e);
        }
    }

    private TableMetadata.Builder updatesForCorrectBase(
            TableMetadata base, TableMetadata newMetadata, boolean isNewTable) {
        TableMetadata.Builder updateBuilder = TableMetadata.buildFrom(base);

        int schemaId = icebergTable.schema().schemaId();
        if (isNewTable) {
            Preconditions.checkArgument(
                    schemaId == 0,
                    "the schema id for newly created iceberg table should be 0, but is %s",
                    schemaId);
            // add all schemas
            addAndSetCurrentSchema(
                    newMetadata.schemas(), newMetadata.currentSchemaId(), updateBuilder);
            updateBuilder.addPartitionSpec(newMetadata.spec());
            updateBuilder.setDefaultPartitionSpec(newMetadata.defaultSpecId());

            // add snapshot
            addNewSnapshot(newMetadata.currentSnapshot(), updateBuilder);

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
                        newMetadata.schemas().stream()
                                .filter(schema -> schema.schemaId() > schemaId)
                                .collect(Collectors.toList()),
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
        }

        updateProperties(updateBuilder);
        return updateBuilder;
    }

    private TableMetadata.Builder updatesForIncorrectBase(TableMetadata newMetadata) {
        LOG.info("the base metadata is incorrect, we'll recreate the iceberg table.");
        icebergTable = recreateTable(newMetadata);
        return updatesForCorrectBase(
                ((BaseTable) icebergTable).operations().current(), newMetadata, true);
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

    private Table createTable(TableMetadata newMetadata) {
        /*
        Handles fieldId incompatibility between Paimon (starts at 0) and Iceberg (starts at 1).

        Direct schema conversion shifts all fieldIds by +1, causing field disorder. While
        schemas can be updated post-creation to start at fieldId 0, creating an empty schema
        first triggers partition evolution issues that break some query engines.

        Strategy based on partition field position:
        - fieldId = 0: Creates empty schema first, partition evolution unavoidable
        - fieldId > 0: Creates dummy schema with offset fields and gap filling to preserve the partition spec
        */
        PartitionSpec spec = newMetadata.spec();
        boolean isPartitionedWithZeroFieldId =
                spec.fields().stream().anyMatch(f -> f.sourceId() == 0);
        if (spec.isUnpartitioned() || isPartitionedWithZeroFieldId) {
            if (isPartitionedWithZeroFieldId) {
                LOG.info(
                        "Partition fieldId = 0. The Iceberg REST committer will use partition evolution to support Iceberg compatibility with the Paimon schema. If you want to avoid this, use a non-zero fieldId partition field");
            }
            return createTable(new Schema(), null, newMetadata);
        } else {
            LOG.info(
                    "Partition fieldId > 0. In order to avoid partition evlolution, dummy schema will be created first");

            int size =
                    spec.fields().stream().mapToInt(PartitionField::sourceId).max().orElseThrow();
            // prefill the schema with dummy fields
            NestedField[] columns = new NestedField[size];
            for (int idx = 0; idx < size; idx++) {
                int fieldId = idx + 1;
                columns[idx] =
                        NestedField.optional(fieldId, "f" + fieldId, Types.BooleanType.get());
            }
            // find and set partition fields with offset -1, so they align correctly after table
            // creation
            for (PartitionField f : spec.fields()) {
                columns[f.sourceId() - 1] = newMetadata.schema().findField(f.sourceId());
            }

            return createTable(new Schema(columns), spec, newMetadata);
        }
    }

    private Table createTable(
            Schema schema, @Nullable PartitionSpec spec, TableMetadata newMetadata) {
        try {
            // Path-based catalogs (e.g. Hadoop) derive and assign the table location themselves
            // and reject a custom one, so first try letting the catalog assign it.
            return newTableBuilder(schema, spec).create();
        } catch (RuntimeException e) {
            // Some Iceberg REST catalogs (notably AWS Glue) do not auto-assign a table location
            // and reject creation without one. Retry with the location Paimon writes its metadata
            // to, normalised to the s3:// scheme such catalogs require.
            try {
                return newTableBuilder(schema, spec)
                        .withLocation(toRestLocation(newMetadata.location()))
                        .create();
            } catch (RuntimeException retryError) {
                e.addSuppressed(retryError);
                throw e;
            }
        }
    }

    private Catalog.TableBuilder newTableBuilder(Schema schema, @Nullable PartitionSpec spec) {
        Catalog.TableBuilder builder = restCatalog.buildTable(icebergTableIdentifier, schema);
        return spec == null ? builder : builder.withPartitionSpec(spec);
    }

    /** Normalises a table location's URI scheme for the Iceberg REST catalog. */
    static String toRestLocation(String location) {
        if (location == null) {
            return null;
        }
        if (location.startsWith("s3a://")) {
            return "s3://" + location.substring("s3a://".length());
        }
        if (location.startsWith("s3n://")) {
            return "s3://" + location.substring("s3n://".length());
        }
        return location;
    }

    private Table getTable() {
        return restCatalog.loadTable(icebergTableIdentifier);
    }

    private void dropTable() {
        // set purge to false, because we don't need to delete the data files
        restCatalog.dropTable(icebergTableIdentifier, false);
    }

    private Table recreateTable(TableMetadata newMetadata) {
        try {
            dropTable();
            return createTable(newMetadata);
        } catch (Exception e) {
            throw new RuntimeException("Fail to recreate iceberg table.", e);
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

    // add schemas and set the current schema id
    private void addAndSetCurrentSchema(
            List<Schema> schemas, int currentSchemaId, TableMetadata.Builder update) {
        for (Schema schema : schemas) {
            update.addSchema(schema);
        }
        update.setCurrentSchema(currentSchemaId);
    }

    // Update Iceberg REST table properties from current IcebergOptions, but only
    // if the values differ from what the REST catalog already has. This avoids
    // emitting a redundant SetProperties update on every commit.
    private void updateProperties(TableMetadata.Builder update) {
        String desiredMax = String.valueOf(icebergOptions.previousVersionsMax());
        String desiredDeleteAfter = String.valueOf(icebergOptions.deleteAfterCommitEnabled());

        Map<String, String> current = icebergTable.properties();
        boolean changed =
                !desiredMax.equals(current.get(METADATA_PREVIOUS_VERSIONS_MAX))
                        || !desiredDeleteAfter.equals(
                                current.get(METADATA_DELETE_AFTER_COMMIT_ENABLED));

        if (changed) {
            Map<String, String> properties = new HashMap<>();
            properties.put(METADATA_PREVIOUS_VERSIONS_MAX, desiredMax);
            properties.put(METADATA_DELETE_AFTER_COMMIT_ENABLED, desiredDeleteAfter);
            update.setProperties(properties);
        }
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
        // base of the new table metadata, we use current snapshot id to check.
        // Note: callers must ensure currentMetadata.currentSnapshot() is non-null before calling
        // this method (guarded in commitMetadataImpl).
        return currentMetadata.currentSnapshot().snapshotId()
                == newMetadata.currentSnapshot().snapshotId() - 1;
    }

    private IcebergMetadata adjustMetadataForRest(IcebergMetadata newIcebergMetadata) {
        // --- Why we shift schema IDs by +1 ---
        // When we create an Iceberg table via the REST catalog, we register it with
        // an empty schema that occupies schema ID 0. Paimon's own schema IDs start
        // at 0 as well, so we shift every Paimon schema ID by +1 to avoid colliding
        // with that placeholder.
        //
        // --- Why we deduplicate schemas ---
        // Option-only alterTable calls in Paimon (e.g. changing table properties
        // like metadata.iceberg.* settings) increment Paimon's internal schema
        // version without modifying the column definitions. This means Paimon can
        // accumulate multiple schema versions (e.g. IDs 0, 1, 2, 3) that all have
        // identical field lists.
        //
        // When these schemas are forwarded to Iceberg, addAndSetCurrentSchema()
        // calls TableMetadata.Builder.addSchema() for each one. Iceberg internally
        // deduplicates identical schemas via its sameSchema() check — it keeps only
        // the first occurrence and silently drops duplicates. So if schemas 1, 2, 3
        // all have the same fields, Iceberg keeps only schema 1.
        //
        // By deduplicating here (using IcebergDataField.equals() which compares id,
        // name, required, type, and doc), we keep only unique schemas and build a
        // remap table so that currentSchemaId and snapshot schemaId references point
        // to the surviving schema's ID. The downstream metadata is then internally
        // consistent with what Iceberg catalog will actually store.

        // Step 1 — Shift and convert: produce Iceberg Schema objects so we can call
        // sameSchema() in the dedup step.
        List<IcebergSchema> shiftedIcebergSchemas =
                newIcebergMetadata.schemas().stream()
                        .map(s -> new IcebergSchema(s.schemaId() + 1, s.fields()))
                        .collect(Collectors.toList());
        int shiftedCurrentSchemaId = newIcebergMetadata.currentSchemaId() + 1;
        // Build a temporary IcebergMetadata with shifted IDs to obtain Iceberg Schema objects.
        IcebergMetadata shiftedForConversion =
                new IcebergMetadata(
                        newIcebergMetadata.formatVersion(),
                        newIcebergMetadata.tableUuid(),
                        newIcebergMetadata.location(),
                        newIcebergMetadata.currentSnapshotId(),
                        newIcebergMetadata.lastColumnId(),
                        shiftedIcebergSchemas,
                        shiftedCurrentSchemaId,
                        newIcebergMetadata.partitionSpecs(),
                        newIcebergMetadata.lastPartitionId(),
                        newIcebergMetadata.snapshots().stream()
                                .map(
                                        s ->
                                                new IcebergSnapshot(
                                                        s.sequenceNumber(),
                                                        s.snapshotId(),
                                                        s.parentSnapshotId(),
                                                        s.timestampMs(),
                                                        s.summary(),
                                                        s.manifestList(),
                                                        s.schemaId() + 1,
                                                        s.firstRowId(),
                                                        s.addedRows()))
                                .collect(Collectors.toList()),
                        newIcebergMetadata.currentSnapshotId(),
                        newIcebergMetadata.refs());
        TableMetadata shiftedTableMetadata =
                TableMetadataParser.fromJson(shiftedForConversion.toJson());
        Map<Integer, IcebergSchema> shiftedById =
                shiftedIcebergSchemas.stream()
                        .collect(Collectors.toMap(IcebergSchema::schemaId, s -> s));

        // Step 2 — Deduplicate using sameSchema(): keeps first occurrence (insertion order
        // preserved by LinkedHashMap), remaps duplicates to the surviving schema ID.
        // Maps each shifted schema ID → the surviving (deduped) schema ID (before renumbering).
        LinkedHashMap<Integer, Schema> survivingById = new LinkedHashMap<>();
        Map<Integer, Integer> schemaIdRemap = new HashMap<>();

        for (Schema schema : shiftedTableMetadata.schemas()) {
            int shiftedId = schema.schemaId();
            Integer survivingId =
                    survivingById.keySet().stream()
                            .filter(sid -> survivingById.get(sid).sameSchema(schema))
                            .findFirst()
                            .orElse(null);
            if (survivingId != null) {
                // Duplicate: remap this ID to the first schema with the same structure
                schemaIdRemap.put(shiftedId, survivingId);
            } else {
                // First occurrence: keep it
                survivingById.put(shiftedId, schema);
                schemaIdRemap.put(shiftedId, shiftedId);
            }
        }

        // Step 2 — Renumber: after dedup, there may be gaps in the schema ID sequence
        // (e.g. [1, 2, 4] when schema 3 was deduped into 2). Iceberg's addSchema() assigns
        // IDs sequentially as max(existing) + 1, so it would produce [1, 2, 3] — not [1, 2, 4].
        // Calling setCurrentSchema(4) would then fail with "unknown schema: 4".
        // We renumber the surviving schemas to 1, 2, 3, ... and update the remap table so
        // that currentSchemaId and snapshot schemaId references stay consistent.
        Map<Integer, Integer> renumberMap = new HashMap<>(); // old deduped ID → new sequential ID
        int nextId = 1;
        List<IcebergSchema> schemas = new ArrayList<>();
        for (int survivingId : survivingById.keySet()) {
            renumberMap.put(survivingId, nextId);
            schemas.add(new IcebergSchema(nextId, shiftedById.get(survivingId).fields()));
            nextId++;
        }
        // Apply renumbering on top of the dedup remap
        schemaIdRemap.replaceAll((k, dedupedId) -> renumberMap.get(dedupedId));

        // 3. Remap currentSchemaId through dedup + renumber
        int currentSchemaId =
                schemaIdRemap.getOrDefault(
                        newIcebergMetadata.currentSchemaId() + 1,
                        newIcebergMetadata.currentSchemaId() + 1);

        // Remap snapshot schema references so they point to surviving schema IDs
        List<IcebergSnapshot> snapshots =
                newIcebergMetadata.snapshots().stream()
                        .map(
                                snapshot -> {
                                    int shiftedSnapshotSchemaId = snapshot.schemaId() + 1;
                                    int remappedSchemaId =
                                            schemaIdRemap.getOrDefault(
                                                    shiftedSnapshotSchemaId,
                                                    shiftedSnapshotSchemaId);
                                    return new IcebergSnapshot(
                                            snapshot.sequenceNumber(),
                                            snapshot.snapshotId(),
                                            snapshot.parentSnapshotId(),
                                            snapshot.timestampMs(),
                                            snapshot.summary(),
                                            snapshot.manifestList(),
                                            remappedSchemaId,
                                            snapshot.firstRowId(),
                                            snapshot.addedRows());
                                })
                        .collect(Collectors.toList());

        return new IcebergMetadata(
                newIcebergMetadata.formatVersion(),
                newIcebergMetadata.tableUuid(),
                newIcebergMetadata.location(),
                newIcebergMetadata.currentSnapshotId(),
                newIcebergMetadata.lastColumnId(),
                schemas,
                currentSchemaId,
                newIcebergMetadata.partitionSpecs(),
                newIcebergMetadata.lastPartitionId(),
                snapshots,
                newIcebergMetadata.currentSnapshotId(),
                newIcebergMetadata.refs());
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
