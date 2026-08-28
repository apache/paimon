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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.CatalogLockFactory;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.factories.Factory;
import org.apache.paimon.factories.FactoryException;
import org.apache.paimon.factories.FactoryUtil;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.iceberg.manifest.IcebergConversions;
import org.apache.paimon.iceberg.manifest.IcebergDataFileMeta;
import org.apache.paimon.iceberg.manifest.IcebergManifestEntry;
import org.apache.paimon.iceberg.manifest.IcebergManifestFile;
import org.apache.paimon.iceberg.manifest.IcebergManifestFileMeta;
import org.apache.paimon.iceberg.manifest.IcebergManifestList;
import org.apache.paimon.iceberg.manifest.IcebergPartitionSummary;
import org.apache.paimon.iceberg.metadata.IcebergDataField;
import org.apache.paimon.iceberg.metadata.IcebergMetadata;
import org.apache.paimon.iceberg.metadata.IcebergPartitionField;
import org.apache.paimon.iceberg.metadata.IcebergPartitionSpec;
import org.apache.paimon.iceberg.metadata.IcebergRef;
import org.apache.paimon.iceberg.metadata.IcebergSchema;
import org.apache.paimon.iceberg.metadata.IcebergSnapshot;
import org.apache.paimon.iceberg.metadata.IcebergSnapshotSummary;
import org.apache.paimon.index.DeletionVectorMeta;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaValidation;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitCallback;
import org.apache.paimon.table.sink.CommitPreCallback;
import org.apache.paimon.table.sink.TagCallback;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.RawFile;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.tag.Tag;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeDefaultVisitor;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.VariantType;
import org.apache.paimon.types.VectorType;
import org.apache.paimon.utils.DataFilePathFactories;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.ManifestReadThreadPool;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;

/**
 * A {@link CommitCallback} to create Iceberg compatible metadata, so Iceberg readers can read
 * Paimon's {@link RawFile}.
 */
public class IcebergCommitCallback implements CommitCallback, CommitPreCallback, TagCallback {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergCommitCallback.class);

    // see org.apache.iceberg.hadoop.Util
    private static final String VERSION_HINT_FILENAME = "version-hint.text";

    private static final String PUFFIN_FORMAT = "puffin";

    // Snapshot summary metric keys
    private static final String SNAPSHOT_SUMMARY_ADDED_DATA_FILES = "added-data-files";
    private static final String SNAPSHOT_SUMMARY_ADDED_RECORDS = "added-records";
    private static final String SNAPSHOT_SUMMARY_ADDED_FILES_SIZE = "added-files-size";
    private static final String SNAPSHOT_SUMMARY_DELETED_DATA_FILES = "deleted-data-files";
    private static final String SNAPSHOT_SUMMARY_DELETED_RECORDS = "deleted-records";
    private static final String SNAPSHOT_SUMMARY_REMOVED_FILES_SIZE = "removed-files-size";
    private static final String SNAPSHOT_SUMMARY_CHANGED_PARTITION_COUNT =
            "changed-partition-count";
    private static final String SNAPSHOT_SUMMARY_TOTAL_RECORDS = "total-records";
    private static final String SNAPSHOT_SUMMARY_TOTAL_DATA_FILES = "total-data-files";
    private static final String SNAPSHOT_SUMMARY_TOTAL_FILES_SIZE = "total-files-size";
    private static final String SNAPSHOT_SUMMARY_TOTAL_DELETE_FILES = "total-delete-files";
    private static final String SNAPSHOT_SUMMARY_TOTAL_POSITION_DELETES = "total-position-deletes";
    private static final String SNAPSHOT_SUMMARY_TOTAL_EQUALITY_DELETES = "total-equality-deletes";

    private final FileStoreTable table;
    private final String commitUser;

    private final IcebergPathFactory pathFactory;
    private final @Nullable IcebergMetadataCommitter metadataCommitter;

    private final FileStorePathFactory fileStorePathFactory;
    private final IcebergManifestFile manifestFile;
    private final IcebergManifestList manifestList;
    // see readManifestListWithFallback
    private IcebergManifestList legacyManifestList;
    private final int formatVersion;

    private final IndexFileHandler indexFileHandler;
    private final boolean needAddDvToIceberg;

    // -------------------------------------------------------------------------------------
    // Public interface
    // -------------------------------------------------------------------------------------

    public IcebergCommitCallback(FileStoreTable table, String commitUser) {
        this.table = table;
        this.commitUser = commitUser;

        IcebergOptions.StorageType storageType =
                table.coreOptions().toConfiguration().get(IcebergOptions.METADATA_ICEBERG_STORAGE);
        this.pathFactory = new IcebergPathFactory(catalogTableMetadataPath(table));

        IcebergMetadataCommitterFactory metadataCommitterFactory;
        try {
            metadataCommitterFactory =
                    FactoryUtil.discoverFactory(
                            IcebergCommitCallback.class.getClassLoader(),
                            IcebergMetadataCommitterFactory.class,
                            storageType.committerFactoryIdentifier());
        } catch (FactoryException e) {
            metadataCommitterFactory = null;
            // storage types without a committer have no factory by design, so a miss is expected
            if (storageType.requiresMetadataCommitter()) {
                LOG.warn(
                        "No IcebergMetadataCommitterFactory for '{}={}' found on the classpath, so "
                                + "table {} will not be synced to the external catalog (commits and "
                                + "metadata files are unaffected). Check that the module providing it "
                                + "is deployed and that its META-INF/services/{} entry survived "
                                + "shading. Cause: {}",
                        IcebergOptions.METADATA_ICEBERG_STORAGE.key(),
                        storageType,
                        table.fullName(),
                        Factory.class.getName(),
                        e.getMessage());
            }
        }
        this.metadataCommitter =
                metadataCommitterFactory == null ? null : metadataCommitterFactory.create(table);

        this.fileStorePathFactory = table.store().pathFactory();
        this.manifestFile = IcebergManifestFile.create(table, pathFactory);
        this.manifestList = IcebergManifestList.create(table, pathFactory);

        this.formatVersion =
                table.coreOptions().toConfiguration().get(IcebergOptions.FORMAT_VERSION);
        checkSupportedFormatVersion(formatVersion);
        // schema and manifest-legacy config are checked in preflightSchemas, not here, so abort()
        // (which emits no Iceberg metadata) is not blocked

        this.indexFileHandler = table.store().newIndexFileHandler();
        this.needAddDvToIceberg = needAddDvToIceberg();
    }

    /** Creates an instance for tag-only use ({@link #notifyCreation} / {@link #notifyDeletion}). */
    public static IcebergCommitCallback forTagCallbacks(FileStoreTable table) {
        return new IcebergCommitCallback(table, "");
    }

    /**
     * Rejects a schema the Iceberg mirror could not represent. A no-op when compatibility is off.
     */
    public static void checkSchemaMirrorable(Options options, RowType rowType) {
        if (options.get(IcebergOptions.METADATA_ICEBERG_STORAGE)
                == IcebergOptions.StorageType.DISABLED) {
            return;
        }
        // geospatial has its own rule upstream; the gates here reach paths it does not
        SchemaValidation.validateIcebergGeospatialTypes(rowType, new CoreOptions(options.toMap()));
        int formatVersion = options.get(IcebergOptions.FORMAT_VERSION);
        checkSupportedFormatVersion(formatVersion);
        Preconditions.checkArgument(
                formatVersion < IcebergMetadata.FORMAT_VERSION_V3
                        || !options.get(IcebergOptions.MANIFEST_LEGACY_VERSION),
                "'%s' cannot be used with Iceberg format version 3: the legacy manifest "
                        + "schema cannot carry the first_row_id field required by v3 row lineage.",
                IcebergOptions.MANIFEST_LEGACY_VERSION.key());
        checkFormatVersionSupportsSchema(formatVersion, rowType);
        // the version rules say what is accepted; the conversion is the authority on what can be
        // expressed at all (precisions out of range, blobs, vectors)
        for (DataField field : rowType.getFields()) {
            new IcebergDataField(field);
        }
    }

    /**
     * The newest schema this commit can publish: the latest one when it is representable, else the
     * newest representable one below it. The snapshot is already durable here, so an unsupported
     * schema that slipped past the pre-commit check degrades instead of failing the commit.
     */
    private int publishableSchemaId(SchemaCache schemaCache, long latestSchemaId) {
        for (long id = latestSchemaId; id >= 0; id--) {
            try {
                schemaCache.getValidated(id);
                return (int) id;
            } catch (IllegalArgumentException | UnsupportedOperationException notRepresentable) {
            }
        }
        throw new IllegalStateException(
                "No schema of this table can be represented in Iceberg metadata; "
                        + "Iceberg compatibility cannot mirror it.");
    }

    /**
     * The schema a snapshot entry points at: always its own. Labelling its files with a different
     * one would make Iceberg read them under fields they were never written with.
     */
    /**
     * Refuses a snapshot whose own schema cannot be emitted, before any manifest is written:
     * rejecting it after the writes would orphan them on every retry.
     */
    private void checkSnapshotSchemaEmittable(
            SchemaCache schemaCache, long snapshotSchemaId, int emittedUpTo) {
        boolean emittable = snapshotSchemaId <= emittedUpTo;
        if (emittable) {
            try {
                schemaCache.getValidated(snapshotSchemaId);
            } catch (IllegalArgumentException | UnsupportedOperationException notRepresentable) {
                emittable = false;
            }
        }
        Preconditions.checkState(
                emittable,
                "Snapshot schema %s cannot be represented in Iceberg metadata, so the snapshot "
                        + "cannot be mirrored. Remove the unsupported fields, or disable Iceberg "
                        + "compatibility for this table.",
                snapshotSchemaId);
    }

    private static int provenanceSchemaId(List<IcebergSchema> emitted, int snapshotSchemaId) {
        Preconditions.checkState(
                emitted.stream().anyMatch(schema -> schema.schemaId() == snapshotSchemaId),
                "Snapshot schema %s cannot be represented in Iceberg metadata, so the snapshot "
                        + "cannot be mirrored. Remove the unsupported fields, or disable Iceberg "
                        + "compatibility for this table.",
                snapshotSchemaId);
        return snapshotSchemaId;
    }

    /**
     * Rejects a schema change the Iceberg mirror cannot represent before it is persisted. Under
     * mirroring only what the change introduces is judged, so a table already carrying an
     * unsupported field can evolve away from it; switching mirroring on judges the whole schema.
     */
    public static void checkSchemaChangeMirrorable(TableSchema oldSchema, TableSchema newSchema) {
        checkSchemaChangeMirrorable(
                Options.fromMap(oldSchema.options()),
                oldSchema.fields(),
                Options.fromMap(newSchema.options()),
                newSchema.fields());
    }

    /** As above, for callers that hold the parts rather than a persisted {@link TableSchema}. */
    public static void checkSchemaChangeMirrorable(
            Options oldOptions,
            List<DataField> oldSchemaFields,
            Options newOptions,
            List<DataField> newSchemaFields) {
        if (newOptions.get(IcebergOptions.METADATA_ICEBERG_STORAGE)
                == IcebergOptions.StorageType.DISABLED) {
            return;
        }
        if (oldOptions.get(IcebergOptions.METADATA_ICEBERG_STORAGE)
                == IcebergOptions.StorageType.DISABLED) {
            // switching mirroring on starts mirroring every field, not only what changed
            checkSchemaMirrorable(newOptions, new RowType(newSchemaFields));
            return;
        }
        int formatVersion = newOptions.get(IcebergOptions.FORMAT_VERSION);
        if (!Objects.equals(
                        oldOptions.get(IcebergOptions.FORMAT_VERSION),
                        newOptions.get(IcebergOptions.FORMAT_VERSION))
                || !Objects.equals(
                        oldOptions.get(IcebergOptions.MANIFEST_LEGACY_VERSION),
                        newOptions.get(IcebergOptions.MANIFEST_LEGACY_VERSION))) {
            checkSupportedFormatVersion(formatVersion);
            Preconditions.checkArgument(
                    formatVersion >= oldOptions.get(IcebergOptions.FORMAT_VERSION),
                    "Iceberg format version cannot be lowered from %s to %s: metadata already "
                            + "written at the higher version cannot be extended by a lower one.",
                    oldOptions.get(IcebergOptions.FORMAT_VERSION),
                    formatVersion);
            Preconditions.checkArgument(
                    formatVersion < IcebergMetadata.FORMAT_VERSION_V3
                            || !newOptions.get(IcebergOptions.MANIFEST_LEGACY_VERSION),
                    "'%s' cannot be used with Iceberg format version 3: the legacy manifest "
                            + "schema cannot carry the first_row_id field required by v3 row lineage.",
                    IcebergOptions.MANIFEST_LEGACY_VERSION.key());
        }
        Map<Integer, DataType> oldFields = new HashMap<>();
        for (DataField field : oldSchemaFields) {
            oldFields.put(field.id(), field.type());
        }
        List<DataField> introduced = new ArrayList<>();
        for (DataField field : newSchemaFields) {
            collectIntroduced(
                    oldFields.get(field.id()), field.type(), field.name(), field.id(), introduced);
        }
        if (introduced.isEmpty()) {
            return;
        }
        checkFormatVersionSupportsSchema(formatVersion, new RowType(introduced));
        for (DataField field : introduced) {
            new IcebergDataField(field);
        }
    }

    /**
     * The part of {@code updated} this change actually introduces, or null when it carries nothing
     * new. Nested types are compared by field id, so adding a supported field next to an
     * unsupported sibling does not drag the sibling back into validation.
     */
    /** As above, collecting into {@code introduced} rather than returning. */
    private static void collectIntroduced(
            @Nullable DataType existing,
            DataType updated,
            String path,
            int id,
            List<DataField> introduced) {
        if (existing == null) {
            introduced.add(new DataField(id, path, updated));
            return;
        }
        if (existing.equals(updated)) {
            return;
        }
        if (existing instanceof RowType && updated instanceof RowType) {
            Map<Integer, DataType> existingFields = new HashMap<>();
            for (DataField field : ((RowType) existing).getFields()) {
                existingFields.put(field.id(), field.type());
            }
            for (DataField field : ((RowType) updated).getFields()) {
                collectIntroduced(
                        existingFields.get(field.id()),
                        field.type(),
                        path + "." + field.name(),
                        field.id(),
                        introduced);
            }
            return;
        }
        if (existing instanceof ArrayType && updated instanceof ArrayType) {
            collectIntroduced(
                    ((ArrayType) existing).getElementType(),
                    ((ArrayType) updated).getElementType(),
                    path + ".element",
                    id,
                    introduced);
            return;
        }
        if (existing instanceof MultisetType && updated instanceof MultisetType) {
            collectIntroduced(
                    ((MultisetType) existing).getElementType(),
                    ((MultisetType) updated).getElementType(),
                    path + ".element",
                    id,
                    introduced);
            return;
        }
        if (existing instanceof MapType && updated instanceof MapType) {
            collectIntroduced(
                    ((MapType) existing).getKeyType(),
                    ((MapType) updated).getKeyType(),
                    path + ".key",
                    id,
                    introduced);
            collectIntroduced(
                    ((MapType) existing).getValueType(),
                    ((MapType) updated).getValueType(),
                    path + ".value",
                    id,
                    introduced);
            return;
        }
        introduced.add(new DataField(id, path, updated));
    }

    /**
     * Refuses re-enabling mirroring at a format version below one this table already published: the
     * metadata written back then cannot be extended by a lower version.
     */
    public static void checkNoFormatVersionRegression(
            Options newOptions, List<TableSchema> history) {
        if (newOptions.get(IcebergOptions.METADATA_ICEBERG_STORAGE)
                == IcebergOptions.StorageType.DISABLED) {
            return;
        }
        checkNoFormatVersionRegressionOnRestore(newOptions, history);
    }

    /**
     * The same rule, applied even when the schema being installed disables mirroring: fast-forward
     * and rollback delete the history that records what was published, so the evidence has to be
     * checked before it is gone.
     */
    public static void checkNoFormatVersionRegressionOnRestore(
            Options newOptions, List<TableSchema> history) {
        int formatVersion = newOptions.get(IcebergOptions.FORMAT_VERSION);
        for (TableSchema past : history) {
            Options pastOptions = Options.fromMap(past.options());
            if (pastOptions.get(IcebergOptions.METADATA_ICEBERG_STORAGE)
                    == IcebergOptions.StorageType.DISABLED) {
                continue;
            }
            Preconditions.checkArgument(
                    formatVersion >= pastOptions.get(IcebergOptions.FORMAT_VERSION),
                    "Iceberg format version cannot be lowered from %s to %s: metadata already "
                            + "written at the higher version cannot be extended by a lower one.",
                    pastOptions.get(IcebergOptions.FORMAT_VERSION),
                    formatVersion);
        }
    }

    static void checkSupportedFormatVersion(int formatVersion) {
        Preconditions.checkArgument(
                formatVersion == IcebergMetadata.FORMAT_VERSION_V2
                        || formatVersion == IcebergMetadata.FORMAT_VERSION_V3,
                "Unsupported iceberg format version! Only version 2 or version 3 is valid, but current version is %s.",
                formatVersion);
    }

    static void checkFormatVersionSupportsSchema(int formatVersion, RowType rowType) {
        RestrictedTypeCollector collector = new RestrictedTypeCollector();
        rowType.accept(collector);
        throwOnNanosecondTimestamps(collector.nanosTimestamps);
        throwOnV3OnlyTypes(collector.v3OnlyTypes, formatVersion);
    }

    /**
     * Collects the types the Iceberg mirror cannot emit, with the path of each offending field.
     * Leaf types not named here are representable, or rejected later by the type conversion itself
     * ({@link IcebergDataField#toTypeString}).
     */
    private static class RestrictedTypeCollector extends DataTypeDefaultVisitor<Void> {

        private final Collection<String> nanosTimestamps = new LinkedHashSet<>();
        private final Collection<String> v3OnlyTypes = new LinkedHashSet<>();
        private final Deque<String> path = new ArrayDeque<>();

        private Void descend(String name, DataType type) {
            path.addLast(name);
            type.accept(this);
            path.removeLast();
            return null;
        }

        private String currentPath(DataType type) {
            return String.join(".", path) + ": " + type.asSQLString();
        }

        @Override
        public Void visit(VariantType variantType) {
            v3OnlyTypes.add(currentPath(variantType));
            return null;
        }

        @Override
        public Void visit(TimestampType timestampType) {
            if (timestampType.getPrecision() >= IcebergDataField.MIN_NANOS_TIMESTAMP_PRECISION) {
                nanosTimestamps.add(currentPath(timestampType));
            }
            return null;
        }

        @Override
        public Void visit(LocalZonedTimestampType localZonedTimestampType) {
            if (localZonedTimestampType.getPrecision()
                    >= IcebergDataField.MIN_NANOS_TIMESTAMP_PRECISION) {
                nanosTimestamps.add(currentPath(localZonedTimestampType));
            }
            return null;
        }

        @Override
        public Void visit(ArrayType arrayType) {
            return descend("element", arrayType.getElementType());
        }

        @Override
        public Void visit(MultisetType multisetType) {
            return descend("element", multisetType.getElementType());
        }

        @Override
        public Void visit(VectorType vectorType) {
            return descend("element", vectorType.getElementType());
        }

        @Override
        public Void visit(MapType mapType) {
            descend("key", mapType.getKeyType());
            return descend("value", mapType.getValueType());
        }

        @Override
        public Void visit(RowType rowType) {
            for (DataField field : rowType.getFields()) {
                descend(field.name(), field.type());
            }
            return null;
        }

        @Override
        protected Void defaultMethod(DataType dataType) {
            return null;
        }
    }

    private static void throwOnNanosecondTimestamps(Collection<String> nanosTimestamps) {
        // Paimon writes a nanosecond timestamp as Parquet INT96, which Iceberg reads as a
        // microsecond zoned timestamp rather than timestamp_ns, so the metadata is unreadable.
        Preconditions.checkArgument(
                nanosTimestamps.isEmpty(),
                "Nanosecond-precision timestamps %s are not supported by Iceberg compatibility "
                        + "because Paimon writes them as Parquet INT96, which Iceberg cannot read "
                        + "as timestamp_ns. Use a timestamp precision of 6 or less.",
                nanosTimestamps);
    }

    // v3-only types stay rejected on every format version: the metadata emitted for format
    // version 3 carries no row lineage yet, so such a table would not be a valid v3 table either
    private static void throwOnV3OnlyTypes(Collection<String> v3OnlyTypes, int formatVersion) {
        Preconditions.checkArgument(
                v3OnlyTypes.isEmpty(),
                "Data types %s are not supported by Iceberg compatibility: they need Iceberg "
                        + "format version 3, whose row lineage this layer does not emit yet "
                        + "(format version %s in use).",
                v3OnlyTypes,
                formatVersion);
    }

    public static Path catalogTableMetadataPath(FileStoreTable table) {
        Path icebergDBPath = catalogDatabasePath(table);
        return new Path(icebergDBPath, String.format("%s/metadata", table.location().getName()));
    }

    public static Path catalogDatabasePath(FileStoreTable table) {
        return catalogDatabasePath(table, resolveStorageLocation(table));
    }

    private static IcebergOptions.StorageLocation resolveStorageLocation(FileStoreTable table) {
        IcebergOptions.StorageType storageType =
                table.coreOptions().toConfiguration().get(IcebergOptions.METADATA_ICEBERG_STORAGE);
        return table.coreOptions()
                .toConfiguration()
                .getOptional(IcebergOptions.METADATA_ICEBERG_STORAGE_LOCATION)
                .orElse(inferDefaultMetadataLocation(storageType));
    }

    private static Path catalogDatabasePath(
            FileStoreTable table, IcebergOptions.StorageLocation storageLocation) {
        Path dbPath = table.location().getParent();
        final String dbSuffix = ".db";
        IcebergOptions.StorageType storageType =
                table.coreOptions().toConfiguration().get(IcebergOptions.METADATA_ICEBERG_STORAGE);

        switch (storageLocation) {
            case TABLE_LOCATION:
                // Iceberg metadata is written beside the table, under the database's own location,
                // so no warehouse (<db>.db) layout is required. This lets the table register in any
                // catalog, including a database whose location is not a Paimon warehouse path (e.g.
                // an externally-provisioned / cross-account catalog database).
                return dbPath;
            case CATALOG_STORAGE:
                // Catalog-storage derives a warehouse-relative iceberg/<db>/ path by stripping the
                // ".db" suffix, so it only applies under the Paimon <db>.db warehouse layout.
                if (!dbPath.getName().endsWith(dbSuffix)) {
                    throw new UnsupportedOperationException(
                            String.format(
                                    "Storage type %s with catalog-location Iceberg metadata requires a "
                                            + "Paimon warehouse database (a <db>.db location); set "
                                            + "metadata.iceberg.storage-location=table-location for a "
                                            + "database with a non-warehouse location.",
                                    storageType.name()));
                }
                String dbName =
                        dbPath.getName()
                                .substring(0, dbPath.getName().length() - dbSuffix.length());
                return new Path(dbPath.getParent(), String.format("iceberg/%s/", dbName));
            default:
                throw new UnsupportedOperationException(
                        "Unknown storage location " + storageLocation.name());
        }
    }

    private static IcebergOptions.StorageLocation inferDefaultMetadataLocation(
            IcebergOptions.StorageType storageType) {
        switch (storageType) {
            case TABLE_LOCATION:
                return IcebergOptions.StorageLocation.TABLE_LOCATION;
            case HIVE_CATALOG:
            case HADOOP_CATALOG:
            case REST_CATALOG:
                return IcebergOptions.StorageLocation.CATALOG_STORAGE;
            default:
                throw new UnsupportedOperationException(
                        "Unknown storage type: " + storageType.name());
        }
    }

    @Override
    public void close() throws Exception {}

    @Override
    public void call(Context context) {
        createMetadata(
                context.snapshot,
                (removedFiles, addedFiles) ->
                        collectFileChanges(context.deltaFiles, removedFiles, addedFiles),
                context.indexFiles);
    }

    @Override
    public void call(
            List<SimpleFileEntry> baseFiles,
            List<ManifestEntry> deltaFiles,
            List<IndexManifestEntry> indexFiles,
            Snapshot snapshot) {
        preflightSchemas(snapshot, deltaFiles);
    }

    /**
     * Validates the schema and partition spec this commit would emit, before the Paimon snapshot is
     * published: throwing here aborts the commit, unlike the post-commit {@link #call(Context)},
     * whose snapshot already exists and can only degrade.
     */
    private void preflightSchemas(Snapshot snapshot, List<ManifestEntry> deltaFiles) {
        try {
            Preconditions.checkArgument(
                    formatVersion < IcebergMetadata.FORMAT_VERSION_V3
                            || !table.coreOptions()
                                    .toConfiguration()
                                    .get(IcebergOptions.MANIFEST_LEGACY_VERSION),
                    "'%s' cannot be used with Iceberg format version 3: the legacy manifest "
                            + "schema cannot carry the first_row_id field required by v3 row lineage.",
                    IcebergOptions.MANIFEST_LEGACY_VERSION.key());
            // judge the base the post-commit rebuild mirrors from, not v(id-1)
            long base = latestMirroredSnapshot(snapshot.id());
            boolean hasUsableBase = false;
            Path baseMetadataPath = pathFactory.toMetadataPath(base);
            if (base >= Snapshot.FIRST_SNAPSHOT_ID && table.fileIO().exists(baseMetadataPath)) {
                // an unreadable base is rebuilt by the post-commit callback; vetoing the data
                // commit over mirror-only state would block writes on something else
                IcebergMetadata baseMetadata = tryReadMetadata(baseMetadataPath);
                if (baseMetadata != null && isSameFormatVersion(baseMetadata.formatVersion())) {
                    hasUsableBase = true;
                }
            }
            // gate the schema this commit's data uses: a concurrent ALTER installing an
            // unsupported one must not veto a commit that does not use it yet
            SchemaCache schemaCache = new SchemaCache();
            schemaCache.getValidated(snapshot.schemaId());
            // a file is mirrored under its own schema, which an ALTER between preparation
            // and commit can make differ from the snapshot's
            for (long fileSchemaId : addedFileSchemaIds(deltaFiles)) {
                schemaCache.getValidated(fileSchemaId);
            }
            if (!hasUsableBase) {
                // without a usable base the partition spec is derived from scratch, where a
                // VARIANT partition key cannot be represented
                checkNoVariantPartitionKeys(
                        table.schema().partitionKeys(), schemaCache.get(snapshot.schemaId()));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Set<Long> addedFileSchemaIds(List<ManifestEntry> deltaFiles) {
        // deletions are mirrored by file path and publish no schema, so removing files written
        // under a since-dropped unsupported schema must not be vetoed
        Set<Long> schemaIds = new LinkedHashSet<>();
        for (ManifestEntry entry : deltaFiles) {
            // only files that will actually be mirrored matter: a primary-key table publishes
            // just the levels shouldAddFileToIceberg accepts
            if (entry.kind() == FileKind.ADD && shouldAddFileToIceberg(entry.file())) {
                schemaIds.add(entry.file().schemaId());
            }
        }
        return schemaIds;
    }

    @Override
    public void retry(ManifestCommittable committable) {
        SnapshotManager snapshotManager = table.snapshotManager();
        Snapshot snapshot =
                snapshotManager
                        .findSnapshotsForIdentifiers(
                                commitUser, Collections.singletonList(committable.identifier()))
                        .stream()
                        .max(Comparator.comparingLong(Snapshot::id))
                        .orElseThrow(
                                () ->
                                        new RuntimeException(
                                                "There is no snapshot for commit user "
                                                        + commitUser
                                                        + " and identifier "
                                                        + committable.identifier()
                                                        + " for table "
                                                        + table.name()
                                                        + ". This is unexpected."));
        long snapshotId = snapshot.id();
        // no veto here: this snapshot is already durable, so an unsupported schema introduced
        // meanwhile must degrade to the newest publishable one, not fail the repair
        createMetadata(
                snapshot,
                (removedFiles, addedFiles) ->
                        collectFileChanges(snapshotId, removedFiles, addedFiles),
                indexFileHandler.scan(snapshot, DELETION_VECTORS_INDEX));
    }

    @Override
    public void setTable(FileStoreTable table) {
        // nothing to do
    }

    private void createMetadata(
            Snapshot snapshot,
            FileChangesCollector fileChangesCollector,
            List<IndexManifestEntry> indexFiles) {
        long snapshotId = snapshot.id();
        try {
            // a stale callback outliving a rollback (or drop and recreate) must do nothing;
            // read the snapshot file directly - snapshot caches may predate the rollback
            Snapshot current;
            try {
                current =
                        SnapshotManager.tryFromPath(
                                table.fileIO(), table.snapshotManager().snapshotPath(snapshotId));
            } catch (FileNotFoundException e) {
                return;
            }
            if (!commitIdentity(current).equals(commitIdentity(snapshot))) {
                return;
            }
            // the snapshot cache may still hold a rolled-back timeline under reused ids;
            // every by-id read below must see the disk state the guard just verified
            table.snapshotManager().invalidateCache();

            if (snapshotId == Snapshot.FIRST_SNAPSHOT_ID) {
                // If Iceberg metadata is stored separately in another directory, dropping the table
                // will not delete old Iceberg metadata. So we delete them here, when the table is
                // created again and the first snapshot is committed.
                table.fileIO().delete(pathFactory.metadataDirectory(), true);
            }

            String abandonedUuid = null;
            int abandonedLastColumnId = 0;
            long abandonedNextRowId = 0;
            if (table.fileIO().exists(pathFactory.toMetadataPath(snapshotId))) {
                if (metadataMatchesSnapshot(snapshotId, snapshot)) {
                    // a retry repairs hint, pointer and files only while this snapshot is
                    // still the head; a replay of an older committable must not move them back
                    Long latestForRepair = table.snapshotManager().latestSnapshotId();
                    if (latestForRepair == null || latestForRepair != snapshotId) {
                        return;
                    }
                    if (readVersionHint() != snapshotId) {
                        table.fileIO()
                                .overwriteFileUtf8(
                                        new Path(
                                                pathFactory.metadataDirectory(),
                                                VERSION_HINT_FILENAME),
                                        String.valueOf(snapshotId));
                    }
                    if (metadataCommitter != null) {
                        // recommit the pointer (previous version as base, so a lagging
                        // catalog advances) before retiring files it may still reference
                        Path existingPath = pathFactory.toMetadataPath(snapshotId);
                        Path basePath = pathFactory.toMetadataPath(snapshotId - 1);
                        IcebergMetadata base =
                                table.fileIO().exists(basePath)
                                        ? IcebergMetadata.fromPath(table.fileIO(), basePath)
                                        : null;
                        commitToExternalCatalog(
                                IcebergMetadata.fromPath(table.fileIO(), existingPath),
                                existingPath,
                                base,
                                base == null ? null : basePath);
                    }
                    // a failed earlier attempt skipped the normal post-publication cleanup
                    deleteApplicableMetadataFiles(snapshotId);
                    retireAbandonedSuffix();
                    table.fileIO()
                            .deleteQuietly(
                                    new Path(
                                            pathFactory.metadataDirectory(),
                                            RETIRE_PENDING_FILENAME));
                    return;
                }
                // a reused snapshot id: only the current head may replace the abandoned
                // metadata; a delayed replay must not move hint or pointer backwards
                Long latestNow = table.snapshotManager().latestSnapshotId();
                if (latestNow == null || latestNow != snapshotId) {
                    return;
                }
                // read the identity now, delete only at the write site: readers and the
                // catalog pointer keep a working file until the replacement is built
                IcebergMetadata abandoned = tryReadMetadata(pathFactory.toMetadataPath(snapshotId));
                if (abandoned != null) {
                    abandonedUuid = abandoned.tableUuid();
                    abandonedLastColumnId = abandoned.lastColumnId();
                    if (abandoned.nextRowId() != null) {
                        abandonedNextRowId = abandoned.nextRowId();
                    }
                }
            }
            // steady-state commits skip the listing; anything suspicious lists the actual
            // files, because the hint alone can lag while readers still probe past it
            Path retirePending = new Path(pathFactory.metadataDirectory(), RETIRE_PENDING_FILENAME);
            boolean suspectRollback =
                    abandonedUuid != null
                            || readVersionHint() != snapshotId - 1
                            || table.fileIO().exists(pathFactory.toMetadataPath(snapshotId + 1))
                            || table.fileIO().exists(retirePending);
            long newestExisting = suspectRollback ? newestExistingMetadataVersion() : -1;
            boolean retireSuffix = abandonedUuid != null || newestExisting > snapshotId;
            if (retireSuffix && newestExisting > snapshotId) {
                // the newest abandoned version carries the authoritative high-water mark
                IcebergMetadata surviving =
                        tryReadMetadata(pathFactory.toMetadataPath(newestExisting));
                if (surviving != null) {
                    if (abandonedUuid == null) {
                        abandonedUuid = surviving.tableUuid();
                    }
                    abandonedLastColumnId =
                            Math.max(abandonedLastColumnId, surviving.lastColumnId());
                    if (surviving.nextRowId() != null) {
                        abandonedNextRowId = Math.max(abandonedNextRowId, surviving.nextRowId());
                    }
                }
            }

            // mirror in snapshot order from the latest already-mirrored metadata: each vK
            // derives its row ids from v(K-1), so no callback reads a stale watermark and two
            // callbacks cannot hand out overlapping ranges
            long base = latestMirroredSnapshot(snapshotId);
            if (base >= Snapshot.FIRST_SNAPSHOT_ID) {
                mirrorFrom(
                        base,
                        snapshot,
                        fileChangesCollector,
                        indexFiles,
                        abandonedLastColumnId,
                        abandonedNextRowId);
            } else {
                // no surviving base: regenerating mints a fresh table uuid, so serialize it —
                // two cold starts would otherwise publish independent chains — and recheck
                // under the lock, since a base or a twin may have appeared meanwhile. Without
                // a catalog lock this is best-effort.
                String inheritUuid = abandonedUuid;
                int lastColumnIdFloor = abandonedLastColumnId;
                long nextRowIdFloor = abandonedNextRowId;
                try (Lock lock = icebergLock()) {
                    lock.runWithLock(
                            () -> {
                                if (table.fileIO().exists(pathFactory.toMetadataPath(snapshotId))
                                        && metadataMatchesSnapshot(snapshotId, snapshot)) {
                                    return null;
                                }
                                long rechecked = latestMirroredSnapshot(snapshotId);
                                if (rechecked >= Snapshot.FIRST_SNAPSHOT_ID) {
                                    mirrorFrom(
                                            rechecked,
                                            snapshot,
                                            fileChangesCollector,
                                            indexFiles,
                                            lastColumnIdFloor,
                                            nextRowIdFloor);
                                } else if (!higherMirroredSnapshotExists(snapshotId)) {
                                    createMetadataWithoutBase(
                                            snapshotId,
                                            inheritUuid,
                                            lastColumnIdFloor,
                                            nextRowIdFloor);
                                }
                                return null;
                            });
                }
            }

            if (retireSuffix) {
                // only after the replacement is durable, so readers keep a working head
                retireAbandonedSuffix();
            }
            if (suspectRollback) {
                // the listing ran and every leftover above the head is gone
                table.fileIO().deleteQuietly(retirePending);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void mirrorFrom(
            long base,
            Snapshot snapshot,
            FileChangesCollector fileChangesCollector,
            List<IndexManifestEntry> indexFiles,
            int lastColumnIdFloor,
            long nextRowIdFloor)
            throws IOException {
        long snapshotId = snapshot.id();
        for (long id = base + 1; id <= snapshotId; id++) {
            if (table.fileIO().exists(pathFactory.toMetadataPath(id))) {
                // only a genuine twin of the target may be skipped; an abandoned twin is
                // replaced at the write site, so readers keep a working file meanwhile
                if (id == snapshotId
                        ? metadataMatchesSnapshot(id, snapshot)
                        : usableMirrorBase(id)) {
                    continue;
                }
                if (id != snapshotId) {
                    // a dead survivor mid-chain (its list is gone, or it sits on a rolled-back
                    // timeline) blocks this walk and every future one
                    table.fileIO().deleteQuietly(pathFactory.toMetadataPath(id));
                }
            }
            long mirrorId = id;
            Snapshot toMirror = id == snapshotId ? snapshot : table.snapshotManager().snapshot(id);
            FileChangesCollector collector =
                    id == snapshotId
                            ? fileChangesCollector
                            : (removed, added) -> collectFileChanges(mirrorId, removed, added);
            // a replayed predecessor may sit past an expired base, so its dv state cannot be
            // diffed against a predecessor scan; rebuild it from the snapshot's own live state
            boolean replay = id != snapshotId;
            createMetadataWithBase(
                    collector,
                    replay ? Collections.emptyList() : deletionVectorIndexes(indexFiles),
                    toMirror,
                    pathFactory.toMetadataPath(id - 1),
                    lastColumnIdFloor,
                    nextRowIdFloor,
                    replay);
        }
    }

    /** A lock serializing base-less regeneration, or an empty lock when the catalog has none. */
    private Lock icebergLock() {
        CatalogEnvironment env = table.catalogEnvironment();
        CatalogLockFactory lockFactory = env.lockFactory();
        if (lockFactory == null || env.identifier() == null) {
            return Lock.empty();
        }
        return Lock.fromCatalog(lockFactory.createLock(env.lockContext()), env.identifier());
    }

    /**
     * Whether a snapshot above {@code target} already has Iceberg metadata (a newer rebuilt chain).
     */
    private boolean higherMirroredSnapshotExists(long target) throws IOException {
        Long latest = table.snapshotManager().latestSnapshotId();
        if (latest == null) {
            return false;
        }
        for (long k = latest; k > target; k--) {
            if (table.fileIO().exists(pathFactory.toMetadataPath(k))) {
                return true;
            }
        }
        return false;
    }

    /**
     * Highest snapshot {@code K < target} whose Iceberg metadata still exists and whose successors
     * up to {@code target} still have Paimon snapshots to rebuild from, or {@code -1} when no such
     * base chain survives (cold start, or history expired past what can be reconstructed).
     */
    private long latestMirroredSnapshot(long target) throws IOException {
        SnapshotManager snapshotManager = table.snapshotManager();
        // target is the snapshot being committed (preflight) or just committed (rebuild); it is
        // always present, so only the snapshots strictly between the base and target need to exist
        for (long k = target - 1; k >= Snapshot.FIRST_SNAPSHOT_ID; k--) {
            if (usableMirrorBase(k)) {
                for (long id = k + 1; id < target; id++) {
                    if (!snapshotManager.snapshotExists(id)) {
                        return -1;
                    }
                }
                return k;
            }
            // json retention is separate from list expiry, so the highest survivor may be
            // unreadable as a base; chaining from it would fail every retry
            if (!snapshotManager.snapshotExists(k)) {
                return -1;
            }
        }
        return -1;
    }

    /**
     * Whether snapshot {@code k}'s metadata can serve as a mirror base: its json parses and the
     * manifest list it points at still exists.
     */
    private boolean usableMirrorBase(long k) throws IOException {
        if (!table.fileIO().exists(pathFactory.toMetadataPath(k))) {
            return false;
        }
        IcebergMetadata metadata;
        try {
            metadata = IcebergMetadata.fromPath(table.fileIO(), pathFactory.toMetadataPath(k));
        } catch (Exception e) {
            // condemning a survivor takes positive evidence (absent, corrupt, wrong timeline);
            // a transient read failure fails the walk instead of retiring a live base
            if (transientReadFailure(e)) {
                throw new IOException(e);
            }
            return false;
        }
        if (metadata.currentSnapshot() == null
                || !table.fileIO().exists(new Path(metadata.currentSnapshot().manifestList()))) {
            return false;
        }
        // a survivor from a rolled-back timeline must not seed the chain: while the live
        // snapshot k is still around, the metadata has to carry its commit identity
        if (table.snapshotManager().snapshotExists(k)) {
            return metadataMatchesSnapshot(metadata, table.snapshotManager().snapshot(k));
        }
        // once snapshot k expired the identity is uncheckable: a pending-rollback marker means
        // the survivor may sit on an abandoned timeline, so only a full rebuild is safe
        return !table.fileIO()
                .exists(new Path(pathFactory.metadataDirectory(), RETIRE_PENDING_FILENAME));
    }

    private static boolean transientReadFailure(Exception e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof FileNotFoundException || t instanceof JsonProcessingException) {
                return false;
            }
        }
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof IOException) {
                return true;
            }
        }
        return false;
    }

    private static List<IndexManifestEntry> deletionVectorIndexes(
            List<IndexManifestEntry> indexFiles) {
        return indexFiles.stream()
                .filter(index -> index.indexFile().indexType().equals(DELETION_VECTORS_INDEX))
                .collect(Collectors.toList());
    }

    // -------------------------------------------------------------------------------------
    // Create metadata afresh
    // -------------------------------------------------------------------------------------

    private void createMetadataWithoutBase(long snapshotId) throws IOException {
        createMetadataWithoutBase(snapshotId, null, 0, 0L);
    }

    private void createMetadataWithoutBase(long snapshotId, @Nullable String inheritUuid)
            throws IOException {
        createMetadataWithoutBase(snapshotId, inheritUuid, 0, 0L);
    }

    private void createMetadataWithoutBase(
            long snapshotId,
            @Nullable String inheritUuid,
            int lastColumnIdFloor,
            long nextRowIdFloor)
            throws IOException {
        SnapshotReader snapshotReader = table.newSnapshotReader().withSnapshot(snapshotId);
        Snapshot paimonSnapshot = table.snapshotManager().snapshot(snapshotId);
        // decide the schema story before any manifest is written: an ALTER between the
        // preflight and this callback would otherwise orphan everything written meanwhile
        SchemaCache schemaCache = new SchemaCache();
        int schemaId = publishableSchemaId(schemaCache, schemaCache.getLatestSchemaId());
        checkSnapshotSchemaEmittable(schemaCache, paimonSnapshot.schemaId(), schemaId);
        IcebergSchema icebergSchema = schemaCache.get(schemaId);
        List<IcebergPartitionField> partitionFields =
                getPartitionFields(table.schema().partitionKeys(), icebergSchema);
        List<IcebergSchema> allSchemas = new ArrayList<>();
        for (int id = 0; id <= schemaId; id++) {
            if (id == schemaId) {
                allSchemas.add(icebergSchema);
                continue;
            }
            try {
                allSchemas.add(schemaCache.getValidated(id));
            } catch (IllegalArgumentException | UnsupportedOperationException notRepresentable) {
                // only a representability verdict is evidence: any other failure (a missing or
                // malformed schema file, a flaky read) must not be mistaken for a schema that
                // was never mirrored, or the snapshot silently loses its schema provenance
            }
        }
        int snapshotSchemaId = provenanceSchemaId(allSchemas, (int) paimonSnapshot.schemaId());
        List<IcebergManifestEntry> dataFileEntries = new ArrayList<>();
        List<IcebergManifestEntry> dvFileEntries = new ArrayList<>();
        SummaryMetrics metrics = new SummaryMetrics();
        Set<BinaryRow> changedPartitions = new HashSet<>();

        List<DataSplit> filteredDataSplits =
                snapshotReader.read().dataSplits().stream()
                        .filter(DataSplit::rawConvertible)
                        .collect(Collectors.toList());
        for (DataSplit dataSplit : filteredDataSplits) {
            changedPartitions.add(dataSplit.partition());
            dataSplitToManifestEntries(
                    dataSplit, snapshotId, schemaCache, dataFileEntries, dvFileEntries);

            for (DataFileMeta paimonFileMeta : dataSplit.dataFiles()) {
                metrics.addedDataFiles++;
                metrics.addedRecords += paimonFileMeta.rowCount();
                metrics.addedFilesSize += paimonFileMeta.fileSize();
            }
        }

        List<IcebergManifestFileMeta> dataManifestFileMetas = new ArrayList<>();
        if (!dataFileEntries.isEmpty()) {
            dataManifestFileMetas.addAll(
                    manifestFile.rollingWrite(dataFileEntries.iterator(), snapshotId));
        }

        List<IcebergManifestFileMeta> dvManifestFileMetas = new ArrayList<>();
        if (!dvFileEntries.isEmpty()) {
            dvManifestFileMetas.addAll(
                    manifestFile.rollingWrite(
                            dvFileEntries.iterator(),
                            snapshotId,
                            IcebergManifestFileMeta.Content.DELETES));
        }

        List<IcebergManifestFileMeta> allManifestFileMetas = new ArrayList<>();
        allManifestFileMetas.addAll(dataManifestFileMetas);
        allManifestFileMetas.addAll(dvManifestFileMetas);

        metrics.changedPartitionCount = changedPartitions.size();
        metrics.totalDataFiles = metrics.addedDataFiles;
        metrics.deletedDataFiles = 0;
        metrics.deletedRecords = 0;
        metrics.deletedFilesSize = 0;
        metrics.totalRecords = metrics.addedRecords;
        metrics.totalFilesSize = metrics.addedFilesSize;
        long totalDeleteFiles = dvFileEntries.stream().filter(IcebergManifestEntry::isLive).count();
        long totalPositionDeleteRecords =
                dvFileEntries.stream()
                        .filter(IcebergManifestEntry::isLive)
                        .mapToLong(entry -> entry.file().recordCount())
                        .sum();
        metrics.totalDeleteFiles = totalDeleteFiles;
        metrics.totalPositionDeletes = totalPositionDeleteRecords;
        metrics.totalEqualityDeletes = 0;

        // a rebuild replaces metadata whose ids are already out with readers: never reuse them
        Long snapshotFirstRowId = computeSnapshotFirstRowId(nextRowIdFloor);
        ManifestRowIdAssignment rowIdAssignment =
                assignManifestFirstRowIds(allManifestFileMetas, snapshotFirstRowId);
        allManifestFileMetas = rowIdAssignment.manifests;
        Long addedRows = snapshotFirstRowId == null ? null : rowIdAssignment.assignedRows;
        Long nextRowId =
                snapshotFirstRowId == null
                        ? null
                        : snapshotFirstRowId + rowIdAssignment.assignedRows;
        String manifestListFileName = manifestList.writeWithoutRolling(allManifestFileMetas);

        IcebergSnapshotSummary snapshotSummary =
                computeSnapshotSummary(
                        IcebergSnapshotSummary.APPEND.operation(), paimonSnapshot, metrics);

        IcebergSnapshot snapshot =
                new IcebergSnapshot(
                        snapshotId,
                        snapshotId,
                        snapshotId == Snapshot.FIRST_SNAPSHOT_ID ? null : (Long) (snapshotId - 1),
                        // the Paimon snapshot's own commit time, the as-of time readers see
                        paimonSnapshot.timeMillis(),
                        snapshotSummary,
                        pathFactory.toManifestListPath(manifestListFileName).toString(),
                        snapshotSchemaId,
                        snapshotFirstRowId,
                        addedRows);

        // Tags can only be included in Iceberg if they point to an Iceberg snapshot that
        // exists. Otherwise, an Iceberg client fails to parse the metadata and all reads fail.
        // Only the latest snapshot ID is added to Iceberg in this code path. Since this snapshot
        // has just been committed to Paimon, it is not possible for any Paimon tag to reference it
        // yet.
        // After https://github.com/apache/paimon/issues/6107 we can add tags here.
        Map<String, IcebergRef> refs = new HashMap<>();

        // keep the identity of the metadata this rebuild replaces, so already loaded readers
        // and external catalogs keep refreshing the same table
        String tableUuid = inheritUuid != null ? inheritUuid : UUID.randomUUID().toString();

        IcebergMetadata metadata =
                new IcebergMetadata(
                        formatVersion,
                        tableUuid,
                        table.location().toString(),
                        snapshotId,
                        // every emitted schema counts, and a rebuild must not regress
                        // below the replaced metadata's high-water mark
                        Math.max(
                                lastColumnIdFloor,
                                allSchemas.stream()
                                        .mapToInt(IcebergSchema::highestFieldId)
                                        .max()
                                        .orElse(icebergSchema.highestFieldId())),
                        allSchemas,
                        schemaId,
                        Collections.singletonList(new IcebergPartitionSpec(partitionFields)),
                        partitionFields.stream()
                                .mapToInt(IcebergPartitionField::fieldId)
                                .max()
                                .orElse(
                                        // not sure why, this is a result tested by hand
                                        IcebergPartitionField.FIRST_FIELD_ID - 1),
                        Collections.singletonList(snapshot),
                        (int) snapshotId,
                        nextRowId,
                        refs);

        Path metadataPath = pathFactory.toMetadataPath(snapshotId);
        // atomic-first: where rename overwrites, a stale twin is replaced with no window at
        // all; otherwise fall back to delete-then-write, the smallest window available
        boolean written = table.fileIO().tryToWriteAtomic(metadataPath, metadata.toJson());
        if (!written
                && table.fileIO().exists(metadataPath)
                && !metadataMatchesSnapshot(snapshotId, paimonSnapshot)) {
            table.fileIO().deleteQuietly(metadataPath);
            written = table.fileIO().tryToWriteAtomic(metadataPath, metadata.toJson());
        }
        if (!written) {
            // decided on one read, like the with-base path above
            if (metadataMatchesSnapshot(snapshotId, paimonSnapshot)) {
                // a concurrent callback published this version first; adopt it rather than hand
                // a divergent twin to the external catalog, and leave expiry to the winner
                metadata = IcebergMetadata.fromPath(table.fileIO(), metadataPath);
            } else {
                // no twin published this snapshot's metadata; fail so the commit retries
                throw new IllegalStateException(
                        "Failed to replace Iceberg metadata " + metadataPath);
            }
        }
        // a delayed callback may still write its metadata (a newer commit extends it), but
        // only the current head may move the hint and the external catalog
        Long latestAtPublish = table.snapshotManager().latestSnapshotId();
        if (latestAtPublish != null && latestAtPublish == snapshotId) {
            table.fileIO()
                    .overwriteFileUtf8(
                            new Path(pathFactory.metadataDirectory(), VERSION_HINT_FILENAME),
                            String.valueOf(snapshotId));
            commitToExternalCatalog(metadata, metadataPath, null, null);
            // cleanup only after the catalog serves the new head, and only by the writer: a
            // skipped or failed publication must not delete files a pointer still references
            if (written) {
                expireAllBefore(snapshotId);
            }
        }
    }

    private void dataSplitToManifestEntries(
            DataSplit dataSplit,
            long snapshotId,
            SchemaCache schemaCache,
            List<IcebergManifestEntry> dataFileEntries,
            List<IcebergManifestEntry> dvFileEntries) {
        List<RawFile> rawFiles = dataSplit.convertToRawFiles().get();

        for (int i = 0; i < dataSplit.dataFiles().size(); i++) {
            DataFileMeta paimonFileMeta = dataSplit.dataFiles().get(i);
            RawFile rawFile = rawFiles.get(i);
            IcebergDataFileMeta fileMeta =
                    IcebergDataFileMeta.create(
                            IcebergDataFileMeta.Content.DATA,
                            rawFile.path(),
                            rawFile.format(),
                            dataSplit.partition(),
                            rawFile.rowCount(),
                            rawFile.fileSize(),
                            schemaCache.get(paimonFileMeta.schemaId()),
                            paimonFileMeta.valueStats(),
                            paimonFileMeta.valueStatsCols());
            dataFileEntries.add(
                    new IcebergManifestEntry(
                            IcebergManifestEntry.Status.ADDED,
                            snapshotId,
                            snapshotId,
                            snapshotId,
                            fileMeta));

            if (needAddDvToIceberg
                    && dataSplit.deletionFiles().isPresent()
                    && dataSplit.deletionFiles().get().get(i) != null) {
                DeletionFile deletionFile = dataSplit.deletionFiles().get().get(i);

                // Iceberg will check the cardinality between deserialized dv and iceberg deletion
                // file, so if deletionFile.cardinality() is null, we should stop synchronizing all
                // dvs.
                Preconditions.checkState(
                        deletionFile.cardinality() != null,
                        "cardinality in DeletionFile is null, stop generating dv for iceberg. "
                                + "dataFile path is {}, deletionFile is {}",
                        rawFile.path(),
                        deletionFile);

                // We can not get the file size of the complete DV index file from the DeletionFile,
                // so we set 'fileSizeInBytes' to -1(default in iceberg)
                IcebergDataFileMeta deleteFileMeta =
                        IcebergDataFileMeta.createForDeleteFile(
                                IcebergDataFileMeta.Content.POSITION_DELETES,
                                deletionFile.path(),
                                PUFFIN_FORMAT,
                                dataSplit.partition(),
                                deletionFile.cardinality(),
                                -1,
                                rawFile.path(),
                                deletionFile.offset(),
                                deletionFile.length());

                dvFileEntries.add(
                        new IcebergManifestEntry(
                                IcebergManifestEntry.Status.ADDED,
                                snapshotId,
                                snapshotId,
                                snapshotId,
                                deleteFileMeta));
            }
        }
    }

    private List<IcebergPartitionField> getPartitionFields(
            List<String> partitionKeys, IcebergSchema icebergSchema) {
        checkNoVariantPartitionKeys(partitionKeys, icebergSchema);

        Map<String, IcebergDataField> fields = new HashMap<>();
        for (IcebergDataField field : icebergSchema.fields()) {
            fields.put(field.name(), field);
        }

        List<IcebergPartitionField> result = new ArrayList<>();
        int fieldId = IcebergPartitionField.FIRST_FIELD_ID;
        for (String partitionKey : partitionKeys) {
            result.add(new IcebergPartitionField(fields.get(partitionKey), fieldId));
            fieldId++;
        }
        return result;
    }

    // Iceberg's identity transform (the only transform Paimon partition values use) rejects
    // VARIANT outright, so a VARIANT partition key can never be represented in Iceberg metadata
    static void checkNoVariantPartitionKeys(
            List<String> partitionKeys, IcebergSchema icebergSchema) {
        Set<String> variantPartitionKeys = new LinkedHashSet<>();
        for (IcebergDataField field : icebergSchema.fields()) {
            if (partitionKeys.contains(field.name()) && field.dataType() instanceof VariantType) {
                variantPartitionKeys.add(field.name());
            }
        }
        Preconditions.checkArgument(
                variantPartitionKeys.isEmpty(),
                "Partition keys %s have type VARIANT, which Iceberg does not support as a "
                        + "partition key.",
                variantPartitionKeys);
    }

    // -------------------------------------------------------------------------------------
    // Create metadata based on old ones
    // -------------------------------------------------------------------------------------

    /**
     * Whether the existing metadata for {@code snapshotId} was built from this very Paimon
     * snapshot, judged by the commit identity in the snapshot summary. Unreadable counts as a
     * mismatch; metadata without an identity (older releases) is trusted, so the protection only
     * covers metadata written since.
     *
     * <p>A replacement reuses its metadata version (versions are keyed by Paimon snapshot id), so
     * readers that already loaded the abandoned version converge only after reloading the table.
     */
    /**
     * Deletes every metadata version above the current Paimon snapshots, which would otherwise
     * shadow the replaced timeline for readers probing past the hint. A failed deletion fails the
     * commit so a retry finishes the job; referenced manifests are left to orphan cleanup (the
     * shared prefix makes reference counting non-trivial).
     */
    private void retireAbandonedSuffix() throws IOException {
        for (FileStatus status : table.fileIO().listStatus(pathFactory.metadataDirectory())) {
            String name = status.getPath().getName();
            if (!name.startsWith("v") || !name.endsWith(".metadata.json")) {
                continue;
            }
            long version;
            try {
                version = Long.parseLong(name.substring(1, name.indexOf('.')));
            } catch (NumberFormatException ignored) {
                continue;
            }
            Long latestNow = table.snapshotManager().latestSnapshotId();
            if (latestNow == null || version <= latestNow) {
                continue;
            }
            table.fileIO().deleteQuietly(status.getPath());
            if (table.fileIO().exists(status.getPath())) {
                throw new IllegalStateException(
                        "Failed to retire abandoned Iceberg metadata " + status.getPath());
            }
        }
    }

    static final String RETIRE_PENDING_FILENAME = "retire-pending";

    /**
     * Marks that a rollback may have left abandoned metadata behind; written before the rollback
     * deletes anything, and cleared once a commit has listed and retired the leftovers.
     */
    public static void markRetirePendingForRollback(FileStoreTable table) {
        // metadata left by an earlier enablement must learn about the rollback even while
        // mirroring is off, or a later re-enable would trust an abandoned base
        for (IcebergOptions.StorageLocation location : IcebergOptions.StorageLocation.values()) {
            try {
                Path dir =
                        new Path(
                                catalogDatabasePath(table, location),
                                String.format("%s/metadata", table.location().getName()));
                if (table.fileIO().exists(dir)) {
                    table.fileIO().overwriteFileUtf8(new Path(dir, RETIRE_PENDING_FILENAME), "");
                }
            } catch (Exception e) {
                // best-effort: the commit-time suspicion gate still covers the common cases
            }
        }
    }

    /** The version recorded in the hint file, or -1 when absent or unreadable. */
    private long readVersionHint() throws IOException {
        // an absent or corrupt hint reads as -1 and is repaired by the next publication; a
        // transient read failure must fail the caller instead of authorizing a stale publish
        String hint;
        try {
            hint =
                    table.fileIO()
                            .readFileUtf8(
                                    new Path(
                                            pathFactory.metadataDirectory(),
                                            VERSION_HINT_FILENAME));
        } catch (FileNotFoundException e) {
            return -1;
        }
        try {
            return Long.parseLong(hint.trim());
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    private void commitToExternalCatalog(
            IcebergMetadata metadata,
            Path metadataPath,
            @Nullable IcebergMetadata baseMetadata,
            @Nullable Path baseMetadataPath) {
        if (metadataCommitter == null) {
            return;
        }
        switch (metadataCommitter.identifier()) {
            case "hive":
                metadataCommitter.commitMetadata(metadataPath, baseMetadataPath);
                break;
            case "rest":
                metadataCommitter.commitMetadata(metadata, baseMetadata);
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported metadata committer: " + metadataCommitter.identifier());
        }
    }

    /** The newest existing metadata file version, or -1 when there is none. */
    private long newestExistingMetadataVersion() throws IOException {
        FileStatus[] statuses;
        try {
            statuses = table.fileIO().listStatus(pathFactory.metadataDirectory());
        } catch (FileNotFoundException e) {
            // only a missing directory counts as empty; a transient listing failure must
            // fail the commit, or a stale suffix would silently survive
            return -1;
        }
        long newest = -1;
        for (FileStatus status : statuses) {
            String name = status.getPath().getName();
            if (!name.startsWith("v") || !name.endsWith(".metadata.json")) {
                continue;
            }
            try {
                newest = Math.max(newest, Long.parseLong(name.substring(1, name.indexOf('.'))));
            } catch (NumberFormatException ignored) {
            }
        }
        return newest;
    }

    /** The given metadata file, or null when unreadable. */
    @Nullable
    private IcebergMetadata tryReadMetadata(Path metadataPath) {
        try {
            return IcebergMetadata.fromPath(table.fileIO(), metadataPath);
        } catch (Exception e) {
            return null;
        }
    }

    private boolean metadataMatchesSnapshot(long snapshotId, Snapshot snapshot) {
        try {
            IcebergMetadata existing =
                    IcebergMetadata.fromPath(
                            table.fileIO(), pathFactory.toMetadataPath(snapshotId));
            return metadataMatchesSnapshot(existing, snapshot);
        } catch (Exception e) {
            return false;
        }
    }

    private static boolean metadataMatchesSnapshot(IcebergMetadata metadata, Snapshot snapshot) {
        if (metadata.currentSnapshot() == null) {
            return false;
        }
        String identity =
                metadata.currentSnapshot().summary().get(SNAPSHOT_SUMMARY_PAIMON_COMMIT_IDENTITY);
        return identity == null || identity.equals(commitIdentity(snapshot));
    }

    private void createMetadataWithBase(
            FileChangesCollector fileChangesCollector,
            List<IndexManifestEntry> indexFiles,
            Snapshot snapshot,
            Path baseMetadataPath,
            int lastColumnIdFloor,
            long nextRowIdFloor,
            boolean rebuildDvFromLiveState)
            throws IOException {
        long snapshotId = snapshot.id();
        IcebergMetadata baseMetadata = IcebergMetadata.fromPath(table.fileIO(), baseMetadataPath);
        // row ids handed out by the base or by abandoned metadata must never be reused
        long rowIdFloor =
                Math.max(
                        nextRowIdFloor,
                        baseMetadata.nextRowId() == null ? 0L : baseMetadata.nextRowId());

        // a base left on the abandoned timeline must be rebuilt, not extended
        if (table.snapshotManager().snapshotExists(snapshotId - 1)
                && !metadataMatchesSnapshot(
                        baseMetadata, table.snapshotManager().snapshot(snapshotId - 1))) {
            Long latestNow = table.snapshotManager().latestSnapshotId();
            if (latestNow == null || latestNow != snapshotId) {
                // a delayed replay on the abandoned timeline: leave publication to the head
                return;
            }
            // keep the stale base's identity so external catalogs do not recreate the table
            createMetadataWithoutBase(
                    snapshotId,
                    baseMetadata.tableUuid(),
                    Math.max(lastColumnIdFloor, baseMetadata.lastColumnId()),
                    rowIdFloor);
            return;
        }

        if (!isSameFormatVersion(baseMetadata.formatVersion())) {
            // we need to recreate iceberg metadata if format version changed
            createMetadataWithoutBase(
                    snapshot.id(),
                    null,
                    Math.max(lastColumnIdFloor, baseMetadata.lastColumnId()),
                    rowIdFloor);
            return;
        }

        if (formatVersion == IcebergMetadata.FORMAT_VERSION_V3
                && baseMetadata.nextRowId() == null) {
            // v3 base metadata written before Paimon emitted row lineage; recreate to self-heal
            createMetadataWithoutBase(
                    snapshot.id(),
                    baseMetadata.tableUuid(),
                    Math.max(lastColumnIdFloor, baseMetadata.lastColumnId()),
                    rowIdFloor);
            return;
        }

        // decide the schema story before any manifest is written: rejecting later would
        // orphan the manifests written meanwhile
        SchemaCache schemaCache = new SchemaCache();
        int schemaId = publishableSchemaId(schemaCache, schemaCache.getLatestSchemaId());
        checkSnapshotSchemaEmittable(schemaCache, snapshot.schemaId(), schemaId);
        IcebergSchema icebergSchema = schemaCache.get(schemaId);
        // re-verified each commit: a rollback re-evolution can redefine an already
        // verified id while this callback only ever sees increasing snapshot ids
        for (IcebergSchema known : baseMetadata.schemas()) {
            if (known.schemaId() > schemaId) {
                continue;
            }
            IcebergSchema current =
                    known.schemaId() == schemaId
                            ? icebergSchema
                            : schemaCache.get(known.schemaId());
            if (!known.equals(current)) {
                // a re-evolution reused this id with different fields; rebuild from scratch
                createMetadataWithoutBase(
                        snapshot.id(),
                        baseMetadata.tableUuid(),
                        Math.max(lastColumnIdFloor, baseMetadata.lastColumnId()),
                        rowIdFloor);
                return;
            }
        }
        if (schemaId < baseMetadata.currentSchemaId()) {
            // pointer-only schema rollback keeps the base; an abandoned-timeline base
            // (snapshot entry mismatching the live snapshot) is rebuilt
            IcebergSnapshot baseCurrent = baseMetadata.currentSnapshot();
            SnapshotManager snapshotManager = table.snapshotManager();
            boolean pointerRollbackOnly =
                    baseCurrent != null
                            && snapshotManager.snapshotExists(snapshotId - 1)
                            && baseCurrent.schemaId()
                                    == (int) snapshotManager.snapshot(snapshotId - 1).schemaId();
            if (!pointerRollbackOnly) {
                createMetadataWithoutBase(
                        snapshot.id(),
                        baseMetadata.tableUuid(),
                        Math.max(lastColumnIdFloor, baseMetadata.lastColumnId()),
                        rowIdFloor);
                return;
            }
        }

        List<IcebergManifestFileMeta> baseManifestFileMetas =
                readManifestListWithFallback(baseMetadata.currentSnapshot().manifestList());

        // base manifest file for data files
        List<IcebergManifestFileMeta> baseDataManifestFileMetas =
                baseManifestFileMetas.stream()
                        .filter(meta -> meta.content() == IcebergManifestFileMeta.Content.DATA)
                        .collect(Collectors.toList());

        // base manifest file for deletion vector index files
        List<IcebergManifestFileMeta> baseDVManifestFileMetas =
                baseManifestFileMetas.stream()
                        .filter(meta -> meta.content() == IcebergManifestFileMeta.Content.DELETES)
                        .collect(Collectors.toList());

        Map<String, Pair<BinaryRow, DataFileMeta>> removedFiles = new LinkedHashMap<>();
        Map<String, Pair<BinaryRow, DataFileMeta>> addedFiles = new LinkedHashMap<>();
        boolean isAddOnly = fileChangesCollector.collect(removedFiles, addedFiles);
        Set<BinaryRow> modifiedPartitionsSet =
                removedFiles.values().stream()
                        .map(Pair::getLeft)
                        .collect(Collectors.toCollection(LinkedHashSet::new));
        addedFiles.values().stream().map(Pair::getLeft).forEach(modifiedPartitionsSet::add);
        List<BinaryRow> modifiedPartitions = new ArrayList<>(modifiedPartitionsSet);

        // Note that this check may be different from `removedFiles.isEmpty()`,
        // because if a file's level is changed, it will first be removed and then added.
        // In this case, if `baseMetadata` already contains this file, we should not add a
        // duplicate.
        List<IcebergManifestFileMeta> newDataManifestFileMetas;
        String operation;

        if (isAddOnly) {
            // Fast case. We don't need to remove files from `baseMetadata`. We only need to append
            // new metadata files.
            newDataManifestFileMetas = new ArrayList<>(baseDataManifestFileMetas);
            newDataManifestFileMetas.addAll(
                    createNewlyAddedManifestFileMetas(addedFiles, snapshotId));
            operation = IcebergSnapshotSummary.APPEND.operation();
        } else {
            Pair<List<IcebergManifestFileMeta>, String> result =
                    createWithDeleteManifestFileMetas(
                            removedFiles,
                            addedFiles,
                            modifiedPartitions,
                            baseDataManifestFileMetas,
                            snapshotId,
                            snapshot.commitKind());
            newDataManifestFileMetas = result.getLeft();
            operation = result.getRight();
        }

        List<IcebergManifestFileMeta> newDVManifestFileMetas = new ArrayList<>();
        if (needAddDvToIceberg) {
            if (rebuildDvFromLiveState || !indexFiles.isEmpty()) {
                // the dv set changed, or a replay cannot trust an empty incremental delta:
                // rebuild from live state, which is empty when every dv was removed
                newDVManifestFileMetas.addAll(createDvManifestFileMetas(snapshot));
            } else {
                // unchanged: keep the base delete manifests
                newDVManifestFileMetas.addAll(baseDVManifestFileMetas);
            }
        }

        newDataManifestFileMetas = compactMetadataIfNeeded(newDataManifestFileMetas, snapshotId);

        SummaryMetrics metrics = new SummaryMetrics();
        metrics.addedDataFiles = addedFiles.size();
        metrics.addedRecords =
                addedFiles.values().stream().mapToLong(p -> p.getRight().rowCount()).sum();
        metrics.addedFilesSize =
                addedFiles.values().stream().mapToLong(p -> p.getRight().fileSize()).sum();
        metrics.deletedDataFiles = removedFiles.size();
        metrics.deletedRecords =
                removedFiles.values().stream().mapToLong(p -> p.getRight().rowCount()).sum();
        metrics.deletedFilesSize =
                removedFiles.values().stream().mapToLong(p -> p.getRight().fileSize()).sum();
        metrics.changedPartitionCount = modifiedPartitionsSet.size();

        IcebergSnapshot baseSnapshot = baseMetadata.currentSnapshot();

        Long previousTotalRecordsValue =
                getSummaryLong(baseSnapshot, SNAPSHOT_SUMMARY_TOTAL_RECORDS);
        long previousTotalRecords =
                previousTotalRecordsValue != null
                        ? previousTotalRecordsValue
                        : computeLiveRowCount(baseDataManifestFileMetas);
        metrics.totalRecords =
                Math.max(0, previousTotalRecords + metrics.addedRecords - metrics.deletedRecords);

        Long previousTotalDataFilesValue =
                getSummaryLong(baseSnapshot, SNAPSHOT_SUMMARY_TOTAL_DATA_FILES);
        long previousTotalDataFiles =
                previousTotalDataFilesValue != null
                        ? previousTotalDataFilesValue
                        : computeLiveFileCount(baseDataManifestFileMetas);
        metrics.totalDataFiles =
                Math.max(
                        0,
                        previousTotalDataFiles + metrics.addedDataFiles - metrics.deletedDataFiles);

        Long previousTotalFilesSizeValue =
                getSummaryLong(baseSnapshot, SNAPSHOT_SUMMARY_TOTAL_FILES_SIZE);
        long previousTotalFilesSize =
                previousTotalFilesSizeValue != null
                        ? previousTotalFilesSizeValue
                        : computeTotalFilesSizeFromManifests(baseDataManifestFileMetas);
        metrics.totalFilesSize =
                Math.max(
                        0,
                        previousTotalFilesSize + metrics.addedFilesSize - metrics.deletedFilesSize);

        metrics.totalDeleteFiles = computeLiveFileCount(newDVManifestFileMetas);
        metrics.totalPositionDeletes = computeLiveRowCount(newDVManifestFileMetas);
        metrics.totalEqualityDeletes = 0;

        Long snapshotFirstRowId = computeSnapshotFirstRowId(rowIdFloor);

        ManifestRowIdAssignment rowIdAssignment =
                assignManifestFirstRowIds(
                        Stream.concat(
                                        newDataManifestFileMetas.stream(),
                                        newDVManifestFileMetas.stream())
                                .collect(Collectors.toList()),
                        snapshotFirstRowId);
        List<IcebergManifestFileMeta> newManifestFileMetasWithRowIds = rowIdAssignment.manifests;
        Long addedRows = snapshotFirstRowId == null ? null : rowIdAssignment.assignedRows;
        Long nextRowId =
                snapshotFirstRowId == null
                        ? null
                        : snapshotFirstRowId + rowIdAssignment.assignedRows;
        String manifestListFileName =
                manifestList.writeWithoutRolling(newManifestFileMetasWithRowIds);

        IcebergSnapshotSummary snapshotSummary =
                computeSnapshotSummary(operation, snapshot, metrics);

        // add new schemas if needed
        List<IcebergSchema> schemas = baseMetadata.schemas();
        if (schemaId > baseMetadata.currentSchemaId()) {
            // append only ids the list does not already carry
            Set<Integer> knownSchemaIds =
                    schemas.stream().map(IcebergSchema::schemaId).collect(Collectors.toSet());
            List<IcebergSchema> added = new ArrayList<>();
            for (int id = baseMetadata.currentSchemaId() + 1; id <= schemaId; id++) {
                if (knownSchemaIds.contains(id)) {
                    continue;
                }
                if (id == schemaId) {
                    added.add(icebergSchema);
                    continue;
                }
                try {
                    added.add(schemaCache.getValidated(id));
                } catch (IllegalArgumentException
                        | UnsupportedOperationException notRepresentable) {
                    // a pending schema that was vetoed at its introduction and since remedied
                    // was never mirrored; leaving it out must not brick every later commit.
                    // Only a representability verdict counts: a missing or malformed schema
                    // file must surface instead of silently dropping a valid schema
                }
            }
            if (!added.isEmpty()) {
                schemas = new ArrayList<>(schemas);
                schemas.addAll(added);
            }
        }
        // a schema-pointer rollback (validated above): only the current pointer moves

        int snapshotSchemaId = provenanceSchemaId(schemas, (int) snapshot.schemaId());

        List<IcebergSnapshot> snapshots = new ArrayList<>(baseMetadata.snapshots());
        snapshots.add(
                new IcebergSnapshot(
                        snapshotId,
                        snapshotId,
                        snapshotId - 1,
                        // the Paimon snapshot's own commit time, the as-of time readers see
                        snapshot.timeMillis(),
                        snapshotSummary,
                        pathFactory.toManifestListPath(manifestListFileName).toString(),
                        // the snapshot's own schema, for time travel
                        snapshotSchemaId,
                        snapshotFirstRowId,
                        addedRows));

        // all snapshots in this list, except the last one, need to expire
        List<IcebergSnapshot> toExpireExceptLast = new ArrayList<>();
        for (int i = 0; i + 1 < snapshots.size(); i++) {
            toExpireExceptLast.add(snapshots.get(i));
            // commit callback is called before expire, so we cannot use current earliest snapshot
            // and have to check expire condition by ourselves
            if (!shouldExpire(snapshots.get(i), snapshotId)) {
                snapshots = snapshots.subList(i, snapshots.size());
                break;
            }
        }

        // Tags can only be included in Iceberg if they point to an Iceberg snapshot that
        // exists. Otherwise an Iceberg client fails to parse the metadata and all reads fail.
        Set<Long> snapshotIds =
                snapshots.stream().map(IcebergSnapshot::snapshotId).collect(Collectors.toSet());
        Map<String, IcebergRef> refs =
                table.tagManager().tags().entrySet().stream()
                        .filter(entry -> snapshotIds.contains(entry.getKey().id()))
                        .collect(
                                Collectors.toMap(
                                        entry -> entry.getValue().get(0),
                                        entry -> new IcebergRef(entry.getKey().id())));

        IcebergMetadata metadata =
                new IcebergMetadata(
                        baseMetadata.formatVersion(),
                        baseMetadata.tableUuid(),
                        baseMetadata.location(),
                        snapshotId,
                        // must not regress when the current schema is older than the base's
                        // never below what the replaced metadata already handed out
                        Math.max(
                                lastColumnIdFloor,
                                Math.max(
                                        baseMetadata.lastColumnId(),
                                        icebergSchema.highestFieldId())),
                        schemas,
                        schemaId,
                        baseMetadata.partitionSpecs(),
                        baseMetadata.lastPartitionId(),
                        snapshots,
                        (int) snapshotId,
                        nextRowId,
                        refs);

        Path metadataPath = pathFactory.toMetadataPath(snapshotId);
        // atomic-first: see the no-base path
        boolean written = table.fileIO().tryToWriteAtomic(metadataPath, metadata.toJson());
        if (!written
                && table.fileIO().exists(metadataPath)
                && !metadataMatchesSnapshot(snapshotId, snapshot)) {
            table.fileIO().deleteQuietly(metadataPath);
            written = table.fileIO().tryToWriteAtomic(metadataPath, metadata.toJson());
        }
        if (!written) {
            // decided on one read: a twin finishing between two reads would otherwise leave
            // this callback publishing neither the file on disk nor a failure
            if (metadataMatchesSnapshot(snapshotId, snapshot)) {
                // a concurrent callback published this version first; later callbacks derive
                // row ids from what is on disk, so adopt the winner wholesale instead of
                // handing a divergent twin to the external catalog, and leave expiry to it
                metadata = IcebergMetadata.fromPath(table.fileIO(), metadataPath);
            } else {
                // no twin published this snapshot's metadata; fail so the commit retries
                throw new IllegalStateException(
                        "Failed to replace Iceberg metadata " + metadataPath);
            }
        }
        // the target publishes only as the live head (a rollback legitimately moves the hint
        // back); a replayed catch-up step publishes only while the hint moves forward, so a
        // stale callback cannot drag hint or catalog backwards
        Long latestAtPublish = table.snapshotManager().latestSnapshotId();
        if ((latestAtPublish != null && latestAtPublish == snapshotId)
                || (rebuildDvFromLiveState && snapshotId >= readVersionHint())) {
            table.fileIO()
                    .overwriteFileUtf8(
                            new Path(pathFactory.metadataDirectory(), VERSION_HINT_FILENAME),
                            String.valueOf(snapshotId));
            commitToExternalCatalog(metadata, metadataPath, baseMetadata, baseMetadataPath);
            // cleanup only after the catalog serves the new head, and only by the writer: a
            // skipped or failed publication must not delete files a pointer still references
            if (written) {
                deleteApplicableMetadataFiles(snapshotId);
                for (int i = 0; i + 1 < toExpireExceptLast.size(); i++) {
                    expireManifestList(
                            new Path(toExpireExceptLast.get(i).manifestList()).getName(),
                            new Path(toExpireExceptLast.get(i + 1).manifestList()).getName());
                }
            }
        }
    }

    private interface FileChangesCollector {
        boolean collect(
                Map<String, Pair<BinaryRow, DataFileMeta>> removedFiles,
                Map<String, Pair<BinaryRow, DataFileMeta>> addedFiles)
                throws IOException;
    }

    private boolean collectFileChanges(
            List<ManifestEntry> manifestEntries,
            Map<String, Pair<BinaryRow, DataFileMeta>> removedFiles,
            Map<String, Pair<BinaryRow, DataFileMeta>> addedFiles) {
        boolean isAddOnly = true;
        DataFilePathFactories factories = new DataFilePathFactories(fileStorePathFactory);
        for (ManifestEntry entry : manifestEntries) {
            DataFilePathFactory dataFilePathFactory =
                    factories.get(entry.partition(), entry.bucket());
            String path = dataFilePathFactory.toPath(entry).toString();
            switch (entry.kind()) {
                case ADD:
                    if (shouldAddFileToIceberg(entry.file())) {
                        removedFiles.remove(path);
                        addedFiles.put(path, Pair.of(entry.partition(), entry.file()));
                    }
                    break;
                case DELETE:
                    isAddOnly = false;
                    addedFiles.remove(path);
                    removedFiles.put(path, Pair.of(entry.partition(), entry.file()));
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Unknown ManifestEntry FileKind " + entry.kind());
            }
        }
        return isAddOnly;
    }

    private boolean collectFileChanges(
            long snapshotId,
            Map<String, Pair<BinaryRow, DataFileMeta>> removedFiles,
            Map<String, Pair<BinaryRow, DataFileMeta>> addedFiles) {
        return collectFileChanges(
                table.store()
                        .newScan()
                        .withKind(ScanMode.DELTA)
                        .withSnapshot(snapshotId)
                        .plan()
                        .files(),
                removedFiles,
                addedFiles);
    }

    private boolean shouldAddFileToIceberg(DataFileMeta meta) {
        if (table.primaryKeys().isEmpty()) {
            return true;
        } else {
            if (needAddDvToIceberg) {
                return meta.level() > 0;
            }
            int maxLevel = table.coreOptions().numLevels() - 1;
            return meta.level() == maxLevel;
        }
    }

    private List<IcebergManifestFileMeta> createNewlyAddedManifestFileMetas(
            Map<String, Pair<BinaryRow, DataFileMeta>> addedFiles, long currentSnapshotId)
            throws IOException {
        if (addedFiles.isEmpty()) {
            return Collections.emptyList();
        }

        SchemaCache schemaCache = new SchemaCache();
        return manifestFile.rollingWrite(
                addedFiles.entrySet().stream()
                        .map(
                                e -> {
                                    DataFileMeta paimonFileMeta = e.getValue().getRight();
                                    IcebergDataFileMeta icebergFileMeta =
                                            IcebergDataFileMeta.create(
                                                    IcebergDataFileMeta.Content.DATA,
                                                    e.getKey(),
                                                    paimonFileMeta.fileFormat(),
                                                    e.getValue().getLeft(),
                                                    paimonFileMeta.rowCount(),
                                                    paimonFileMeta.fileSize(),
                                                    schemaCache.get(paimonFileMeta.schemaId()),
                                                    paimonFileMeta.valueStats(),
                                                    paimonFileMeta.valueStatsCols());
                                    return new IcebergManifestEntry(
                                            IcebergManifestEntry.Status.ADDED,
                                            currentSnapshotId,
                                            currentSnapshotId,
                                            currentSnapshotId,
                                            icebergFileMeta);
                                })
                        .iterator(),
                currentSnapshotId);
    }

    private Pair<List<IcebergManifestFileMeta>, String> createWithDeleteManifestFileMetas(
            Map<String, Pair<BinaryRow, DataFileMeta>> removedFiles,
            Map<String, Pair<BinaryRow, DataFileMeta>> addedFiles,
            List<BinaryRow> modifiedPartitions,
            List<IcebergManifestFileMeta> baseManifestFileMetas,
            long currentSnapshotId,
            Snapshot.CommitKind commitKind)
            throws IOException {
        String operation = IcebergSnapshotSummary.APPEND.operation();
        List<IcebergManifestFileMeta> newManifestFileMetas = new ArrayList<>();

        RowType partitionType = table.schema().logicalPartitionType();
        PartitionPredicate predicate =
                PartitionPredicate.fromMultiple(partitionType, modifiedPartitions);

        for (IcebergManifestFileMeta fileMeta : baseManifestFileMetas) {
            // use partition predicate to only check modified partitions
            int numFields = partitionType.getFieldCount();
            GenericRow minValues = new GenericRow(numFields);
            GenericRow maxValues = new GenericRow(numFields);
            long[] nullCounts = new long[numFields];
            for (int i = 0; i < numFields; i++) {
                IcebergPartitionSummary summary = fileMeta.partitions().get(i);
                DataType fieldType = partitionType.getTypeAt(i);
                // an omitted bound means the value is unknown; keep the slot null
                byte[] lowerBound = summary.lowerBound();
                byte[] upperBound = summary.upperBound();
                if (lowerBound != null) {
                    minValues.setField(i, IcebergConversions.toPaimonObject(fieldType, lowerBound));
                }
                if (upperBound != null) {
                    maxValues.setField(i, IcebergConversions.toPaimonObject(fieldType, upperBound));
                }
                // IcebergPartitionSummary only has `containsNull` field and does not have the
                // exact number of nulls.
                nullCounts[i] = summary.containsNull() ? 1 : 0;
            }

            if (predicate == null
                    || predicate.test(
                            fileMeta.liveRowsCount(),
                            minValues,
                            maxValues,
                            new GenericArray(nullCounts))) {
                // check if any IcebergManifestEntry in this manifest file meta is removed
                List<IcebergManifestEntry> entries =
                        manifestFile.read(new Path(fileMeta.manifestPath()).getName());
                boolean canReuseFile = true;
                for (IcebergManifestEntry entry : entries) {
                    if (entry.isLive()) {
                        String path = entry.file().filePath();
                        if (addedFiles.containsKey(path)) {
                            // added file already exists (most probably due to level changes),
                            // remove it to not add a duplicate.
                            addedFiles.remove(path);
                        } else if (removedFiles.containsKey(path)) {
                            canReuseFile = false;
                        }
                    }
                }

                if (canReuseFile) {
                    // nothing is removed, use this file meta again
                    newManifestFileMetas.add(fileMeta);
                } else {
                    // some file is removed, rewrite this file meta
                    operation =
                            commitKind == Snapshot.CommitKind.COMPACT
                                    ? IcebergSnapshotSummary.REPLACE.operation()
                                    : IcebergSnapshotSummary.OVERWRITE.operation();
                    List<IcebergManifestEntry> sourceEntries =
                            materializeFirstRowIds(fileMeta, entries);
                    List<IcebergManifestEntry> newEntries = new ArrayList<>();
                    for (IcebergManifestEntry entry : sourceEntries) {
                        if (entry.isLive()) {
                            boolean removed = removedFiles.containsKey(entry.file().filePath());
                            newEntries.add(
                                    new IcebergManifestEntry(
                                            removed
                                                    ? IcebergManifestEntry.Status.DELETED
                                                    : IcebergManifestEntry.Status.EXISTING,
                                            // a deleted entry records the snapshot that
                                            // deleted the file, not the one that added it
                                            removed ? currentSnapshotId : entry.snapshotId(),
                                            entry.sequenceNumber(),
                                            entry.fileSequenceNumber(),
                                            entry.file()));
                        }
                    }
                    newManifestFileMetas.addAll(
                            manifestFile.rollingWrite(newEntries.iterator(), currentSnapshotId));
                }
            } else {
                // partition of this file meta is not modified in this snapshot,
                // use this file meta again
                newManifestFileMetas.add(fileMeta);
            }
        }

        newManifestFileMetas.addAll(
                createNewlyAddedManifestFileMetas(addedFiles, currentSnapshotId));
        return Pair.of(newManifestFileMetas, operation);
    }

    // -------------------------------------------------------------------------------------
    // Compact
    // -------------------------------------------------------------------------------------

    private List<IcebergManifestFileMeta> compactMetadataIfNeeded(
            List<IcebergManifestFileMeta> toCompact, long currentSnapshotId) throws IOException {
        List<IcebergManifestFileMeta> result = new ArrayList<>();
        long targetSizeInBytes = table.coreOptions().manifestTargetSize().getBytes();

        List<IcebergManifestFileMeta> candidates = new ArrayList<>();
        long totalSizeInBytes = 0;
        for (IcebergManifestFileMeta meta : toCompact) {
            if (meta.manifestLength() < targetSizeInBytes * 2 / 3) {
                candidates.add(meta);
                totalSizeInBytes += meta.manifestLength();
            } else {
                result.add(meta);
            }
        }

        Options options = new Options(table.options());
        if (candidates.size() < options.get(IcebergOptions.COMPACT_MIN_FILE_NUM)) {
            return toCompact;
        }
        if (candidates.size() < options.get(IcebergOptions.COMPACT_MAX_FILE_NUM)
                && totalSizeInBytes < targetSizeInBytes) {
            return toCompact;
        }

        Function<IcebergManifestFileMeta, List<IcebergManifestEntry>> processor =
                meta -> {
                    List<IcebergManifestEntry> sourceEntries =
                            materializeFirstRowIds(
                                    meta,
                                    IcebergManifestFile.create(table, pathFactory)
                                            .read(new Path(meta.manifestPath()).getName()));
                    List<IcebergManifestEntry> entries = new ArrayList<>();
                    for (IcebergManifestEntry entry : sourceEntries) {
                        // a deletion made by this commit is recorded against the current
                        // snapshot but keeps the file sequence number of the older snapshot
                        // that added the file, so it has to be recognised by snapshot id
                        if (entry.fileSequenceNumber() == currentSnapshotId
                                || entry.snapshotId() == currentSnapshotId
                                || entry.status() == IcebergManifestEntry.Status.EXISTING) {
                            entries.add(entry);
                        } else {
                            // rewrite status if this entry is from an older snapshot
                            IcebergManifestEntry.Status newStatus;
                            if (entry.status() == IcebergManifestEntry.Status.ADDED) {
                                newStatus = IcebergManifestEntry.Status.EXISTING;
                            } else if (entry.status() == IcebergManifestEntry.Status.DELETED) {
                                continue;
                            } else {
                                throw new UnsupportedOperationException(
                                        "Unknown IcebergManifestEntry.Status " + entry.status());
                            }
                            entries.add(
                                    new IcebergManifestEntry(
                                            newStatus,
                                            entry.snapshotId(),
                                            entry.sequenceNumber(),
                                            entry.fileSequenceNumber(),
                                            entry.file()));
                        }
                    }
                    if (meta.sequenceNumber() == currentSnapshotId) {
                        // this file is created for this snapshot, so it is not recorded in any
                        // iceberg metas, we need to clean it
                        table.fileIO().deleteQuietly(new Path(meta.manifestPath()));
                    }
                    return entries;
                };
        Iterable<IcebergManifestEntry> newEntries =
                ManifestReadThreadPool.sequentialBatchedExecute(processor, candidates, null);
        result.addAll(manifestFile.rollingWrite(newEntries.iterator(), currentSnapshotId));
        return result;
    }

    // -------------------------------------------------------------------------------------
    // Expire
    // -------------------------------------------------------------------------------------

    private boolean shouldExpire(IcebergSnapshot snapshot, long currentSnapshotId) {
        Options options = new Options(table.options());
        if (snapshot.snapshotId()
                > currentSnapshotId - options.get(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN)) {
            return false;
        }
        if (snapshot.snapshotId()
                <= currentSnapshotId - options.get(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX)) {
            return true;
        }
        return snapshot.timestampMs()
                < System.currentTimeMillis()
                        - options.get(CoreOptions.SNAPSHOT_TIME_RETAINED).toMillis();
    }

    /**
     * Reads a historical manifest list, falling back to the legacy (iceberg-1.4) reader for lists
     * written while {@code manifest.legacy-version} was in effect: a table that upgrades to v3 has
     * to turn the option off, so its older lists stay in the legacy schema. A genuine read failure
     * still propagates, because the legacy reader fails too and the original error is rethrown.
     */
    private List<IcebergManifestFileMeta> readManifestListWithFallback(String listName) {
        try {
            return manifestList.read(listName);
        } catch (RuntimeException primary) {
            if (legacyManifestList == null) {
                legacyManifestList =
                        IcebergManifestList.create(
                                table.copy(
                                        Collections.singletonMap(
                                                IcebergOptions.MANIFEST_LEGACY_VERSION.key(),
                                                "true")),
                                pathFactory);
            }
            try {
                return legacyManifestList.read(listName);
            } catch (RuntimeException notLegacy) {
                throw primary;
            }
        }
    }

    @VisibleForTesting
    void expireManifestList(String toExpire, String next) {
        Set<String> manifestPathsInUse;
        try {
            manifestPathsInUse =
                    readManifestListWithFallback(next).stream()
                            .map(IcebergManifestFileMeta::manifestPath)
                            .collect(Collectors.toSet());
        } catch (RuntimeException e) {
            // without the surviving side there is no safe in-use set; leave this pair to a
            // later expiry pass instead of failing a commit that already published
            return;
        }
        List<IcebergManifestFileMeta> expiredMetas;
        try {
            expiredMetas = readManifestListWithFallback(toExpire);
        } catch (RuntimeException e) {
            if (transientReadFailure(e)) {
                // deleting the list on a flaky read would orphan every manifest it still
                // indexes; leave the pair to a later expiry pass
                return;
            }
            // an aged base can retain entries whose list a newer, since-lost metadata already
            // expired; nothing of such a list remains to expire
            table.fileIO().deleteQuietly(pathFactory.toManifestListPath(toExpire));
            return;
        }
        // compare by path: a reused manifest can reappear under different meta fields and
        // must not be deleted
        for (IcebergManifestFileMeta meta : expiredMetas) {
            if (manifestPathsInUse.contains(meta.manifestPath())) {
                continue;
            }
            table.fileIO().deleteQuietly(new Path(meta.manifestPath()));
        }
        table.fileIO().deleteQuietly(pathFactory.toManifestListPath(toExpire));
    }

    private void expireAllBefore(long snapshotId) throws IOException {
        Set<String> expiredManifestLists = new HashSet<>();
        Set<String> expiredManifestFileMetas = new HashSet<>();
        Iterator<Path> it =
                pathFactory.getAllMetadataPathBefore(table.fileIO(), snapshotId).iterator();

        while (it.hasNext()) {
            Path path = it.next();
            IcebergMetadata metadata = IcebergMetadata.fromPath(table.fileIO(), path);

            for (IcebergSnapshot snapshot : metadata.snapshots()) {
                Path listPath = new Path(snapshot.manifestList());
                String listName = listPath.getName();
                if (expiredManifestLists.contains(listName)) {
                    continue;
                }
                expiredManifestLists.add(listName);

                // a retained metadata json can reference a list an earlier rebuild deleted
                if (!table.fileIO().exists(listPath)) {
                    continue;
                }
                List<IcebergManifestFileMeta> expiredMetas;
                try {
                    expiredMetas = readManifestListWithFallback(listName);
                } catch (RuntimeException e) {
                    if (transientReadFailure(e)) {
                        // deleting the list on a flaky read would orphan every manifest it
                        // still indexes; leave it to a later expiry pass
                        continue;
                    }
                    // an aged metadata json can reference lists already expired by a newer,
                    // since-lost metadata; nothing of such a list remains to expire
                    table.fileIO().deleteQuietly(listPath);
                    continue;
                }
                for (IcebergManifestFileMeta meta : expiredMetas) {
                    String metaName = new Path(meta.manifestPath()).getName();
                    if (expiredManifestFileMetas.contains(metaName)) {
                        continue;
                    }
                    expiredManifestFileMetas.add(metaName);
                    table.fileIO().deleteQuietly(new Path(meta.manifestPath()));
                }
                table.fileIO().deleteQuietly(listPath);
            }
        }
        deleteApplicableMetadataFiles(snapshotId);
    }

    private void deleteApplicableMetadataFiles(long snapshotId) throws IOException {
        Options options = new Options(table.options());
        if (options.get(IcebergOptions.METADATA_DELETE_AFTER_COMMIT)) {
            long earliestMetadataId =
                    snapshotId - options.get(IcebergOptions.METADATA_PREVIOUS_VERSIONS_MAX);
            if (earliestMetadataId > 0) {
                Iterator<Path> it =
                        pathFactory
                                .getAllMetadataPathBefore(table.fileIO(), earliestMetadataId)
                                .iterator();
                while (it.hasNext()) {
                    Path path = it.next();
                    table.fileIO().deleteQuietly(path);
                }
            }
        }
    }

    @Override
    public void notifyCreation(String tagName) {
        // The base TagCallback API does not carry a snapshot id, but Iceberg refs
        // require one. The tag is persisted by TagManager before this callback
        // fires, so resolve the snapshot the tag points to and delegate to the
        // snapshot aware overload.
        Optional<Tag> tag = table.tagManager().get(tagName);
        if (!tag.isPresent()) {
            LOG.info(
                    "Tag {} not found in Paimon TagManager when creating Iceberg ref. Unable to create tag.",
                    tagName);
            return;
        }
        notifyCreation(tagName, tag.get().id());
    }

    @Override
    public void notifyCreation(String tagName, long snapshotId) {
        try {
            Snapshot latestSnapshot = table.snapshotManager().latestSnapshot();
            if (latestSnapshot == null) {
                LOG.info(
                        "Latest Iceberg snapshot not found when creating tag {} for snapshot {}. Unable to create tag.",
                        tagName,
                        snapshotId);
                return;
            }

            Path baseMetadataPath = pathFactory.toMetadataPath(latestSnapshot.id());
            if (!table.fileIO().exists(baseMetadataPath)) {
                LOG.info(
                        "Iceberg metadata file {} not found when creating tag {} for snapshot {}. Unable to create tag.",
                        baseMetadataPath,
                        tagName,
                        snapshotId);
                return;
            }

            IcebergMetadata baseMetadata =
                    IcebergMetadata.fromPath(table.fileIO(), baseMetadataPath);

            // Tags can only be included in Iceberg if they point to an Iceberg snapshot that
            // exists. Otherwise an Iceberg client fails to parse the metadata and all reads fail.
            boolean tagSnapshotInIceberg = false;
            for (IcebergSnapshot snapshot : baseMetadata.snapshots()) {
                if (snapshot.snapshotId() == snapshotId) {
                    tagSnapshotInIceberg = true;
                    break;
                }
            }

            if (!tagSnapshotInIceberg) {
                LOG.warn(
                        "Snapshot {} does not exist in Iceberg metadata. Unable to create tag {}.",
                        snapshotId,
                        tagName);
                return;
            }

            baseMetadata.refs().put(tagName, new IcebergRef(snapshotId));

            IcebergMetadata metadata =
                    new IcebergMetadata(
                            baseMetadata.formatVersion(),
                            baseMetadata.tableUuid(),
                            baseMetadata.location(),
                            baseMetadata.currentSnapshotId(),
                            baseMetadata.lastColumnId(),
                            baseMetadata.schemas(),
                            baseMetadata.currentSchemaId(),
                            baseMetadata.partitionSpecs(),
                            baseMetadata.lastPartitionId(),
                            baseMetadata.snapshots(),
                            baseMetadata.currentSnapshotId(),
                            baseMetadata.nextRowId(),
                            baseMetadata.refs());

            /*
            Overwrite the latest metadata file
            Currently the Paimon table snapshot id value is the same as the Iceberg metadata
            version number. Tag creation overwrites the latest metadata file to maintain this.
            There is no need to update the catalog after overwrite.
             */
            table.fileIO().overwriteFileUtf8(baseMetadataPath, metadata.toJson());
            LOG.info(
                    "Iceberg metadata file {} overwritten to add tag {} for snapshot {}.",
                    baseMetadataPath,
                    tagName,
                    snapshotId);

        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create tag " + tagName, e);
        }
    }

    @Override
    public void notifyDeletion(String tagName) {
        try {
            Snapshot latestSnapshot = table.snapshotManager().latestSnapshot();
            if (latestSnapshot == null) {
                LOG.info(
                        "Latest Iceberg snapshot not found when deleting tag {}. Unable to delete tag.",
                        tagName);
                return;
            }

            Path baseMetadataPath = pathFactory.toMetadataPath(latestSnapshot.id());
            if (!table.fileIO().exists(baseMetadataPath)) {
                LOG.info(
                        "Iceberg metadata file {} not found when deleting tag {}. Unable to delete tag.",
                        baseMetadataPath,
                        tagName);
                return;
            }

            IcebergMetadata baseMetadata =
                    IcebergMetadata.fromPath(table.fileIO(), baseMetadataPath);

            baseMetadata.refs().remove(tagName);

            IcebergMetadata metadata =
                    new IcebergMetadata(
                            baseMetadata.formatVersion(),
                            baseMetadata.tableUuid(),
                            baseMetadata.location(),
                            baseMetadata.currentSnapshotId(),
                            baseMetadata.lastColumnId(),
                            baseMetadata.schemas(),
                            baseMetadata.currentSchemaId(),
                            baseMetadata.partitionSpecs(),
                            baseMetadata.lastPartitionId(),
                            baseMetadata.snapshots(),
                            baseMetadata.currentSnapshotId(),
                            baseMetadata.nextRowId(),
                            baseMetadata.refs());

            /*
            Overwrite the latest metadata file
            Currently the Paimon table snapshot id value is the same as the Iceberg metadata
            version number. Tag creation overwrites the latest metadata file to maintain this.
            There is no need to update the catalog after overwrite.
             */
            table.fileIO().overwriteFileUtf8(baseMetadataPath, metadata.toJson());
            LOG.info(
                    "Iceberg metadata file {} overwritten to delete tag {}.",
                    baseMetadataPath,
                    tagName);

        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create tag " + tagName, e);
        }
    }

    // -------------------------------------------------------------------------------------
    // Deletion vectors
    // -------------------------------------------------------------------------------------

    private boolean needAddDvToIceberg() {
        CoreOptions options = table.coreOptions();
        // there may be dv indexes using bitmap32 in index files even if 'deletion-vectors.bitmap64'
        // is true, but analyzing all deletion vectors is very costly, so we do not check exactly
        // currently.
        return options.deletionVectorsEnabled()
                && options.deletionVectorBitmap64()
                && formatVersion == IcebergMetadata.FORMAT_VERSION_V3;
    }

    private List<IcebergManifestFileMeta> createDvManifestFileMetas(Snapshot snapshot) {
        List<IcebergManifestEntry> icebergDvEntries = new ArrayList<>();

        long snapshotId = snapshot.id();
        List<IndexManifestEntry> newIndexes =
                indexFileHandler.scan(snapshot, DELETION_VECTORS_INDEX);
        if (newIndexes.isEmpty()) {
            return Collections.emptyList();
        }
        for (IndexManifestEntry entry : newIndexes) {
            LinkedHashMap<String, DeletionVectorMeta> dvMetas = entry.indexFile().dvRanges();
            Path bucketPath = fileStorePathFactory.bucketPath(entry.partition(), entry.bucket());
            if (dvMetas != null) {
                for (DeletionVectorMeta dvMeta : dvMetas.values()) {

                    // Iceberg will check the cardinality between deserialized dv and iceberg
                    // deletion file, so if deletionFile.cardinality() is null, we should stop
                    // synchronizing all dvs.
                    Preconditions.checkState(
                            dvMeta.cardinality() != null,
                            "cardinality in DeletionVector is null, stop generate dv for iceberg. "
                                    + "dataFile path is {}, indexFile path is {}",
                            new Path(bucketPath, dvMeta.dataFileName()),
                            indexFileHandler.filePath(entry).toString());

                    IcebergDataFileMeta deleteFileMeta =
                            IcebergDataFileMeta.createForDeleteFile(
                                    IcebergDataFileMeta.Content.POSITION_DELETES,
                                    indexFileHandler.filePath(entry).toString(),
                                    PUFFIN_FORMAT,
                                    entry.partition(),
                                    dvMeta.cardinality(),
                                    entry.indexFile().fileSize(),
                                    new Path(bucketPath, dvMeta.dataFileName()).toString(),
                                    (long) dvMeta.offset(),
                                    (long) dvMeta.length());

                    icebergDvEntries.add(
                            new IcebergManifestEntry(
                                    IcebergManifestEntry.Status.ADDED,
                                    snapshotId,
                                    snapshotId,
                                    snapshotId,
                                    deleteFileMeta));
                }
            }
        }

        if (icebergDvEntries.isEmpty()) {
            return Collections.emptyList();
        }

        return manifestFile.rollingWrite(
                icebergDvEntries.iterator(), snapshotId, IcebergManifestFileMeta.Content.DELETES);
    }

    // -------------------------------------------------------------------------------------
    // Snapshot Summary Computation
    // -------------------------------------------------------------------------------------

    private static class SummaryMetrics {
        long addedDataFiles;
        long addedRecords;
        long addedFilesSize;
        long deletedDataFiles;
        long deletedRecords;
        long deletedFilesSize;
        long changedPartitionCount;
        long totalDataFiles;
        long totalRecords;
        long totalFilesSize;
        long totalDeleteFiles;
        long totalPositionDeletes;
        long totalEqualityDeletes;
    }

    /**
     * Summary entry identifying the Paimon snapshot this metadata was built from; it tells live
     * metadata from metadata a rollback abandoned.
     */
    static final String SNAPSHOT_SUMMARY_PAIMON_COMMIT_IDENTITY = "paimon-commit-identity";

    private static String commitIdentity(Snapshot snapshot) {
        // snapshot uuid when present; legacy snapshots fall back to user/identifier/time
        if (snapshot.uuid() != null) {
            return snapshot.uuid();
        }
        return snapshot.commitUser()
                + ":"
                + snapshot.commitIdentifier()
                + ":"
                + snapshot.timeMillis();
    }

    private IcebergSnapshotSummary computeSnapshotSummary(
            String operation, Snapshot snapshot, SummaryMetrics metrics) {

        IcebergSnapshotSummary summary = new IcebergSnapshotSummary(operation);

        long addedDataFiles = Math.max(0, metrics.addedDataFiles);
        long addedRecords = Math.max(0, metrics.addedRecords);
        long addedFilesSize = Math.max(0, metrics.addedFilesSize);
        long deletedDataFiles = Math.max(0, metrics.deletedDataFiles);
        long deletedRecords = Math.max(0, metrics.deletedRecords);
        long deletedFilesSize = Math.max(0, metrics.deletedFilesSize);
        long changedPartitionCount = Math.max(0, metrics.changedPartitionCount);
        long totalRecords = Math.max(0, metrics.totalRecords);
        long totalDataFiles = Math.max(0, metrics.totalDataFiles);
        long totalFilesSize = Math.max(0, metrics.totalFilesSize);
        long totalDeleteFiles = Math.max(0, metrics.totalDeleteFiles);
        long totalPositionDeletes = Math.max(0, metrics.totalPositionDeletes);
        long totalEqualityDeletes = Math.max(0, metrics.totalEqualityDeletes);

        summary.put(SNAPSHOT_SUMMARY_ADDED_DATA_FILES, Long.toString(addedDataFiles));
        summary.put(SNAPSHOT_SUMMARY_ADDED_RECORDS, Long.toString(addedRecords));
        summary.put(SNAPSHOT_SUMMARY_ADDED_FILES_SIZE, Long.toString(addedFilesSize));
        summary.put(SNAPSHOT_SUMMARY_DELETED_DATA_FILES, Long.toString(deletedDataFiles));
        summary.put(SNAPSHOT_SUMMARY_DELETED_RECORDS, Long.toString(deletedRecords));
        summary.put(SNAPSHOT_SUMMARY_REMOVED_FILES_SIZE, Long.toString(deletedFilesSize));
        summary.put(SNAPSHOT_SUMMARY_CHANGED_PARTITION_COUNT, Long.toString(changedPartitionCount));
        summary.put(SNAPSHOT_SUMMARY_TOTAL_RECORDS, Long.toString(totalRecords));
        summary.put(SNAPSHOT_SUMMARY_TOTAL_DATA_FILES, Long.toString(totalDataFiles));
        summary.put(SNAPSHOT_SUMMARY_TOTAL_FILES_SIZE, Long.toString(totalFilesSize));
        summary.put(SNAPSHOT_SUMMARY_TOTAL_DELETE_FILES, Long.toString(totalDeleteFiles));
        summary.put(SNAPSHOT_SUMMARY_TOTAL_POSITION_DELETES, Long.toString(totalPositionDeletes));
        summary.put(SNAPSHOT_SUMMARY_TOTAL_EQUALITY_DELETES, Long.toString(totalEqualityDeletes));

        Map<String, String> properties = snapshot.properties();
        if (properties != null) {
            properties.forEach(
                    (key, value) -> {
                        if (value != null) {
                            summary.put(key, value);
                        }
                    });
        }
        // after the user-property copy, so a same-key property cannot overwrite it
        summary.put(SNAPSHOT_SUMMARY_PAIMON_COMMIT_IDENTITY, commitIdentity(snapshot));

        return summary;
    }

    private long computeLiveFileCount(List<IcebergManifestFileMeta> manifestMetas) {
        return manifestMetas.stream()
                .mapToLong(
                        meta ->
                                meta.addedFilesCount()
                                        + meta.existingFilesCount()
                                        - meta.deletedFilesCount())
                .sum();
    }

    private long computeLiveRowCount(List<IcebergManifestFileMeta> manifestMetas) {
        return manifestMetas.stream()
                .mapToLong(
                        meta ->
                                meta.addedRowsCount()
                                        + meta.existingRowsCount()
                                        - meta.deletedRowsCount())
                .sum();
    }

    @Nullable
    private Long getSummaryLong(@Nullable IcebergSnapshot snapshot, String key) {
        if (snapshot == null) {
            return null;
        }
        Map<String, String> summaryMap = snapshot.summary().getSummary();
        String value = summaryMap.get(key);
        if (value == null) {
            return null;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            LOG.warn(
                    "Unable to parse snapshot summary field {}={} as long. The value will be recomputed.",
                    key,
                    value);
            return null;
        }
    }

    private long computeTotalFilesSizeFromManifests(List<IcebergManifestFileMeta> manifestMetas)
            throws IOException {
        long total = 0;
        for (IcebergManifestFileMeta meta : manifestMetas) {
            for (IcebergManifestEntry entry :
                    manifestFile.read(new Path(meta.manifestPath()).getName())) {
                if (entry.isLive()) {
                    total += entry.file().fileSizeInBytes();
                }
            }
        }
        return total;
    }

    // -------------------------------------------------------------------------------------
    // Utils
    // -------------------------------------------------------------------------------------

    private boolean isSameFormatVersion(int baseFormatVersion) {
        if (baseFormatVersion != formatVersion) {
            Preconditions.checkArgument(
                    formatVersion > baseFormatVersion,
                    "format version in base metadata is {}, and it's bigger than the current format version {}, "
                            + "this is not allowed!");

            LOG.info(
                    "format version in base metadata is {}, and it's different from the current format version {}. "
                            + "New metadata will be recreated using format version {}.",
                    baseFormatVersion,
                    formatVersion,
                    formatVersion);
            return false;
        }
        return true;
    }

    /**
     * Row-lineage bookkeeping for a new snapshot, mandatory in Iceberg format version 3: the
     * snapshot's first-row-id starts at the base metadata's next-row-id watermark. The snapshot's
     * added-rows and the table's next-row-id are NOT derived here: they depend on how many rows
     * {@link #assignManifestFirstRowIds} actually assigns (which can exceed this commit's added
     * records when a carried-over manifest is assigned for the first time, e.g. a Layer-1-written
     * manifest being upgraded), so callers must recompute them from the assignment's result. For
     * format version 2 the field stays null so nothing is written.
     */
    @Nullable
    private Long computeSnapshotFirstRowId(long baseNextRowId) {
        return formatVersion >= IcebergMetadata.FORMAT_VERSION_V3 ? baseNextRowId : null;
    }

    /**
     * Result of {@link #assignManifestFirstRowIds}: the manifests with first_row_id assigned, and
     * the total number of rows actually consumed from the row-id space by that assignment (which
     * may be larger than this commit's added-records count; see the class-level note there).
     */
    private static class ManifestRowIdAssignment {
        private final List<IcebergManifestFileMeta> manifests;
        private final long assignedRows;

        private ManifestRowIdAssignment(
                List<IcebergManifestFileMeta> manifests, long assignedRows) {
            this.manifests = manifests;
            this.assignedRows = assignedRows;
        }
    }

    /**
     * Iceberg v3: assign first_row_id (field 520) to data manifests that do not have one yet.
     * Manifests carried over from base metadata that are already assigned keep their value; delete
     * manifests stay null. The watermark starts at the snapshot's first-row-id and advances by each
     * newly-assigned manifest's TRUE inheriting-rows count (see {@link #trueInheritingRowsCount}),
     * returned as {@link ManifestRowIdAssignment#assignedRows}.
     *
     * <p>A manifest written entirely under manifest-level assignment satisfies "inheriting rows ==
     * ADDED rows", so the bound is exact for it. A manifest carried over from before assignment
     * existed may hold EXISTING entries whose field 142 is also still null; the bound covers them
     * without reading the manifest, at the cost of spec-legal id gaps when some of those entries
     * were already materialized. DELETED entries never inherit ids and are excluded. Callers MUST
     * use {@code assignedRows} (not this commit's added-records count) to advance the snapshot's
     * added-rows / table next-row-id, precisely because of that mismatch.
     */
    private ManifestRowIdAssignment assignManifestFirstRowIds(
            List<IcebergManifestFileMeta> manifests, @Nullable Long snapshotFirstRowId) {
        if (snapshotFirstRowId == null) {
            return new ManifestRowIdAssignment(manifests, 0L);
        }
        List<IcebergManifestFileMeta> result = new ArrayList<>();
        long watermark = snapshotFirstRowId;
        for (IcebergManifestFileMeta meta : manifests) {
            if (meta.content() == IcebergManifestFileMeta.Content.DATA
                    && meta.firstRowId() == null) {
                result.add(meta.withFirstRowId(watermark));
                // spec-sanctioned upper bound: only ADDED and EXISTING rows can inherit
                // ids from this manifest (readers never assign ids to DELETED entries).
                // Rows whose field 142 is already materialized merely widen the reserved
                // range, leaving legal id gaps - in exchange the commit path never has to
                // read manifest contents.
                watermark += meta.addedRowsCount() + meta.existingRowsCount();
            } else {
                result.add(meta);
            }
        }
        return new ManifestRowIdAssignment(result, watermark - snapshotFirstRowId);
    }

    /**
     * Iceberg v3 requires the inherited first_row_id to be written into file metadata when entries
     * are copied into a rewritten manifest. Computes each entry's effective id in base manifest
     * order (explicit field 142, or inherited from the manifest's first_row_id, skipping DELETED
     * entries exactly like GA readers do) and returns entries with the id materialized. No-op for
     * delete manifests and for base manifests without an assigned first_row_id (v2 metadata, or v3
     * metadata written before manifest-level assignment existed — those stay in the spec's
     * upgraded-table state).
     */
    private static List<IcebergManifestEntry> materializeFirstRowIds(
            IcebergManifestFileMeta baseMeta, List<IcebergManifestEntry> entries) {
        if (baseMeta.content() != IcebergManifestFileMeta.Content.DATA
                || baseMeta.firstRowId() == null) {
            return entries;
        }
        List<IcebergManifestEntry> result = new ArrayList<>();
        long watermark = baseMeta.firstRowId();
        for (IcebergManifestEntry entry : entries) {
            if (entry.status() != IcebergManifestEntry.Status.DELETED
                    && entry.file().firstRowId() == null) {
                result.add(entry.withFile(entry.file().withFirstRowId(watermark)));
                watermark += entry.file().recordCount();
            } else {
                // DELETED entries never inherit an id (GA readers skip them when
                // assigning), so their field 142 stays null and the walk does not advance
                result.add(entry);
            }
        }
        return result;
    }

    private class SchemaCache {

        SchemaManager schemaManager = new SchemaManager(table.fileIO(), table.location());
        Map<Long, IcebergSchema> schemas = new HashMap<>();

        private IcebergSchema get(long schemaId) {
            return schemas.computeIfAbsent(
                    schemaId,
                    id -> {
                        TableSchema schema = schemaManager.schema(id);
                        // the variant backstop that used to sit here is superseded by the type
                        // gate: it judges the schemas a commit publishes, so history that can no
                        // longer be represented degrades instead of bricking later commits
                        SchemaValidation.validateIcebergGeospatialTypes(
                                schema.logicalRowType(), table.coreOptions());
                        return IcebergSchema.create(schema);
                    });
        }

        private IcebergSchema getValidated(long schemaId) {
            // only schemas a commit newly publishes are gated; history already published
            // must not brick later commits (e.g. after the offending column was dropped)
            checkFormatVersionSupportsSchema(
                    formatVersion, schemaManager.schema(schemaId).logicalRowType());
            return get(schemaId);
        }

        private long getLatestSchemaId() {
            return schemaManager.latest().get().id();
        }
    }
}
