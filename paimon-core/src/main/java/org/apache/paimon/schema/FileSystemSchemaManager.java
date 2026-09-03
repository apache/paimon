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

package org.apache.paimon.schema;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.SchemaModification;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.LazyField;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.paimon.catalog.Identifier.DEFAULT_MAIN_BRANCH;
import static org.apache.paimon.schema.ColumnDirectiveUtils.applyDirectives;
import static org.apache.paimon.utils.FileUtils.listVersionedFiles;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** File system implementation of {@link SchemaManager}. */
@ThreadSafe
public class FileSystemSchemaManager implements SchemaManager {

    public static final String SCHEMA_PREFIX = "schema-";

    private final FileIO fileIO;
    private final Path tableRoot;

    private final String branch;

    public FileSystemSchemaManager(FileIO fileIO, Path tableRoot) {
        this(fileIO, tableRoot, DEFAULT_MAIN_BRANCH);
    }

    /** Specify the default branch for data writing. */
    public FileSystemSchemaManager(FileIO fileIO, Path tableRoot, String branch) {
        this.fileIO = fileIO;
        this.tableRoot = tableRoot;
        this.branch = BranchManager.normalizeBranch(branch);
    }

    public FileSystemSchemaManager copyWithBranch(String branchName) {
        return new FileSystemSchemaManager(fileIO, tableRoot, branchName);
    }

    public Optional<TableSchema> latest() {
        try {
            return listVersionedFiles(fileIO, schemaDirectory(), SCHEMA_PREFIX)
                    .reduce(Math::max)
                    .map(this::schema);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public TableSchema latestOrThrow(String message) {
        return latest().orElseThrow(() -> new RuntimeException(message));
    }

    public long earliestCreationTime() {
        try {
            long earliest = 0;
            if (!schemaExists(0)) {
                Optional<Long> min =
                        listVersionedFiles(fileIO, schemaDirectory(), SCHEMA_PREFIX)
                                .reduce(Math::min);
                checkArgument(min.isPresent());
                earliest = min.get();
            }

            Path schemaPath = toSchemaPath(earliest);
            return fileIO.getFileStatus(schemaPath).getModificationTime();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public List<TableSchema> listAll() {
        return listAllIds().stream().map(this::schema).collect(Collectors.toList());
    }

    /** List all schema IDs. */
    public List<Long> listAllIds() {
        try {
            return listVersionedFiles(fileIO, schemaDirectory(), SCHEMA_PREFIX)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public TableSchema createTable(Schema schema) throws Exception {
        return createTable(schema, false);
    }

    public TableSchema createTable(Schema schema, boolean externalTable) throws Exception {
        while (true) {
            Optional<TableSchema> latest = latest();
            if (latest.isPresent()) {
                TableSchema latestSchema = latest.get();
                if (externalTable) {
                    checkSchemaForExternalTable(latestSchema.toSchema(), schema);
                    return latestSchema;
                } else {
                    throw new IllegalStateException(
                            "Schema in filesystem exists, creation is not allowed.");
                }
            }

            schema = applyDirectives(schema);
            TableSchema newSchema = TableSchema.create(0, schema);

            // validate table from creating table
            FileStoreTableFactory.create(fileIO, tableRoot, newSchema).store();

            boolean success = commit(newSchema);
            if (success) {
                return newSchema;
            }
        }
    }

    private void checkSchemaForExternalTable(Schema existsSchema, Schema newSchema) {
        // When creating an external table, if the table already exists in the location, we can
        // choose not to specify the fields.
        if ((newSchema.fields().isEmpty()
                        || newSchema.rowType().equalsIgnoreFieldId(existsSchema.rowType()))
                && (newSchema.partitionKeys().isEmpty()
                        || Objects.equals(newSchema.partitionKeys(), existsSchema.partitionKeys()))
                && (newSchema.primaryKeys().isEmpty()
                        || Objects.equals(newSchema.primaryKeys(), existsSchema.primaryKeys()))) {
            // check for options
            Map<String, String> existsOptions = existsSchema.options();
            Map<String, String> newOptions = newSchema.options();
            newOptions.forEach(
                    (key, value) -> {
                        // ignore `owner` and `path`
                        if (!key.equals(Catalog.OWNER_PROP)
                                && !key.equals(CoreOptions.PATH.key())
                                && (!existsOptions.containsKey(key)
                                        || !existsOptions.get(key).equals(value))) {
                            throw new RuntimeException(
                                    "New schema's options are not equal to the exists schema's, new schema: "
                                            + newOptions
                                            + ", exists schema: "
                                            + existsOptions);
                        }
                    });
        } else {
            throw new RuntimeException(
                    "New schema is not equal to exists schema, new schema: "
                            + newSchema
                            + ", exists schema: "
                            + existsSchema);
        }
    }

    /** Update {@link SchemaChange}s. */
    public TableSchema commitChanges(SchemaChange... changes) throws Exception {
        return commitChanges(Arrays.asList(changes));
    }

    /** Update {@link SchemaChange}s. */
    public TableSchema commitChanges(List<SchemaChange> changes)
            throws Catalog.TableNotExistException, Catalog.ColumnAlreadyExistException,
                    Catalog.ColumnNotExistException {
        SnapshotManager snapshotManager =
                new SnapshotManager(fileIO, tableRoot, branch, null, null);
        LazyField<Boolean> hasSnapshots =
                new LazyField<>(() -> snapshotManager.latestSnapshot() != null);

        while (true) {
            TableSchema oldTableSchema =
                    latest().orElseThrow(
                                    () ->
                                            new Catalog.TableNotExistException(
                                                    SchemaManager.identifierFromPath(
                                                            tableRoot.toString(), true, branch)));
            LazyField<Identifier> lazyIdentifier =
                    new LazyField<>(
                            () ->
                                    SchemaManager.identifierFromPath(
                                            tableRoot.toString(), true, branch));
            TableSchema newTableSchema =
                    SchemaManager.generateTableSchema(
                            oldTableSchema, changes, hasSnapshots, lazyIdentifier);
            try {
                boolean success = commit(newTableSchema);
                if (success) {
                    return newTableSchema;
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Merge {@code rowType} into the current schema (via {@link SchemaMergingUtils#mergeSchemas})
     * and persist the result. Returns {@code true} if the schema changed and was committed, {@code
     * false} if the merge was a no-op. See {@code SchemaMergingUtils} for how {@code typeWidening}
     * / {@code allowExplicitCast} drive existing-column type evolution.
     */
    public boolean mergeSchema(
            RowType rowType,
            boolean typeWidening,
            boolean allowExplicitCast,
            boolean caseSensitive,
            @Nullable SchemaModification schemaModification) {
        TableSchema current =
                latest().orElseThrow(
                                () ->
                                        new RuntimeException(
                                                "It requires that the current schema to exist when calling 'mergeSchema'"));
        TableSchema update =
                SchemaMergingUtils.mergeSchemas(
                        current, rowType, typeWidening, allowExplicitCast, caseSensitive);
        if (current.equals(update)) {
            return false;
        }
        try {
            if (schemaModification != null) {
                List<SchemaChange> changes =
                        SchemaMergingUtils.diffSchemaChanges(current, update, caseSensitive);
                schemaModification.alterSchema(changes);
                return true;
            } else {
                return commit(update);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to commit the schema.", e);
        }
    }

    @VisibleForTesting
    public boolean commit(TableSchema newSchema) throws Exception {
        SchemaValidation.validateTableSchema(newSchema);
        SchemaValidation.validateHistoricalIcebergTypes(
                this::listAll, new CoreOptions(newSchema.options()));
        SchemaValidation.validateFallbackBranch(this, newSchema);
        Path schemaPath = toSchemaPath(newSchema.id());
        return fileIO.tryToWriteAtomic(schemaPath, newSchema.toString());
    }

    /** Read schema for schema id. */
    public TableSchema schema(long id) {
        return SchemaManager.fromPath(fileIO, toSchemaPath(id));
    }

    @Override
    public TableSchema tryGetSchema(long id) throws FileNotFoundException {
        return SchemaManager.tryFromPath(fileIO, toSchemaPath(id));
    }

    /** Check if a schema exists. */
    public boolean schemaExists(long id) {
        Path path = toSchemaPath(id);
        try {
            return fileIO.exists(path);
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format(
                            "Failed to determine if schema '%s' exists in path %s.", id, path),
                    e);
        }
    }

    private String branchPath() {
        return BranchManager.branchPath(tableRoot, branch);
    }

    public Path schemaDirectory() {
        return new Path(branchPath() + "/schema");
    }

    @VisibleForTesting
    public Path toSchemaPath(long schemaId) {
        return new Path(branchPath() + "/schema/" + SCHEMA_PREFIX + schemaId);
    }

    public List<Path> schemaPaths(Predicate<Long> predicate) throws IOException {
        return listVersionedFiles(fileIO, schemaDirectory(), SCHEMA_PREFIX)
                .filter(predicate)
                .map(this::toSchemaPath)
                .collect(Collectors.toList());
    }

    /**
     * Delete schema with specific id.
     *
     * @param schemaId the schema id to delete.
     */
    public void deleteSchema(long schemaId) {
        fileIO.deleteQuietly(toSchemaPath(schemaId));
    }

    /**
     * Rollback to a specific schema version. All schema versions greater than the target will be
     * deleted. This operation will fail if any snapshot, tag, or changelog references a schema
     * version greater than the target.
     *
     * @param targetSchemaId the schema version to rollback to.
     * @param snapshotManager the snapshot manager to check snapshot references.
     * @param tagManager the tag manager to check tag references.
     * @param changelogManager the changelog manager to check changelog references.
     */
    public void rollbackTo(
            long targetSchemaId,
            SnapshotManager snapshotManager,
            TagManager tagManager,
            ChangelogManager changelogManager)
            throws IOException {
        checkArgument(schemaExists(targetSchemaId), "Schema %s does not exist.", targetSchemaId);

        // Collect all schemaIds referenced by snapshots, tags, and changelogs
        Set<Long> usedSchemaIds = new HashSet<>();

        snapshotManager.pickOrLatest(
                snapshot -> {
                    usedSchemaIds.add(snapshot.schemaId());
                    return false;
                });
        tagManager.taggedSnapshots().forEach(s -> usedSchemaIds.add(s.schemaId()));
        changelogManager.changelogs().forEachRemaining(c -> usedSchemaIds.add(c.schemaId()));

        // Check if any referenced schema is newer than the target
        Optional<Long> conflict =
                usedSchemaIds.stream().filter(id -> id > targetSchemaId).min(Long::compareTo);
        if (conflict.isPresent()) {
            throw new RuntimeException(
                    String.format(
                            "Cannot rollback to schema %d, schema %d is still referenced by snapshots/tags/changelogs.",
                            targetSchemaId, conflict.get()));
        }

        // Delete all schemas newer than the target
        List<Long> toBeDeleted =
                listAllIds().stream()
                        .filter(id -> id > targetSchemaId)
                        .collect(Collectors.toList());
        toBeDeleted.sort((o1, o2) -> Long.compare(o2, o1));
        for (Long id : toBeDeleted) {
            fileIO.delete(toSchemaPath(id), false);
        }
    }
}
