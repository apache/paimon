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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.SchemaModification;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.FunctionWithException;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;
import org.apache.paimon.utils.ThrowingConsumer;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * A {@link SchemaManager} implementation that delegates every schema query and mutation to the
 * owning {@link Catalog}. This mirrors the {@link org.apache.paimon.utils.CatalogBranchManager}
 * pattern for the {@code BranchManager} interface: on-filesystem operations (reading raw {@code
 * schema-*} files, deleting schemas, resolving schema paths) are intentionally not supported.
 *
 * <p>Read side collapses onto the single {@link Catalog#listSchemas(Identifier, SchemaFilter)}
 * endpoint; different callers populate the filter differently (latest / earliest / by id / by range
 * / all). Write side is routed through {@link Catalog#createTable(Identifier, Schema, boolean)},
 * {@link Catalog#alterTable(Identifier, List, boolean)} and {@link
 * Catalog#rollbackSchema(Identifier, long)}.
 */
@ThreadSafe
public class CatalogSchemaManager implements SchemaManager {

    private static final long serialVersionUID = 1L;

    private final CatalogLoader catalogLoader;
    private final Identifier identifier;

    public CatalogSchemaManager(CatalogLoader catalogLoader, Identifier identifier) {
        this.catalogLoader = catalogLoader;
        this.identifier = identifier;
    }

    @Override
    public SchemaManager copyWithBranch(String branchName) {
        Identifier branchIdentifier =
                new Identifier(identifier.getDatabaseName(), identifier.getTableName(), branchName);
        return new CatalogSchemaManager(catalogLoader, branchIdentifier);
    }

    @Override
    public Optional<TableSchema> latest() {
        return executeGet(
                catalog -> {
                    List<TableSchema> schemas =
                            catalog.listSchemas(identifier, SchemaFilter.latest());
                    if (schemas.isEmpty()) {
                        return Optional.empty();
                    }
                    return Optional.of(schemas.get(0));
                });
    }

    @Override
    public TableSchema latestOrThrow(String message) {
        return latest().orElseThrow(() -> new RuntimeException(message));
    }

    @Override
    public long earliestCreationTime() {
        return executeGet(
                catalog -> {
                    List<TableSchema> schemas =
                            catalog.listSchemas(identifier, SchemaFilter.earliest());
                    if (schemas.isEmpty()) {
                        throw new IllegalStateException("Table " + identifier + " has no schema.");
                    }
                    return schemas.get(0).timeMillis();
                });
    }

    @Override
    public List<TableSchema> listAll() {
        return executeGet(
                catalog -> {
                    List<TableSchema> schemas = catalog.listSchemas(identifier, SchemaFilter.all());
                    schemas.sort(Comparator.comparingLong(TableSchema::id));
                    return schemas;
                });
    }

    @Override
    public List<Long> listAllIds() {
        return listAll().stream().map(TableSchema::id).collect(Collectors.toList());
    }

    @Override
    public TableSchema createTable(Schema schema) throws Exception {
        return createTable(schema, false);
    }

    @Override
    public TableSchema createTable(Schema schema, boolean externalTable) throws Exception {
        executePost(catalog -> catalog.createTable(identifier, schema, false));
        return latestOrThrow(
                "Failed to load the newly created schema for table " + identifier + ".");
    }

    @Override
    public TableSchema commitChanges(SchemaChange... changes) throws Exception {
        return commitChanges(java.util.Arrays.asList(changes));
    }

    @Override
    public TableSchema commitChanges(List<SchemaChange> changes)
            throws Catalog.TableNotExistException, Catalog.ColumnAlreadyExistException,
                    Catalog.ColumnNotExistException {
        try (Catalog catalog = catalogLoader.load()) {
            catalog.alterTable(identifier, changes, false);
        } catch (Catalog.TableNotExistException
                | Catalog.ColumnAlreadyExistException
                | Catalog.ColumnNotExistException e) {
            throw e;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return latestOrThrow(
                "Failed to load the latest schema after altering table " + identifier + ".");
    }

    @Override
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
        List<SchemaChange> changes =
                SchemaMergingUtils.diffSchemaChanges(current, update, caseSensitive);
        try {
            if (schemaModification != null) {
                schemaModification.alterSchema(changes);
            } else {
                commitChanges(changes);
            }
            return true;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to commit the schema.", e);
        }
    }

    @Override
    public boolean commit(TableSchema newSchema) {
        throw new UnsupportedOperationException(
                "CatalogSchemaManager does not support committing a TableSchema directly; "
                        + "use commitChanges or the catalog createTable / alterTable APIs instead.");
    }

    @Override
    public TableSchema schema(long id) {
        return executeGet(
                catalog -> {
                    List<TableSchema> schemas =
                            catalog.listSchemas(identifier, SchemaFilter.withId(id));
                    if (schemas.isEmpty()) {
                        throw new IllegalStateException(
                                "Schema " + id + " not found for table " + identifier + ".");
                    }
                    return schemas.get(0);
                });
    }

    @Override
    public TableSchema tryGetSchema(long id) throws FileNotFoundException {
        List<TableSchema> schemas =
                executeGet(catalog -> catalog.listSchemas(identifier, SchemaFilter.withId(id)));
        if (schemas.isEmpty()) {
            throw new FileNotFoundException(
                    "Schema " + id + " not found for table " + identifier + ".");
        }
        return schemas.get(0);
    }

    @Override
    public boolean schemaExists(long id) {
        List<TableSchema> schemas =
                executeGet(catalog -> catalog.listSchemas(identifier, SchemaFilter.withId(id)));
        return !schemas.isEmpty();
    }

    @Override
    public Path schemaDirectory() {
        throw new UnsupportedOperationException(
                "CatalogSchemaManager does not expose a filesystem schema directory.");
    }

    @Override
    public Path toSchemaPath(long schemaId) {
        throw new UnsupportedOperationException(
                "CatalogSchemaManager does not expose a filesystem schema path.");
    }

    @Override
    public List<Path> schemaPaths(Predicate<Long> predicate) throws IOException {
        throw new UnsupportedOperationException(
                "CatalogSchemaManager does not expose filesystem schema paths.");
    }

    @Override
    public void deleteSchema(long schemaId) {
        throw new UnsupportedOperationException(
                "CatalogSchemaManager does not support deleting a single schema; "
                        + "use catalog.rollbackSchema instead.");
    }

    @Override
    public void rollbackTo(
            long targetSchemaId,
            SnapshotManager snapshotManager,
            TagManager tagManager,
            ChangelogManager changelogManager) {
        executePost(catalog -> catalog.rollbackSchema(identifier, targetSchemaId));
    }

    private void executePost(ThrowingConsumer<Catalog, Exception> func) {
        executeGet(
                catalog -> {
                    try {
                        func.accept(catalog);
                        return null;
                    } catch (Catalog.TableNotExistException e) {
                        throw new IllegalArgumentException(
                                String.format(
                                        "Table '%s' doesn't exist.", e.identifier().getFullName()));
                    } catch (Catalog.DatabaseNotExistException e) {
                        throw new IllegalArgumentException(
                                String.format("Database '%s' doesn't exist.", e.database()));
                    } catch (Catalog.TableAlreadyExistException e) {
                        throw new IllegalArgumentException(
                                String.format(
                                        "Table '%s' already exists.",
                                        e.identifier().getFullName()));
                    }
                });
    }

    private <T> T executeGet(FunctionWithException<Catalog, T, Exception> func) {
        try (Catalog catalog = catalogLoader.load()) {
            return func.apply(catalog);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
