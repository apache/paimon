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
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.SchemaModification;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.LazyField;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * Schema manager abstraction.
 *
 * <p>The default implementation is {@link FileSystemSchemaManager}. Catalogs can provide another
 * implementation while table components continue to depend on this interface.
 */
@ThreadSafe
public interface SchemaManager extends Serializable {

    String SCHEMA_PREFIX = "schema-";

    SchemaManager copyWithBranch(String branchName);

    Optional<TableSchema> latest();

    TableSchema latestOrThrow(String message);

    long earliestCreationTime();

    List<TableSchema> listAll();

    List<Long> listAllIds();

    TableSchema createTable(Schema schema) throws Exception;

    TableSchema createTable(Schema schema, boolean externalTable) throws Exception;

    TableSchema commitChanges(SchemaChange... changes) throws Exception;

    TableSchema commitChanges(List<SchemaChange> changes)
            throws Catalog.TableNotExistException, Catalog.ColumnAlreadyExistException,
                    Catalog.ColumnNotExistException;

    boolean mergeSchema(
            RowType rowType,
            boolean typeWidening,
            boolean allowExplicitCast,
            boolean caseSensitive,
            @Nullable SchemaModification schemaModification);

    boolean commit(TableSchema newSchema) throws Exception;

    TableSchema schema(long id);

    TableSchema tryGetSchema(long id) throws FileNotFoundException;

    boolean schemaExists(long id);

    Path schemaDirectory();

    Path toSchemaPath(long schemaId);

    List<Path> schemaPaths(Predicate<Long> predicate) throws IOException;

    void deleteSchema(long schemaId);

    void rollbackTo(
            long targetSchemaId,
            SnapshotManager snapshotManager,
            TagManager tagManager,
            ChangelogManager changelogManager)
            throws IOException;

    static TableSchema generateTableSchema(
            TableSchema oldTableSchema,
            List<SchemaChange> changes,
            LazyField<Boolean> hasSnapshots,
            LazyField<Identifier> lazyIdentifier)
            throws Catalog.ColumnAlreadyExistException, Catalog.ColumnNotExistException {
        return SchemaManagerUtils.generateTableSchema(
                oldTableSchema, changes, hasSnapshots, lazyIdentifier);
    }

    static void applyMove(List<DataField> newFields, SchemaChange.Move move) {
        SchemaManagerUtils.applyMove(newFields, move);
    }

    static boolean isUnchangedNormalizedKey(
            String key,
            @Nullable String oldValue,
            @Nullable String newValue,
            TableSchema tableSchema) {
        return SchemaManagerUtils.isUnchangedNormalizedKey(key, oldValue, newValue, tableSchema);
    }

    static boolean isUnchangedNormalizedKey(
            String key,
            @Nullable String oldValue,
            @Nullable String newValue,
            List<String> primaryKeys,
            List<String> partitionKeys) {
        return SchemaManagerUtils.isUnchangedNormalizedKey(
                key, oldValue, newValue, primaryKeys, partitionKeys);
    }

    static void checkAlterTableOption(
            Map<String, String> options, String key, @Nullable String oldValue, String newValue) {
        SchemaManagerUtils.checkAlterTableOption(options, key, oldValue, newValue);
    }

    static void checkResetTableOption(Map<String, String> options, String key) {
        SchemaManagerUtils.checkResetTableOption(options, key);
    }

    static void checkAlterTablePath(String key) {
        SchemaManagerUtils.checkAlterTablePath(key);
    }

    static Identifier identifierFromPath(String tablePath, boolean ignoreIfUnknownDatabase) {
        return SchemaManagerUtils.identifierFromPath(tablePath, ignoreIfUnknownDatabase);
    }

    static Identifier identifierFromPath(
            String tablePath, boolean ignoreIfUnknownDatabase, @Nullable String branchName) {
        return SchemaManagerUtils.identifierFromPath(
                tablePath, ignoreIfUnknownDatabase, branchName);
    }

    static TableSchema fromPath(FileIO fileIO, Path path) {
        return SchemaManagerUtils.fromPath(fileIO, path);
    }

    static TableSchema tryFromPath(FileIO fileIO, Path path) throws FileNotFoundException {
        return SchemaManagerUtils.tryFromPath(fileIO, path);
    }
}
