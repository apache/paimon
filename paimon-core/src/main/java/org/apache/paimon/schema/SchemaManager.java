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
import org.apache.paimon.casting.CastExecutors;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.schema.SchemaChange.AddColumn;
import org.apache.paimon.schema.SchemaChange.DropColumn;
import org.apache.paimon.schema.SchemaChange.RemoveOption;
import org.apache.paimon.schema.SchemaChange.RenameColumn;
import org.apache.paimon.schema.SchemaChange.SetOption;
import org.apache.paimon.schema.SchemaChange.UpdateColumnComment;
import org.apache.paimon.schema.SchemaChange.UpdateColumnNullability;
import org.apache.paimon.schema.SchemaChange.UpdateColumnPosition;
import org.apache.paimon.schema.SchemaChange.UpdateColumnType;
import org.apache.paimon.schema.SchemaChange.UpdateComment;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeCasts;
import org.apache.paimon.types.ReassignFieldId;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.StringUtils;

import org.apache.paimon.shade.guava30.com.google.common.base.Joiner;
import org.apache.paimon.shade.guava30.com.google.common.collect.Iterables;
import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.apache.paimon.CoreOptions.BUCKET_KEY;
import static org.apache.paimon.catalog.AbstractCatalog.DB_SUFFIX;
import static org.apache.paimon.catalog.Identifier.UNKNOWN_DATABASE;
import static org.apache.paimon.utils.BranchManager.DEFAULT_MAIN_BRANCH;
import static org.apache.paimon.utils.FileUtils.listVersionedFiles;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Schema Manager to manage schema versions. */
@ThreadSafe
public class SchemaManager implements Serializable {

    private static final String SCHEMA_PREFIX = "schema-";

    private final FileIO fileIO;
    private final Path tableRoot;

    @Nullable private transient Lock lock;

    private final String branch;

    public SchemaManager(FileIO fileIO, Path tableRoot) {
        this(fileIO, tableRoot, DEFAULT_MAIN_BRANCH);
    }

    /** Specify the default branch for data writing. */
    public SchemaManager(FileIO fileIO, Path tableRoot, String branch) {
        this.fileIO = fileIO;
        this.tableRoot = tableRoot;
        this.branch = BranchManager.normalizeBranch(branch);
    }

    public SchemaManager copyWithBranch(String branchName) {
        return new SchemaManager(fileIO, tableRoot, branchName);
    }

    public SchemaManager withLock(@Nullable Lock lock) {
        this.lock = lock;
        return this;
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

    public List<TableSchema> schemasWithId(List<Long> schemaIds) {
        return schemaIds.stream().map(this::schema).collect(Collectors.toList());
    }

    public List<TableSchema> listWithRange(
            Optional<Long> optionalMaxSchemaId, Optional<Long> optionalMinSchemaId) {
        Long lowerBoundSchemaId = 0L;
        Long upperBoundSchematId = latest().get().id();

        // null check on optionalMaxSchemaId & optionalMinSchemaId return all schemas
        if (!optionalMaxSchemaId.isPresent() && !optionalMinSchemaId.isPresent()) {
            return listAll();
        }

        if (optionalMaxSchemaId.isPresent()) {
            if (optionalMaxSchemaId.get() < lowerBoundSchemaId) {
                throw new RuntimeException(
                        String.format(
                                "schema id: %s should not lower than min schema id: %s",
                                optionalMaxSchemaId.get(), lowerBoundSchemaId));
            }
            upperBoundSchematId =
                    optionalMaxSchemaId.get() > upperBoundSchematId
                            ? upperBoundSchematId
                            : optionalMaxSchemaId.get();
        }

        if (optionalMinSchemaId.isPresent()) {
            if (optionalMinSchemaId.get() > upperBoundSchematId) {
                throw new RuntimeException(
                        String.format(
                                "schema id: %s should not greater than max schema id: %s",
                                optionalMinSchemaId.get(), upperBoundSchematId));
            }
            lowerBoundSchemaId =
                    optionalMinSchemaId.get() > lowerBoundSchemaId
                            ? optionalMinSchemaId.get()
                            : lowerBoundSchemaId;
        }

        // +1 here to include the upperBoundSchemaId
        return LongStream.range(lowerBoundSchemaId, upperBoundSchematId + 1)
                .mapToObj(this::schema)
                .sorted(Comparator.comparingLong(TableSchema::id))
                .collect(Collectors.toList());
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

    public TableSchema createTable(Schema schema, boolean ignoreIfExistsSame) throws Exception {
        while (true) {
            Optional<TableSchema> latest = latest();
            if (latest.isPresent()) {
                TableSchema oldSchema = latest.get();
                boolean isSame =
                        Objects.equals(oldSchema.fields(), schema.fields())
                                && Objects.equals(oldSchema.partitionKeys(), schema.partitionKeys())
                                && Objects.equals(oldSchema.primaryKeys(), schema.primaryKeys())
                                && Objects.equals(oldSchema.options(), schema.options());
                if (ignoreIfExistsSame && isSame) {
                    return oldSchema;
                }

                throw new IllegalStateException(
                        "Schema in filesystem exists, please use updating,"
                                + " latest schema is: "
                                + oldSchema);
            }

            List<DataField> fields = schema.fields();
            List<String> partitionKeys = schema.partitionKeys();
            List<String> primaryKeys = schema.primaryKeys();
            Map<String, String> options = schema.options();
            int highestFieldId = RowType.currentHighestFieldId(fields);

            TableSchema newSchema =
                    new TableSchema(
                            0,
                            fields,
                            highestFieldId,
                            partitionKeys,
                            primaryKeys,
                            options,
                            schema.comment());

            // validate table from creating table
            FileStoreTableFactory.create(fileIO, tableRoot, newSchema).store();

            boolean success = commit(newSchema);
            if (success) {
                return newSchema;
            }
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
        SnapshotManager snapshotManager = new SnapshotManager(fileIO, tableRoot, branch);
        boolean hasSnapshots = (snapshotManager.latestSnapshotId() != null);

        while (true) {
            TableSchema oldTableSchema =
                    latest().orElseThrow(
                                    () ->
                                            new Catalog.TableNotExistException(
                                                    identifierFromPath(
                                                            tableRoot.toString(), true, branch)));
            Map<String, String> oldOptions = new HashMap<>(oldTableSchema.options());
            Map<String, String> newOptions = new HashMap<>(oldTableSchema.options());
            List<DataField> newFields = new ArrayList<>(oldTableSchema.fields());
            AtomicInteger highestFieldId = new AtomicInteger(oldTableSchema.highestFieldId());
            String newComment = oldTableSchema.comment();
            for (SchemaChange change : changes) {
                if (change instanceof SetOption) {
                    SetOption setOption = (SetOption) change;
                    if (hasSnapshots) {
                        checkAlterTableOption(
                                setOption.key(),
                                oldOptions.get(setOption.key()),
                                setOption.value(),
                                false);
                    }
                    newOptions.put(setOption.key(), setOption.value());
                } else if (change instanceof RemoveOption) {
                    RemoveOption removeOption = (RemoveOption) change;
                    if (hasSnapshots) {
                        checkResetTableOption(removeOption.key());
                    }
                    newOptions.remove(removeOption.key());
                } else if (change instanceof UpdateComment) {
                    UpdateComment updateComment = (UpdateComment) change;
                    newComment = updateComment.comment();
                } else if (change instanceof AddColumn) {
                    AddColumn addColumn = (AddColumn) change;
                    SchemaChange.Move move = addColumn.move();
                    Preconditions.checkArgument(
                            addColumn.dataType().isNullable(),
                            "Column %s cannot specify NOT NULL in the %s table.",
                            String.join(".", addColumn.fieldNames()),
                            identifierFromPath(tableRoot.toString(), true, branch).getFullName());
                    int id = highestFieldId.incrementAndGet();
                    DataType dataType =
                            ReassignFieldId.reassign(addColumn.dataType(), highestFieldId);

                    new NestedColumnModifier(addColumn.fieldNames().toArray(new String[0])) {
                        @Override
                        protected void updateLastColumn(List<DataField> newFields, String fieldName)
                                throws Catalog.ColumnAlreadyExistException {
                            assertColumnNotExists(newFields, fieldName);

                            DataField dataField =
                                    new DataField(id, fieldName, dataType, addColumn.description());

                            // key: name ; value : index
                            Map<String, Integer> map = new HashMap<>();
                            for (int i = 0; i < newFields.size(); i++) {
                                map.put(newFields.get(i).name(), i);
                            }

                            if (null != move) {
                                if (move.type().equals(SchemaChange.Move.MoveType.FIRST)) {
                                    newFields.add(0, dataField);
                                } else if (move.type().equals(SchemaChange.Move.MoveType.AFTER)) {
                                    int fieldIndex = map.get(move.referenceFieldName());
                                    newFields.add(fieldIndex + 1, dataField);
                                }
                            } else {
                                newFields.add(dataField);
                            }
                        }
                    }.updateIntermediateColumn(newFields, 0);
                } else if (change instanceof RenameColumn) {
                    RenameColumn rename = (RenameColumn) change;
                    assertNotUpdatingPrimaryKeys(oldTableSchema, rename.fieldNames(), "rename");
                    new NestedColumnModifier(rename.fieldNames().toArray(new String[0])) {
                        @Override
                        protected void updateLastColumn(List<DataField> newFields, String fieldName)
                                throws Catalog.ColumnNotExistException,
                                        Catalog.ColumnAlreadyExistException {
                            assertColumnExists(newFields, fieldName);
                            assertColumnNotExists(newFields, rename.newName());
                            for (int i = 0; i < newFields.size(); i++) {
                                DataField field = newFields.get(i);
                                if (!field.name().equals(fieldName)) {
                                    continue;
                                }

                                DataField newField =
                                        new DataField(
                                                field.id(),
                                                rename.newName(),
                                                field.type(),
                                                field.description());
                                newFields.set(i, newField);
                                return;
                            }
                        }
                    }.updateIntermediateColumn(newFields, 0);
                } else if (change instanceof DropColumn) {
                    DropColumn drop = (DropColumn) change;
                    dropColumnValidation(oldTableSchema, drop);
                    new NestedColumnModifier(drop.fieldNames().toArray(new String[0])) {
                        @Override
                        protected void updateLastColumn(List<DataField> newFields, String fieldName)
                                throws Catalog.ColumnNotExistException {
                            assertColumnExists(newFields, fieldName);
                            newFields.removeIf(f -> f.name().equals(fieldName));
                            if (newFields.isEmpty()) {
                                throw new IllegalArgumentException(
                                        "Cannot drop all fields in table");
                            }
                        }
                    }.updateIntermediateColumn(newFields, 0);
                } else if (change instanceof UpdateColumnType) {
                    UpdateColumnType update = (UpdateColumnType) change;
                    assertNotUpdatingPrimaryKeys(oldTableSchema, update.fieldNames(), "update");
                    updateNestedColumn(
                            newFields,
                            update.fieldNames().toArray(new String[0]),
                            (field) -> {
                                DataType targetType = update.newDataType();
                                if (update.keepNullability()) {
                                    targetType = targetType.copy(field.type().isNullable());
                                }
                                checkState(
                                        DataTypeCasts.supportsExplicitCast(field.type(), targetType)
                                                && CastExecutors.resolve(field.type(), targetType)
                                                        != null,
                                        String.format(
                                                "Column type %s[%s] cannot be converted to %s without loosing information.",
                                                field.name(), field.type(), targetType));
                                return new DataField(
                                        field.id(), field.name(), targetType, field.description());
                            });
                } else if (change instanceof UpdateColumnNullability) {
                    UpdateColumnNullability update = (UpdateColumnNullability) change;
                    if (update.fieldNames().length == 1
                            && update.newNullability()
                            && oldTableSchema.primaryKeys().contains(update.fieldNames()[0])) {
                        throw new UnsupportedOperationException(
                                "Cannot change nullability of primary key");
                    }
                    updateNestedColumn(
                            newFields,
                            update.fieldNames(),
                            (field) ->
                                    new DataField(
                                            field.id(),
                                            field.name(),
                                            field.type().copy(update.newNullability()),
                                            field.description()));
                } else if (change instanceof UpdateColumnComment) {
                    UpdateColumnComment update = (UpdateColumnComment) change;
                    updateNestedColumn(
                            newFields,
                            update.fieldNames(),
                            (field) ->
                                    new DataField(
                                            field.id(),
                                            field.name(),
                                            field.type(),
                                            update.newDescription()));
                } else if (change instanceof UpdateColumnPosition) {
                    UpdateColumnPosition update = (UpdateColumnPosition) change;
                    SchemaChange.Move move = update.move();
                    applyMove(newFields, move);
                } else {
                    throw new UnsupportedOperationException(
                            "Unsupported change: " + change.getClass());
                }
            }

            // We change TableSchema to Schema, because we want to deal with primary-key and
            // partition in options.
            Schema newSchema =
                    new Schema(
                            newFields,
                            oldTableSchema.partitionKeys(),
                            applyNotNestedColumnRename(
                                    oldTableSchema.primaryKeys(),
                                    Iterables.filter(changes, RenameColumn.class)),
                            applySchemaChanges(newOptions, changes),
                            newComment);
            TableSchema newTableSchema =
                    new TableSchema(
                            oldTableSchema.id() + 1,
                            newSchema.fields(),
                            highestFieldId.get(),
                            newSchema.partitionKeys(),
                            newSchema.primaryKeys(),
                            newSchema.options(),
                            newSchema.comment());

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

    public void applyMove(List<DataField> newFields, SchemaChange.Move move) {
        Map<String, Integer> map = new HashMap<>();
        for (int i = 0; i < newFields.size(); i++) {
            map.put(newFields.get(i).name(), i);
        }

        int fieldIndex = map.getOrDefault(move.fieldName(), -1);
        if (fieldIndex == -1) {
            throw new IllegalArgumentException("Field name not found: " + move.fieldName());
        }

        // Handling FIRST and LAST cases directly since they don't need refIndex
        switch (move.type()) {
            case FIRST:
                checkMoveIndexEqual(move, fieldIndex, 0);
                moveField(newFields, fieldIndex, 0);
                return;
            case LAST:
                checkMoveIndexEqual(move, fieldIndex, newFields.size() - 1);
                moveField(newFields, fieldIndex, newFields.size() - 1);
                return;
        }

        Integer refIndex = map.getOrDefault(move.referenceFieldName(), -1);
        if (refIndex == -1) {
            throw new IllegalArgumentException(
                    "Reference field name not found: " + move.referenceFieldName());
        }

        checkMoveIndexEqual(move, fieldIndex, refIndex);

        // For AFTER and BEFORE, adjust the target index based on current and reference positions
        int targetIndex = refIndex;
        if (move.type() == SchemaChange.Move.MoveType.AFTER && fieldIndex > refIndex) {
            targetIndex++;
        }
        // Ensure adjustments for moving element forwards or backwards
        if (move.type() == SchemaChange.Move.MoveType.BEFORE && fieldIndex < refIndex) {
            targetIndex--;
        }

        if (targetIndex > (newFields.size() - 1)) {
            targetIndex = newFields.size() - 1;
        }

        moveField(newFields, fieldIndex, targetIndex);
    }

    // Utility method to move a field within the list, handling range checks
    private void moveField(List<DataField> newFields, int fromIndex, int toIndex) {
        if (fromIndex < 0 || fromIndex >= newFields.size() || toIndex < 0) {
            return;
        }
        DataField fieldToMove = newFields.remove(fromIndex);
        newFields.add(toIndex, fieldToMove);
    }

    private static void checkMoveIndexEqual(SchemaChange.Move move, int fieldIndex, int refIndex) {
        if (refIndex == fieldIndex) {
            throw new UnsupportedOperationException(
                    String.format("Cannot move itself for column %s", move.fieldName()));
        }
    }

    public boolean mergeSchema(RowType rowType, boolean allowExplicitCast) {
        TableSchema current =
                latest().orElseThrow(
                                () ->
                                        new RuntimeException(
                                                "It requires that the current schema to exist when calling 'mergeSchema'"));
        TableSchema update = SchemaMergingUtils.mergeSchemas(current, rowType, allowExplicitCast);
        if (current.equals(update)) {
            return false;
        } else {
            try {
                return commit(update);
            } catch (Exception e) {
                throw new RuntimeException("Failed to commit the schema.", e);
            }
        }
    }

    private static Map<String, String> applySchemaChanges(
            Map<String, String> options, Iterable<SchemaChange> changes) {
        Map<String, String> newOptions = Maps.newHashMap(options);
        String bucketKeysStr = options.get(BUCKET_KEY.key());
        if (!StringUtils.isNullOrWhitespaceOnly(bucketKeysStr)) {
            List<String> bucketColumns = Arrays.asList(bucketKeysStr.split(","));
            List<String> newBucketColumns =
                    applyNotNestedColumnRename(
                            bucketColumns, Iterables.filter(changes, RenameColumn.class));
            newOptions.put(BUCKET_KEY.key(), Joiner.on(',').join(newBucketColumns));
        }

        // TODO: Apply changes to other options that contain column names, such as `sequence.field`
        return newOptions;
    }

    // Apply column rename changes on not nested columns to the list of column names, this will not
    // change the order of the column names
    private static List<String> applyNotNestedColumnRename(
            List<String> columns, Iterable<RenameColumn> renames) {
        if (Iterables.isEmpty(renames)) {
            return columns;
        }

        Map<String, String> columnNames = Maps.newHashMap();
        for (RenameColumn renameColumn : renames) {
            if (renameColumn.fieldNames().size() == 1) {
                columnNames.put(renameColumn.fieldNames().get(0), renameColumn.newName());
            }
        }

        // The order of the column names will be preserved, as a non-parallel stream is used here.
        return columns.stream()
                .map(column -> columnNames.getOrDefault(column, column))
                .collect(Collectors.toList());
    }

    private static void dropColumnValidation(TableSchema schema, DropColumn change) {
        // primary keys and partition keys can't be nested columns
        if (change.fieldNames().size() > 1) {
            return;
        }
        String columnToDrop = change.fieldNames().get(0);
        if (schema.partitionKeys().contains(columnToDrop)
                || schema.primaryKeys().contains(columnToDrop)) {
            throw new UnsupportedOperationException(
                    String.format("Cannot drop partition key or primary key: [%s]", columnToDrop));
        }
    }

    private static void assertNotUpdatingPrimaryKeys(
            TableSchema schema, List<String> fieldNames, String operation) {
        // partition keys can't be nested columns
        if (fieldNames.size() > 1) {
            return;
        }
        String columnToRename = fieldNames.get(0);
        if (schema.partitionKeys().contains(columnToRename)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Cannot " + operation + " partition column: [%s]", columnToRename));
        }
    }

    private abstract class NestedColumnModifier {

        private final String[] updateFieldNames;

        private NestedColumnModifier(String[] updateFieldNames) {
            this.updateFieldNames = updateFieldNames;
        }

        public void updateIntermediateColumn(List<DataField> newFields, int depth)
                throws Catalog.ColumnNotExistException, Catalog.ColumnAlreadyExistException {
            if (depth == updateFieldNames.length - 1) {
                updateLastColumn(newFields, updateFieldNames[depth]);
                return;
            }

            for (int i = 0; i < newFields.size(); i++) {
                DataField field = newFields.get(i);
                if (!field.name().equals(updateFieldNames[depth])) {
                    continue;
                }

                List<DataField> nestedFields =
                        new ArrayList<>(
                                ((org.apache.paimon.types.RowType) field.type()).getFields());
                updateIntermediateColumn(nestedFields, depth + 1);
                newFields.set(
                        i,
                        new DataField(
                                field.id(),
                                field.name(),
                                new org.apache.paimon.types.RowType(
                                        field.type().isNullable(), nestedFields),
                                field.description()));
                return;
            }

            throw new Catalog.ColumnNotExistException(
                    identifierFromPath(tableRoot.toString(), true, branch),
                    String.join(".", Arrays.asList(updateFieldNames).subList(0, depth + 1)));
        }

        protected abstract void updateLastColumn(List<DataField> newFields, String fieldName)
                throws Catalog.ColumnNotExistException, Catalog.ColumnAlreadyExistException;

        protected void assertColumnExists(List<DataField> newFields, String fieldName)
                throws Catalog.ColumnNotExistException {
            for (DataField field : newFields) {
                if (field.name().equals(fieldName)) {
                    return;
                }
            }
            throw new Catalog.ColumnNotExistException(
                    identifierFromPath(tableRoot.toString(), true, branch),
                    getLastFieldName(fieldName));
        }

        protected void assertColumnNotExists(List<DataField> newFields, String fieldName)
                throws Catalog.ColumnAlreadyExistException {
            for (DataField field : newFields) {
                if (field.name().equals(fieldName)) {
                    throw new Catalog.ColumnAlreadyExistException(
                            identifierFromPath(tableRoot.toString(), true, branch),
                            getLastFieldName(fieldName));
                }
            }
        }

        private String getLastFieldName(String fieldName) {
            List<String> fieldNames = new ArrayList<>();
            for (int i = 0; i + 1 < updateFieldNames.length; i++) {
                fieldNames.add(updateFieldNames[i]);
            }
            fieldNames.add(fieldName);
            return String.join(".", fieldNames);
        }
    }

    private void updateNestedColumn(
            List<DataField> newFields,
            String[] updateFieldNames,
            Function<DataField, DataField> updateFunc)
            throws Catalog.ColumnNotExistException, Catalog.ColumnAlreadyExistException {
        new NestedColumnModifier(updateFieldNames) {
            @Override
            protected void updateLastColumn(List<DataField> newFields, String fieldName)
                    throws Catalog.ColumnNotExistException {
                for (int i = 0; i < newFields.size(); i++) {
                    DataField field = newFields.get(i);
                    if (!field.name().equals(fieldName)) {
                        continue;
                    }

                    newFields.set(i, updateFunc.apply(field));
                    return;
                }

                throw new Catalog.ColumnNotExistException(
                        identifierFromPath(tableRoot.toString(), true, branch),
                        String.join(".", updateFieldNames));
            }
        }.updateIntermediateColumn(newFields, 0);
    }

    @VisibleForTesting
    boolean commit(TableSchema newSchema) throws Exception {
        SchemaValidation.validateTableSchema(newSchema);
        SchemaValidation.validateFallbackBranch(this, newSchema);
        Path schemaPath = toSchemaPath(newSchema.id());
        Callable<Boolean> callable =
                () -> fileIO.tryToWriteAtomic(schemaPath, newSchema.toString());
        if (lock == null) {
            return callable.call();
        }
        return lock.runWithLock(callable);
    }

    /** Read schema for schema id. */
    public TableSchema schema(long id) {
        try {
            return JsonSerdeUtil.fromJson(fileIO.readFileUtf8(toSchemaPath(id)), TableSchema.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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

    public static TableSchema fromPath(FileIO fileIO, Path path) {
        try {
            return JsonSerdeUtil.fromJson(fileIO.readFileUtf8(path), TableSchema.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
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

    public static void checkAlterTableOption(
            String key, @Nullable String oldValue, String newValue, boolean fromDynamicOptions) {
        if (CoreOptions.IMMUTABLE_OPTIONS.contains(key)) {
            throw new UnsupportedOperationException(
                    String.format("Change '%s' is not supported yet.", key));
        }

        if (CoreOptions.BUCKET.key().equals(key)) {
            int oldBucket =
                    oldValue == null
                            ? CoreOptions.BUCKET.defaultValue()
                            : Integer.parseInt(oldValue);
            int newBucket = Integer.parseInt(newValue);

            if (fromDynamicOptions) {
                throw new UnsupportedOperationException(
                        "Cannot change bucket number through dynamic options. You might need to rescale bucket.");
            }
            if (oldBucket == -1) {
                throw new UnsupportedOperationException("Cannot change bucket when it is -1.");
            }
            if (newBucket == -1) {
                throw new UnsupportedOperationException("Cannot change bucket to -1.");
            }
        }
    }

    public static void checkResetTableOption(String key) {
        if (CoreOptions.IMMUTABLE_OPTIONS.contains(key)) {
            throw new UnsupportedOperationException(
                    String.format("Change '%s' is not supported yet.", key));
        }

        if (CoreOptions.BUCKET.key().equals(key)) {
            throw new UnsupportedOperationException(String.format("Cannot reset %s.", key));
        }
    }

    public static void checkAlterTablePath(String key) {
        if (CoreOptions.PATH.key().equalsIgnoreCase(key)) {
            throw new UnsupportedOperationException("Change path is not supported yet.");
        }
    }

    public static Identifier identifierFromPath(String tablePath, boolean ignoreIfUnknownDatabase) {
        return identifierFromPath(tablePath, ignoreIfUnknownDatabase, null);
    }

    public static Identifier identifierFromPath(
            String tablePath, boolean ignoreIfUnknownDatabase, @Nullable String branchName) {
        if (DEFAULT_MAIN_BRANCH.equals(branchName)) {
            branchName = null;
        }

        String[] paths = tablePath.split("/");
        if (paths.length < 2) {
            if (!ignoreIfUnknownDatabase) {
                throw new IllegalArgumentException(
                        String.format(
                                "Path '%s' is not a valid path, please use catalog table path instead: 'warehouse_path/your_database.db/your_table'.",
                                tablePath));
            }
            return new Identifier(UNKNOWN_DATABASE, paths[0]);
        }

        String database = paths[paths.length - 2];
        int index = database.lastIndexOf(DB_SUFFIX);
        if (index == -1) {
            if (!ignoreIfUnknownDatabase) {
                throw new IllegalArgumentException(
                        String.format(
                                "Path '%s' is not a valid path, please use catalog table path instead: 'warehouse_path/your_database.db/your_table'.",
                                tablePath));
            }
            return new Identifier(UNKNOWN_DATABASE, paths[paths.length - 1], branchName, null);
        }
        database = database.substring(0, index);

        return new Identifier(database, paths[paths.length - 1], branchName, null);
    }
}
