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
import org.apache.paimon.schema.SchemaChange.AddColumn;
import org.apache.paimon.schema.SchemaChange.DropColumn;
import org.apache.paimon.schema.SchemaChange.RemoveOption;
import org.apache.paimon.schema.SchemaChange.RenameColumn;
import org.apache.paimon.schema.SchemaChange.SetOption;
import org.apache.paimon.schema.SchemaChange.UpdateColumnComment;
import org.apache.paimon.schema.SchemaChange.UpdateColumnDefaultValue;
import org.apache.paimon.schema.SchemaChange.UpdateColumnNullability;
import org.apache.paimon.schema.SchemaChange.UpdateColumnPosition;
import org.apache.paimon.schema.SchemaChange.UpdateColumnType;
import org.apache.paimon.schema.SchemaChange.UpdateComment;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeCasts;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.ReassignFieldId;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.LazyField;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.StringUtils;

import org.apache.paimon.shade.guava30.com.google.common.collect.FluentIterable;
import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;
import org.apache.paimon.shade.guava30.com.google.common.collect.Iterables;
import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;
import org.apache.paimon.shade.guava30.com.google.common.collect.Streams;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.AGG_FUNCTION;
import static org.apache.paimon.CoreOptions.BUCKET_KEY;
import static org.apache.paimon.CoreOptions.DELETION_VECTORS_ENABLED;
import static org.apache.paimon.CoreOptions.DELETION_VECTORS_MODIFIABLE;
import static org.apache.paimon.CoreOptions.DISTINCT;
import static org.apache.paimon.CoreOptions.FIELDS_PREFIX;
import static org.apache.paimon.CoreOptions.IGNORE_DELETE;
import static org.apache.paimon.CoreOptions.IGNORE_RETRACT;
import static org.apache.paimon.CoreOptions.IGNORE_UPDATE_BEFORE;
import static org.apache.paimon.CoreOptions.LIST_AGG_DELIMITER;
import static org.apache.paimon.CoreOptions.NESTED_KEY;
import static org.apache.paimon.CoreOptions.SEQUENCE_FIELD;
import static org.apache.paimon.catalog.AbstractCatalog.DB_SUFFIX;
import static org.apache.paimon.catalog.Identifier.DEFAULT_MAIN_BRANCH;
import static org.apache.paimon.catalog.Identifier.UNKNOWN_DATABASE;
import static org.apache.paimon.mergetree.compact.PartialUpdateMergeFunction.SEQUENCE_GROUP;
import static org.apache.paimon.utils.DefaultValueUtils.validateDefaultValue;
import static org.apache.paimon.utils.FileUtils.listVersionedFiles;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Schema Manager to manage schema versions. */
@ThreadSafe
public class SchemaManager implements Serializable {

    private static final String SCHEMA_PREFIX = "schema-";

    private final FileIO fileIO;
    private final Path tableRoot;

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
                                                    identifierFromPath(
                                                            tableRoot.toString(), true, branch)));
            LazyField<Identifier> lazyIdentifier =
                    new LazyField<>(() -> identifierFromPath(tableRoot.toString(), true, branch));
            TableSchema newTableSchema =
                    generateTableSchema(oldTableSchema, changes, hasSnapshots, lazyIdentifier);
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

    public static TableSchema generateTableSchema(
            TableSchema oldTableSchema,
            List<SchemaChange> changes,
            LazyField<Boolean> hasSnapshots,
            LazyField<Identifier> lazyIdentifier)
            throws Catalog.ColumnAlreadyExistException, Catalog.ColumnNotExistException {
        Map<String, String> oldOptions = new HashMap<>(oldTableSchema.options());
        Map<String, String> newOptions = new HashMap<>(oldTableSchema.options());
        boolean disableNullToNotNull =
                Boolean.parseBoolean(
                        oldOptions.getOrDefault(
                                CoreOptions.DISABLE_ALTER_COLUMN_NULL_TO_NOT_NULL.key(),
                                CoreOptions.DISABLE_ALTER_COLUMN_NULL_TO_NOT_NULL
                                        .defaultValue()
                                        .toString()));

        boolean disableExplicitTypeCasting =
                Boolean.parseBoolean(
                        oldOptions.getOrDefault(
                                CoreOptions.DISABLE_EXPLICIT_TYPE_CASTING.key(),
                                CoreOptions.DISABLE_EXPLICIT_TYPE_CASTING
                                        .defaultValue()
                                        .toString()));
        List<DataField> newFields = new ArrayList<>(oldTableSchema.fields());
        AtomicInteger highestFieldId = new AtomicInteger(oldTableSchema.highestFieldId());
        String newComment = oldTableSchema.comment();
        for (SchemaChange change : changes) {
            if (change instanceof SetOption) {
                SetOption setOption = (SetOption) change;
                if (hasSnapshots.get()) {
                    checkAlterTableOption(
                            oldOptions,
                            setOption.key(),
                            oldOptions.get(setOption.key()),
                            setOption.value());
                }
                newOptions.put(setOption.key(), setOption.value());
            } else if (change instanceof RemoveOption) {
                RemoveOption removeOption = (RemoveOption) change;
                if (hasSnapshots.get()) {
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
                        lazyIdentifier.get().getFullName());
                int id = highestFieldId.incrementAndGet();
                DataType dataType = ReassignFieldId.reassign(addColumn.dataType(), highestFieldId);
                new NestedColumnModifier(addColumn.fieldNames(), lazyIdentifier) {
                    @Override
                    protected void updateLastColumn(
                            int depth, List<DataField> newFields, String fieldName)
                            throws Catalog.ColumnAlreadyExistException,
                                    Catalog.ColumnNotExistException {
                        assertColumnNotExists(newFields, fieldName, lazyIdentifier);

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
                                if (map.containsKey(move.referenceFieldName())) {
                                    int fieldIndex = map.get(move.referenceFieldName());
                                    newFields.add(fieldIndex + 1, dataField);
                                } else {
                                    throw new Catalog.ColumnNotExistException(
                                            lazyIdentifier.get(), move.referenceFieldName());
                                }
                            } else if (move.type().equals(SchemaChange.Move.MoveType.BEFORE)) {
                                if (map.containsKey(move.referenceFieldName())) {
                                    int fieldIndex = map.get(move.referenceFieldName());
                                    newFields.add(fieldIndex, dataField);
                                } else {
                                    throw new Catalog.ColumnNotExistException(
                                            lazyIdentifier.get(), move.referenceFieldName());
                                }
                            } else if (move.type().equals(SchemaChange.Move.MoveType.LAST)) {
                                newFields.add(dataField);
                            } else {
                                throw new UnsupportedOperationException(
                                        "Unsupported move type: " + move.type());
                            }
                        } else {
                            newFields.add(dataField);
                        }
                    }
                }.updateIntermediateColumn(newFields, 0);
            } else if (change instanceof RenameColumn) {
                RenameColumn rename = (RenameColumn) change;
                assertNotUpdatingPartitionKeys(oldTableSchema, rename.fieldNames(), "rename");
                new NestedColumnModifier(rename.fieldNames(), lazyIdentifier) {
                    @Override
                    protected void updateLastColumn(
                            int depth, List<DataField> newFields, String fieldName)
                            throws Catalog.ColumnNotExistException,
                                    Catalog.ColumnAlreadyExistException {
                        assertColumnExists(newFields, fieldName, lazyIdentifier);
                        assertColumnNotExists(newFields, rename.newName(), lazyIdentifier);
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
                                            field.description(),
                                            field.defaultValue());
                            newFields.set(i, newField);
                            return;
                        }
                    }
                }.updateIntermediateColumn(newFields, 0);
            } else if (change instanceof DropColumn) {
                DropColumn drop = (DropColumn) change;
                dropColumnValidation(oldTableSchema, drop);
                new NestedColumnModifier(drop.fieldNames(), lazyIdentifier) {
                    @Override
                    protected void updateLastColumn(
                            int depth, List<DataField> newFields, String fieldName)
                            throws Catalog.ColumnNotExistException {
                        assertColumnExists(newFields, fieldName, lazyIdentifier);
                        newFields.removeIf(f -> f.name().equals(fieldName));
                        if (newFields.isEmpty()) {
                            throw new IllegalArgumentException("Cannot drop all fields in table");
                        }
                    }
                }.updateIntermediateColumn(newFields, 0);
            } else if (change instanceof UpdateColumnType) {
                UpdateColumnType update = (UpdateColumnType) change;
                assertNotUpdatingPartitionKeys(oldTableSchema, update.fieldNames(), "update");
                assertNotUpdatingPrimaryKeys(oldTableSchema, update.fieldNames(), "update");
                updateNestedColumn(
                        newFields,
                        update.fieldNames(),
                        (field, depth) -> {
                            // find the dataType at depth and update the type for it
                            DataType sourceRootType =
                                    getRootType(field.type(), depth, update.fieldNames().length);
                            DataType targetRootType = update.newDataType();
                            if (update.keepNullability()) {
                                targetRootType = targetRootType.copy(sourceRootType.isNullable());
                            } else {
                                assertNullabilityChange(
                                        sourceRootType.isNullable(),
                                        targetRootType.isNullable(),
                                        StringUtils.join(Arrays.asList(update.fieldNames()), "."),
                                        disableNullToNotNull);
                            }
                            checkState(
                                    DataTypeCasts.supportsCast(
                                                    sourceRootType,
                                                    targetRootType,
                                                    !disableExplicitTypeCasting)
                                            && CastExecutors.resolve(sourceRootType, targetRootType)
                                                    != null,
                                    String.format(
                                            "Column type %s[%s] cannot be converted to %s without loosing information.",
                                            field.name(), sourceRootType, targetRootType));
                            return new DataField(
                                    field.id(),
                                    field.name(),
                                    getArrayMapTypeWithTargetTypeRoot(
                                            field.type(),
                                            targetRootType,
                                            depth,
                                            update.fieldNames().length),
                                    field.description(),
                                    field.defaultValue());
                        },
                        lazyIdentifier);
            } else if (change instanceof UpdateColumnNullability) {
                UpdateColumnNullability update = (UpdateColumnNullability) change;
                if (update.newNullability()) {
                    assertNotUpdatingPrimaryKeys(
                            oldTableSchema, update.fieldNames(), "change nullability of");
                }
                updateNestedColumn(
                        newFields,
                        update.fieldNames(),
                        (field, depth) -> {
                            // find the DataType at depth and update that DataTypes nullability
                            DataType sourceRootType =
                                    getRootType(field.type(), depth, update.fieldNames().length);
                            assertNullabilityChange(
                                    sourceRootType.isNullable(),
                                    update.newNullability(),
                                    StringUtils.join(Arrays.asList(update.fieldNames()), "."),
                                    disableNullToNotNull);
                            sourceRootType = sourceRootType.copy(update.newNullability());
                            return new DataField(
                                    field.id(),
                                    field.name(),
                                    getArrayMapTypeWithTargetTypeRoot(
                                            field.type(),
                                            sourceRootType,
                                            depth,
                                            update.fieldNames().length),
                                    field.description(),
                                    field.defaultValue());
                        },
                        lazyIdentifier);
            } else if (change instanceof UpdateColumnComment) {
                UpdateColumnComment update = (UpdateColumnComment) change;
                updateNestedColumn(
                        newFields,
                        update.fieldNames(),
                        (field, depth) ->
                                new DataField(
                                        field.id(),
                                        field.name(),
                                        field.type(),
                                        update.newDescription(),
                                        field.defaultValue()),
                        lazyIdentifier);
            } else if (change instanceof UpdateColumnPosition) {
                UpdateColumnPosition update = (UpdateColumnPosition) change;
                SchemaChange.Move move = update.move();
                applyMove(newFields, move);
            } else if (change instanceof UpdateColumnDefaultValue) {
                UpdateColumnDefaultValue update = (UpdateColumnDefaultValue) change;
                updateNestedColumn(
                        newFields,
                        update.fieldNames(),
                        (field, depth) -> {
                            validateDefaultValue(field.type(), update.newDefaultValue());
                            return new DataField(
                                    field.id(),
                                    field.name(),
                                    field.type(),
                                    field.description(),
                                    update.newDefaultValue());
                        },
                        lazyIdentifier);
            } else {
                throw new UnsupportedOperationException("Unsupported change: " + change.getClass());
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
                        applyRenameColumnsToOptions(newOptions, changes),
                        newComment);

        return new TableSchema(
                oldTableSchema.id() + 1,
                newSchema.fields(),
                highestFieldId.get(),
                newSchema.partitionKeys(),
                newSchema.primaryKeys(),
                newSchema.options(),
                newSchema.comment());
    }

    // gets the rootType at the defined depth
    // ex: ARRAY<MAP<STRING, ARRAY<INT>>>
    // if we want to update ARRAY<INT> -> ARRAY<BIGINT>
    // the maxDepth will be based on updateFieldNames
    // which in the case will be [v, element, value, element],
    // so maxDepth is 4 and return DataType will be INT
    private static DataType getRootType(DataType type, int currDepth, int maxDepth) {
        if (currDepth == maxDepth - 1) {
            return type;
        }
        switch (type.getTypeRoot()) {
            case ARRAY:
                return getRootType(((ArrayType) type).getElementType(), currDepth + 1, maxDepth);
            case MAP:
                return getRootType(((MapType) type).getValueType(), currDepth + 1, maxDepth);
            default:
                return type;
        }
    }

    // builds the targetType from source type based on the maxDepth which needs to be updated
    // ex: ARRAY<MAP<STRING, ARRAY<INT>>> -> ARRAY<MAP<STRING, ARRAY<BIGINT>>>
    // here we only need to update type of ARRAY<INT> to ARRAY<BIGINT> and rest of the type
    // remains same. This function achieves this.
    private static DataType getArrayMapTypeWithTargetTypeRoot(
            DataType source, DataType target, int currDepth, int maxDepth) {
        if (currDepth == maxDepth - 1) {
            return target;
        }
        switch (source.getTypeRoot()) {
            case ARRAY:
                return new ArrayType(
                        source.isNullable(),
                        getArrayMapTypeWithTargetTypeRoot(
                                ((ArrayType) source).getElementType(),
                                target,
                                currDepth + 1,
                                maxDepth));
            case MAP:
                return new MapType(
                        source.isNullable(),
                        ((MapType) source).getKeyType(),
                        getArrayMapTypeWithTargetTypeRoot(
                                ((MapType) source).getValueType(),
                                target,
                                currDepth + 1,
                                maxDepth));
            default:
                return target;
        }
    }

    private static void assertNullabilityChange(
            boolean oldNullability,
            boolean newNullability,
            String fieldName,
            boolean disableNullToNotNull) {
        if (disableNullToNotNull && oldNullability && !newNullability) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Cannot update column type from nullable to non nullable for %s. "
                                    + "You can set table configuration option 'alter-column-null-to-not-null.disabled' = 'false' "
                                    + "to allow converting null columns to not null",
                            fieldName));
        }
    }

    public static void applyMove(List<DataField> newFields, SchemaChange.Move move) {
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
    private static void moveField(List<DataField> newFields, int fromIndex, int toIndex) {
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

    private static Map<String, String> applyRenameColumnsToOptions(
            Map<String, String> options, Iterable<SchemaChange> changes) {
        Iterable<RenameColumn> renameColumns =
                FluentIterable.from(changes).filter(RenameColumn.class);

        if (Iterables.isEmpty(renameColumns)) {
            return options;
        }

        Map<String, String> newOptions = Maps.newHashMap(options);

        Map<String, String> renameMappings =
                Streams.stream(renameColumns)
                        .collect(
                                Collectors.toMap(
                                        // currently only non-nested columns are supported
                                        rename -> rename.fieldNames()[0],
                                        RenameColumn::newName));

        // case 1: the option key is fixed and only value may contain field names

        // bucket key rename
        String bucketKeysStr = options.get(BUCKET_KEY.key());
        if (!StringUtils.isNullOrWhitespaceOnly(bucketKeysStr)) {
            List<String> bucketColumns = Arrays.asList(bucketKeysStr.split(","));
            List<String> newBucketColumns =
                    applyNotNestedColumnRename(bucketColumns, renameMappings);
            newOptions.put(BUCKET_KEY.key(), String.join(",", newBucketColumns));
        }

        // sequence field rename
        String sequenceFieldsStr = options.get(SEQUENCE_FIELD.key());
        if (!StringUtils.isNullOrWhitespaceOnly(sequenceFieldsStr)) {
            List<String> sequenceFields = Arrays.asList(sequenceFieldsStr.split(","));
            List<String> newSequenceFields =
                    applyNotNestedColumnRename(sequenceFields, renameMappings);
            newOptions.put(SEQUENCE_FIELD.key(), String.join(",", newSequenceFields));
        }

        // case 2: the option key is composed of certain fixed prefixes, suffixes, and the field
        // name, while the option value doesn't contain field names.
        List<Function<String, String>> fieldNameToOptionKeys =
                ImmutableList.of(
                        // NESTED_KEY is not added since renaming nested columns is not supported
                        // currently
                        fieldName -> FIELDS_PREFIX + "." + fieldName + "." + AGG_FUNCTION,
                        fieldName -> FIELDS_PREFIX + "." + fieldName + "." + IGNORE_RETRACT,
                        fieldName -> FIELDS_PREFIX + "." + fieldName + "." + DISTINCT,
                        fieldName -> FIELDS_PREFIX + "." + fieldName + "." + LIST_AGG_DELIMITER);

        for (RenameColumn rename : renameColumns) {
            String fieldName = rename.fieldNames()[0];
            String newFieldName = rename.newName();

            for (Function<String, String> fieldNameToKey : fieldNameToOptionKeys) {
                String key = fieldNameToKey.apply(fieldName);
                if (newOptions.containsKey(key)) {
                    String value = newOptions.remove(key);
                    newOptions.put(fieldNameToKey.apply(newFieldName), value);
                }
            }
        }

        // case 3: both option key and option value may contain field names
        for (String key : options.keySet()) {
            if (key.startsWith(FIELDS_PREFIX)) {
                String matchedSuffix = null;
                if (key.endsWith(SEQUENCE_GROUP)) {
                    matchedSuffix = SEQUENCE_GROUP;
                } else if (key.endsWith(NESTED_KEY)) {
                    matchedSuffix = NESTED_KEY;
                }

                if (matchedSuffix != null) {
                    // Both the key and value may contain field names. If we were to perform a
                    // "match then replace" operation, the conditions would become quite complex.
                    // Instead, we directly make a replacement across all instances
                    String keyFieldsStr =
                            key.substring(
                                    FIELDS_PREFIX.length() + 1,
                                    key.length() - matchedSuffix.length() - 1);
                    List<String> keyFields = Arrays.asList(keyFieldsStr.split(","));
                    List<String> newKeyFields =
                            applyNotNestedColumnRename(keyFields, renameMappings);

                    String valueFieldsStr = newOptions.remove(key);
                    List<String> valueFields = Arrays.asList(valueFieldsStr.split(","));
                    List<String> newValueFields =
                            applyNotNestedColumnRename(valueFields, renameMappings);
                    newOptions.put(
                            FIELDS_PREFIX
                                    + "."
                                    + String.join(",", newKeyFields)
                                    + "."
                                    + matchedSuffix,
                            String.join(",", newValueFields));
                }
            }
        }

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
            if (renameColumn.fieldNames().length == 1) {
                columnNames.put(renameColumn.fieldNames()[0], renameColumn.newName());
            }
        }
        return applyNotNestedColumnRename(columns, columnNames);
    }

    private static List<String> applyNotNestedColumnRename(
            List<String> columns, Map<String, String> renameMapping) {

        // The order of the column names will be preserved, as a non-parallel stream is used here.
        return columns.stream()
                .map(column -> renameMapping.getOrDefault(column, column))
                .collect(Collectors.toList());
    }

    private static void dropColumnValidation(TableSchema schema, DropColumn change) {
        // primary keys and partition keys can't be nested columns
        if (change.fieldNames().length > 1) {
            return;
        }
        String columnToDrop = change.fieldNames()[0];
        if (schema.partitionKeys().contains(columnToDrop)
                || schema.primaryKeys().contains(columnToDrop)) {
            throw new UnsupportedOperationException(
                    String.format("Cannot drop partition key or primary key: [%s]", columnToDrop));
        }
    }

    private static void assertNotUpdatingPartitionKeys(
            TableSchema schema, String[] fieldNames, String operation) {
        // partition keys can't be nested columns
        if (fieldNames.length > 1) {
            return;
        }
        String fieldName = fieldNames[0];
        if (schema.partitionKeys().contains(fieldName)) {
            throw new UnsupportedOperationException(
                    String.format("Cannot %s partition column: [%s]", operation, fieldName));
        }
    }

    private static void assertNotUpdatingPrimaryKeys(
            TableSchema schema, String[] fieldNames, String operation) {
        // primary keys can't be nested columns
        if (fieldNames.length > 1) {
            return;
        }
        String fieldName = fieldNames[0];
        if (schema.primaryKeys().contains(fieldName)) {
            throw new UnsupportedOperationException(
                    String.format("Cannot %s primary key", operation));
        }
    }

    private abstract static class NestedColumnModifier {

        private final String[] updateFieldNames;
        private final LazyField<Identifier> identifier;

        private NestedColumnModifier(String[] updateFieldNames, LazyField<Identifier> identifier) {
            this.updateFieldNames = updateFieldNames;
            this.identifier = identifier;
        }

        private void updateIntermediateColumn(
                List<DataField> newFields, List<DataField> previousFields, int depth, int prevDepth)
                throws Catalog.ColumnNotExistException, Catalog.ColumnAlreadyExistException {
            if (depth == updateFieldNames.length - 1) {
                updateLastColumn(depth, newFields, updateFieldNames[depth]);
                return;
            } else if (depth >= updateFieldNames.length) {
                // to handle the case of ARRAY or MAP type evolution
                // for instance : ARRAY<INT> -> ARRAY<BIGINT>
                // the updateFieldNames in this case is [v, element] where v is array field name
                // the depth returned by extractRowDataFields is 2 which will overflow.
                // So the logic is to go to previous depth and update the column using previous
                // fields which will have DataFields from prevDepth
                // The reason for this handling is the addition of element and value for array
                // and map type in FlinkCatalog as dummy column name
                updateLastColumn(prevDepth, previousFields, updateFieldNames[prevDepth]);
                return;
            }

            for (int i = 0; i < newFields.size(); i++) {
                DataField field = newFields.get(i);
                if (!field.name().equals(updateFieldNames[depth])) {
                    continue;
                }
                List<DataField> nestedFields = new ArrayList<>();
                int newDepth = depth + extractRowDataFields(field.type(), nestedFields);
                updateIntermediateColumn(nestedFields, newFields, newDepth, depth);
                field = newFields.get(i);
                newFields.set(
                        i,
                        new DataField(
                                field.id(),
                                field.name(),
                                wrapNewRowType(field.type(), nestedFields),
                                field.description(),
                                field.defaultValue()));
                return;
            }

            throw new Catalog.ColumnNotExistException(
                    identifier.get(),
                    String.join(".", Arrays.asList(updateFieldNames).subList(0, depth + 1)));
        }

        public void updateIntermediateColumn(List<DataField> newFields, int depth)
                throws Catalog.ColumnNotExistException, Catalog.ColumnAlreadyExistException {
            updateIntermediateColumn(newFields, newFields, depth, depth);
        }

        private int extractRowDataFields(DataType type, List<DataField> nestedFields) {
            switch (type.getTypeRoot()) {
                case ROW:
                    nestedFields.addAll(((RowType) type).getFields());
                    return 1;
                case ARRAY:
                    return extractRowDataFields(((ArrayType) type).getElementType(), nestedFields)
                            + 1;
                case MAP:
                    return extractRowDataFields(((MapType) type).getValueType(), nestedFields) + 1;
                default:
                    return 1;
            }
        }

        private DataType wrapNewRowType(DataType type, List<DataField> nestedFields) {
            switch (type.getTypeRoot()) {
                case ROW:
                    return new RowType(type.isNullable(), nestedFields);
                case ARRAY:
                    return new ArrayType(
                            type.isNullable(),
                            wrapNewRowType(((ArrayType) type).getElementType(), nestedFields));
                case MAP:
                    MapType mapType = (MapType) type;
                    return new MapType(
                            type.isNullable(),
                            mapType.getKeyType(),
                            wrapNewRowType(mapType.getValueType(), nestedFields));
                default:
                    return type;
            }
        }

        protected abstract void updateLastColumn(
                int depth, List<DataField> newFields, String fieldName)
                throws Catalog.ColumnNotExistException, Catalog.ColumnAlreadyExistException;

        protected void assertColumnExists(
                List<DataField> newFields, String fieldName, LazyField<Identifier> lazyIdentifier)
                throws Catalog.ColumnNotExistException {
            for (DataField field : newFields) {
                if (field.name().equals(fieldName)) {
                    return;
                }
            }
            throw new Catalog.ColumnNotExistException(
                    lazyIdentifier.get(), getLastFieldName(fieldName));
        }

        protected void assertColumnNotExists(
                List<DataField> newFields, String fieldName, LazyField<Identifier> lazyIdentifier)
                throws Catalog.ColumnAlreadyExistException {
            for (DataField field : newFields) {
                if (field.name().equals(fieldName)) {
                    throw new Catalog.ColumnAlreadyExistException(
                            lazyIdentifier.get(), getLastFieldName(fieldName));
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

    private static void updateNestedColumn(
            List<DataField> newFields,
            String[] updateFieldNames,
            BiFunction<DataField, Integer, DataField> updateFunc,
            LazyField<Identifier> lazyIdentifier)
            throws Catalog.ColumnNotExistException, Catalog.ColumnAlreadyExistException {
        new NestedColumnModifier(updateFieldNames, lazyIdentifier) {
            @Override
            protected void updateLastColumn(int depth, List<DataField> newFields, String fieldName)
                    throws Catalog.ColumnNotExistException {
                for (int i = 0; i < newFields.size(); i++) {
                    DataField field = newFields.get(i);
                    if (!field.name().equals(fieldName)) {
                        continue;
                    }

                    newFields.set(i, updateFunc.apply(field, depth));
                    return;
                }

                throw new Catalog.ColumnNotExistException(
                        lazyIdentifier.get(), String.join(".", updateFieldNames));
            }
        }.updateIntermediateColumn(newFields, 0);
    }

    @VisibleForTesting
    public boolean commit(TableSchema newSchema) throws Exception {
        SchemaValidation.validateTableSchema(newSchema);
        SchemaValidation.validateFallbackBranch(this, newSchema);
        Path schemaPath = toSchemaPath(newSchema.id());
        return fileIO.tryToWriteAtomic(schemaPath, newSchema.toString());
    }

    /** Read schema for schema id. */
    public TableSchema schema(long id) {
        return fromPath(fileIO, toSchemaPath(id));
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

    public static void checkAlterTableOption(
            Map<String, String> options, String key, @Nullable String oldValue, String newValue) {
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

            if (oldBucket == -1) {
                throw new UnsupportedOperationException("Cannot change bucket when it is -1.");
            }
            if (newBucket == -1) {
                throw new UnsupportedOperationException("Cannot change bucket to -1.");
            }
        }

        if (DELETION_VECTORS_ENABLED.key().equals(key)) {
            boolean dvModifiable =
                    Boolean.parseBoolean(
                            options.getOrDefault(
                                    DELETION_VECTORS_MODIFIABLE.key(),
                                    DELETION_VECTORS_MODIFIABLE.defaultValue().toString()));
            if (!dvModifiable) {
                boolean oldDv =
                        oldValue == null
                                ? DELETION_VECTORS_ENABLED.defaultValue()
                                : Boolean.parseBoolean(oldValue);
                boolean newDv = Boolean.parseBoolean(newValue);

                if (oldDv != newDv) {
                    throw new UnsupportedOperationException(
                            String.format(
                                    "Cannot change deletion vectors mode from %s to %s. If modifying table deletion-vectors mode without full-compaction, this may result in data duplication. "
                                            + "If you are confident, you can set table option '%s' = 'true' to allow deletion vectors modification.",
                                    oldDv, newDv, DELETION_VECTORS_MODIFIABLE.key()));
                }
            }
        }

        if (IGNORE_DELETE.key().equals(key)) {
            boolean oldIgnoreDelete =
                    oldValue == null
                            ? IGNORE_DELETE.defaultValue()
                            : Boolean.parseBoolean(oldValue);
            boolean newIgnoreDelete = Boolean.parseBoolean(newValue);
            if (oldIgnoreDelete && !newIgnoreDelete) {
                throw new UnsupportedOperationException(
                        String.format("Cannot change %s from true to false.", IGNORE_DELETE.key()));
            }
        }

        if (IGNORE_UPDATE_BEFORE.key().equals(key)) {
            boolean oldIgnoreUpdateBefore =
                    oldValue == null
                            ? IGNORE_UPDATE_BEFORE.defaultValue()
                            : Boolean.parseBoolean(oldValue);
            boolean newIgnoreUpdateBefore = Boolean.parseBoolean(newValue);
            if (oldIgnoreUpdateBefore && !newIgnoreUpdateBefore) {
                throw new UnsupportedOperationException(
                        String.format(
                                "Cannot change %s from true to false.",
                                IGNORE_UPDATE_BEFORE.key()));
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

    public static TableSchema fromPath(FileIO fileIO, Path path) {
        try {
            return tryFromPath(fileIO, path);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public static TableSchema tryFromPath(FileIO fileIO, Path path) throws FileNotFoundException {
        try {
            return TableSchema.fromJson(fileIO.readFileUtf8(path));
        } catch (FileNotFoundException e) {
            throw e;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
