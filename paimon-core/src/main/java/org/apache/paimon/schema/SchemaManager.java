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
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeCasts;
import org.apache.paimon.types.DataTypeVisitor;
import org.apache.paimon.types.ReassignFieldId;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.catalog.AbstractCatalog.DB_SUFFIX;
import static org.apache.paimon.catalog.Identifier.UNKNOWN_DATABASE;
import static org.apache.paimon.utils.FileUtils.listVersionedFiles;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Schema Manager to manage schema versions. */
public class SchemaManager implements Serializable {

    private static final String SCHEMA_PREFIX = "schema-";

    private final FileIO fileIO;
    private final Path tableRoot;

    @Nullable private transient Lock lock;

    public SchemaManager(FileIO fileIO, Path tableRoot) {
        this.fileIO = fileIO;
        this.tableRoot = tableRoot;
    }

    public SchemaManager withLock(@Nullable Lock lock) {
        this.lock = lock;
        return this;
    }

    /** @return latest schema. */
    public Optional<TableSchema> latest() {
        try {
            return listVersionedFiles(fileIO, schemaDirectory(), SCHEMA_PREFIX)
                    .reduce(Math::max)
                    .map(this::schema);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** List all schema. */
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

    /** Create a new schema from {@link Schema}. */
    public TableSchema createTable(Schema schema) throws Exception {
        while (true) {
            latest().ifPresent(
                            latest -> {
                                throw new IllegalStateException(
                                        "Schema in filesystem exists, please use updating,"
                                                + " latest schema is: "
                                                + latest());
                            });

            List<DataField> fields = schema.fields();
            List<String> partitionKeys = schema.partitionKeys();
            List<String> primaryKeys = schema.primaryKeys();
            Map<String, String> options = schema.options();
            int highestFieldId = RowType.currentHighestFieldId(fields);

            List<String> columnNames =
                    schema.fields().stream().map(DataField::name).collect(Collectors.toList());
            if (options.containsKey(CoreOptions.PRIMARY_KEY.key())) {
                if (!primaryKeys.isEmpty()) {
                    throw new RuntimeException(
                            "Cannot define primary key on DDL and table options at the same time.");
                }
                String pk = options.get(CoreOptions.PRIMARY_KEY.key());
                primaryKeys = Arrays.asList(pk.split(","));
                boolean exists = primaryKeys.stream().allMatch(columnNames::contains);
                if (!exists) {
                    throw new RuntimeException(
                            String.format(
                                    "Primary key column '%s' is not defined in the schema.",
                                    primaryKeys));
                }
                options.remove(CoreOptions.PRIMARY_KEY.key());
            }

            if (options.containsKey(CoreOptions.PARTITION.key())) {
                if (!partitionKeys.isEmpty()) {
                    throw new RuntimeException(
                            "Cannot define partition on DDL and table options at the same time.");
                }
                String partitions = options.get(CoreOptions.PARTITION.key());
                partitionKeys = Arrays.asList(partitions.split(","));
                boolean exists = partitionKeys.stream().allMatch(columnNames::contains);
                if (!exists) {
                    throw new RuntimeException(
                            String.format(
                                    "Partition column '%s' is not defined in the schema.",
                                    partitionKeys));
                }
                options.remove(CoreOptions.PARTITION.key());
            }

            TableSchema newSchema =
                    new TableSchema(
                            0,
                            fields,
                            highestFieldId,
                            partitionKeys,
                            primaryKeys,
                            options,
                            schema.comment());

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
        while (true) {
            TableSchema schema =
                    latest().orElseThrow(
                                    () ->
                                            new Catalog.TableNotExistException(
                                                    fromPath(tableRoot.getPath(), true)));
            Map<String, String> newOptions = new HashMap<>(schema.options());
            List<DataField> newFields = new ArrayList<>(schema.fields());
            AtomicInteger highestFieldId = new AtomicInteger(schema.highestFieldId());
            for (SchemaChange change : changes) {
                if (change instanceof SetOption) {
                    SetOption setOption = (SetOption) change;
                    checkAlterTableOption(setOption.key());
                    newOptions.put(setOption.key(), setOption.value());
                } else if (change instanceof RemoveOption) {
                    RemoveOption removeOption = (RemoveOption) change;
                    checkAlterTableOption(removeOption.key());
                    newOptions.remove(removeOption.key());
                } else if (change instanceof AddColumn) {
                    AddColumn addColumn = (AddColumn) change;
                    SchemaChange.Move move = addColumn.move();
                    if (newFields.stream().anyMatch(f -> f.name().equals(addColumn.fieldName()))) {
                        throw new Catalog.ColumnAlreadyExistException(
                                fromPath(tableRoot.getPath(), true), addColumn.fieldName());
                    }
                    Preconditions.checkArgument(
                            addColumn.dataType().isNullable(),
                            "ADD COLUMN cannot specify NOT NULL.");
                    int id = highestFieldId.incrementAndGet();
                    DataType dataType =
                            ReassignFieldId.reassign(addColumn.dataType(), highestFieldId);

                    DataField dataField =
                            new DataField(
                                    id, addColumn.fieldName(), dataType, addColumn.description());

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

                } else if (change instanceof RenameColumn) {
                    RenameColumn rename = (RenameColumn) change;
                    validateNotPrimaryAndPartitionKey(schema, rename.fieldName());
                    if (newFields.stream().anyMatch(f -> f.name().equals(rename.newName()))) {
                        throw new Catalog.ColumnAlreadyExistException(
                                fromPath(tableRoot.getPath(), true), rename.fieldName());
                    }

                    updateNestedColumn(
                            newFields,
                            new String[] {rename.fieldName()},
                            0,
                            (field) ->
                                    new DataField(
                                            field.id(),
                                            rename.newName(),
                                            field.type(),
                                            field.description()));
                } else if (change instanceof DropColumn) {
                    DropColumn drop = (DropColumn) change;
                    validateNotPrimaryAndPartitionKey(schema, drop.fieldName());
                    if (!newFields.removeIf(
                            f -> f.name().equals(((DropColumn) change).fieldName()))) {
                        throw new Catalog.ColumnNotExistException(
                                fromPath(tableRoot.getPath(), true), drop.fieldName());
                    }
                    if (newFields.isEmpty()) {
                        throw new IllegalArgumentException("Cannot drop all fields in table");
                    }
                } else if (change instanceof UpdateColumnType) {
                    UpdateColumnType update = (UpdateColumnType) change;
                    if (schema.partitionKeys().contains(update.fieldName())) {
                        throw new IllegalArgumentException(
                                String.format(
                                        "Cannot update partition column [%s] type in the table[%s].",
                                        update.fieldName(), tableRoot.getName()));
                    }
                    updateColumn(
                            newFields,
                            update.fieldName(),
                            (field) -> {
                                checkState(
                                        DataTypeCasts.supportsExplicitCast(
                                                        field.type(), update.newDataType())
                                                && CastExecutors.resolve(
                                                                field.type(), update.newDataType())
                                                        != null,
                                        String.format(
                                                "Column type %s[%s] cannot be converted to %s without loosing information.",
                                                field.name(), field.type(), update.newDataType()));
                                AtomicInteger dummyId = new AtomicInteger(0);
                                if (dummyId.get() != 0) {
                                    throw new RuntimeException(
                                            String.format(
                                                    "Update column to nested row type '%s' is not supported.",
                                                    update.newDataType()));
                                }
                                return new DataField(
                                        field.id(),
                                        field.name(),
                                        update.newDataType(),
                                        field.description());
                            });
                } else if (change instanceof UpdateColumnNullability) {
                    UpdateColumnNullability update = (UpdateColumnNullability) change;
                    if (update.fieldNames().length == 1
                            && update.newNullability()
                            && schema.primaryKeys().contains(update.fieldNames()[0])) {
                        throw new UnsupportedOperationException(
                                "Cannot change nullability of primary key");
                    }
                    updateNestedColumn(
                            newFields,
                            update.fieldNames(),
                            0,
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
                            0,
                            (field) ->
                                    new DataField(
                                            field.id(),
                                            field.name(),
                                            field.type(),
                                            update.newDescription()));
                } else if (change instanceof UpdateColumnPosition) {
                    UpdateColumnPosition update = (UpdateColumnPosition) change;
                    SchemaChange.Move move = update.move();

                    // key: name ; value : index
                    Map<String, Integer> map = new HashMap<>();
                    for (int i = 0; i < newFields.size(); i++) {
                        map.put(newFields.get(i).name(), i);
                    }

                    int fieldIndex = map.get(move.fieldName());
                    int refIndex = 0;
                    if (move.type().equals(SchemaChange.Move.MoveType.FIRST)) {
                        checkMoveIndexEqual(move, fieldIndex, refIndex);
                        newFields.add(refIndex, newFields.remove(fieldIndex));
                    } else if (move.type().equals(SchemaChange.Move.MoveType.AFTER)) {
                        refIndex = map.get(move.referenceFieldName());
                        checkMoveIndexEqual(move, fieldIndex, refIndex);
                        if (fieldIndex > refIndex) {
                            newFields.add(refIndex + 1, newFields.remove(fieldIndex));
                        } else {
                            newFields.add(refIndex, newFields.remove(fieldIndex));
                        }
                    }

                } else {
                    throw new UnsupportedOperationException(
                            "Unsupported change: " + change.getClass());
                }
            }

            TableSchema newSchema =
                    new TableSchema(
                            schema.id() + 1,
                            newFields,
                            highestFieldId.get(),
                            schema.partitionKeys(),
                            schema.primaryKeys(),
                            newOptions,
                            schema.comment());

            try {
                boolean success = commit(newSchema);
                if (success) {
                    return newSchema;
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
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

    private static void checkMoveIndexEqual(SchemaChange.Move move, int fieldIndex, int refIndex) {
        if (refIndex == fieldIndex) {
            throw new UnsupportedOperationException(
                    String.format("Cannot move itself for column %s", move.fieldName()));
        }
    }

    private void validateNotPrimaryAndPartitionKey(TableSchema schema, String fieldName) {
        /// TODO support partition and primary keys schema evolution
        if (schema.partitionKeys().contains(fieldName)) {
            throw new UnsupportedOperationException(
                    String.format("Cannot drop/rename partition key[%s]", fieldName));
        }
        if (schema.primaryKeys().contains(fieldName)) {
            throw new UnsupportedOperationException(
                    String.format("Cannot drop/rename primary key[%s]", fieldName));
        }
    }

    /** This method is hacky, newFields may be immutable. We should use {@link DataTypeVisitor}. */
    private void updateNestedColumn(
            List<DataField> newFields,
            String[] updateFieldNames,
            int index,
            Function<DataField, DataField> updateFunc)
            throws Catalog.ColumnNotExistException {
        boolean found = false;
        for (int i = 0; i < newFields.size(); i++) {
            DataField field = newFields.get(i);
            if (field.name().equals(updateFieldNames[index])) {
                found = true;
                if (index == updateFieldNames.length - 1) {
                    newFields.set(i, updateFunc.apply(field));
                    break;
                } else {
                    List<DataField> nestedFields =
                            new ArrayList<>(
                                    ((org.apache.paimon.types.RowType) field.type()).getFields());
                    updateNestedColumn(nestedFields, updateFieldNames, index + 1, updateFunc);
                    newFields.set(
                            i,
                            new DataField(
                                    field.id(),
                                    field.name(),
                                    new org.apache.paimon.types.RowType(
                                            field.type().isNullable(), nestedFields),
                                    field.description()));
                }
            }
        }
        if (!found) {
            throw new Catalog.ColumnNotExistException(
                    fromPath(tableRoot.getPath(), true), Arrays.toString(updateFieldNames));
        }
    }

    private void updateColumn(
            List<DataField> newFields,
            String updateFieldName,
            Function<DataField, DataField> updateFunc)
            throws Catalog.ColumnNotExistException {
        updateNestedColumn(newFields, new String[] {updateFieldName}, 0, updateFunc);
    }

    @VisibleForTesting
    boolean commit(TableSchema newSchema) throws Exception {
        SchemaValidation.validateTableSchema(newSchema);

        Path schemaPath = toSchemaPath(newSchema.id());
        Callable<Boolean> callable = () -> fileIO.writeFileUtf8(schemaPath, newSchema.toString());
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

    private Path schemaDirectory() {
        return new Path(tableRoot + "/schema");
    }

    @VisibleForTesting
    public Path toSchemaPath(long id) {
        return new Path(tableRoot + "/schema/" + SCHEMA_PREFIX + id);
    }

    /**
     * Delete schema with specific id.
     *
     * @param schemaId the schema id to delete.
     */
    public void deleteSchema(long schemaId) {
        fileIO.deleteQuietly(toSchemaPath(schemaId));
    }

    public static void checkAlterTableOption(String key) {
        if (CoreOptions.getImmutableOptionKeys().contains(key)) {
            throw new UnsupportedOperationException(
                    String.format("Change '%s' is not supported yet.", key));
        }
    }

    public static void checkAlterTablePath(String key) {
        if (CoreOptions.PATH.key().equalsIgnoreCase(key)) {
            throw new UnsupportedOperationException("Change path is not supported yet.");
        }
    }

    public static Identifier fromPath(String tablePath, boolean ignoreIfUnknownDatabase) {
        String[] paths = tablePath.split("/");
        if (paths.length < 2) {
            if (!ignoreIfUnknownDatabase) {
                throw new IllegalArgumentException(
                        String.format(
                                "Path '%s' is not a legacy path, please use catalog table path instead: 'warehouse_path/your_database.db/your_table'.",
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
                                "Path '%s' is not a legacy path, please use catalog table path instead: 'warehouse_path/your_database.db/your_table'.",
                                tablePath));
            }
            return new Identifier(UNKNOWN_DATABASE, paths[paths.length - 1]);
        }
        database = database.substring(0, index);
        return new Identifier(database, paths[paths.length - 1]);
    }
}
