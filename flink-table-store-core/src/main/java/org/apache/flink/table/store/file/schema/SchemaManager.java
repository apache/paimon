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

package org.apache.flink.table.store.file.schema;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.file.casting.CastExecutors;
import org.apache.flink.table.store.file.operation.Lock;
import org.apache.flink.table.store.file.schema.SchemaChange.AddColumn;
import org.apache.flink.table.store.file.schema.SchemaChange.DropColumn;
import org.apache.flink.table.store.file.schema.SchemaChange.RemoveOption;
import org.apache.flink.table.store.file.schema.SchemaChange.RenameColumn;
import org.apache.flink.table.store.file.schema.SchemaChange.SetOption;
import org.apache.flink.table.store.file.schema.SchemaChange.UpdateColumnComment;
import org.apache.flink.table.store.file.schema.SchemaChange.UpdateColumnNullability;
import org.apache.flink.table.store.file.schema.SchemaChange.UpdateColumnType;
import org.apache.flink.table.store.file.utils.JsonSerdeUtil;
import org.apache.flink.table.store.fs.FileIO;
import org.apache.flink.table.store.fs.Path;
import org.apache.flink.table.store.types.ArrayType;
import org.apache.flink.table.store.types.DataField;
import org.apache.flink.table.store.types.DataType;
import org.apache.flink.table.store.types.DataTypeCasts;
import org.apache.flink.table.store.types.DataTypeVisitor;
import org.apache.flink.table.store.types.MapType;
import org.apache.flink.table.store.types.MultisetType;
import org.apache.flink.table.store.types.ReassignFieldId;
import org.apache.flink.table.store.types.RowType;
import org.apache.flink.table.store.utils.Preconditions;

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

import static org.apache.flink.table.store.file.utils.FileUtils.listVersionedFiles;
import static org.apache.flink.table.store.utils.Preconditions.checkState;

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

    /** Create a new schema from {@link UpdateSchema}. */
    public TableSchema commitNewVersion(UpdateSchema updateSchema) throws Exception {
        RowType rowType = updateSchema.rowType();
        List<String> partitionKeys = updateSchema.partitionKeys();
        List<String> primaryKeys = updateSchema.primaryKeys();
        Map<String, String> options = updateSchema.options();

        validatePrimaryKeysType(updateSchema, primaryKeys);
        while (true) {
            long id;
            int highestFieldId;
            List<DataField> fields;
            Optional<TableSchema> latest = latest();
            if (latest.isPresent()) {
                TableSchema oldTableSchema = latest.get();
                Preconditions.checkArgument(
                        oldTableSchema.primaryKeys().equals(primaryKeys),
                        "Primary key modification is not supported, "
                                + "old primaryKeys is %s, new primaryKeys is %s",
                        oldTableSchema.primaryKeys(),
                        primaryKeys);

                if (!updateSchema
                                .rowType()
                                .getFields()
                                .equals(oldTableSchema.logicalRowType().getFields())
                        || !updateSchema.partitionKeys().equals(oldTableSchema.partitionKeys())) {
                    throw new UnsupportedOperationException(
                            "TODO: support update field types and partition keys. ");
                }

                fields = oldTableSchema.fields();
                id = oldTableSchema.id() + 1;
                highestFieldId = oldTableSchema.highestFieldId();
            } else {
                fields = TableSchema.newFields(rowType);
                highestFieldId = TableSchema.currentHighestFieldId(fields);
                id = 0;
            }

            String sequenceField = options.get(CoreOptions.SEQUENCE_FIELD.key());
            Preconditions.checkArgument(
                    sequenceField == null || rowType.getFieldNames().contains(sequenceField),
                    "Nonexistent sequence field: '%s'",
                    sequenceField);

            TableSchema newSchema =
                    new TableSchema(
                            id,
                            fields,
                            highestFieldId,
                            partitionKeys,
                            primaryKeys,
                            options,
                            updateSchema.comment());

            boolean success = commit(newSchema);
            if (success) {
                return newSchema;
            }
        }
    }

    private void validatePrimaryKeysType(UpdateSchema updateSchema, List<String> primaryKeys) {
        if (!primaryKeys.isEmpty()) {
            Map<String, DataField> rowFields = new HashMap<>();
            for (DataField rowField : updateSchema.rowType().getFields()) {
                rowFields.put(rowField.name(), rowField);
            }
            for (String primaryKeyName : primaryKeys) {
                DataField rowField = rowFields.get(primaryKeyName);
                DataType dataType = rowField.type();
                if (PRIMARY_KEY_UNSUPPORTED_LOGICAL_TYPES.stream()
                        .anyMatch(c -> c.isInstance(dataType))) {
                    throw new UnsupportedOperationException(
                            String.format(
                                    "The type %s in primary key field %s is unsupported",
                                    dataType.getClass().getSimpleName(), primaryKeyName));
                }
            }
        }
    }

    public static final List<Class<? extends DataType>> PRIMARY_KEY_UNSUPPORTED_LOGICAL_TYPES =
            Arrays.asList(MapType.class, ArrayType.class, RowType.class, MultisetType.class);

    /** Create {@link SchemaChange}s. */
    public TableSchema commitChanges(List<SchemaChange> changes) throws Exception {
        while (true) {
            TableSchema schema =
                    latest().orElseThrow(
                                    () -> new RuntimeException("Table not exists: " + tableRoot));
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
                    if (newFields.stream().anyMatch(f -> f.name().equals(addColumn.fieldName()))) {
                        throw new IllegalArgumentException(
                                String.format(
                                        "The column [%s] exists in the table[%s].",
                                        addColumn.fieldName(), tableRoot));
                    }
                    Preconditions.checkArgument(
                            addColumn.dataType().isNullable(),
                            "ADD COLUMN cannot specify NOT NULL.");
                    int id = highestFieldId.incrementAndGet();
                    DataType dataType =
                            ReassignFieldId.reassign(addColumn.dataType(), highestFieldId);
                    newFields.add(
                            new DataField(
                                    id, addColumn.fieldName(), dataType, addColumn.description()));
                } else if (change instanceof RenameColumn) {
                    RenameColumn rename = (RenameColumn) change;
                    validateNotPrimaryAndPartitionKey(schema, rename.fieldName());
                    if (newFields.stream().anyMatch(f -> f.name().equals(rename.newName()))) {
                        throw new IllegalArgumentException(
                                String.format(
                                        "The column [%s] exists in the table[%s].",
                                        rename.newName(), tableRoot));
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
                        throw new IllegalArgumentException(
                                String.format(
                                        "The column [%s] doesn't exist in the table[%s].",
                                        drop.fieldName(), tableRoot));
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
                                        update.fieldName(), tableRoot));
                    }
                    updateColumn(
                            newFields,
                            update.fieldName(),
                            (field) -> {
                                checkState(
                                        DataTypeCasts.supportsImplicitCast(
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
                                        field.id(), field.name(), update.newDataType());
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

            boolean success = commit(newSchema);
            if (success) {
                return newSchema;
            }
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
            Function<DataField, DataField> updateFunc) {
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
                                    ((org.apache.flink.table.store.types.RowType) field.type())
                                            .getFields());
                    updateNestedColumn(nestedFields, updateFieldNames, index + 1, updateFunc);
                    newFields.set(
                            i,
                            new DataField(
                                    field.id(),
                                    field.name(),
                                    new org.apache.flink.table.store.types.RowType(
                                            field.type().isNullable(), nestedFields),
                                    field.description()));
                }
            }
        }
        if (!found) {
            throw new RuntimeException("Can not find column: " + Arrays.asList(updateFieldNames));
        }
    }

    private void updateColumn(
            List<DataField> newFields,
            String updateFieldName,
            Function<DataField, DataField> updateFunc) {
        updateNestedColumn(newFields, new String[] {updateFieldName}, 0, updateFunc);
    }

    private boolean commit(TableSchema newSchema) throws Exception {
        CoreOptions.validateTableSchema(newSchema);

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

    public static void checkAlterTableOption(String key) {
        if (CoreOptions.getImmutableOptionKeys().contains(key)) {
            throw new UnsupportedOperationException(
                    String.format("Change '%s' is not supported yet.", key));
        }
    }
}
