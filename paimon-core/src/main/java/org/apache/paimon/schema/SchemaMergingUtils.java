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

import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeCasts;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.ReassignFieldId;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

/** The util class for merging the schemas. */
public class SchemaMergingUtils {

    public static final String ARRAY_ELEMENT_FIELD_NAME = "element";
    public static final String MAP_VALUE_FIELD_NAME = "value";

    public static TableSchema mergeSchemas(
            TableSchema currentTableSchema,
            RowType targetType,
            boolean typeWidening,
            boolean allowExplicitCast,
            boolean caseSensitive) {
        RowType currentType = currentTableSchema.logicalRowType();
        if (currentType.equals(targetType)) {
            return currentTableSchema;
        }

        AtomicInteger highestFieldId = new AtomicInteger(currentTableSchema.highestFieldId());
        RowType newRowType =
                mergeSchemas(
                        currentType,
                        targetType,
                        highestFieldId,
                        typeWidening,
                        allowExplicitCast,
                        caseSensitive);
        if (newRowType.equals(currentType)) {
            // It happens if the `targetType` only changes `nullability` but we always respect the
            // current's.
            return currentTableSchema;
        }

        return new TableSchema(
                currentTableSchema.id() + 1,
                newRowType.getFields(),
                highestFieldId.get(),
                currentTableSchema.partitionKeys(),
                currentTableSchema.primaryKeys(),
                currentTableSchema.options(),
                currentTableSchema.comment());
    }

    public static RowType mergeSchemas(
            RowType tableSchema,
            RowType dataSchema,
            AtomicInteger highestFieldId,
            boolean typeWidening,
            boolean allowExplicitCast,
            boolean caseSensitive) {
        return (RowType)
                merge(
                        tableSchema,
                        dataSchema,
                        highestFieldId,
                        typeWidening,
                        allowExplicitCast,
                        caseSensitive);
    }

    /**
     * Merge the base (target) data type with the update (incoming) data type.
     *
     * <ul>
     *   <li>RowType: merge existing fields recursively, keep base-only fields, append update-only
     *       fields as new columns.
     *   <li>Complex types (Array/Map/Multiset): recursively merge element/value types.
     *   <li>Leaf types when {@code typeWidening=false} (default): keep the base type unchanged —
     *       incoming data is cast to it by the alignment layer.
     *   <li>Leaf types when {@code typeWidening=true}: widen the base type to the update type if
     *       the cast is safe (or explicit when {@code allowExplicitCast=true}).
     * </ul>
     */
    public static DataType merge(
            DataType base0,
            DataType update0,
            AtomicInteger highestFieldId,
            boolean typeWidening,
            boolean allowExplicitCast,
            boolean caseSensitive) {
        // Compare ignoring nullability; the base's nullability flows to the result.
        DataType base = base0.copy(true);
        DataType update = update0.copy(true);

        if (base.equals(update)) {
            return base0;
        } else if (base instanceof RowType && update instanceof RowType) {
            List<DataField> baseFields = ((RowType) base).getFields();
            List<DataField> updateFields = ((RowType) update).getFields();
            Map<String, DataField> updateFieldMap = buildFieldMap(updateFields, caseSensitive);
            List<DataField> updatedFields = new ArrayList<>();
            for (DataField baseField : baseFields) {
                if (updateFieldMap.containsKey(baseField.name())) {
                    DataField updateField = updateFieldMap.get(baseField.name());
                    DataType updatedDataType =
                            merge(
                                    baseField.type(),
                                    updateField.type(),
                                    highestFieldId,
                                    typeWidening,
                                    allowExplicitCast,
                                    caseSensitive);
                    updatedFields.add(
                            new DataField(
                                    baseField.id(),
                                    baseField.name(),
                                    updatedDataType,
                                    baseField.description(),
                                    baseField.defaultValue()));
                } else {
                    updatedFields.add(baseField);
                }
            }

            Map<String, DataField> baseFieldMap = buildFieldMap(baseFields, caseSensitive);
            for (DataField field : updateFields) {
                if (!baseFieldMap.containsKey(field.name())) {
                    updatedFields.add(assignIdForNewField(field, highestFieldId).copy(true));
                }
            }

            return new RowType(base0.isNullable(), updatedFields);
        } else if (base instanceof MapType && update instanceof MapType) {
            return new MapType(
                    base0.isNullable(),
                    merge(
                            ((MapType) base).getKeyType(),
                            ((MapType) update).getKeyType(),
                            highestFieldId,
                            typeWidening,
                            allowExplicitCast,
                            caseSensitive),
                    merge(
                            ((MapType) base).getValueType(),
                            ((MapType) update).getValueType(),
                            highestFieldId,
                            typeWidening,
                            allowExplicitCast,
                            caseSensitive));
        } else if (base instanceof ArrayType && update instanceof ArrayType) {
            return new ArrayType(
                    base0.isNullable(),
                    merge(
                            ((ArrayType) base).getElementType(),
                            ((ArrayType) update).getElementType(),
                            highestFieldId,
                            typeWidening,
                            allowExplicitCast,
                            caseSensitive));
        } else if (base instanceof MultisetType && update instanceof MultisetType) {
            return new MultisetType(
                    base0.isNullable(),
                    merge(
                            ((MultisetType) base).getElementType(),
                            ((MultisetType) update).getElementType(),
                            highestFieldId,
                            typeWidening,
                            allowExplicitCast,
                            caseSensitive));
        } else if (!typeWidening) {
            // Default: keep the existing leaf type — only column additions evolve the schema.
            // Incoming values are cast to this type by the alignment layer.
            return base0;
        } else if (base instanceof DecimalType && update instanceof DecimalType) {
            if (((DecimalType) base).getScale() == ((DecimalType) update).getScale()) {
                return new DecimalType(
                        base0.isNullable(),
                        Math.max(
                                ((DecimalType) base).getPrecision(),
                                ((DecimalType) update).getPrecision()),
                        ((DecimalType) base).getScale());
            } else {
                throw new UnsupportedOperationException(
                        String.format(
                                "Failed to merge decimal types with different scale: %s and %s",
                                base, update));
            }
        } else if (supportsDataTypesCast(base, update, allowExplicitCast)) {
            if (DataTypes.getLength(base).isPresent() && DataTypes.getLength(update).isPresent()) {
                // this will check and merge types which has a `length` attribute, like BinaryType,
                // CharType, VarBinaryType, VarCharType.
                if (allowExplicitCast
                        || DataTypes.getLength(base).getAsInt()
                                <= DataTypes.getLength(update).getAsInt()) {
                    return update.copy(base0.isNullable());
                } else {
                    throw new UnsupportedOperationException(
                            String.format(
                                    "Failed to merge the target type that has a smaller length: %s and %s",
                                    base, update));
                }
            } else if (DataTypes.getPrecision(base).isPresent()
                    && DataTypes.getPrecision(update).isPresent()) {
                // this will check and merge types which has a `precision` attribute, like
                // LocalZonedTimestampType, TimeType, TimestampType.
                if (allowExplicitCast
                        || DataTypes.getPrecision(base).getAsInt()
                                <= DataTypes.getPrecision(update).getAsInt()) {
                    return update.copy(base0.isNullable());
                } else {
                    throw new UnsupportedOperationException(
                            String.format(
                                    "Failed to merge the target type that has a lower precision: %s and %s",
                                    base, update));
                }
            } else {
                return update.copy(base0.isNullable());
            }
        } else {
            throw new UnsupportedOperationException(
                    String.format("Failed to merge data types %s and %s", base, update));
        }
    }

    private static boolean supportsDataTypesCast(
            DataType sourceType, DataType targetType, boolean allowExplicitCast) {
        return DataTypeCasts.supportsCast(sourceType, targetType, allowExplicitCast);
    }

    private static DataField assignIdForNewField(DataField field, AtomicInteger highestFieldId) {
        DataType dataType = ReassignFieldId.reassign(field.type(), highestFieldId);
        return new DataField(
                highestFieldId.incrementAndGet(),
                field.name(),
                dataType,
                field.description(),
                field.defaultValue());
    }

    /**
     * Generate a list of {@link SchemaChange} by comparing the old and new {@link TableSchema}.
     * This supports detecting added columns and type changes (including nested structs).
     */
    public static List<SchemaChange> diffSchemaChanges(
            TableSchema oldSchema, TableSchema newSchema, boolean caseSensitive) {
        List<SchemaChange> changes = new ArrayList<>();
        diffFields(
                oldSchema.logicalRowType().getFields(),
                newSchema.logicalRowType().getFields(),
                new String[0],
                changes,
                caseSensitive);
        return changes;
    }

    private static void diffFields(
            List<DataField> oldFields,
            List<DataField> newFields,
            String[] parentNames,
            List<SchemaChange> changes,
            boolean caseSensitive) {
        Map<String, DataField> oldFieldMap = buildFieldMap(oldFields, caseSensitive);

        for (DataField newField : newFields) {
            String[] fieldNames = appendFieldName(parentNames, newField.name());
            DataField oldField = oldFieldMap.get(newField.name());
            if (oldField == null) {
                // new column added
                changes.add(
                        SchemaChange.addColumn(
                                fieldNames, newField.type(), newField.description(), null));
            } else if (!oldField.type().equals(newField.type())
                    && !diffNestedTypeChanges(
                            oldField.type(), newField.type(), fieldNames, changes, caseSensitive)) {
                changes.add(SchemaChange.updateColumnType(fieldNames, newField.type(), true));
            }
        }
    }

    /**
     * Returns true only when the type difference has been fully represented by nested schema
     * changes. Returns false to let the caller fall back to {@link SchemaChange.UpdateColumnType}.
     */
    private static boolean diffNestedTypeChanges(
            DataType oldType,
            DataType newType,
            String[] fieldNames,
            List<SchemaChange> changes,
            boolean caseSensitive) {
        List<SchemaChange> stagedChanges = new ArrayList<>();
        boolean handled =
                diffNestedTypeChangesInner(
                        oldType, newType, fieldNames, stagedChanges, caseSensitive);
        if (handled) {
            changes.addAll(stagedChanges);
        }
        return handled;
    }

    private static boolean diffNestedTypeChangesInner(
            DataType oldType,
            DataType newType,
            String[] fieldNames,
            List<SchemaChange> changes,
            boolean caseSensitive) {
        if (oldType instanceof RowType && newType instanceof RowType) {
            List<DataField> oldFields = ((RowType) oldType).getFields();
            List<DataField> newFields = ((RowType) newType).getFields();
            if (hasRemovedFields(oldFields, newFields, caseSensitive)) {
                return false;
            }
            diffFields(oldFields, newFields, fieldNames, changes, caseSensitive);
            return true;
        } else if (oldType instanceof ArrayType && newType instanceof ArrayType) {
            return diffNestedTypeChanges(
                    ((ArrayType) oldType).getElementType(),
                    ((ArrayType) newType).getElementType(),
                    appendFieldName(fieldNames, ARRAY_ELEMENT_FIELD_NAME),
                    changes,
                    caseSensitive);
        } else if (oldType instanceof MapType && newType instanceof MapType) {
            MapType oldMapType = (MapType) oldType;
            MapType newMapType = (MapType) newType;
            if (!oldMapType.getKeyType().equals(newMapType.getKeyType())) {
                return false;
            }
            return diffNestedTypeChanges(
                    oldMapType.getValueType(),
                    newMapType.getValueType(),
                    appendFieldName(fieldNames, MAP_VALUE_FIELD_NAME),
                    changes,
                    caseSensitive);
        }
        return false;
    }

    private static boolean hasRemovedFields(
            List<DataField> oldFields, List<DataField> newFields, boolean caseSensitive) {
        Map<String, DataField> newFieldMap = buildFieldMap(newFields, caseSensitive);
        for (DataField oldField : oldFields) {
            if (!newFieldMap.containsKey(oldField.name())) {
                return true;
            }
        }
        return false;
    }

    private static Map<String, DataField> buildFieldMap(
            List<DataField> fields, boolean caseSensitive) {
        Map<String, DataField> map =
                caseSensitive ? new HashMap<>() : new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (DataField field : fields) {
            map.put(field.name(), field);
        }
        return map;
    }

    private static String[] appendFieldName(String[] parentNames, String fieldName) {
        String[] result = new String[parentNames.length + 1];
        System.arraycopy(parentNames, 0, result, 0, parentNames.length);
        result[parentNames.length] = fieldName;
        return result;
    }
}
