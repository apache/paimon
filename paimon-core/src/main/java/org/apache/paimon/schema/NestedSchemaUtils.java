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
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Preconditions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Utility class for handling nested column schema changes. This provides shared logic for both
 * FlinkCatalog and CDC sources to handle schema evolution for nested types (arrays, maps,
 * multisets, rows) consistently.
 */
public class NestedSchemaUtils {

    /**
     * Generates nested column updates for schema evolution. Handles all nested types: ROW, ARRAY,
     * MAP, MULTISET. Creates proper field paths with markers like "element" for arrays and "value"
     * for maps.
     *
     * @param fieldNames The current field path as a list of field names
     * @param oldType The old data type
     * @param newType The new data type
     * @param schemaChanges List to collect the generated schema changes
     */
    public static void generateNestedColumnUpdates(
            List<String> fieldNames,
            DataType oldType,
            DataType newType,
            List<SchemaChange> schemaChanges) {

        String joinedNames = String.join(".", fieldNames);

        if (oldType.getTypeRoot() == DataTypeRoot.ROW) {
            handleRowTypeUpdate(fieldNames, oldType, newType, schemaChanges, joinedNames);
        } else if (oldType.getTypeRoot() == DataTypeRoot.ARRAY) {
            handleArrayTypeUpdate(fieldNames, oldType, newType, schemaChanges, joinedNames);
        } else if (oldType.getTypeRoot() == DataTypeRoot.MAP) {
            handleMapTypeUpdate(fieldNames, oldType, newType, schemaChanges, joinedNames);
        } else if (oldType.getTypeRoot() == DataTypeRoot.MULTISET) {
            handleMultisetTypeUpdate(fieldNames, oldType, newType, schemaChanges, joinedNames);
        } else {
            // For primitive types, update the column type directly
            handlePrimitiveTypeUpdate(fieldNames, oldType, newType, schemaChanges);
        }

        // Handle nullability changes for all types
        handleNullabilityChange(fieldNames, oldType, newType, schemaChanges);
    }

    private static void handleRowTypeUpdate(
            List<String> fieldNames,
            DataType oldType,
            DataType newType,
            List<SchemaChange> schemaChanges,
            String joinedNames) {

        Preconditions.checkArgument(
                newType.getTypeRoot() == DataTypeRoot.ROW,
                "Column %s can only be updated to row type, and cannot be updated to %s type",
                joinedNames,
                newType.getTypeRoot());

        RowType oldRowType = (RowType) oldType;
        RowType newRowType = (RowType) newType;

        // check that existing fields maintain their order
        Map<String, Integer> oldFieldOrders = new HashMap<>();
        for (int i = 0; i < oldRowType.getFieldCount(); i++) {
            oldFieldOrders.put(oldRowType.getFields().get(i).name(), i);
        }

        int lastIdx = -1;
        String lastFieldName = "";
        for (DataField newField : newRowType.getFields()) {
            String name = newField.name();
            if (oldFieldOrders.containsKey(name)) {
                int idx = oldFieldOrders.get(name);
                Preconditions.checkState(
                        lastIdx < idx,
                        "Order of existing fields in column %s must be kept the same. "
                                + "However, field %s and %s have changed their orders.",
                        joinedNames,
                        lastFieldName,
                        name);
                lastIdx = idx;
                lastFieldName = name;
            }
        }

        // drop fields
        Set<String> newFieldNames = new HashSet<>(newRowType.getFieldNames());
        for (String name : oldRowType.getFieldNames()) {
            if (!newFieldNames.contains(name)) {
                List<String> dropColumnNames = new ArrayList<>(fieldNames);
                dropColumnNames.add(name);
                schemaChanges.add(SchemaChange.dropColumn(dropColumnNames.toArray(new String[0])));
            }
        }

        for (int i = 0; i < newRowType.getFieldCount(); i++) {
            DataField field = newRowType.getFields().get(i);
            String name = field.name();
            List<String> fullFieldNames = new ArrayList<>(fieldNames);
            fullFieldNames.add(name);

            if (!oldFieldOrders.containsKey(name)) {
                // add fields
                SchemaChange.Move move;
                if (i == 0) {
                    move = SchemaChange.Move.first(name);
                } else {
                    String lastName = newRowType.getFields().get(i - 1).name();
                    move = SchemaChange.Move.after(name, lastName);
                }
                schemaChanges.add(
                        SchemaChange.addColumn(
                                fullFieldNames.toArray(new String[0]),
                                field.type(),
                                field.description(),
                                move));
            } else {
                // update existing fields
                DataField oldField = oldRowType.getFields().get(oldFieldOrders.get(name));
                if (!Objects.equals(oldField.description(), field.description())) {
                    schemaChanges.add(
                            SchemaChange.updateColumnComment(
                                    fullFieldNames.toArray(new String[0]), field.description()));
                }
                generateNestedColumnUpdates(
                        fullFieldNames, oldField.type(), field.type(), schemaChanges);
            }
        }
    }

    private static void handleArrayTypeUpdate(
            List<String> fieldNames,
            DataType oldType,
            DataType newType,
            List<SchemaChange> schemaChanges,
            String joinedNames) {

        Preconditions.checkArgument(
                newType.getTypeRoot() == DataTypeRoot.ARRAY,
                "Column %s can only be updated to array type, and cannot be updated to %s type",
                joinedNames,
                newType);

        List<String> fullFieldNames = new ArrayList<>(fieldNames);
        // add a dummy column name indicating the element of array
        fullFieldNames.add("element");

        generateNestedColumnUpdates(
                fullFieldNames,
                ((ArrayType) oldType).getElementType(),
                ((ArrayType) newType).getElementType(),
                schemaChanges);
    }

    private static void handleMapTypeUpdate(
            List<String> fieldNames,
            DataType oldType,
            DataType newType,
            List<SchemaChange> schemaChanges,
            String joinedNames) {

        Preconditions.checkArgument(
                newType.getTypeRoot() == DataTypeRoot.MAP,
                "Column %s can only be updated to map type, and cannot be updated to %s type",
                joinedNames,
                newType);

        MapType oldMapType = (MapType) oldType;
        MapType newMapType = (MapType) newType;

        Preconditions.checkArgument(
                oldMapType.getKeyType().equals(newMapType.getKeyType()),
                "Cannot update key type of column %s from %s type to %s type",
                joinedNames,
                oldMapType.getKeyType(),
                newMapType.getKeyType());

        List<String> fullFieldNames = new ArrayList<>(fieldNames);
        // add a dummy column name indicating the value of map
        fullFieldNames.add("value");

        generateNestedColumnUpdates(
                fullFieldNames,
                oldMapType.getValueType(),
                newMapType.getValueType(),
                schemaChanges);
    }

    private static void handleMultisetTypeUpdate(
            List<String> fieldNames,
            DataType oldType,
            DataType newType,
            List<SchemaChange> schemaChanges,
            String joinedNames) {

        Preconditions.checkArgument(
                newType.getTypeRoot() == DataTypeRoot.MULTISET,
                "Column %s can only be updated to multiset type, and cannot be updated to %s type",
                joinedNames,
                newType);

        List<String> fullFieldNames = new ArrayList<>(fieldNames);
        // Add the special "element" marker for multiset element access
        fullFieldNames.add("element");

        generateNestedColumnUpdates(
                fullFieldNames,
                ((MultisetType) oldType).getElementType(),
                ((MultisetType) newType).getElementType(),
                schemaChanges);
    }

    private static void handlePrimitiveTypeUpdate(
            List<String> fieldNames,
            DataType oldType,
            DataType newType,
            List<SchemaChange> schemaChanges) {

        if (!oldType.equalsIgnoreNullable(newType)) {
            schemaChanges.add(
                    SchemaChange.updateColumnType(
                            fieldNames.toArray(new String[0]), newType, false));
        }
    }

    private static void handleNullabilityChange(
            List<String> fieldNames,
            DataType oldType,
            DataType newType,
            List<SchemaChange> schemaChanges) {

        if (oldType.isNullable() != newType.isNullable()) {
            schemaChanges.add(
                    SchemaChange.updateColumnNullability(
                            fieldNames.toArray(new String[0]), newType.isNullable()));
        }
    }
}
