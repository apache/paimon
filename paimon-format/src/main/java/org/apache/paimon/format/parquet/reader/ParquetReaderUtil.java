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

package org.apache.paimon.format.parquet.reader;

import org.apache.paimon.format.parquet.ParquetListLayoutResolver;
import org.apache.paimon.format.parquet.type.ParquetField;
import org.apache.paimon.format.parquet.type.ParquetGroupField;
import org.apache.paimon.format.parquet.type.ParquetPrimitiveField;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VectorType;
import org.apache.paimon.utils.Pair;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;

import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.GroupColumnIO;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.PrimitiveColumnIO;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.paimon.format.parquet.ParquetSchemaConverter.convertToPaimonField;
import static org.apache.paimon.format.parquet.ParquetSchemaConverter.parquetMapKeyValueType;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

/** Util for generating parquet readers. */
public class ParquetReaderUtil {

    public static List<ParquetField> buildFieldsList(
            DataField[] readFields,
            MessageColumnIO columnIO,
            MessageType requestedSchema,
            ParquetListLayoutResolver.LayoutContext listLayout) {
        List<ParquetField> list = new ArrayList<>();
        for (int i = 0; i < readFields.length; i++) {
            list.add(
                    constructField(
                            readFields[i],
                            lookupColumnByName(columnIO, readFields[i].name()),
                            requestedSchema.getType(i),
                            listLayout));
        }
        return list;
    }

    private static ParquetField constructField(
            DataField dataField,
            ColumnIO columnIO,
            Type parquetType,
            ParquetListLayoutResolver.LayoutContext listLayout) {
        boolean required = columnIO.getType().getRepetition() == REQUIRED;
        int repetitionLevel = columnIO.getRepetitionLevel();
        int definitionLevel = columnIO.getDefinitionLevel();
        DataType type = dataField.type();
        if (type instanceof RowType) {
            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
            RowType rowType = (RowType) type;

            ImmutableList.Builder<ParquetField> fieldsBuilder = ImmutableList.builder();
            List<String> fieldNames = rowType.getFieldNames();
            List<DataField> children = rowType.getFields();
            for (int i = 0; i < children.size(); i++) {
                String childName = fieldNames.get(i);
                fieldsBuilder.add(
                        constructField(
                                children.get(i),
                                lookupColumnByName(groupColumnIO, childName),
                                getTypeIgnoreCase(parquetType.asGroupType(), childName),
                                listLayout));
            }
            GroupType parquetGroup = parquetType.asGroupType();
            for (int i = children.size(); i < parquetGroup.getFieldCount(); i++) {
                Type extraType = parquetGroup.getType(i);
                DataField extraField = convertToPaimonField(addFallbackFieldIds(extraType));
                fieldsBuilder.add(
                        constructField(
                                extraField,
                                lookupColumnByName(groupColumnIO, extraType.getName()),
                                extraType,
                                listLayout));
            }

            return new ParquetGroupField(
                    type,
                    repetitionLevel,
                    definitionLevel,
                    required,
                    fieldsBuilder.build(),
                    groupColumnIO.getFieldPath());
        }

        if (type instanceof MapType) {
            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
            GroupColumnIO keyValueColumnIO = getMapKeyValueColumn(groupColumnIO);
            Pair<Type, Type> keyValueType = parquetMapKeyValueType(parquetType.asGroupType());
            MapType mapType = (MapType) type;
            ParquetField keyField =
                    constructField(
                            new DataField(0, "", mapType.getKeyType()),
                            keyValueColumnIO.getChild(0),
                            keyValueType.getKey(),
                            listLayout);
            ParquetField valueField =
                    constructField(
                            new DataField(0, "", mapType.getValueType()),
                            keyValueColumnIO.getChild(1),
                            keyValueType.getValue(),
                            listLayout);
            return new ParquetGroupField(
                    type,
                    repetitionLevel,
                    definitionLevel,
                    required,
                    ImmutableList.of(keyField, valueField),
                    groupColumnIO.getFieldPath());
        }

        if (type instanceof MultisetType) {
            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
            GroupColumnIO keyValueColumnIO = getMapKeyValueColumn(groupColumnIO);
            Pair<Type, Type> keyValueType = parquetMapKeyValueType(parquetType.asGroupType());
            MultisetType multisetType = (MultisetType) type;
            ParquetField keyField =
                    constructField(
                            new DataField(0, "", multisetType.getElementType()),
                            keyValueColumnIO.getChild(0),
                            keyValueType.getKey(),
                            listLayout);
            ParquetField valueField =
                    constructField(
                            new DataField(0, "", new IntType()),
                            keyValueColumnIO.getChild(1),
                            keyValueType.getValue(),
                            listLayout);
            return new ParquetGroupField(
                    type,
                    repetitionLevel,
                    definitionLevel,
                    required,
                    ImmutableList.of(keyField, valueField),
                    groupColumnIO.getFieldPath());
        }

        if (type instanceof ArrayType || type instanceof VectorType) {
            DataType elementType =
                    type instanceof ArrayType
                            ? ((ArrayType) type).getElementType()
                            : ((VectorType) type).getElementType();
            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
            GroupType requestedGroup = parquetType.asGroupType();

            boolean threeLevel =
                    listLayout.isThreeLevelList(requestedGroup, groupColumnIO.getFieldPath());
            Type requestedElementType =
                    threeLevel
                            ? requestedGroup.getType(0).asGroupType().getType(0)
                            : requestedGroup.getType(0);

            ColumnIO middleColumnIO = groupColumnIO.getChild(0);
            ColumnIO elementColumnIO =
                    threeLevel ? ((GroupColumnIO) middleColumnIO).getChild(0) : middleColumnIO;

            ParquetField field =
                    constructField(
                            new DataField(0, "", elementType),
                            elementColumnIO,
                            requestedElementType,
                            listLayout);
            if (repetitionLevel == field.getRepetitionLevel()) {
                repetitionLevel = columnIO.getParent().getRepetitionLevel();
            }
            return new ParquetGroupField(
                    type,
                    repetitionLevel,
                    definitionLevel,
                    required,
                    ImmutableList.of(field),
                    columnIO.getFieldPath());
        }

        PrimitiveColumnIO primitiveColumnIO = (PrimitiveColumnIO) columnIO;
        return new ParquetPrimitiveField(
                type,
                required,
                primitiveColumnIO.getColumnDescriptor(),
                primitiveColumnIO.getId(),
                primitiveColumnIO.getFieldPath());
    }

    private static Type addFallbackFieldIds(Type type) {
        Type result = type;
        if (!type.isPrimitive()) {
            List<Type> children =
                    type.asGroupType().getFields().stream()
                            .map(ParquetReaderUtil::addFallbackFieldIds)
                            .collect(Collectors.toList());
            result = type.asGroupType().withNewFields(children);
        }
        return result.getId() == null ? result.withId(0) : result;
    }

    /**
     * Parquet's column names are case insensitive. So when we look up columns we first check for
     * exact match, and if that can not find we look for a case-insensitive match.
     */
    public static ColumnIO lookupColumnByName(GroupColumnIO groupColumnIO, String columnName) {
        ColumnIO columnIO = groupColumnIO.getChild(columnName);

        if (columnIO != null) {
            return columnIO;
        }

        for (int i = 0; i < groupColumnIO.getChildrenCount(); i++) {
            if (groupColumnIO.getChild(i).getName().equalsIgnoreCase(columnName)) {
                return groupColumnIO.getChild(i);
            }
        }

        throw new RuntimeException(
                String.format(
                        "ColumnIO for '%s' not found in Parquet schema under '%s'.",
                        columnName, String.join(".", groupColumnIO.getFieldPath())));
    }

    /**
     * Resolves a child {@link Type} by name, first by exact match then case-insensitively,
     * mirroring {@link #lookupColumnByName}. Falls back to {@link GroupType#getType(String)} (which
     * throws) so a genuinely missing field keeps the original failure behavior.
     */
    private static Type getTypeIgnoreCase(GroupType groupType, String fieldName) {
        if (groupType.containsField(fieldName)) {
            return groupType.getType(fieldName);
        }
        for (Type field : groupType.getFields()) {
            if (field.getName().equalsIgnoreCase(fieldName)) {
                return field;
            }
        }
        return groupType.getType(fieldName);
    }

    public static GroupColumnIO getMapKeyValueColumn(GroupColumnIO groupColumnIO) {
        while (groupColumnIO.getChildrenCount() == 1) {
            groupColumnIO = (GroupColumnIO) groupColumnIO.getChild(0);
        }
        return groupColumnIO;
    }
}
