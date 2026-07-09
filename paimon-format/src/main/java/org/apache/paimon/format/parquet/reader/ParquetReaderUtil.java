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
import org.apache.paimon.utils.StringUtils;

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
import java.util.Objects;

import static org.apache.paimon.format.parquet.ParquetSchemaConverter.parquetListElementType;
import static org.apache.paimon.format.parquet.ParquetSchemaConverter.parquetMapKeyValueType;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

/** Util for generating parquet readers. */
public class ParquetReaderUtil {

    public static List<ParquetField> buildFieldsList(
            DataField[] readFields, MessageColumnIO columnIO, MessageType requestedFileSchema) {
        List<ParquetField> list = new ArrayList<>();
        for (int i = 0; i < readFields.length; i++) {
            list.add(
                    constructField(
                            readFields[i],
                            lookupColumnByName(columnIO, readFields[i].name()),
                            requestedFileSchema.getType(i)));
        }
        return list;
    }

    private static ParquetField constructField(
            DataField dataField, ColumnIO columnIO, Type parquetType) {
        boolean required = columnIO.getType().getRepetition() == REQUIRED;
        int repetitionLevel = columnIO.getRepetitionLevel();
        int definitionLevel = columnIO.getDefinitionLevel();
        DataType type = dataField.type();
        String fieldName = dataField.name();
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
                                getTypeIgnoreCase(parquetType.asGroupType(), childName)));
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
                            keyValueType.getKey());
            ParquetField valueField =
                    constructField(
                            new DataField(0, "", mapType.getValueType()),
                            keyValueColumnIO.getChild(1),
                            keyValueType.getValue());
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
                            keyValueType.getKey());
            ParquetField valueField =
                    constructField(
                            new DataField(0, "", new IntType()),
                            keyValueColumnIO.getChild(1),
                            keyValueType.getValue());
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
            ColumnIO elementTypeColumnIO;
            if (columnIO instanceof GroupColumnIO) {
                GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
                if (!StringUtils.isNullOrWhitespaceOnly(fieldName)) {
                    while (!Objects.equals(groupColumnIO.getName(), fieldName)) {
                        groupColumnIO = (GroupColumnIO) groupColumnIO.getChild(0);
                    }
                    elementTypeColumnIO = groupColumnIO;
                } else {
                    if (elementType instanceof RowType) {
                        elementTypeColumnIO = groupColumnIO;
                    } else {
                        elementTypeColumnIO = groupColumnIO.getChild(0);
                    }
                }
            } else if (columnIO instanceof PrimitiveColumnIO) {
                elementTypeColumnIO = columnIO;
            } else {
                throw new RuntimeException(String.format("Unknown ColumnIO, %s", columnIO));
            }

            ParquetField field =
                    constructField(
                            new DataField(0, "", elementType),
                            getArrayElementColumn(elementTypeColumnIO),
                            parquetListElementType(parquetType.asGroupType()));
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

    public static ColumnIO getArrayElementColumn(ColumnIO columnIO) {
        while (columnIO instanceof GroupColumnIO && !columnIO.getType().isRepetition(REPEATED)) {
            columnIO = ((GroupColumnIO) columnIO).getChild(0);
        }

        /* Compatible with array has a standard 3-level structure:
         *  optional group my_list (LIST) {
         *     repeated group element {
         *        required binary str (UTF8);
         *     };
         *  }
         */
        if (columnIO instanceof GroupColumnIO
                && columnIO.getType().getLogicalTypeAnnotation() == null
                && ((GroupColumnIO) columnIO).getChildrenCount() == 1
                && !columnIO.getName().equals("array")
                && !columnIO.getName().equals(columnIO.getParent().getName() + "_tuple")) {
            return ((GroupColumnIO) columnIO).getChild(0);
        }

        /* Compatible with array for 2-level arrays where a repeated field is not a group:
         *   optional group my_list (LIST) {
         *      repeated int32 element;
         *   }
         */
        return columnIO;
    }
}
