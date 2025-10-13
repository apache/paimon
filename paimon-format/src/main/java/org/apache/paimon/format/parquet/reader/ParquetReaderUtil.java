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

import org.apache.paimon.data.columnar.heap.HeapArrayVector;
import org.apache.paimon.data.columnar.heap.HeapBooleanVector;
import org.apache.paimon.data.columnar.heap.HeapByteVector;
import org.apache.paimon.data.columnar.heap.HeapBytesVector;
import org.apache.paimon.data.columnar.heap.HeapDoubleVector;
import org.apache.paimon.data.columnar.heap.HeapFloatVector;
import org.apache.paimon.data.columnar.heap.HeapIntVector;
import org.apache.paimon.data.columnar.heap.HeapLongVector;
import org.apache.paimon.data.columnar.heap.HeapMapVector;
import org.apache.paimon.data.columnar.heap.HeapRowVector;
import org.apache.paimon.data.columnar.heap.HeapShortVector;
import org.apache.paimon.data.columnar.heap.HeapTimestampVector;
import org.apache.paimon.data.columnar.writable.WritableColumnVector;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.format.parquet.ParquetSchemaConverter;
import org.apache.paimon.format.parquet.type.ParquetField;
import org.apache.paimon.format.parquet.type.ParquetGroupField;
import org.apache.paimon.format.parquet.type.ParquetPrimitiveField;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VariantType;
import org.apache.paimon.utils.StringUtils;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;

import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.GroupColumnIO;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.PrimitiveColumnIO;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

/** Util for generating parquet readers. */
public class ParquetReaderUtil {

    public static WritableColumnVector createWritableColumnVector(
            int batchSize, DataType fieldType) {
        switch (fieldType.getTypeRoot()) {
            case BOOLEAN:
                return new HeapBooleanVector(batchSize);
            case TINYINT:
                return new HeapByteVector(batchSize);
            case DOUBLE:
                return new HeapDoubleVector(batchSize);
            case FLOAT:
                return new HeapFloatVector(batchSize);
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return new HeapIntVector(batchSize);
            case BIGINT:
                return new HeapLongVector(batchSize);
            case SMALLINT:
                return new HeapShortVector(batchSize);
            case CHAR:
            case VARCHAR:
            case VARBINARY:
                return new HeapBytesVector(batchSize);
            case BINARY:
                return new HeapBytesVector(batchSize);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                int precision = DataTypeChecks.getPrecision(fieldType);
                if (precision > 6) {
                    return new HeapTimestampVector(batchSize);
                } else {
                    return new HeapLongVector(batchSize);
                }
            case DECIMAL:
                DecimalType decimalType = (DecimalType) fieldType;
                if (ParquetSchemaConverter.is32BitDecimal(decimalType.getPrecision())) {
                    return new HeapIntVector(batchSize);
                } else if (ParquetSchemaConverter.is64BitDecimal(decimalType.getPrecision())) {
                    return new HeapLongVector(batchSize);
                } else {
                    return new HeapBytesVector(batchSize);
                }
            case ARRAY:
                ArrayType arrayType = (ArrayType) fieldType;
                return new HeapArrayVector(
                        batchSize,
                        createWritableColumnVector(batchSize, arrayType.getElementType()));
            case MAP:
                MapType mapType = (MapType) fieldType;
                return new HeapMapVector(
                        batchSize,
                        createWritableColumnVector(batchSize, mapType.getKeyType()),
                        createWritableColumnVector(batchSize, mapType.getValueType()));
            case MULTISET:
                MultisetType multisetType = (MultisetType) fieldType;
                return new HeapMapVector(
                        batchSize,
                        createWritableColumnVector(batchSize, multisetType.getElementType()),
                        createWritableColumnVector(batchSize, new IntType(false)));
            case ROW:
                RowType rowType = (RowType) fieldType;
                WritableColumnVector[] columnVectors =
                        new WritableColumnVector[rowType.getFieldCount()];
                for (int i = 0; i < columnVectors.length; i++) {
                    columnVectors[i] = createWritableColumnVector(batchSize, rowType.getTypeAt(i));
                }
                return new HeapRowVector(batchSize, columnVectors);
            case VARIANT:
                WritableColumnVector[] vectors = new WritableColumnVector[2];
                vectors[0] = new HeapBytesVector(batchSize);
                vectors[1] = new HeapBytesVector(batchSize);
                return new HeapRowVector(batchSize, vectors);
            default:
                throw new UnsupportedOperationException(fieldType + " is not supported now.");
        }
    }

    public static List<ParquetField> buildFieldsList(
            DataField[] readFields, MessageColumnIO columnIO, RowType[] shreddingSchemas) {
        List<ParquetField> list = new ArrayList<>();
        for (int i = 0; i < readFields.length; i++) {
            list.add(
                    constructField(
                            readFields[i],
                            lookupColumnByName(columnIO, readFields[i].name()),
                            shreddingSchemas[i]));
        }
        return list;
    }

    private static ParquetField constructField(DataField dataField, ColumnIO columnIO) {
        return constructField(dataField, columnIO, null);
    }

    private static ParquetField constructField(
            DataField dataField, ColumnIO columnIO, @Nullable RowType shreddingSchema) {
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
                fieldsBuilder.add(
                        constructField(
                                children.get(i),
                                lookupColumnByName(groupColumnIO, fieldNames.get(i))));
            }

            return new ParquetGroupField(
                    type,
                    repetitionLevel,
                    definitionLevel,
                    required,
                    fieldsBuilder.build(),
                    groupColumnIO.getFieldPath());
        }

        if (type instanceof VariantType) {
            if (shreddingSchema != null) {
                ParquetGroupField parquetField =
                        (ParquetGroupField)
                                constructField(dataField.newType(shreddingSchema), columnIO);
                return new ParquetGroupField(
                        type,
                        parquetField.getRepetitionLevel(),
                        parquetField.getDefinitionLevel(),
                        parquetField.isRequired(),
                        parquetField.getChildren(),
                        parquetField.path(),
                        parquetField);
            }

            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
            ImmutableList.Builder<ParquetField> fieldsBuilder = ImmutableList.builder();
            PrimitiveColumnIO value =
                    (PrimitiveColumnIO) lookupColumnByName(groupColumnIO, Variant.VALUE);
            fieldsBuilder.add(
                    new ParquetPrimitiveField(
                            new BinaryType(),
                            required,
                            value.getColumnDescriptor(),
                            value.getId(),
                            value.getFieldPath()));
            PrimitiveColumnIO metadata =
                    (PrimitiveColumnIO) lookupColumnByName(groupColumnIO, Variant.METADATA);
            fieldsBuilder.add(
                    new ParquetPrimitiveField(
                            new BinaryType(),
                            required,
                            metadata.getColumnDescriptor(),
                            metadata.getId(),
                            metadata.getFieldPath()));
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
            MapType mapType = (MapType) type;
            ParquetField keyField =
                    constructField(
                            new DataField(0, "", mapType.getKeyType()),
                            keyValueColumnIO.getChild(0));
            ParquetField valueField =
                    constructField(
                            new DataField(0, "", mapType.getValueType()),
                            keyValueColumnIO.getChild(1));
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
            MultisetType multisetType = (MultisetType) type;
            ParquetField keyField =
                    constructField(
                            new DataField(0, "", multisetType.getElementType()),
                            keyValueColumnIO.getChild(0));
            ParquetField valueField =
                    constructField(
                            new DataField(0, "", new IntType()), keyValueColumnIO.getChild(1));
            return new ParquetGroupField(
                    type,
                    repetitionLevel,
                    definitionLevel,
                    required,
                    ImmutableList.of(keyField, valueField),
                    groupColumnIO.getFieldPath());
        }

        if (type instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) type;
            ColumnIO elementTypeColumnIO;
            if (columnIO instanceof GroupColumnIO) {
                GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
                if (!StringUtils.isNullOrWhitespaceOnly(fieldName)) {
                    while (!Objects.equals(groupColumnIO.getName(), fieldName)) {
                        groupColumnIO = (GroupColumnIO) groupColumnIO.getChild(0);
                    }
                    elementTypeColumnIO = groupColumnIO;
                } else {
                    if (arrayType.getElementType() instanceof RowType) {
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
                            new DataField(0, "", arrayType.getElementType()),
                            getArrayElementColumn(elementTypeColumnIO));
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
     * Parquet's column names are case in sensitive. So when we look up columns we first check for
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

        throw new RuntimeException("Can not find column io for parquet reader.");
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
