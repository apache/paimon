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

import org.apache.parquet.ParquetRuntimeException;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.GroupColumnIO;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.PrimitiveColumnIO;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.InvalidSchemaException;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

/** Util for generating {@link ColumnReader}. */
public class ParquetSplitReaderUtil {

    @SuppressWarnings("rawtypes")
    public static ColumnReader createColumnReader(
            DataType fieldType,
            Type type,
            List<ColumnDescriptor> columnDescriptors,
            PageReadStore pages,
            ParquetField field,
            int depth)
            throws IOException {
        List<ColumnDescriptor> descriptors =
                getAllColumnDescriptorByType(depth, type, columnDescriptors);
        switch (fieldType.getTypeRoot()) {
            case BOOLEAN:
                return new BooleanColumnReader(descriptors.get(0), pages);
            case TINYINT:
                return new ByteColumnReader(descriptors.get(0), pages);
            case DOUBLE:
                return new DoubleColumnReader(descriptors.get(0), pages);
            case FLOAT:
                return new FloatColumnReader(descriptors.get(0), pages);
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return new IntColumnReader(descriptors.get(0), pages);
            case BIGINT:
                return new LongColumnReader(descriptors.get(0), pages);
            case SMALLINT:
                return new ShortColumnReader(descriptors.get(0), pages);
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
                if (descriptors.get(0).getPrimitiveType().getPrimitiveTypeName()
                        == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
                    return new FixedLenBytesBinaryColumnReader(
                            descriptors.get(0), pages, ((BinaryType) fieldType).getLength());
                }
                return new BytesColumnReader(descriptors.get(0), pages);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if (descriptors.get(0).getPrimitiveType().getPrimitiveTypeName()
                        == PrimitiveType.PrimitiveTypeName.INT64) {
                    return new LongColumnReader(descriptors.get(0), pages);
                }
                return new TimestampColumnReader(true, descriptors.get(0), pages);
            case DECIMAL:
                switch (descriptors.get(0).getPrimitiveType().getPrimitiveTypeName()) {
                    case INT32:
                        return new IntColumnReader(descriptors.get(0), pages);
                    case INT64:
                        return new LongColumnReader(descriptors.get(0), pages);
                    case BINARY:
                        return new BytesColumnReader(descriptors.get(0), pages);
                    case FIXED_LEN_BYTE_ARRAY:
                        return new FixedLenBytesDecimalColumnReader(
                                descriptors.get(0),
                                pages,
                                ((DecimalType) fieldType).getPrecision());
                }
            case VARIANT:
                List<ColumnReader> fieldReaders = new ArrayList<>();
                fieldReaders.add(new BytesColumnReader(descriptors.get(0), pages));
                fieldReaders.add(new BytesColumnReader(descriptors.get(1), pages));
                return new RowColumnReader(fieldReaders);
            case ARRAY:
            case MAP:
            case MULTISET:
            case ROW:
                return new NestedColumnReader(true, pages, field);
            default:
                throw new UnsupportedOperationException(fieldType + " is not supported now.");
        }
    }

    public static WritableColumnVector createWritableColumnVector(
            int batchSize,
            DataType fieldType,
            Type type,
            List<ColumnDescriptor> columnDescriptors,
            int depth) {
        List<ColumnDescriptor> descriptors =
                getAllColumnDescriptorByType(depth, type, columnDescriptors);
        PrimitiveType primitiveType = descriptors.get(0).getPrimitiveType();
        PrimitiveType.PrimitiveTypeName typeName = primitiveType.getPrimitiveTypeName();
        switch (fieldType.getTypeRoot()) {
            case BOOLEAN:
                checkArgument(
                        typeName == PrimitiveType.PrimitiveTypeName.BOOLEAN,
                        "Unexpected type: %s",
                        typeName);
                return new HeapBooleanVector(batchSize);
            case TINYINT:
                checkArgument(
                        typeName == PrimitiveType.PrimitiveTypeName.INT32,
                        "Unexpected type: %s",
                        typeName);
                return new HeapByteVector(batchSize);
            case DOUBLE:
                checkArgument(
                        typeName == PrimitiveType.PrimitiveTypeName.DOUBLE,
                        "Unexpected type: %s",
                        typeName);
                return new HeapDoubleVector(batchSize);
            case FLOAT:
                checkArgument(
                        typeName == PrimitiveType.PrimitiveTypeName.FLOAT,
                        "Unexpected type: %s",
                        typeName);
                return new HeapFloatVector(batchSize);
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                checkArgument(
                        typeName == PrimitiveType.PrimitiveTypeName.INT32,
                        "Unexpected type: %s",
                        typeName);
                return new HeapIntVector(batchSize);
            case BIGINT:
                checkArgument(
                        typeName == PrimitiveType.PrimitiveTypeName.INT64,
                        "Unexpected type: %s",
                        typeName);
                return new HeapLongVector(batchSize);
            case SMALLINT:
                checkArgument(
                        typeName == PrimitiveType.PrimitiveTypeName.INT32,
                        "Unexpected type: %s",
                        typeName);
                return new HeapShortVector(batchSize);
            case CHAR:
            case VARCHAR:
            case VARBINARY:
                checkArgument(
                        typeName == PrimitiveType.PrimitiveTypeName.BINARY,
                        "Unexpected type: %s",
                        typeName);
                return new HeapBytesVector(batchSize);
            case BINARY:
                checkArgument(
                        typeName == PrimitiveType.PrimitiveTypeName.BINARY
                                || typeName == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
                        "Unexpected type: %s",
                        typeName);
                return new HeapBytesVector(batchSize);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                int precision = DataTypeChecks.getPrecision(fieldType);
                if (precision > 6) {
                    checkArgument(
                            typeName == PrimitiveType.PrimitiveTypeName.INT96,
                            "Unexpected type: %s",
                            typeName);
                    return new HeapTimestampVector(batchSize);
                } else {
                    return new HeapLongVector(batchSize);
                }
            case DECIMAL:
                DecimalType decimalType = (DecimalType) fieldType;
                if (ParquetSchemaConverter.is32BitDecimal(decimalType.getPrecision())) {
                    checkArgument(
                            (typeName == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY
                                            || typeName == PrimitiveType.PrimitiveTypeName.INT32)
                                    && primitiveType.getLogicalTypeAnnotation()
                                            instanceof DecimalLogicalTypeAnnotation,
                            "Unexpected type: %s",
                            typeName);
                    return new HeapIntVector(batchSize);
                } else if (ParquetSchemaConverter.is64BitDecimal(decimalType.getPrecision())) {
                    checkArgument(
                            (typeName == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY
                                            || typeName == PrimitiveType.PrimitiveTypeName.INT64)
                                    && primitiveType.getLogicalTypeAnnotation()
                                            instanceof DecimalLogicalTypeAnnotation,
                            "Unexpected type: %s",
                            typeName);
                    return new HeapLongVector(batchSize);
                } else {
                    checkArgument(
                            (typeName == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY
                                            || typeName == PrimitiveType.PrimitiveTypeName.BINARY)
                                    && primitiveType.getLogicalTypeAnnotation()
                                            instanceof DecimalLogicalTypeAnnotation,
                            "Unexpected type: %s",
                            typeName);
                    return new HeapBytesVector(batchSize);
                }
            case ARRAY:
                ArrayType arrayType = (ArrayType) fieldType;
                return new HeapArrayVector(
                        batchSize,
                        createWritableColumnVector(
                                batchSize,
                                arrayType.getElementType(),
                                type,
                                columnDescriptors,
                                depth));
            case MAP:
                MapType mapType = (MapType) fieldType;
                LogicalTypeAnnotation mapTypeAnnotation = type.getLogicalTypeAnnotation();
                GroupType mapRepeatedType = type.asGroupType().getType(0).asGroupType();
                if (mapTypeAnnotation.equals(LogicalTypeAnnotation.listType())) {
                    mapRepeatedType = mapRepeatedType.getType(0).asGroupType();
                    depth++;
                    if (mapRepeatedType
                            .getLogicalTypeAnnotation()
                            .equals(LogicalTypeAnnotation.mapType())) {
                        mapRepeatedType = mapRepeatedType.getType(0).asGroupType();
                        depth++;
                    }
                }
                return new HeapMapVector(
                        batchSize,
                        createWritableColumnVector(
                                batchSize,
                                mapType.getKeyType(),
                                mapRepeatedType.getType(0),
                                descriptors,
                                depth + 2),
                        createWritableColumnVector(
                                batchSize,
                                mapType.getValueType(),
                                mapRepeatedType.getType(1),
                                descriptors,
                                depth + 2));
            case MULTISET:
                MultisetType multisetType = (MultisetType) fieldType;
                LogicalTypeAnnotation multisetTypeAnnotation = type.getLogicalTypeAnnotation();
                GroupType multisetRepeatedType = type.asGroupType().getType(0).asGroupType();
                if (multisetTypeAnnotation.equals(LogicalTypeAnnotation.listType())) {
                    multisetRepeatedType = multisetRepeatedType.getType(0).asGroupType();
                    depth++;
                    if (multisetRepeatedType
                            .getLogicalTypeAnnotation()
                            .equals(LogicalTypeAnnotation.mapType())) {
                        multisetRepeatedType = multisetRepeatedType.getType(0).asGroupType();
                        depth++;
                    }
                }
                return new HeapMapVector(
                        batchSize,
                        createWritableColumnVector(
                                batchSize,
                                multisetType.getElementType(),
                                multisetRepeatedType.getType(0),
                                descriptors,
                                depth + 2),
                        createWritableColumnVector(
                                batchSize,
                                new IntType(false),
                                multisetRepeatedType.getType(1),
                                descriptors,
                                depth + 2));
            case ROW:
                RowType rowType = (RowType) fieldType;
                GroupType groupType = type.asGroupType();
                if (LogicalTypeAnnotation.listType().equals(groupType.getLogicalTypeAnnotation())) {
                    // this means there was two outside struct, need to get group twice.
                    groupType = groupType.getType(0).asGroupType();
                    groupType = groupType.getType(0).asGroupType();
                    depth = depth + 2;
                }
                WritableColumnVector[] columnVectors =
                        new WritableColumnVector[rowType.getFieldCount()];
                for (int i = 0; i < columnVectors.length; i++) {
                    columnVectors[i] =
                            createWritableColumnVector(
                                    batchSize,
                                    rowType.getTypeAt(i),
                                    groupType.getType(i),
                                    descriptors,
                                    depth + 1);
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

    private static List<ColumnDescriptor> getAllColumnDescriptorByType(
            int depth, Type type, List<ColumnDescriptor> columns) throws ParquetRuntimeException {
        List<ColumnDescriptor> res = new ArrayList<>();
        for (ColumnDescriptor descriptor : columns) {
            if (depth >= descriptor.getPath().length) {
                throw new InvalidSchemaException("Corrupted Parquet schema");
            }
            if (type.getName().equals(descriptor.getPath()[depth])) {
                res.add(descriptor);
            }
        }

        // If doesn't find the type descriptor in corresponding depth, throw exception
        if (res.isEmpty()) {
            throw new InvalidSchemaException(
                    "Failed to find related Parquet column descriptor with type " + type);
        }
        return res;
    }

    public static List<ParquetField> buildFieldsList(
            List<DataField> children, List<String> fieldNames, MessageColumnIO columnIO) {
        List<ParquetField> list = new ArrayList<>();
        for (int i = 0; i < children.size(); i++) {
            list.add(
                    constructField(
                            children.get(i), lookupColumnByName(columnIO, fieldNames.get(i))));
        }
        return list;
    }

    private static ParquetField constructField(DataField dataField, ColumnIO columnIO) {
        boolean required = columnIO.getType().getRepetition() == REQUIRED;
        int repetitionLevel = columnIO.getRepetitionLevel();
        int definitionLevel = columnIO.getDefinitionLevel();
        DataType type = dataField.type();
        String filedName = dataField.name();
        if (type instanceof RowType) {
            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
            RowType rowType = (RowType) type;
            ImmutableList.Builder<ParquetField> fieldsBuilder = ImmutableList.builder();
            List<String> fieldNames = rowType.getFieldNames();
            List<DataField> childrens = rowType.getFields();
            for (int i = 0; i < childrens.size(); i++) {
                fieldsBuilder.add(
                        constructField(
                                childrens.get(i),
                                lookupColumnByName(groupColumnIO, fieldNames.get(i))));
            }

            return new ParquetGroupField(
                    type, repetitionLevel, definitionLevel, required, fieldsBuilder.build());
        }

        if (type instanceof VariantType) {
            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
            ImmutableList.Builder<ParquetField> fieldsBuilder = ImmutableList.builder();
            PrimitiveColumnIO value =
                    (PrimitiveColumnIO) lookupColumnByName(groupColumnIO, Variant.VALUE);
            fieldsBuilder.add(
                    new ParquetPrimitiveField(
                            new BinaryType(),
                            required,
                            value.getColumnDescriptor(),
                            value.getId()));
            PrimitiveColumnIO metadata =
                    (PrimitiveColumnIO) lookupColumnByName(groupColumnIO, Variant.METADATA);
            fieldsBuilder.add(
                    new ParquetPrimitiveField(
                            new BinaryType(),
                            required,
                            metadata.getColumnDescriptor(),
                            metadata.getId()));
            return new ParquetGroupField(
                    type, repetitionLevel, definitionLevel, required, fieldsBuilder.build());
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
                    ImmutableList.of(keyField, valueField));
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
                    ImmutableList.of(keyField, valueField));
        }

        if (type instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) type;
            ColumnIO elementTypeColumnIO;
            if (columnIO instanceof GroupColumnIO) {
                GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
                if (!StringUtils.isNullOrWhitespaceOnly(filedName)) {
                    while (!Objects.equals(groupColumnIO.getName(), filedName)) {
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
                    type, repetitionLevel, definitionLevel, required, ImmutableList.of(field));
        }

        PrimitiveColumnIO primitiveColumnIO = (PrimitiveColumnIO) columnIO;
        return new ParquetPrimitiveField(
                type, required, primitiveColumnIO.getColumnDescriptor(), primitiveColumnIO.getId());
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
