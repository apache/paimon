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
import org.apache.paimon.format.parquet.ParquetSchemaConverter;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;

import org.apache.parquet.ParquetRuntimeException;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.InvalidSchemaException;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Util for generating {@link ColumnReader}. */
public class ParquetSplitReaderUtil {

    @SuppressWarnings("rawtypes")
    public static ColumnReader createColumnReader(
            DataType fieldType,
            Type type,
            List<ColumnDescriptor> columnDescriptors,
            PageReadStore pages,
            int depth)
            throws IOException {
        List<ColumnDescriptor> descriptors =
                getAllColumnDescriptorByType(depth, type, columnDescriptors);
        switch (fieldType.getTypeRoot()) {
            case BOOLEAN:
                return new BooleanColumnReader(
                        descriptors.get(0), pages.getPageReader(descriptors.get(0)));
            case TINYINT:
                return new ByteColumnReader(
                        descriptors.get(0), pages.getPageReader(descriptors.get(0)));
            case DOUBLE:
                return new DoubleColumnReader(
                        descriptors.get(0), pages.getPageReader(descriptors.get(0)));
            case FLOAT:
                return new FloatColumnReader(
                        descriptors.get(0), pages.getPageReader(descriptors.get(0)));
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return new IntColumnReader(
                        descriptors.get(0), pages.getPageReader(descriptors.get(0)));
            case BIGINT:
                return new LongColumnReader(
                        descriptors.get(0), pages.getPageReader(descriptors.get(0)));
            case SMALLINT:
                return new ShortColumnReader(
                        descriptors.get(0), pages.getPageReader(descriptors.get(0)));
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
                return new BytesColumnReader(
                        descriptors.get(0), pages.getPageReader(descriptors.get(0)));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if (descriptors.get(0).getPrimitiveType().getPrimitiveTypeName()
                        == PrimitiveType.PrimitiveTypeName.INT64) {
                    return new LongColumnReader(
                            descriptors.get(0), pages.getPageReader(descriptors.get(0)));
                }
                return new TimestampColumnReader(
                        true, descriptors.get(0), pages.getPageReader(descriptors.get(0)));
            case DECIMAL:
                switch (descriptors.get(0).getPrimitiveType().getPrimitiveTypeName()) {
                    case INT32:
                        return new IntColumnReader(
                                descriptors.get(0), pages.getPageReader(descriptors.get(0)));
                    case INT64:
                        return new LongColumnReader(
                                descriptors.get(0), pages.getPageReader(descriptors.get(0)));
                    case BINARY:
                        return new BytesColumnReader(
                                descriptors.get(0), pages.getPageReader(descriptors.get(0)));
                    case FIXED_LEN_BYTE_ARRAY:
                        return new FixedLenBytesColumnReader(
                                descriptors.get(0),
                                pages.getPageReader(descriptors.get(0)),
                                ((DecimalType) fieldType).getPrecision());
                }
            case ARRAY:
                return new ArrayColumnReader(
                        descriptors.get(0),
                        pages.getPageReader(descriptors.get(0)),
                        true,
                        descriptors.get(0).getPrimitiveType(),
                        fieldType);
            case MAP:
                MapType mapType = (MapType) fieldType;
                ArrayColumnReader mapKeyReader =
                        new ArrayColumnReader(
                                descriptors.get(0),
                                pages.getPageReader(descriptors.get(0)),
                                true,
                                descriptors.get(0).getPrimitiveType(),
                                new ArrayType(mapType.getKeyType()));
                ArrayColumnReader mapValueReader =
                        new ArrayColumnReader(
                                descriptors.get(1),
                                pages.getPageReader(descriptors.get(1)),
                                true,
                                descriptors.get(1).getPrimitiveType(),
                                new ArrayType(mapType.getValueType()));
                return new MapColumnReader(mapKeyReader, mapValueReader);
            case MULTISET:
                MultisetType multisetType = (MultisetType) fieldType;
                ArrayColumnReader multisetKeyReader =
                        new ArrayColumnReader(
                                descriptors.get(0),
                                pages.getPageReader(descriptors.get(0)),
                                true,
                                descriptors.get(0).getPrimitiveType(),
                                new ArrayType(multisetType.getElementType()));
                ArrayColumnReader multisetValueReader =
                        new ArrayColumnReader(
                                descriptors.get(1),
                                pages.getPageReader(descriptors.get(1)),
                                true,
                                descriptors.get(1).getPrimitiveType(),
                                new ArrayType(new IntType(false)));
                return new MapColumnReader(multisetKeyReader, multisetValueReader);
            case ROW:
                RowType rowType = (RowType) fieldType;
                GroupType groupType = type.asGroupType();
                List<ColumnReader> fieldReaders = new ArrayList<>();
                for (int i = 0; i < rowType.getFieldCount(); i++) {
                    fieldReaders.add(
                            createColumnReader(
                                    rowType.getTypeAt(i),
                                    groupType.getType(i),
                                    descriptors,
                                    pages,
                                    depth + 1));
                }
                return new RowColumnReader(fieldReaders);
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
            case BINARY:
            case VARBINARY:
                checkArgument(
                        typeName == PrimitiveType.PrimitiveTypeName.BINARY,
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
                                    && primitiveType.getOriginalType() == OriginalType.DECIMAL,
                            "Unexpected type: %s",
                            typeName);
                    return new HeapIntVector(batchSize);
                } else if (ParquetSchemaConverter.is64BitDecimal(decimalType.getPrecision())) {
                    checkArgument(
                            (typeName == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY
                                            || typeName == PrimitiveType.PrimitiveTypeName.INT64)
                                    && primitiveType.getOriginalType() == OriginalType.DECIMAL,
                            "Unexpected type: %s",
                            typeName);
                    return new HeapLongVector(batchSize);
                } else {
                    checkArgument(
                            (typeName == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY
                                            || typeName == PrimitiveType.PrimitiveTypeName.BINARY)
                                    && primitiveType.getOriginalType() == OriginalType.DECIMAL,
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
                GroupType mapRepeatedType = type.asGroupType().getType(0).asGroupType();
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
                GroupType multisetRepeatedType = type.asGroupType().getType(0).asGroupType();
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
}
