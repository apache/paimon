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

package org.apache.paimon.data.columnar;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.DataGetters;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalVector;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.columnar.heap.HeapArrayVector;
import org.apache.paimon.data.columnar.heap.HeapMapVector;
import org.apache.paimon.data.columnar.heap.HeapRowVector;
import org.apache.paimon.data.columnar.heap.HeapVectorColumnVector;
import org.apache.paimon.data.columnar.writable.WritableBooleanVector;
import org.apache.paimon.data.columnar.writable.WritableByteVector;
import org.apache.paimon.data.columnar.writable.WritableBytesVector;
import org.apache.paimon.data.columnar.writable.WritableColumnVector;
import org.apache.paimon.data.columnar.writable.WritableDoubleVector;
import org.apache.paimon.data.columnar.writable.WritableFloatVector;
import org.apache.paimon.data.columnar.writable.WritableIntVector;
import org.apache.paimon.data.columnar.writable.WritableLongVector;
import org.apache.paimon.data.columnar.writable.WritableShortVector;
import org.apache.paimon.data.columnar.writable.WritableTimestampVector;
import org.apache.paimon.data.variant.GenericVariant;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BlobType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeVisitor;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.types.VariantType;
import org.apache.paimon.types.VectorType;

import java.io.Serializable;
import java.util.List;

/** Covert row based data to columnar data. */
public class RowToColumnConverter {

    private final TypeConverter[] converters;

    public RowToColumnConverter(RowType rowType) {
        List<DataType> fieldTypes = rowType.getFieldTypes();
        this.converters = new TypeConverter[fieldTypes.size()];
        for (int i = 0; i < fieldTypes.size(); i++) {
            converters[i] = TypeConverter.getConverterForType(fieldTypes.get(i));
        }
    }

    public void convert(InternalRow row, WritableColumnVector[] vectors) {
        for (int i = 0; i < row.getFieldCount(); i++) {
            converters[i].append(row, i, vectors[i]);
        }
    }

    /** Create a reusable element converter for materializing column vectors. */
    public static ElementConverter createElementConverter(DataType type) {
        return new ElementConverter(TypeConverter.getConverterForType(type));
    }

    /** Converts one element into a writable column vector. */
    public static class ElementConverter implements Serializable {

        private static final long serialVersionUID = 1L;

        private final TypeConverter converter;

        private ElementConverter(TypeConverter converter) {
            this.converter = converter;
        }

        public void append(DataGetters getters, int pos, WritableColumnVector vector) {
            converter.append(getters, pos, vector);
        }

        public void append(ColumnVector source, int rowId, WritableColumnVector vector) {
            converter.append(new ColumnVectorDataGetters(source, rowId), 0, vector);
        }

        public void append(Object value, WritableColumnVector vector) {
            converter.append(new ObjectDataGetters(value), 0, vector);
        }
    }

    private static class ColumnVectorDataGetters implements DataGetters {

        private final ColumnVector vector;
        private final int rowId;

        private ColumnVectorDataGetters(ColumnVector vector, int rowId) {
            this.vector = vector;
            this.rowId = rowId;
        }

        @Override
        public boolean isNullAt(int pos) {
            return vector.isNullAt(rowId);
        }

        @Override
        public boolean getBoolean(int pos) {
            return ((BooleanColumnVector) vector).getBoolean(rowId);
        }

        @Override
        public byte getByte(int pos) {
            return ((ByteColumnVector) vector).getByte(rowId);
        }

        @Override
        public short getShort(int pos) {
            return ((ShortColumnVector) vector).getShort(rowId);
        }

        @Override
        public int getInt(int pos) {
            return ((IntColumnVector) vector).getInt(rowId);
        }

        @Override
        public long getLong(int pos) {
            return ((LongColumnVector) vector).getLong(rowId);
        }

        @Override
        public float getFloat(int pos) {
            return ((FloatColumnVector) vector).getFloat(rowId);
        }

        @Override
        public double getDouble(int pos) {
            return ((DoubleColumnVector) vector).getDouble(rowId);
        }

        @Override
        public BinaryString getString(int pos) {
            BytesColumnVector.Bytes bytes = ((BytesColumnVector) vector).getBytes(rowId);
            return BinaryString.fromBytes(bytes.data, bytes.offset, bytes.len);
        }

        @Override
        public Decimal getDecimal(int pos, int precision, int scale) {
            return ((DecimalColumnVector) vector).getDecimal(rowId, precision, scale);
        }

        @Override
        public Timestamp getTimestamp(int pos, int precision) {
            return ((TimestampColumnVector) vector).getTimestamp(rowId, precision);
        }

        @Override
        public byte[] getBinary(int pos) {
            return ((BytesColumnVector) vector).getBytes(rowId).getBytes();
        }

        @Override
        public Variant getVariant(int pos) {
            InternalRow row = getRow(pos, 2);
            return new GenericVariant(row.getBinary(0), row.getBinary(1));
        }

        @Override
        public Blob getBlob(int pos) {
            throw new UnsupportedOperationException();
        }

        @Override
        public InternalArray getArray(int pos) {
            return ((ArrayColumnVector) vector).getArray(rowId);
        }

        @Override
        public InternalVector getVector(int pos) {
            return ((VecColumnVector) vector).getVector(rowId);
        }

        @Override
        public InternalMap getMap(int pos) {
            return ((MapColumnVector) vector).getMap(rowId);
        }

        @Override
        public InternalRow getRow(int pos, int numFields) {
            return ((RowColumnVector) vector).getRow(rowId);
        }
    }

    private static class ObjectDataGetters implements DataGetters {

        private final Object value;

        private ObjectDataGetters(Object value) {
            this.value = value;
        }

        @Override
        public boolean isNullAt(int pos) {
            return value == null;
        }

        @Override
        public boolean getBoolean(int pos) {
            return (Boolean) value;
        }

        @Override
        public byte getByte(int pos) {
            return (Byte) value;
        }

        @Override
        public short getShort(int pos) {
            return (Short) value;
        }

        @Override
        public int getInt(int pos) {
            return (Integer) value;
        }

        @Override
        public long getLong(int pos) {
            return (Long) value;
        }

        @Override
        public float getFloat(int pos) {
            return (Float) value;
        }

        @Override
        public double getDouble(int pos) {
            return (Double) value;
        }

        @Override
        public BinaryString getString(int pos) {
            return (BinaryString) value;
        }

        @Override
        public Decimal getDecimal(int pos, int precision, int scale) {
            return (Decimal) value;
        }

        @Override
        public Timestamp getTimestamp(int pos, int precision) {
            return (Timestamp) value;
        }

        @Override
        public byte[] getBinary(int pos) {
            return (byte[]) value;
        }

        @Override
        public Variant getVariant(int pos) {
            return (Variant) value;
        }

        @Override
        public Blob getBlob(int pos) {
            return (Blob) value;
        }

        @Override
        public InternalArray getArray(int pos) {
            return (InternalArray) value;
        }

        @Override
        public InternalVector getVector(int pos) {
            return (InternalVector) value;
        }

        @Override
        public InternalMap getMap(int pos) {
            return (InternalMap) value;
        }

        @Override
        public InternalRow getRow(int pos, int numFields) {
            return (InternalRow) value;
        }
    }

    private interface TypeConverter extends Serializable {

        void append(DataGetters row, int column, WritableColumnVector cv);

        static TypeConverter getConverterForType(DataType dataType) {
            return dataType.accept(TypeConverterVisitor.INSTANCE);
        }

        class TypeConverterVisitor implements DataTypeVisitor<TypeConverter> {

            static final TypeConverterVisitor INSTANCE = new TypeConverterVisitor();

            @FunctionalInterface
            interface ValueWriter {
                void write(DataGetters row, int column, WritableColumnVector cv);
            }

            @Override
            public TypeConverter visit(CharType charType) {
                return stringConverter(charType.isNullable());
            }

            @Override
            public TypeConverter visit(VarCharType varCharType) {
                return stringConverter(varCharType.isNullable());
            }

            @Override
            public TypeConverter visit(BooleanType booleanType) {
                return createConverter(
                        booleanType.isNullable(),
                        (row, column, cv) ->
                                ((WritableBooleanVector) cv).appendBoolean(row.getBoolean(column)));
            }

            @Override
            public TypeConverter visit(BinaryType binaryType) {
                return binaryConverter(binaryType.isNullable());
            }

            @Override
            public TypeConverter visit(VarBinaryType varBinaryType) {
                return binaryConverter(varBinaryType.isNullable());
            }

            @Override
            public TypeConverter visit(DecimalType decimalType) {
                return createConverter(
                        decimalType.isNullable(),
                        (row, column, cv) -> {
                            Decimal decimal =
                                    row.getDecimal(
                                            column,
                                            decimalType.getPrecision(),
                                            decimalType.getScale());
                            if (cv instanceof WritableIntVector) {
                                ((WritableIntVector) cv).appendInt((int) decimal.toUnscaledLong());
                            } else if (cv instanceof WritableLongVector) {
                                ((WritableLongVector) cv).appendLong(decimal.toUnscaledLong());
                            } else if (cv instanceof WritableBytesVector) {
                                byte[] bytes = decimal.toUnscaledBytes();
                                ((WritableBytesVector) cv).appendByteArray(bytes, 0, bytes.length);
                            } else {
                                throw new UnsupportedOperationException(
                                        "Unsupported column vector: " + cv);
                            }
                        });
            }

            @Override
            public TypeConverter visit(TinyIntType tinyIntType) {
                return createConverter(
                        tinyIntType.isNullable(),
                        (row, column, cv) ->
                                ((WritableByteVector) cv).appendByte(row.getByte(column)));
            }

            @Override
            public TypeConverter visit(SmallIntType smallIntType) {
                return createConverter(
                        smallIntType.isNullable(),
                        (row, column, cv) ->
                                ((WritableShortVector) cv).appendShort(row.getShort(column)));
            }

            @Override
            public TypeConverter visit(IntType intType) {
                return createConverter(
                        intType.isNullable(),
                        (row, column, cv) ->
                                ((WritableIntVector) cv).appendInt(row.getInt(column)));
            }

            @Override
            public TypeConverter visit(BigIntType bigIntType) {
                return createConverter(
                        bigIntType.isNullable(),
                        (row, column, cv) ->
                                ((WritableLongVector) cv).appendLong(row.getLong(column)));
            }

            @Override
            public TypeConverter visit(FloatType floatType) {
                return createConverter(
                        floatType.isNullable(),
                        (row, column, cv) ->
                                ((WritableFloatVector) cv).appendFloat(row.getFloat(column)));
            }

            @Override
            public TypeConverter visit(DoubleType doubleType) {
                return createConverter(
                        doubleType.isNullable(),
                        (row, column, cv) ->
                                ((WritableDoubleVector) cv).appendDouble(row.getDouble(column)));
            }

            @Override
            public TypeConverter visit(DateType dateType) {
                return createConverter(
                        dateType.isNullable(),
                        (row, column, cv) ->
                                ((WritableIntVector) cv).appendInt(row.getInt(column)));
            }

            @Override
            public TypeConverter visit(TimeType timeType) {
                return createConverter(
                        timeType.isNullable(),
                        (row, column, cv) ->
                                ((WritableIntVector) cv).appendInt(row.getInt(column)));
            }

            @Override
            public TypeConverter visit(TimestampType timestampType) {
                return timestampConverter(timestampType.isNullable(), timestampType.getPrecision());
            }

            @Override
            public TypeConverter visit(LocalZonedTimestampType localZonedTimestampType) {
                return timestampConverter(
                        localZonedTimestampType.isNullable(),
                        localZonedTimestampType.getPrecision());
            }

            @Override
            public TypeConverter visit(VariantType variantType) {
                return createConverter(
                        variantType.isNullable(),
                        (row, column, cv) -> {
                            ((HeapRowVector) cv).appendRow();
                            WritableBytesVector valueVector =
                                    (WritableBytesVector) cv.getChildren()[0];
                            WritableBytesVector metaDataVector =
                                    (WritableBytesVector) cv.getChildren()[1];

                            Variant variant = row.getVariant(column);
                            byte[] value = variant.value();
                            byte[] metadata = variant.metadata();
                            valueVector.appendByteArray(value, 0, value.length);
                            metaDataVector.appendByteArray(metadata, 0, metadata.length);
                        });
            }

            @Override
            public TypeConverter visit(BlobType blobType) {
                throw new UnsupportedOperationException();
            }

            @Override
            public TypeConverter visit(ArrayType arrayType) {
                return createConverter(
                        arrayType.isNullable(),
                        (row, column, cv) -> {
                            HeapArrayVector arrayVector = (HeapArrayVector) cv;
                            InternalArray values = row.getArray(column);
                            int numElements = values.size();
                            arrayVector.appendArray(numElements);

                            WritableColumnVector arrData =
                                    (WritableColumnVector) arrayVector.getColumnVector();
                            TypeConverter elementConverter =
                                    getConverterForType(arrayType.getElementType());
                            for (int i = 0; i < numElements; i++) {
                                elementConverter.append(values, i, arrData);
                            }
                        });
            }

            @Override
            public TypeConverter visit(VectorType vectorType) {
                TypeConverter elementConverter =
                        getConverterForType(vectorType.getElementType().notNull());
                return createConverter(
                        vectorType.isNullable(),
                        (row, column, cv) -> {
                            HeapVectorColumnVector vectorColumn = (HeapVectorColumnVector) cv;
                            if (vectorColumn.getVectorSize() != vectorType.getLength()) {
                                throw new IllegalArgumentException(
                                        "Vector column length mismatch: expected "
                                                + vectorType.getLength()
                                                + " but got "
                                                + vectorColumn.getVectorSize());
                            }

                            InternalVector values = row.getVector(column);
                            checkVectorLength(values, vectorType.getLength());
                            checkVectorElementsNonNull(values);
                            vectorColumn.appendVector();

                            WritableColumnVector vectorData =
                                    (WritableColumnVector) vectorColumn.getColumnVector();
                            for (int i = 0; i < values.size(); i++) {
                                elementConverter.append(values, i, vectorData);
                            }
                        });
            }

            @Override
            public TypeConverter visit(MultisetType multisetType) {
                throw new UnsupportedOperationException();
            }

            @Override
            public TypeConverter visit(MapType mapType) {
                return createConverter(
                        mapType.isNullable(),
                        (row, column, cv) -> {
                            HeapMapVector mapVector = (HeapMapVector) cv;
                            InternalMap m = row.getMap(column);
                            WritableColumnVector keys = (WritableColumnVector) mapVector.getKeys();
                            WritableColumnVector values =
                                    (WritableColumnVector) mapVector.getValues();
                            int numElements = m.size();
                            mapVector.appendArray(numElements);

                            InternalArray srcKeys = m.keyArray();
                            InternalArray srcValues = m.valueArray();
                            TypeConverter keyConverter = getConverterForType(mapType.getKeyType());
                            TypeConverter valueConverter =
                                    getConverterForType(mapType.getValueType());
                            for (int i = 0; i < numElements; i++) {
                                keyConverter.append(srcKeys, i, keys);
                                valueConverter.append(srcValues, i, values);
                            }
                        });
            }

            @Override
            public TypeConverter visit(RowType rowType) {
                return createConverter(
                        rowType.isNullable(),
                        (row, column, cv) -> {
                            HeapRowVector rowVector = (HeapRowVector) cv;
                            rowVector.appendRow();
                            InternalRow data = row.getRow(column, rowType.getFieldCount());
                            ColumnVector[] children = cv.getChildren();
                            for (int i = 0; i < rowType.getFieldCount(); i++) {
                                TypeConverter fieldConverter =
                                        getConverterForType(rowType.getTypeAt(i));
                                fieldConverter.append(data, i, (WritableColumnVector) children[i]);
                            }
                        });
            }

            private static TypeConverter createConverter(boolean nullable, ValueWriter writer) {
                if (nullable) {
                    return (row, column, cv) -> {
                        if (row.isNullAt(column)) {
                            cv.appendNull();
                        } else {
                            writer.write(row, column, cv);
                        }
                    };
                } else {
                    return writer::write;
                }
            }

            private static TypeConverter binaryConverter(boolean nullable) {
                return createConverter(
                        nullable,
                        (row, column, cv) -> {
                            byte[] bytes = row.getBinary(column);
                            ((WritableBytesVector) cv).appendByteArray(bytes, 0, bytes.length);
                        });
            }

            private static TypeConverter stringConverter(boolean nullable) {
                return createConverter(
                        nullable,
                        (row, column, cv) -> {
                            byte[] bytes = row.getString(column).toBytes();
                            ((WritableBytesVector) cv).appendByteArray(bytes, 0, bytes.length);
                        });
            }

            private static TypeConverter timestampConverter(boolean nullable, int precision) {
                return createConverter(
                        nullable,
                        (row, column, cv) -> {
                            Timestamp timestamp = row.getTimestamp(column, precision);
                            if (cv instanceof WritableTimestampVector) {
                                ((WritableTimestampVector) cv).appendTimestamp(timestamp);
                            } else if (cv instanceof WritableLongVector && precision <= 3) {
                                ((WritableLongVector) cv).appendLong(timestamp.getMillisecond());
                            } else if (cv instanceof WritableLongVector && precision <= 6) {
                                ((WritableLongVector) cv).appendLong(timestamp.toMicros());
                            } else {
                                throw new UnsupportedOperationException(
                                        "Unsupported column vector: " + cv);
                            }
                        });
            }

            private static void checkVectorLength(InternalVector vector, int expectedLength) {
                if (vector.size() != expectedLength) {
                    throw new IllegalArgumentException(
                            "Vector length mismatch: expected "
                                    + expectedLength
                                    + " but got "
                                    + vector.size());
                }
            }

            private static void checkVectorElementsNonNull(InternalVector vector) {
                for (int i = 0; i < vector.size(); i++) {
                    if (vector.isNullAt(i)) {
                        throw new UnsupportedOperationException(
                                "Vector elements must not be null.");
                    }
                }
            }
        }
    }
}
