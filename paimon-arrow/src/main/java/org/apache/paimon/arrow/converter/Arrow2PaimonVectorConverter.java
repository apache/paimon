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

package org.apache.paimon.arrow.converter;

import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.columnar.ArrayColumnVector;
import org.apache.paimon.data.columnar.BooleanColumnVector;
import org.apache.paimon.data.columnar.ByteColumnVector;
import org.apache.paimon.data.columnar.BytesColumnVector;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.ColumnarArray;
import org.apache.paimon.data.columnar.ColumnarMap;
import org.apache.paimon.data.columnar.ColumnarRow;
import org.apache.paimon.data.columnar.DecimalColumnVector;
import org.apache.paimon.data.columnar.DoubleColumnVector;
import org.apache.paimon.data.columnar.FloatColumnVector;
import org.apache.paimon.data.columnar.IntColumnVector;
import org.apache.paimon.data.columnar.LongColumnVector;
import org.apache.paimon.data.columnar.MapColumnVector;
import org.apache.paimon.data.columnar.RowColumnVector;
import org.apache.paimon.data.columnar.ShortColumnVector;
import org.apache.paimon.data.columnar.TimestampColumnVector;
import org.apache.paimon.data.columnar.VectorizedColumnBatch;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
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

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;

import java.util.ArrayList;
import java.util.List;

/** Convert a {@link FieldVector} to {@link ColumnVector}. */
public interface Arrow2PaimonVectorConverter {

    static Arrow2PaimonVectorConverter construct(DataType type) {
        return type.accept(Arrow2PaimonVectorConvertorVisitor.INSTANCE);
    }

    ColumnVector convertVector(FieldVector vector);

    /** Visitor to create convertor from arrow to paimon. */
    class Arrow2PaimonVectorConvertorVisitor
            implements DataTypeVisitor<Arrow2PaimonVectorConverter> {

        private static final Arrow2PaimonVectorConvertorVisitor INSTANCE =
                new Arrow2PaimonVectorConvertorVisitor();

        @Override
        public Arrow2PaimonVectorConverter visit(CharType charType) {
            return vector ->
                    new BytesColumnVector() {

                        @Override
                        public boolean isNullAt(int index) {
                            return vector.isNull(index);
                        }

                        @Override
                        public Bytes getBytes(int index) {
                            byte[] bytes = ((VarCharVector) vector).get(index);
                            return new Bytes(bytes, 0, bytes.length) {
                                @Override
                                public byte[] getBytes() {
                                    return bytes;
                                }
                            };
                        }
                    };
        }

        @Override
        public Arrow2PaimonVectorConverter visit(VarCharType varCharType) {
            return vector ->
                    new BytesColumnVector() {

                        @Override
                        public boolean isNullAt(int index) {
                            return vector.isNull(index);
                        }

                        @Override
                        public Bytes getBytes(int index) {
                            byte[] bytes = ((VarCharVector) vector).get(index);
                            return new Bytes(bytes, 0, bytes.length) {
                                @Override
                                public byte[] getBytes() {
                                    return bytes;
                                }
                            };
                        }
                    };
        }

        @Override
        public Arrow2PaimonVectorConverter visit(BooleanType booleanType) {
            return vector ->
                    new BooleanColumnVector() {
                        @Override
                        public boolean isNullAt(int index) {
                            return vector.isNull(index);
                        }

                        @Override
                        public boolean getBoolean(int index) {
                            return ((BitVector) vector).getObject(index);
                        }
                    };
        }

        @Override
        public Arrow2PaimonVectorConverter visit(BinaryType binaryType) {
            return vector ->
                    new BytesColumnVector() {

                        @Override
                        public boolean isNullAt(int index) {
                            return vector.isNull(index);
                        }

                        @Override
                        public Bytes getBytes(int index) {
                            byte[] bytes = ((VarBinaryVector) vector).getObject(index);
                            return new Bytes(bytes, 0, bytes.length) {
                                @Override
                                public byte[] getBytes() {
                                    return bytes;
                                }
                            };
                        }
                    };
        }

        @Override
        public Arrow2PaimonVectorConverter visit(VarBinaryType varBinaryType) {
            return vector ->
                    new BytesColumnVector() {

                        @Override
                        public boolean isNullAt(int index) {
                            return vector.isNull(index);
                        }

                        @Override
                        public Bytes getBytes(int index) {
                            byte[] bytes = ((VarBinaryVector) vector).getObject(index);
                            return new Bytes(bytes, 0, bytes.length) {
                                @Override
                                public byte[] getBytes() {
                                    return bytes;
                                }
                            };
                        }
                    };
        }

        @Override
        public Arrow2PaimonVectorConverter visit(DecimalType decimalType) {
            return vector ->
                    new DecimalColumnVector() {

                        @Override
                        public boolean isNullAt(int index) {
                            return vector.isNull(index);
                        }

                        @Override
                        public Decimal getDecimal(int index, int precision, int scale) {
                            return Decimal.fromBigDecimal(
                                    ((DecimalVector) vector).getObject(index), precision, scale);
                        }
                    };
        }

        @Override
        public Arrow2PaimonVectorConverter visit(TinyIntType tinyIntType) {
            return vector ->
                    new ByteColumnVector() {

                        @Override
                        public boolean isNullAt(int index) {
                            return vector.isNull(index);
                        }

                        @Override
                        public byte getByte(int index) {
                            return ((TinyIntVector) vector).getObject(index);
                        }
                    };
        }

        @Override
        public Arrow2PaimonVectorConverter visit(SmallIntType smallIntType) {
            return vector ->
                    new ShortColumnVector() {

                        @Override
                        public boolean isNullAt(int index) {
                            return vector.isNull(index);
                        }

                        @Override
                        public short getShort(int index) {
                            return ((SmallIntVector) vector).getObject(index);
                        }
                    };
        }

        @Override
        public Arrow2PaimonVectorConverter visit(IntType intType) {
            return vector ->
                    new IntColumnVector() {

                        @Override
                        public boolean isNullAt(int index) {
                            return vector.isNull(index);
                        }

                        @Override
                        public int getInt(int index) {
                            return ((IntVector) vector).getObject(index);
                        }
                    };
        }

        @Override
        public Arrow2PaimonVectorConverter visit(BigIntType bigIntType) {
            return vector ->
                    new LongColumnVector() {

                        @Override
                        public boolean isNullAt(int index) {
                            return vector.isNull(index);
                        }

                        @Override
                        public long getLong(int index) {
                            return ((BigIntVector) vector).getObject(index);
                        }
                    };
        }

        @Override
        public Arrow2PaimonVectorConverter visit(FloatType floatType) {
            return vector ->
                    new FloatColumnVector() {

                        @Override
                        public boolean isNullAt(int index) {
                            return vector.isNull(index);
                        }

                        @Override
                        public float getFloat(int index) {
                            return ((Float4Vector) vector).getObject(index);
                        }
                    };
        }

        @Override
        public Arrow2PaimonVectorConverter visit(DoubleType doubleType) {
            return vector ->
                    new DoubleColumnVector() {

                        @Override
                        public boolean isNullAt(int index) {
                            return vector.isNull(index);
                        }

                        @Override
                        public double getDouble(int index) {
                            return ((Float8Vector) vector).getObject(index);
                        }
                    };
        }

        @Override
        public Arrow2PaimonVectorConverter visit(DateType dateType) {
            return vector ->
                    new IntColumnVector() {

                        @Override
                        public boolean isNullAt(int index) {
                            return vector.isNull(index);
                        }

                        @Override
                        public int getInt(int index) {
                            return ((DateDayVector) vector).getObject(index);
                        }
                    };
        }

        @Override
        public Arrow2PaimonVectorConverter visit(TimeType timeType) {
            return vector ->
                    new IntColumnVector() {

                        @Override
                        public boolean isNullAt(int index) {
                            return vector.isNull(index);
                        }

                        @Override
                        public int getInt(int index) {
                            return ((TimeMilliVector) vector).get(index);
                        }
                    };
        }

        @Override
        public Arrow2PaimonVectorConverter visit(TimestampType timestampType) {
            return vector ->
                    new TimestampColumnVector() {

                        @Override
                        public boolean isNullAt(int index) {
                            return vector.isNull(index);
                        }

                        @Override
                        public Timestamp getTimestamp(int i, int precision) {
                            long value = ((TimeStampVector) vector).get(i);
                            if (precision == 0) {
                                return Timestamp.fromEpochMillis(value * 1000);
                            } else if (precision >= 1 && precision <= 3) {
                                return Timestamp.fromEpochMillis(value);
                            } else if (precision >= 4 && precision <= 6) {
                                return Timestamp.fromMicros(value);
                            } else {
                                return Timestamp.fromEpochMillis(
                                        value / 1_000_000, (int) value % 1_000_000);
                            }
                        }
                    };
        }

        @Override
        public Arrow2PaimonVectorConverter visit(LocalZonedTimestampType localZonedTimestampType) {
            return vector ->
                    new TimestampColumnVector() {

                        @Override
                        public boolean isNullAt(int index) {
                            return vector.isNull(index);
                        }

                        @Override
                        public Timestamp getTimestamp(int i, int precision) {
                            long value = (long) vector.getObject(i);
                            if (precision == 0) {
                                return Timestamp.fromEpochMillis(value * 1000);
                            } else if (precision >= 1 && precision <= 3) {
                                return Timestamp.fromEpochMillis(value);
                            } else if (precision >= 4 && precision <= 6) {
                                return Timestamp.fromMicros(value);
                            } else {
                                return Timestamp.fromEpochMillis(
                                        value / 1_000_000, (int) value % 1_000_000);
                            }
                        }
                    };
        }

        @Override
        public Arrow2PaimonVectorConverter visit(ArrayType arrayType) {
            final Arrow2PaimonVectorConverter arrowVectorConvertor =
                    arrayType.getElementType().accept(this);

            return vector ->
                    new ArrayColumnVector() {

                        private boolean inited = false;
                        private ColumnVector columnVector;

                        private void init() {
                            if (!inited) {
                                FieldVector child = ((ListVector) vector).getDataVector();
                                this.columnVector = arrowVectorConvertor.convertVector(child);
                                inited = true;
                            }
                        }

                        @Override
                        public boolean isNullAt(int index) {
                            return vector.isNull(index);
                        }

                        @Override
                        public InternalArray getArray(int index) {
                            init();
                            ListVector listVector = (ListVector) vector;
                            int start = listVector.getElementStartIndex(index);
                            int end = listVector.getElementEndIndex(index);
                            return new ColumnarArray(columnVector, start, end - start);
                        }

                        @Override
                        public ColumnVector getColumnVector() {
                            init();
                            return columnVector;
                        }
                    };
        }

        @Override
        public Arrow2PaimonVectorConverter visit(MultisetType multisetType) {
            throw new UnsupportedOperationException("Doesn't support MultisetType.");
        }

        @Override
        public Arrow2PaimonVectorConverter visit(MapType mapType) {
            final Arrow2PaimonVectorConverter keyConvertor = mapType.getKeyType().accept(this);
            final Arrow2PaimonVectorConverter valueConverter = mapType.getValueType().accept(this);

            return vector ->
                    new MapColumnVector() {

                        private boolean inited = false;
                        private ListVector mapVector;
                        private ColumnVector keyColumnVector;
                        private ColumnVector valueColumnVector;

                        private void init() {
                            if (!inited) {
                                this.mapVector = (ListVector) vector;
                                StructVector listVector = (StructVector) mapVector.getDataVector();

                                FieldVector keyVector = listVector.getChildrenFromFields().get(0);
                                FieldVector valueVector = listVector.getChildrenFromFields().get(1);

                                this.keyColumnVector = keyConvertor.convertVector(keyVector);
                                this.valueColumnVector = valueConverter.convertVector(valueVector);
                                inited = true;
                            }
                        }

                        @Override
                        public boolean isNullAt(int index) {
                            return vector.isNull(index);
                        }

                        @Override
                        public InternalMap getMap(int index) {
                            init();

                            int start = mapVector.getElementStartIndex(index);
                            int end = mapVector.getElementEndIndex(index);

                            return new ColumnarMap(
                                    keyColumnVector, valueColumnVector, start, end - start);
                        }

                        @Override
                        public ColumnVector getKeyColumnVector() {
                            init();
                            return keyColumnVector;
                        }

                        @Override
                        public ColumnVector getValueColumnVector() {
                            init();
                            return valueColumnVector;
                        }
                    };
        }

        @Override
        public Arrow2PaimonVectorConverter visit(RowType rowType) {
            final List<Arrow2PaimonVectorConverter> convertors = new ArrayList<>();
            for (int i = 0; i < rowType.getFields().size(); i++) {
                convertors.add(rowType.getTypeAt(i).accept(this));
            }

            return vector ->
                    new RowColumnVector() {

                        private boolean inited = false;
                        private VectorizedColumnBatch vectorizedColumnBatch;

                        private void init() {
                            if (!inited) {
                                List<FieldVector> children =
                                        ((StructVector) vector).getChildrenFromFields();
                                ColumnVector[] vectors = new ColumnVector[children.size()];
                                for (int i = 0; i < children.size(); i++) {
                                    vectors[i] = convertors.get(i).convertVector(children.get(i));
                                }
                                this.vectorizedColumnBatch = new VectorizedColumnBatch(vectors);
                                inited = true;
                            }
                        }

                        @Override
                        public boolean isNullAt(int index) {
                            return vector.isNull(index);
                        }

                        @Override
                        public InternalRow getRow(int index) {
                            init();
                            return new ColumnarRow(vectorizedColumnBatch, index);
                        }

                        @Override
                        public VectorizedColumnBatch getBatch() {
                            init();
                            return vectorizedColumnBatch;
                        }
                    };
        }
    }
}
