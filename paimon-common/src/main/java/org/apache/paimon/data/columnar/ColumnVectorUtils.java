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

import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalVector;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.columnar.heap.CastedArrayColumnVector;
import org.apache.paimon.data.columnar.heap.CastedMapColumnVector;
import org.apache.paimon.data.columnar.heap.CastedRowColumnVector;
import org.apache.paimon.data.columnar.heap.CastedVectorColumnVector;
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
import org.apache.paimon.data.columnar.heap.HeapVectorColumnVector;
import org.apache.paimon.data.columnar.writable.WritableBytesVector;
import org.apache.paimon.data.columnar.writable.WritableColumnVector;
import org.apache.paimon.data.columnar.writable.WritableIntVector;
import org.apache.paimon.data.columnar.writable.WritableLongVector;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VectorType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Utilities for creating and wrapping column vectors. */
public class ColumnVectorUtils {

    private ColumnVectorUtils() {}

    /** Create writable vectors for logical row-to-column materialization. */
    public static WritableColumnVector createWritableColumnVector(int capacity, DataType dataType) {
        return createWritableColumnVector(capacity, dataType, false);
    }

    /** Create writable vectors matching Parquet reader physical representations. */
    public static WritableColumnVector createParquetWritableColumnVector(
            int capacity, DataType dataType) {
        return createWritableColumnVector(capacity, dataType, true);
    }

    private static WritableColumnVector createWritableColumnVector(
            int capacity, DataType dataType, boolean parquetPhysical) {
        switch (dataType.getTypeRoot()) {
            case BOOLEAN:
                return new HeapBooleanVector(capacity);
            case TINYINT:
                return new HeapByteVector(capacity);
            case SMALLINT:
                return new HeapShortVector(capacity);
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return new HeapIntVector(capacity);
            case BIGINT:
                return new HeapLongVector(capacity);
            case FLOAT:
                return new HeapFloatVector(capacity);
            case DOUBLE:
                return new HeapDoubleVector(capacity);
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
            case BLOB:
                return new HeapBytesVector(capacity);
            case DECIMAL:
                return createDecimalVector(capacity, (DecimalType) dataType, parquetPhysical);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if (parquetPhysical && DataTypeChecks.getPrecision(dataType) <= 6) {
                    return new HeapLongVector(capacity);
                }
                return new HeapTimestampVector(capacity);
            case ARRAY:
                ArrayType arrayType = (ArrayType) dataType;
                return new HeapArrayVector(
                        capacity,
                        createWritableColumnVector(
                                capacity, arrayType.getElementType(), parquetPhysical));
            case VECTOR:
                VectorType vectorType = (VectorType) dataType;
                WritableColumnVector elementVector =
                        createWritableColumnVector(
                                capacity, vectorType.getElementType(), parquetPhysical);
                return parquetPhysical
                        ? new HeapArrayVector(capacity, elementVector)
                        : new HeapVectorColumnVector(
                                capacity, elementVector, vectorType.getLength());
            case MAP:
                MapType mapType = (MapType) dataType;
                return new HeapMapVector(
                        capacity,
                        createWritableColumnVector(capacity, mapType.getKeyType(), parquetPhysical),
                        createWritableColumnVector(
                                capacity, mapType.getValueType(), parquetPhysical));
            case MULTISET:
                MultisetType multisetType = (MultisetType) dataType;
                return new HeapMapVector(
                        capacity,
                        createWritableColumnVector(
                                capacity, multisetType.getElementType(), parquetPhysical),
                        createWritableColumnVector(capacity, new IntType(false), parquetPhysical));
            case ROW:
                RowType rowType = (RowType) dataType;
                WritableColumnVector[] fieldVectors =
                        new WritableColumnVector[rowType.getFieldCount()];
                for (int i = 0; i < fieldVectors.length; i++) {
                    fieldVectors[i] =
                            createWritableColumnVector(
                                    capacity, rowType.getTypeAt(i), parquetPhysical);
                }
                return new HeapRowVector(capacity, fieldVectors);
            case VARIANT:
                return new HeapRowVector(
                        capacity, new HeapBytesVector(capacity), new HeapBytesVector(capacity));
            default:
                throw new UnsupportedOperationException(dataType + " is not supported now.");
        }
    }

    private static WritableColumnVector createDecimalVector(
            int capacity, DecimalType decimalType, boolean parquetPhysical) {
        if (parquetPhysical) {
            if (is32BitDecimal(decimalType.getPrecision())) {
                return new HeapIntVector(capacity);
            } else if (is64BitDecimal(decimalType.getPrecision())) {
                return new HeapLongVector(capacity);
            }
            return new HeapBytesVector(capacity);
        }
        return Decimal.isCompact(decimalType.getPrecision())
                ? new HeapLongVector(capacity)
                : new HeapBytesVector(capacity);
    }

    private static boolean is32BitDecimal(int precision) {
        return precision <= 9;
    }

    private static boolean is64BitDecimal(int precision) {
        return precision <= 18 && precision > 9;
    }

    /** Create readable vectors from writable vectors. */
    public static ColumnVector[] createReadableColumnVectors(
            List<DataType> types, WritableColumnVector[] writableVectors) {
        ColumnVector[] vectors = new ColumnVector[writableVectors.length];
        for (int i = 0; i < writableVectors.length; i++) {
            vectors[i] = createReadableColumnVector(types.get(i), writableVectors[i]);
        }
        return vectors;
    }

    /** Create a readable vector from a writable vector. */
    public static ColumnVector createReadableColumnVector(
            DataType type, WritableColumnVector writableVector) {
        switch (type.getTypeRoot()) {
            case DECIMAL:
                return new ReadableDecimalColumnVector(writableVector);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return new ReadableTimestampColumnVector(writableVector);
            case BLOB:
                return writableVector;
            case ARRAY:
                return new CastedArrayColumnVector(
                        (HeapArrayVector) writableVector,
                        createReadableColumnVectors(
                                Collections.singletonList(((ArrayType) type).getElementType()),
                                writableChildren(writableVector)));
            case VECTOR:
                VectorType vectorType = (VectorType) type;
                ColumnVector child =
                        createReadableColumnVector(
                                vectorType.getElementType(),
                                (WritableColumnVector) writableVector.getChildren()[0]);
                if (writableVector instanceof HeapVectorColumnVector) {
                    return new ReadableVectorColumnVector(
                            (HeapVectorColumnVector) writableVector, child, vectorType.getLength());
                }
                return new CastedVectorColumnVector(
                        (HeapArrayVector) writableVector, child, vectorType.getLength());
            case MAP:
                MapType mapType = (MapType) type;
                return new CastedMapColumnVector(
                        (HeapMapVector) writableVector,
                        createReadableColumnVectors(
                                Arrays.asList(mapType.getKeyType(), mapType.getValueType()),
                                writableChildren(writableVector)));
            case MULTISET:
                MultisetType multisetType = (MultisetType) type;
                return new CastedMapColumnVector(
                        (HeapMapVector) writableVector,
                        createReadableColumnVectors(
                                Arrays.asList(multisetType.getElementType(), new IntType(false)),
                                writableChildren(writableVector)));
            case ROW:
                RowType rowType = (RowType) type;
                return new CastedRowColumnVector(
                        (HeapRowVector) writableVector,
                        createReadableColumnVectors(
                                rowType.getFieldTypes(), writableChildren(writableVector)));
            default:
                return writableVector;
        }
    }

    private static WritableColumnVector[] writableChildren(WritableColumnVector writableVector) {
        return Arrays.stream(writableVector.getChildren())
                .map(WritableColumnVector.class::cast)
                .toArray(WritableColumnVector[]::new);
    }

    private static class ReadableDecimalColumnVector implements DecimalColumnVector {

        private final WritableColumnVector vector;

        private ReadableDecimalColumnVector(WritableColumnVector vector) {
            this.vector = vector;
        }

        @Override
        public Decimal getDecimal(int i, int precision, int scale) {
            if (vector instanceof WritableIntVector) {
                return Decimal.fromUnscaledLong(
                        ((WritableIntVector) vector).getInt(i), precision, scale);
            } else if (vector instanceof WritableLongVector) {
                return Decimal.fromUnscaledLong(
                        ((WritableLongVector) vector).getLong(i), precision, scale);
            } else if (vector instanceof WritableBytesVector) {
                BytesColumnVector.Bytes bytes = ((WritableBytesVector) vector).getBytes(i);
                return Decimal.fromUnscaledBytes(bytes.getBytes(), precision, scale);
            }
            throw new UnsupportedOperationException("Unsupported decimal vector: " + vector);
        }

        @Override
        public boolean isNullAt(int i) {
            return vector.isNullAt(i);
        }

        @Override
        public int getCapacity() {
            return vector.getCapacity();
        }
    }

    private static class ReadableTimestampColumnVector implements TimestampColumnVector {

        private final ColumnVector vector;

        private ReadableTimestampColumnVector(ColumnVector vector) {
            this.vector = vector;
        }

        @Override
        public Timestamp getTimestamp(int i, int precision) {
            if (precision <= 3 && vector instanceof LongColumnVector) {
                return Timestamp.fromEpochMillis(((LongColumnVector) vector).getLong(i));
            } else if (precision <= 6 && vector instanceof LongColumnVector) {
                return Timestamp.fromMicros(((LongColumnVector) vector).getLong(i));
            }
            checkArgument(
                    vector instanceof TimestampColumnVector,
                    "Reading timestamp type occur unsupported vector type: %s",
                    vector.getClass());
            return ((TimestampColumnVector) vector).getTimestamp(i, precision);
        }

        @Override
        public boolean isNullAt(int i) {
            return vector.isNullAt(i);
        }

        @Override
        public int getCapacity() {
            return vector.getCapacity();
        }
    }

    private static class ReadableVectorColumnVector implements VecColumnVector {

        private final HeapVectorColumnVector vector;
        private final ColumnVector child;
        private final int vectorSize;

        private ReadableVectorColumnVector(
                HeapVectorColumnVector vector, ColumnVector child, int vectorSize) {
            this.vector = vector;
            this.child = child;
            this.vectorSize = vectorSize;
        }

        @Override
        public InternalVector getVector(int i) {
            long offset = vector.getOffsets()[i];
            long length = vector.getLengths()[i];
            if (length != vectorSize) {
                throw new IllegalArgumentException(
                        "Vector length mismatch: expected " + vectorSize + " but got " + length);
            }
            return ColumnarVec.DEFAULT_FACTORY.create(child, (int) offset, (int) length);
        }

        @Override
        public ColumnVector getColumnVector() {
            return child;
        }

        @Override
        public int getVectorSize() {
            return vectorSize;
        }

        @Override
        public boolean isNullAt(int i) {
            return vector.isNullAt(i);
        }

        @Override
        public int getCapacity() {
            return vector.getCapacity();
        }

        @Override
        public ColumnVector[] getChildren() {
            return new ColumnVector[] {child};
        }
    }
}
