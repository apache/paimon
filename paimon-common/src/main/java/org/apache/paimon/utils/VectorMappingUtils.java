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

package org.apache.paimon.utils;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.PartitionInfo;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.columnar.ArrayColumnVector;
import org.apache.paimon.data.columnar.BooleanColumnVector;
import org.apache.paimon.data.columnar.ByteColumnVector;
import org.apache.paimon.data.columnar.BytesColumnVector;
import org.apache.paimon.data.columnar.ColumnVector;
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
import org.apache.paimon.types.VariantType;

/**
 * This is a util about how to expand the {@link ColumnVector}s with the partition row and index
 * mapping. For example, we fetch the column of a, b, c from orc file, but the schema of table is a,
 * a1, a2, pt, b, a, b1, c Then we use `createPartitionMappedVectors` to fill to partition pt to a,
 * b, c, the filled columns are a, pt, b, c After this, we use index mapping to expand to a, pt, b,
 * c to a, a1(null), a2(null), pt, b, a, b1(null), c In this way, we don't need ProjectedRow to
 * perform a per-record replace action.
 */
public class VectorMappingUtils {

    public static ColumnVector[] createPartitionMappedVectors(
            PartitionInfo partitionInfo, ColumnVector[] vectors) {
        int length = partitionInfo.size();
        ColumnVector[] newVectors = new ColumnVector[length];
        for (int i = 0; i < length; i++) {
            if (partitionInfo.inPartitionRow(i)) {
                newVectors[i] =
                        createFixedVector(
                                partitionInfo.getType(i),
                                partitionInfo.getPartitionRow(),
                                partitionInfo.getRealIndex(i));
            } else {
                newVectors[i] = vectors[partitionInfo.getRealIndex(i)];
            }
        }
        return newVectors;
    }

    public static ColumnVector createFixedVector(
            DataType dataType, BinaryRow partition, int index) {
        Visitor visitor = new Visitor(partition, index);
        return dataType.accept(visitor);
    }

    public static ColumnVector[] createMappedVectors(int[] indexMapping, ColumnVector[] vectors) {
        ColumnVector[] newVectors = new ColumnVector[indexMapping.length];
        for (int i = 0; i < indexMapping.length; i++) {
            int realIndex = indexMapping[i];
            if (realIndex >= 0) {
                newVectors[i] = vectors[indexMapping[i]];
            } else {
                newVectors[i] = index -> true;
            }
        }
        return newVectors;
    }

    private static class Visitor implements DataTypeVisitor<ColumnVector> {

        private final BinaryRow partition;
        private final int index;

        public Visitor(BinaryRow partition, int i) {
            this.partition = partition;
            this.index = i;
        }

        @Override
        public ColumnVector visit(CharType charType) {
            return bytesColumnVector();
        }

        @Override
        public ColumnVector visit(VarCharType varCharType) {
            return bytesColumnVector();
        }

        @Override
        public ColumnVector visit(BooleanType booleanType) {
            final boolean b = partition.getBoolean(index);
            return new BooleanColumnVector() {
                @Override
                public boolean getBoolean(int i) {
                    return b;
                }

                @Override
                public boolean isNullAt(int i) {
                    return partition.isNullAt(index);
                }
            };
        }

        @Override
        public ColumnVector visit(BinaryType binaryType) {
            return bytesColumnVector();
        }

        @Override
        public ColumnVector visit(VarBinaryType varBinaryType) {
            return bytesColumnVector();
        }

        @Override
        public ColumnVector visit(DecimalType decimalType) {
            return new DecimalColumnVector() {
                @Override
                public Decimal getDecimal(int i, int precision, int scale) {
                    return partition.getDecimal(index, precision, scale);
                }

                @Override
                public boolean isNullAt(int i) {
                    return partition.isNullAt(index);
                }
            };
        }

        @Override
        public ColumnVector visit(TinyIntType tinyIntType) {
            return new ByteColumnVector() {
                @Override
                public byte getByte(int i) {
                    return partition.getByte(index);
                }

                @Override
                public boolean isNullAt(int i) {
                    return partition.isNullAt(index);
                }
            };
        }

        @Override
        public ColumnVector visit(SmallIntType smallIntType) {
            return new ShortColumnVector() {
                @Override
                public short getShort(int i) {
                    return partition.getShort(index);
                }

                @Override
                public boolean isNullAt(int i) {
                    return partition.isNullAt(index);
                }
            };
        }

        @Override
        public ColumnVector visit(IntType intType) {
            return new IntColumnVector() {
                @Override
                public int getInt(int i) {
                    return partition.getInt(index);
                }

                @Override
                public boolean isNullAt(int i) {
                    return partition.isNullAt(index);
                }
            };
        }

        @Override
        public ColumnVector visit(BigIntType bigIntType) {
            return new LongColumnVector() {
                @Override
                public long getLong(int i) {
                    return partition.getLong(index);
                }

                @Override
                public boolean isNullAt(int i) {
                    return partition.isNullAt(index);
                }
            };
        }

        @Override
        public ColumnVector visit(FloatType floatType) {
            return new FloatColumnVector() {
                @Override
                public float getFloat(int i) {
                    return partition.getFloat(index);
                }

                @Override
                public boolean isNullAt(int i) {
                    return partition.isNullAt(index);
                }
            };
        }

        @Override
        public ColumnVector visit(DoubleType doubleType) {
            return new DoubleColumnVector() {
                @Override
                public double getDouble(int i) {
                    return partition.getDouble(index);
                }

                @Override
                public boolean isNullAt(int i) {
                    return partition.isNullAt(index);
                }
            };
        }

        @Override
        public ColumnVector visit(DateType dateType) {
            return new IntColumnVector() {
                @Override
                public int getInt(int i) {
                    return partition.getInt(index);
                }

                @Override
                public boolean isNullAt(int i) {
                    return partition.isNullAt(index);
                }
            };
        }

        @Override
        public ColumnVector visit(TimeType timeType) {
            return new IntColumnVector() {
                @Override
                public int getInt(int i) {
                    return partition.getInt(index);
                }

                @Override
                public boolean isNullAt(int i) {
                    return partition.isNullAt(index);
                }
            };
        }

        @Override
        public ColumnVector visit(TimestampType timestampType) {
            return new TimestampColumnVector() {
                @Override
                public Timestamp getTimestamp(int i, int precision) {
                    return partition.getTimestamp(index, precision);
                }

                @Override
                public boolean isNullAt(int i) {
                    return partition.isNullAt(index);
                }
            };
        }

        @Override
        public ColumnVector visit(LocalZonedTimestampType localZonedTimestampType) {
            return new TimestampColumnVector() {
                @Override
                public Timestamp getTimestamp(int i, int precision) {
                    return partition.getTimestamp(index, precision);
                }

                @Override
                public boolean isNullAt(int i) {
                    return partition.isNullAt(index);
                }
            };
        }

        @Override
        public ColumnVector visit(VariantType variantType) {
            throw new UnsupportedOperationException("VariantType is not supported.");
        }

        @Override
        public ColumnVector visit(ArrayType arrayType) {
            return new ArrayColumnVector() {
                @Override
                public InternalArray getArray(int i) {
                    return partition.getArray(index);
                }

                @Override
                public boolean isNullAt(int i) {
                    return partition.isNullAt(index);
                }

                @Override
                public ColumnVector getColumnVector() {
                    throw new UnsupportedOperationException(
                            "Doesn't support getting ColumnVector.");
                }
            };
        }

        @Override
        public ColumnVector visit(MultisetType multisetType) {
            return new MapColumnVector() {
                @Override
                public InternalMap getMap(int i) {
                    return partition.getMap(index);
                }

                @Override
                public boolean isNullAt(int i) {
                    return partition.isNullAt(index);
                }

                @Override
                public ColumnVector getKeyColumnVector() {
                    throw new UnsupportedOperationException(
                            "Doesn't support getting key ColumnVector.");
                }

                @Override
                public ColumnVector getValueColumnVector() {
                    throw new UnsupportedOperationException(
                            "Doesn't support getting value ColumnVector.");
                }
            };
        }

        @Override
        public ColumnVector visit(MapType mapType) {
            return new MapColumnVector() {
                @Override
                public InternalMap getMap(int i) {
                    return partition.getMap(index);
                }

                @Override
                public boolean isNullAt(int i) {
                    return partition.isNullAt(index);
                }

                @Override
                public ColumnVector getKeyColumnVector() {
                    throw new UnsupportedOperationException(
                            "Doesn't support getting key ColumnVector.");
                }

                @Override
                public ColumnVector getValueColumnVector() {
                    throw new UnsupportedOperationException(
                            "Doesn't support getting value ColumnVector.");
                }
            };
        }

        @Override
        public ColumnVector visit(RowType rowType) {
            return new RowColumnVector() {
                @Override
                public InternalRow getRow(int i) {
                    return partition.getRow(index, rowType.getFieldCount());
                }

                @Override
                public boolean isNullAt(int i) {
                    return partition.isNullAt(index);
                }

                @Override
                public VectorizedColumnBatch getBatch() {
                    throw new UnsupportedOperationException("Doesn't support getting batch.");
                }
            };
        }

        private BytesColumnVector bytesColumnVector() {
            final byte[] bytes = partition.getString(index).toBytes();
            final BytesColumnVector.Bytes b = new BytesColumnVector.Bytes(bytes, 0, bytes.length);

            return new BytesColumnVector() {
                @Override
                public Bytes getBytes(int i) {
                    return b;
                }

                @Override
                public boolean isNullAt(int i) {
                    return partition.isNullAt(index);
                }
            };
        }
    }
}
