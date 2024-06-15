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

package org.apache.paimon.types;

import org.apache.paimon.data.DataGetters;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;

import java.util.List;
import java.util.function.BiFunction;

/** The class is to calculate the occupied space size based on Datatype. */
public class InternalRowToSizeVisitor
        implements DataTypeVisitor<BiFunction<DataGetters, Integer, Integer>> {

    public static final int NULL_SIZE = 0;

    @Override
    public BiFunction<DataGetters, Integer, Integer> visit(CharType charType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_SIZE;
            } else {
                return row.getString(index).toBytes().length;
            }
        };
    }

    @Override
    public BiFunction<DataGetters, Integer, Integer> visit(VarCharType varCharType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_SIZE;
            } else {
                return row.getString(index).toBytes().length;
            }
        };
    }

    @Override
    public BiFunction<DataGetters, Integer, Integer> visit(BooleanType booleanType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_SIZE;
            } else {
                return 1;
            }
        };
    }

    @Override
    public BiFunction<DataGetters, Integer, Integer> visit(BinaryType binaryType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_SIZE;
            } else {
                return row.getBinary(index).length;
            }
        };
    }

    @Override
    public BiFunction<DataGetters, Integer, Integer> visit(VarBinaryType varBinaryType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_SIZE;
            } else {
                return row.getBinary(index).length;
            }
        };
    }

    @Override
    public BiFunction<DataGetters, Integer, Integer> visit(DecimalType decimalType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_SIZE;
            } else {
                return row.getDecimal(index, decimalType.getPrecision(), decimalType.getScale())
                        .toUnscaledBytes()
                        .length;
            }
        };
    }

    @Override
    public BiFunction<DataGetters, Integer, Integer> visit(TinyIntType tinyIntType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_SIZE;
            } else {
                return 1;
            }
        };
    }

    @Override
    public BiFunction<DataGetters, Integer, Integer> visit(SmallIntType smallIntType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_SIZE;
            } else {
                return 2;
            }
        };
    }

    @Override
    public BiFunction<DataGetters, Integer, Integer> visit(IntType intType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_SIZE;
            } else {
                return 4;
            }
        };
    }

    @Override
    public BiFunction<DataGetters, Integer, Integer> visit(BigIntType bigIntType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_SIZE;
            } else {
                return 8;
            }
        };
    }

    @Override
    public BiFunction<DataGetters, Integer, Integer> visit(FloatType floatType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_SIZE;
            } else {
                return 4;
            }
        };
    }

    @Override
    public BiFunction<DataGetters, Integer, Integer> visit(DoubleType doubleType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_SIZE;
            } else {
                return 8;
            }
        };
    }

    @Override
    public BiFunction<DataGetters, Integer, Integer> visit(DateType dateType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_SIZE;
            } else {
                return 4;
            }
        };
    }

    @Override
    public BiFunction<DataGetters, Integer, Integer> visit(TimeType timeType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_SIZE;
            } else {
                return 4;
            }
        };
    }

    @Override
    public BiFunction<DataGetters, Integer, Integer> visit(TimestampType timestampType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_SIZE;
            } else {
                return 8;
            }
        };
    }

    @Override
    public BiFunction<DataGetters, Integer, Integer> visit(
            LocalZonedTimestampType localZonedTimestampType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_SIZE;
            } else {
                return 8;
            }
        };
    }

    @Override
    public BiFunction<DataGetters, Integer, Integer> visit(ArrayType arrayType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_SIZE;
            } else {
                BiFunction<DataGetters, Integer, Integer> function =
                        arrayType.getElementType().accept(this);
                InternalArray internalArray = row.getArray(index);

                int size = 0;
                for (int i = 0; i < internalArray.size(); i++) {
                    size += function.apply(internalArray, i);
                }

                return size;
            }
        };
    }

    @Override
    public BiFunction<DataGetters, Integer, Integer> visit(MultisetType multisetType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_SIZE;
            } else {
                BiFunction<DataGetters, Integer, Integer> function =
                        multisetType.getElementType().accept(this);
                InternalMap map = row.getMap(index);

                int size = 0;
                for (int i = 0; i < map.size(); i++) {
                    size += function.apply(map.keyArray(), i);
                }

                return size;
            }
        };
    }

    @Override
    public BiFunction<DataGetters, Integer, Integer> visit(MapType mapType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_SIZE;
            } else {

                BiFunction<DataGetters, Integer, Integer> keyFunction =
                        mapType.getKeyType().accept(this);
                BiFunction<DataGetters, Integer, Integer> valueFunction =
                        mapType.getValueType().accept(this);

                InternalMap map = row.getMap(index);

                int size = 0;
                for (int i = 0; i < map.size(); i++) {
                    size += keyFunction.apply(map.keyArray(), i);
                }

                for (int i = 0; i < map.size(); i++) {
                    size += valueFunction.apply(map.valueArray(), i);
                }

                return size;
            }
        };
    }

    @Override
    public BiFunction<DataGetters, Integer, Integer> visit(RowType rowType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_SIZE;
            } else {
                int size = 0;
                List<DataType> fieldTypes = rowType.getFieldTypes();
                InternalRow nestRow = row.getRow(index, rowType.getFieldCount());
                for (int i = 0; i < fieldTypes.size(); i++) {
                    DataType dataType = fieldTypes.get(i);
                    size += dataType.accept(this).apply(nestRow, i);
                }
                return size;
            }
        };
    }
}
