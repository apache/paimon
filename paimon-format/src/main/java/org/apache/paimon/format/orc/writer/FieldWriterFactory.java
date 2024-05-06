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

package org.apache.paimon.format.orc.writer;

import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
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

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;

import java.sql.Timestamp;
import java.util.List;
import java.util.stream.Collectors;

/** Factory to create {@link FieldWriter}. */
public class FieldWriterFactory implements DataTypeVisitor<FieldWriter> {

    public static final FieldWriterFactory WRITER_FACTORY = new FieldWriterFactory();

    private static final FieldWriter STRING_WRITER =
            (rowId, column, getters, columnId) -> {
                BytesColumnVector vector = (BytesColumnVector) column;
                byte[] bytes = getters.getString(columnId).toBytes();
                vector.setVal(rowId, bytes, 0, bytes.length);
            };

    private static final FieldWriter BYTES_WRITER =
            (rowId, column, getters, columnId) -> {
                BytesColumnVector vector = (BytesColumnVector) column;
                byte[] bytes = getters.getBinary(columnId);
                vector.setVal(rowId, bytes, 0, bytes.length);
            };

    private static final FieldWriter BOOLEAN_WRITER =
            (rowId, column, getters, columnId) ->
                    ((LongColumnVector) column).vector[rowId] =
                            getters.getBoolean(columnId) ? 1 : 0;

    private static final FieldWriter INT_WRITER =
            (rowId, column, getters, columnId) ->
                    ((LongColumnVector) column).vector[rowId] = getters.getInt(columnId);

    private static final FieldWriter TINYINT_WRITER =
            (rowId, column, getters, columnId) ->
                    ((LongColumnVector) column).vector[rowId] = getters.getByte(columnId);

    private static final FieldWriter SMALLINT_WRITER =
            (rowId, column, getters, columnId) ->
                    ((LongColumnVector) column).vector[rowId] = getters.getShort(columnId);

    private static final FieldWriter BIGINT_WRITER =
            (rowId, column, getters, columnId) ->
                    ((LongColumnVector) column).vector[rowId] = getters.getLong(columnId);

    private static final FieldWriter FLOAT_WRITER =
            (rowId, column, getters, columnId) ->
                    ((DoubleColumnVector) column).vector[rowId] = getters.getFloat(columnId);

    private static final FieldWriter DOUBLE_WRITER =
            (rowId, column, getters, columnId) ->
                    ((DoubleColumnVector) column).vector[rowId] = getters.getDouble(columnId);

    @Override
    public FieldWriter visit(CharType charType) {
        return STRING_WRITER;
    }

    @Override
    public FieldWriter visit(VarCharType varCharType) {
        return STRING_WRITER;
    }

    @Override
    public FieldWriter visit(BooleanType booleanType) {
        return BOOLEAN_WRITER;
    }

    @Override
    public FieldWriter visit(BinaryType binaryType) {
        return BYTES_WRITER;
    }

    @Override
    public FieldWriter visit(VarBinaryType varBinaryType) {
        return BYTES_WRITER;
    }

    @Override
    public FieldWriter visit(TinyIntType tinyIntType) {
        return TINYINT_WRITER;
    }

    @Override
    public FieldWriter visit(SmallIntType smallIntType) {
        return SMALLINT_WRITER;
    }

    @Override
    public FieldWriter visit(IntType intType) {
        return INT_WRITER;
    }

    @Override
    public FieldWriter visit(BigIntType bigIntType) {
        return BIGINT_WRITER;
    }

    @Override
    public FieldWriter visit(FloatType floatType) {
        return FLOAT_WRITER;
    }

    @Override
    public FieldWriter visit(DoubleType doubleType) {
        return DOUBLE_WRITER;
    }

    @Override
    public FieldWriter visit(DateType dateType) {
        return INT_WRITER;
    }

    @Override
    public FieldWriter visit(TimeType timeType) {
        return INT_WRITER;
    }

    @Override
    public FieldWriter visit(TimestampType timestampType) {
        return (rowId, column, getters, columnId) -> {
            Timestamp timestamp =
                    getters.getTimestamp(columnId, timestampType.getPrecision()).toSQLTimestamp();
            TimestampColumnVector vector = (TimestampColumnVector) column;
            vector.set(rowId, timestamp);
        };
    }

    @Override
    public FieldWriter visit(LocalZonedTimestampType localZonedTimestampType) {
        return (rowId, column, getters, columnId) -> {
            Timestamp timestamp =
                    Timestamp.from(
                            getters.getTimestamp(columnId, localZonedTimestampType.getPrecision())
                                    .toInstant());
            TimestampColumnVector vector = (TimestampColumnVector) column;
            vector.set(rowId, timestamp);
        };
    }

    @Override
    public FieldWriter visit(DecimalType decimalType) {
        return (rowId, column, getters, columnId) -> {
            DecimalColumnVector vector = (DecimalColumnVector) column;
            Decimal decimal =
                    getters.getDecimal(
                            columnId, decimalType.getPrecision(), decimalType.getScale());
            HiveDecimal hiveDecimal = HiveDecimal.create(decimal.toBigDecimal());
            vector.set(rowId, hiveDecimal);
        };
    }

    @Override
    public FieldWriter visit(ArrayType arrayType) {
        FieldWriter elementWriter = arrayType.getElementType().accept(this);
        return (rowId, column, getters, columnId) -> {
            ListColumnVector listColumnVector = (ListColumnVector) column;
            InternalArray arrayData = getters.getArray(columnId);
            listColumnVector.lengths[rowId] = arrayData.size();
            listColumnVector.offsets[rowId] = listColumnVector.childCount;
            listColumnVector.childCount += listColumnVector.lengths[rowId];
            ensureSize(
                    listColumnVector.child,
                    listColumnVector.childCount,
                    listColumnVector.offsets[rowId] != 0);

            for (int i = 0; i < arrayData.size(); i++) {
                ColumnVector fieldColumn = listColumnVector.child;
                int fieldIndex = (int) listColumnVector.offsets[rowId] + i;
                if (arrayData.isNullAt(i)) {
                    fieldColumn.noNulls = false;
                    fieldColumn.isNull[fieldIndex] = true;
                } else {
                    elementWriter.write(fieldIndex, fieldColumn, arrayData, i);
                }
            }
        };
    }

    @Override
    public FieldWriter visit(MapType mapType) {
        FieldWriter keyWriter = mapType.getKeyType().accept(this);
        FieldWriter valueWriter = mapType.getValueType().accept(this);
        return (rowId, column, getters, columnId) -> {
            MapColumnVector mapColumnVector = (MapColumnVector) column;
            InternalMap mapData = getters.getMap(columnId);
            InternalArray keyArray = mapData.keyArray();
            InternalArray valueArray = mapData.valueArray();
            mapColumnVector.lengths[rowId] = mapData.size();
            mapColumnVector.offsets[rowId] = mapColumnVector.childCount;
            mapColumnVector.childCount += mapColumnVector.lengths[rowId];
            ensureSize(
                    mapColumnVector.keys,
                    mapColumnVector.childCount,
                    mapColumnVector.offsets[rowId] != 0);
            ensureSize(
                    mapColumnVector.values,
                    mapColumnVector.childCount,
                    mapColumnVector.offsets[rowId] != 0);

            for (int i = 0; i < keyArray.size(); i++) {
                int fieldIndex = (int) mapColumnVector.offsets[rowId] + i;

                ColumnVector keyColumn = mapColumnVector.keys;
                if (keyArray.isNullAt(i)) {
                    keyColumn.noNulls = false;
                    keyColumn.isNull[fieldIndex] = true;
                } else {
                    keyWriter.write(fieldIndex, keyColumn, keyArray, i);
                }

                ColumnVector valueColumn = mapColumnVector.values;
                if (valueArray.isNullAt(i)) {
                    valueColumn.noNulls = false;
                    valueColumn.isNull[fieldIndex] = true;
                } else {
                    valueWriter.write(fieldIndex, valueColumn, valueArray, i);
                }
            }
        };
    }

    @Override
    public FieldWriter visit(RowType rowType) {
        List<FieldWriter> fieldWriters =
                rowType.getFieldTypes().stream()
                        .map(t -> t.accept(this))
                        .collect(Collectors.toList());
        return (rowId, column, getters, columnId) -> {
            StructColumnVector structColumnVector = (StructColumnVector) column;
            InternalRow structRow = getters.getRow(columnId, structColumnVector.fields.length);
            for (int i = 0; i < structRow.getFieldCount(); i++) {
                ColumnVector fieldColumn = structColumnVector.fields[i];
                if (structRow.isNullAt(i)) {
                    fieldColumn.noNulls = false;
                    fieldColumn.isNull[rowId] = true;
                } else {
                    fieldWriters.get(i).write(rowId, fieldColumn, structRow, i);
                }
            }
        };
    }

    @Override
    public FieldWriter visit(MultisetType multisetType) {
        throw new UnsupportedOperationException("Unsupported multisetType: " + multisetType);
    }

    private static void ensureSize(ColumnVector cv, int size, boolean preserveData) {
        int currentLength = cv.isNull.length;
        if (currentLength < size) {
            cv.ensureSize(Math.max(currentLength * 2, size), preserveData);
        }
    }
}
