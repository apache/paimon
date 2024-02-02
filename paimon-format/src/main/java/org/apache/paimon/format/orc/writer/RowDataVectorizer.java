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

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;

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
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

import java.sql.Timestamp;

/** A {@link Vectorizer} of {@link InternalRow} type element. */
public class RowDataVectorizer extends Vectorizer<InternalRow> {

    private final DataType[] fieldTypes;

    public RowDataVectorizer(String schema, DataType[] fieldTypes) {
        super(schema);
        this.fieldTypes = fieldTypes;
    }

    @Override
    public void vectorize(InternalRow row, VectorizedRowBatch batch) {
        int rowId = batch.size++;
        for (int i = 0; i < row.getFieldCount(); ++i) {
            setColumn(rowId, batch.cols[i], fieldTypes[i], row, i);
        }
    }

    private static void setColumn(
            int rowId, ColumnVector column, DataType type, InternalRow row, int columnId) {
        if (row.isNullAt(columnId)) {
            column.noNulls = false;
            column.isNull[rowId] = true;
            return;
        }

        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                {
                    BytesColumnVector vector = (BytesColumnVector) column;
                    byte[] bytes = row.getString(columnId).toBytes();
                    vector.setVal(rowId, bytes, 0, bytes.length);
                    break;
                }
            case BOOLEAN:
                {
                    LongColumnVector vector = (LongColumnVector) column;
                    vector.vector[rowId] = row.getBoolean(columnId) ? 1 : 0;
                    break;
                }
            case BINARY:
            case VARBINARY:
                {
                    BytesColumnVector vector = (BytesColumnVector) column;
                    byte[] bytes = row.getBinary(columnId);
                    vector.setVal(rowId, bytes, 0, bytes.length);
                    break;
                }
            case DECIMAL:
                {
                    DecimalType dt = (DecimalType) type;
                    DecimalColumnVector vector = (DecimalColumnVector) column;
                    vector.set(
                            rowId,
                            HiveDecimal.create(
                                    row.getDecimal(columnId, dt.getPrecision(), dt.getScale())
                                            .toBigDecimal()));
                    break;
                }
            case TINYINT:
                {
                    LongColumnVector vector = (LongColumnVector) column;
                    vector.vector[rowId] = row.getByte(columnId);
                    break;
                }
            case SMALLINT:
                {
                    LongColumnVector vector = (LongColumnVector) column;
                    vector.vector[rowId] = row.getShort(columnId);
                    break;
                }
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case INTEGER:
                {
                    LongColumnVector vector = (LongColumnVector) column;
                    vector.vector[rowId] = row.getInt(columnId);
                    break;
                }
            case BIGINT:
                {
                    LongColumnVector vector = (LongColumnVector) column;
                    vector.vector[rowId] = row.getLong(columnId);
                    break;
                }
            case FLOAT:
                {
                    DoubleColumnVector vector = (DoubleColumnVector) column;
                    vector.vector[rowId] = row.getFloat(columnId);
                    break;
                }
            case DOUBLE:
                {
                    DoubleColumnVector vector = (DoubleColumnVector) column;
                    vector.vector[rowId] = row.getDouble(columnId);
                    break;
                }
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                {
                    TimestampType tt = (TimestampType) type;
                    Timestamp timestamp =
                            row.getTimestamp(columnId, tt.getPrecision()).toSQLTimestamp();
                    TimestampColumnVector vector = (TimestampColumnVector) column;
                    vector.set(rowId, timestamp);
                    break;
                }
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                {
                    LocalZonedTimestampType lt = (LocalZonedTimestampType) type;
                    Timestamp timestamp =
                            row.getTimestamp(columnId, lt.getPrecision()).toSQLTimestamp();
                    TimestampColumnVector vector = (TimestampColumnVector) column;
                    vector.set(rowId, timestamp);
                    break;
                }
            case ARRAY:
                {
                    ListColumnVector listColumnVector = (ListColumnVector) column;
                    setColumn(rowId, listColumnVector, type, row, columnId);
                    break;
                }
            case MAP:
                {
                    MapColumnVector mapColumnVector = (MapColumnVector) column;
                    setColumn(rowId, mapColumnVector, type, row, columnId);
                    break;
                }
            case ROW:
                {
                    StructColumnVector structColumnVector = (StructColumnVector) column;
                    setColumn(rowId, structColumnVector, type, row, columnId);
                    break;
                }
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    private static void setColumn(
            int rowId,
            ListColumnVector listColumnVector,
            DataType type,
            InternalRow row,
            int columnId) {
        InternalArray arrayData = row.getArray(columnId);
        ArrayType arrayType = (ArrayType) type;
        listColumnVector.lengths[rowId] = arrayData.size();
        listColumnVector.offsets[rowId] = listColumnVector.childCount;
        listColumnVector.childCount += listColumnVector.lengths[rowId];
        ensureSize(
                listColumnVector.child,
                listColumnVector.childCount,
                listColumnVector.offsets[rowId] != 0);

        InternalRow convertedRowData = convert(arrayData, arrayType.getElementType());
        for (int i = 0; i < arrayData.size(); i++) {
            setColumn(
                    (int) listColumnVector.offsets[rowId] + i,
                    listColumnVector.child,
                    arrayType.getElementType(),
                    convertedRowData,
                    i);
        }
    }

    private static void setColumn(
            int rowId,
            MapColumnVector mapColumnVector,
            DataType type,
            InternalRow row,
            int columnId) {
        InternalMap mapData = row.getMap(columnId);
        MapType mapType = (MapType) type;
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

        InternalRow convertedKeyRowData = convert(keyArray, mapType.getKeyType());
        InternalRow convertedValueRowData = convert(valueArray, mapType.getValueType());
        for (int i = 0; i < keyArray.size(); i++) {
            setColumn(
                    (int) mapColumnVector.offsets[rowId] + i,
                    mapColumnVector.keys,
                    mapType.getKeyType(),
                    convertedKeyRowData,
                    i);
            setColumn(
                    (int) mapColumnVector.offsets[rowId] + i,
                    mapColumnVector.values,
                    mapType.getValueType(),
                    convertedValueRowData,
                    i);
        }
    }

    private static void setColumn(
            int rowId,
            StructColumnVector structColumnVector,
            DataType type,
            InternalRow row,
            int columnId) {
        InternalRow structRow = row.getRow(columnId, structColumnVector.fields.length);
        RowType rowType = (RowType) type;
        for (int i = 0; i < structRow.getFieldCount(); i++) {
            ColumnVector cv = structColumnVector.fields[i];
            setColumn(rowId, cv, rowType.getTypeAt(i), structRow, i);
        }
    }

    private static void ensureSize(ColumnVector cv, int size, boolean preserveData) {
        int currentLength = cv.isNull.length;
        if (currentLength < size) {
            cv.ensureSize(Math.max(currentLength * 2, size), preserveData);
        }
    }

    /**
     * Converting ArrayData to RowData for calling {@link RowDataVectorizer#setColumn(int,
     * ColumnVector, DataType, InternalRow, int)} recursively with array.
     *
     * @param arrayData input ArrayData.
     * @param arrayFieldType DataType of input ArrayData.
     * @return RowData.
     */
    private static InternalRow convert(InternalArray arrayData, DataType arrayFieldType) {
        GenericRow rowData = new GenericRow(arrayData.size());
        InternalArray.ElementGetter elementGetter =
                InternalArray.createElementGetter(arrayFieldType);
        for (int i = 0; i < arrayData.size(); i++) {
            rowData.setField(i, elementGetter.getElementOrNull(arrayData, i));
        }
        return rowData;
    }
}
