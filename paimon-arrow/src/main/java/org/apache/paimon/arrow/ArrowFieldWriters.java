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

package org.apache.paimon.arrow;

import org.apache.paimon.data.DataGetters;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
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
import org.apache.paimon.utils.IntArrayList;

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
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;

/** Registry of {@link ArrowFieldWriter}s. */
public class ArrowFieldWriters {

    /** Writer for CHAR & VARCHAR. */
    public static class StringWriter extends ArrowFieldWriter {

        public StringWriter(FieldVector fieldVector) {
            super(fieldVector);
        }

        @Override
        protected void doWrite(
                ColumnVector columnVector,
                @Nullable int[] pickedInColumn,
                int startIndex,
                int batchRows) {
            VarCharVector varCharVector = (VarCharVector) fieldVector;
            for (int i = 0; i < batchRows; i++) {
                int row = getRowNumber(startIndex, i, pickedInColumn);
                if (columnVector.isNullAt(row)) {
                    varCharVector.setNull(row);
                } else {
                    byte[] value = ((BytesColumnVector) columnVector).getBytes(row).getBytes();
                    varCharVector.setSafe(i, value);
                }
            }
        }

        @Override
        protected void doWrite(int rowIndex, DataGetters getters, int pos) {
            ((VarCharVector) fieldVector).setSafe(rowIndex, getters.getString(pos).toBytes());
        }
    }

    /** Writer for BOOLEAN. */
    public static class BooleanWriter extends ArrowFieldWriter {

        public BooleanWriter(FieldVector fieldVector) {
            super(fieldVector);
        }

        @Override
        protected void doWrite(
                ColumnVector columnVector,
                @Nullable int[] pickedInColumn,
                int startIndex,
                int batchRows) {
            BitVector bitVector = (BitVector) fieldVector;
            for (int i = 0; i < batchRows; i++) {
                int row = getRowNumber(startIndex, i, pickedInColumn);
                if (columnVector.isNullAt(row)) {
                    bitVector.setNull(row);
                } else {
                    int value = ((BooleanColumnVector) columnVector).getBoolean(row) ? 1 : 0;
                    bitVector.setSafe(i, value);
                }
            }
        }

        @Override
        protected void doWrite(int rowIndex, DataGetters getters, int pos) {
            ((BitVector) fieldVector).setSafe(rowIndex, getters.getBoolean(pos) ? 1 : 0);
        }
    }

    /** Writer for BINARY & VARBINARY. */
    public static class BinaryWriter extends ArrowFieldWriter {

        public BinaryWriter(FieldVector fieldVector) {
            super(fieldVector);
        }

        @Override
        protected void doWrite(
                ColumnVector columnVector,
                @Nullable int[] pickedInColumn,
                int startIndex,
                int batchRows) {
            VarBinaryVector varBinaryVector = (VarBinaryVector) fieldVector;
            for (int i = 0; i < batchRows; i++) {
                int row = getRowNumber(startIndex, i, pickedInColumn);
                if (columnVector.isNullAt(row)) {
                    varBinaryVector.setNull(i);
                } else {
                    byte[] value = ((BytesColumnVector) columnVector).getBytes(row).getBytes();
                    varBinaryVector.setSafe(i, value);
                }
            }
        }

        @Override
        protected void doWrite(int rowIndex, DataGetters getters, int pos) {
            ((VarBinaryVector) fieldVector).setSafe(rowIndex, getters.getBinary(pos));
        }
    }

    /** Writer for DECIMAL. */
    public static class DecimalWriter extends ArrowFieldWriter {

        private final int precision;
        private final int scale;

        public DecimalWriter(FieldVector fieldVector, int precision, int scale) {
            super(fieldVector);
            this.precision = precision;
            this.scale = scale;
        }

        @Override
        protected void doWrite(
                ColumnVector columnVector,
                @Nullable int[] pickedInColumn,
                int startIndex,
                int batchRows) {
            DecimalVector decimalVector = (DecimalVector) fieldVector;
            for (int i = 0; i < batchRows; i++) {
                int row = getRowNumber(startIndex, i, pickedInColumn);
                if (columnVector.isNullAt(row)) {
                    decimalVector.setNull(i);
                } else {
                    BigDecimal value =
                            ((DecimalColumnVector) columnVector)
                                    .getDecimal(row, precision, scale)
                                    .toBigDecimal();
                    decimalVector.setSafe(i, value);
                }
            }
        }

        @Override
        protected void doWrite(int rowIndex, DataGetters getters, int pos) {
            ((DecimalVector) fieldVector)
                    .setSafe(rowIndex, getters.getDecimal(pos, precision, scale).toBigDecimal());
        }
    }

    /** Writer for TINYINT. */
    public static class TinyIntWriter extends ArrowFieldWriter {

        public TinyIntWriter(FieldVector fieldVector) {
            super(fieldVector);
        }

        @Override
        protected void doWrite(
                ColumnVector columnVector,
                @Nullable int[] pickedInColumn,
                int startIndex,
                int batchRows) {
            TinyIntVector tinyIntVector = (TinyIntVector) fieldVector;
            for (int i = 0; i < batchRows; i++) {
                int row = getRowNumber(startIndex, i, pickedInColumn);
                if (columnVector.isNullAt(row)) {
                    tinyIntVector.setNull(i);
                } else {
                    byte value = ((ByteColumnVector) columnVector).getByte(row);
                    tinyIntVector.setSafe(i, value);
                }
            }
        }

        @Override
        protected void doWrite(int rowIndex, DataGetters getters, int pos) {
            ((TinyIntVector) fieldVector).setSafe(rowIndex, getters.getByte(pos));
        }
    }

    /** Writer for SMALLINT. */
    public static class SmallIntWriter extends ArrowFieldWriter {

        public SmallIntWriter(FieldVector fieldVector) {
            super(fieldVector);
        }

        @Override
        protected void doWrite(
                ColumnVector columnVector,
                @Nullable int[] pickedInColumn,
                int startIndex,
                int batchRows) {
            SmallIntVector smallIntVector = (SmallIntVector) fieldVector;
            for (int i = 0; i < batchRows; i++) {
                int row = getRowNumber(startIndex, i, pickedInColumn);
                if (columnVector.isNullAt(row)) {
                    smallIntVector.setNull(i);
                } else {
                    short value = ((ShortColumnVector) columnVector).getShort(row);
                    smallIntVector.setSafe(i, value);
                }
            }
        }

        @Override
        protected void doWrite(int rowIndex, DataGetters getters, int pos) {
            ((SmallIntVector) fieldVector).setSafe(rowIndex, getters.getShort(pos));
        }
    }

    /** Writer for INT. */
    public static class IntWriter extends ArrowFieldWriter {

        public IntWriter(FieldVector fieldVector) {
            super(fieldVector);
        }

        @Override
        protected void doWrite(
                ColumnVector columnVector,
                @Nullable int[] pickedInColumn,
                int startIndex,
                int batchRows) {
            IntVector intVector = (IntVector) fieldVector;
            for (int i = 0; i < batchRows; i++) {
                int row = getRowNumber(startIndex, i, pickedInColumn);
                if (columnVector.isNullAt(row)) {
                    intVector.setNull(i);
                } else {
                    int value = ((IntColumnVector) columnVector).getInt(row);
                    intVector.setSafe(i, value);
                }
            }
        }

        @Override
        protected void doWrite(int rowIndex, DataGetters getters, int pos) {
            ((IntVector) fieldVector).setSafe(rowIndex, getters.getInt(pos));
        }
    }

    /** Writer for BIGINT. */
    public static class BigIntWriter extends ArrowFieldWriter {

        public BigIntWriter(FieldVector fieldVector) {
            super(fieldVector);
        }

        @Override
        protected void doWrite(
                ColumnVector columnVector, int[] pickedInColumn, int startIndex, int batchRows) {
            BigIntVector bigIntVector = (BigIntVector) fieldVector;
            for (int i = 0; i < batchRows; i++) {
                int row = getRowNumber(startIndex, i, pickedInColumn);
                if (columnVector.isNullAt(row)) {
                    bigIntVector.setNull(i);
                } else {
                    long value = ((LongColumnVector) columnVector).getLong(row);
                    bigIntVector.setSafe(i, value);
                }
            }
        }

        @Override
        protected void doWrite(int rowIndex, DataGetters getters, int pos) {
            ((BigIntVector) fieldVector).setSafe(rowIndex, getters.getLong(pos));
        }
    }

    /** Writer for FLOAT. */
    public static class FloatWriter extends ArrowFieldWriter {

        public FloatWriter(FieldVector fieldVector) {
            super(fieldVector);
        }

        @Override
        protected void doWrite(
                ColumnVector columnVector,
                @Nullable int[] pickedInColumn,
                int startIndex,
                int batchRows) {
            Float4Vector float4Vector = (Float4Vector) fieldVector;
            for (int i = 0; i < batchRows; i++) {
                int row = getRowNumber(startIndex, i, pickedInColumn);
                if (columnVector.isNullAt(row)) {
                    float4Vector.setNull(i);
                } else {
                    float value = ((FloatColumnVector) columnVector).getFloat(row);
                    float4Vector.setSafe(i, value);
                }
            }
        }

        @Override
        protected void doWrite(int rowIndex, DataGetters getters, int pos) {
            ((Float4Vector) fieldVector).setSafe(rowIndex, getters.getFloat(pos));
        }
    }

    /** Writer for DOUBLE. */
    public static class DoubleWriter extends ArrowFieldWriter {

        public DoubleWriter(FieldVector fieldVector) {
            super(fieldVector);
        }

        @Override
        protected void doWrite(
                ColumnVector columnVector,
                @Nullable int[] pickedInColumn,
                int startIndex,
                int batchRows) {
            Float8Vector float8Vector = (Float8Vector) fieldVector;
            for (int i = 0; i < batchRows; i++) {
                int row = getRowNumber(startIndex, i, pickedInColumn);
                if (columnVector.isNullAt(row)) {
                    float8Vector.setNull(i);
                } else {
                    double value = ((DoubleColumnVector) columnVector).getDouble(row);
                    float8Vector.setSafe(i, value);
                }
            }
        }

        @Override
        protected void doWrite(int rowIndex, DataGetters getters, int pos) {
            ((Float8Vector) fieldVector).setSafe(rowIndex, getters.getDouble(pos));
        }
    }

    /** Writer for DATE. */
    public static class DateWriter extends ArrowFieldWriter {

        public DateWriter(FieldVector fieldVector) {
            super(fieldVector);
        }

        @Override
        protected void doWrite(
                ColumnVector columnVector,
                @Nullable int[] pickedInColumn,
                int startIndex,
                int batchRows) {
            DateDayVector dateDayVector = (DateDayVector) fieldVector;
            for (int i = 0; i < batchRows; i++) {
                int row = getRowNumber(startIndex, i, pickedInColumn);
                if (columnVector.isNullAt(row)) {
                    dateDayVector.setNull(i);
                } else {
                    int value = ((IntColumnVector) columnVector).getInt(row);
                    dateDayVector.setSafe(i, value);
                }
            }
        }

        @Override
        protected void doWrite(int rowIndex, DataGetters getters, int pos) {
            ((DateDayVector) fieldVector).setSafe(rowIndex, getters.getInt(pos));
        }
    }

    /** Writer for TIME. */
    public static class TimeWriter extends ArrowFieldWriter {

        public TimeWriter(FieldVector fieldVector) {
            super(fieldVector);
        }

        @Override
        protected void doWrite(
                ColumnVector columnVector,
                @Nullable int[] pickedInColumn,
                int startIndex,
                int batchRows) {
            TimeMilliVector timeMilliVector = (TimeMilliVector) fieldVector;
            for (int i = 0; i < batchRows; i++) {
                int row = getRowNumber(startIndex, i, pickedInColumn);
                if (columnVector.isNullAt(row)) {
                    timeMilliVector.setNull(i);
                } else {
                    int value = ((IntColumnVector) columnVector).getInt(i);
                    timeMilliVector.setSafe(i, value);
                }
            }
        }

        @Override
        protected void doWrite(int rowIndex, DataGetters getters, int pos) {
            ((TimeMilliVector) fieldVector).setSafe(rowIndex, getters.getInt(pos));
        }
    }

    /** Writer for TIMESTAMP & TIMESTAMP_LTZ. */
    public static class TimestampWriter extends ArrowFieldWriter {

        private final int precision;
        @Nullable private final ZoneId castZoneId;

        public TimestampWriter(
                FieldVector fieldVector, int precision, @Nullable ZoneId castZoneId) {
            super(fieldVector);
            this.precision = precision;
            this.castZoneId = castZoneId;
        }

        @Override
        protected void doWrite(
                ColumnVector columnVector,
                @Nullable int[] pickedInColumn,
                int startIndex,
                int batchRows) {
            TimeStampNanoVector timeStampNanoVector = (TimeStampNanoVector) fieldVector;
            for (int i = 0; i < batchRows; i++) {
                int row = getRowNumber(startIndex, i, pickedInColumn);
                if (columnVector.isNullAt(row)) {
                    timeStampNanoVector.setNull(i);
                } else {
                    Timestamp timestamp =
                            ((TimestampColumnVector) columnVector).getTimestamp(row, precision);
                    long value = timestampToEpochNano(timestamp);
                    timeStampNanoVector.setSafe(i, value);
                }
            }
        }

        @Override
        protected void doWrite(int rowIndex, DataGetters getters, int pos) {
            TimeStampNanoVector timeStampNanoVector = (TimeStampNanoVector) fieldVector;
            Timestamp timestamp = getters.getTimestamp(pos, precision);
            long value = timestampToEpochNano(timestamp);
            timeStampNanoVector.setSafe(rowIndex, value);
        }

        private long timestampToEpochNano(Timestamp timestamp) {
            if (castZoneId != null) {
                Instant instant = timestamp.toLocalDateTime().atZone(castZoneId).toInstant();
                return instant.getEpochSecond() * 1_000_000_000 + instant.getNano();
            } else {
                return timestamp.getMillisecond() * 1_000_000 + timestamp.getNanoOfMillisecond();
            }
        }
    }

    /** Writer for ARRAY. */
    public static class ArrayWriter extends ArrowFieldWriter {

        private final ArrowFieldWriter elementWriter;

        public ArrayWriter(FieldVector fieldVector, ArrowFieldWriter elementWriter) {
            super(fieldVector);
            this.elementWriter = elementWriter;
        }

        @Override
        protected void doWrite(
                ColumnVector columnVector,
                @Nullable int[] pickedInColumn,
                int startIndex,
                int batchRows) {
            ArrayColumnVector arrayColumnVector = (ArrayColumnVector) columnVector;

            int lenSize;
            if (pickedInColumn == null) {
                lenSize = startIndex + batchRows;
            } else {
                lenSize = pickedInColumn[startIndex + batchRows - 1] + 1;
            }

            // length for arrays in [0, startIndex + batchRows)
            // TODO: reuse this
            int[] lengths = new int[lenSize];
            for (int i = 0; i < lenSize; i++) {
                if (arrayColumnVector.isNullAt(i)) {
                    // null values don't occupy space
                    lengths[i] = 0;
                } else {
                    int size = arrayColumnVector.getArray(i).size();
                    lengths[i] = size;
                }
            }

            ArrayChildWriteInfo arrayChildWriteInfo =
                    getArrayChildWriteInfo(pickedInColumn, startIndex, lengths);
            elementWriter.write(
                    arrayColumnVector.getColumnVector(),
                    arrayChildWriteInfo.pickedInColumn,
                    arrayChildWriteInfo.startIndex,
                    arrayChildWriteInfo.batchRows);

            // set ListVector
            ListVector listVector = (ListVector) fieldVector;
            for (int i = 0; i < batchRows; i++) {
                int row = getRowNumber(startIndex, i, pickedInColumn);
                if (arrayColumnVector.isNullAt(row)) {
                    listVector.setNull(i);
                } else {
                    listVector.startNewValue(i);
                    listVector.endValue(i, lengths[row]);
                }
            }
        }

        @Override
        protected void doWrite(int rowIndex, DataGetters getters, int pos) {
            InternalArray array = getters.getArray(pos);
            ListVector listVector = (ListVector) fieldVector;
            FieldVector dataVector = listVector.getDataVector();
            listVector.startNewValue(rowIndex);
            int offset = dataVector.getValueCount();
            for (int arrIndex = 0; arrIndex < array.size(); arrIndex++) {
                int fieldIndex = offset + arrIndex;
                elementWriter.write(fieldIndex, array, arrIndex);
            }
            listVector.endValue(rowIndex, array.size());
        }
    }

    /** Writer for MAP. */
    public static class MapWriter extends ArrowFieldWriter {

        private final ArrowFieldWriter keyWriter;
        private final ArrowFieldWriter valueWriter;

        public MapWriter(
                FieldVector fieldVector, ArrowFieldWriter keyWriter, ArrowFieldWriter valueWriter) {
            super(fieldVector);
            this.keyWriter = keyWriter;
            this.valueWriter = valueWriter;
        }

        @Override
        protected void doWrite(
                ColumnVector columnVector,
                @Nullable int[] pickedInColumn,
                int startIndex,
                int batchRows) {
            MapColumnVector mapColumnVector = (MapColumnVector) columnVector;

            int lenSize;
            if (pickedInColumn == null) {
                lenSize = startIndex + batchRows;
            } else {
                lenSize = pickedInColumn[startIndex + batchRows - 1] + 1;
            }

            // length for arrays in [0, startIndex + batchRows)
            // TODO: reuse this
            int[] lengths = new int[lenSize];
            for (int i = 0; i < lenSize; i++) {
                if (mapColumnVector.isNullAt(i)) {
                    // null values don't occupy space
                    lengths[i] = 0;
                } else {
                    int size = mapColumnVector.getMap(i).size();
                    lengths[i] = size;
                }
            }

            ArrayChildWriteInfo arrayChildWriteInfo =
                    getArrayChildWriteInfo(pickedInColumn, startIndex, lengths);
            keyWriter.write(
                    mapColumnVector.getKeyColumnVector(),
                    arrayChildWriteInfo.pickedInColumn,
                    arrayChildWriteInfo.startIndex,
                    arrayChildWriteInfo.batchRows);
            valueWriter.write(
                    mapColumnVector.getValueColumnVector(),
                    arrayChildWriteInfo.pickedInColumn,
                    arrayChildWriteInfo.startIndex,
                    arrayChildWriteInfo.batchRows);

            // set inner struct and map
            MapVector mapVector = (MapVector) fieldVector;
            StructVector innerStructVector = (StructVector) mapVector.getDataVector();
            for (int i = 0; i < arrayChildWriteInfo.batchRows; i++) {
                innerStructVector.setIndexDefined(i);
            }

            ListVector listVector = (ListVector) fieldVector;
            for (int i = 0; i < batchRows; i++) {
                int row = getRowNumber(startIndex, i, pickedInColumn);
                if (mapColumnVector.isNullAt(row)) {
                    listVector.setNull(i);
                } else {
                    listVector.startNewValue(i);
                    listVector.endValue(i, lengths[row]);
                }
            }
        }

        @Override
        protected void doWrite(int rowIndex, DataGetters getters, int pos) {
            InternalMap map = getters.getMap(pos);
            InternalArray keyArray = map.keyArray();
            InternalArray valueArray = map.valueArray();
            MapVector mapVector = (MapVector) fieldVector;
            StructVector structVector = (StructVector) mapVector.getDataVector();

            mapVector.startNewValue(rowIndex);
            int offset = structVector.getValueCount();
            for (int mapIndex = 0; mapIndex < map.size(); mapIndex++) {
                int fieldIndex = offset + mapIndex;
                keyWriter.write(fieldIndex, keyArray, mapIndex);
                valueWriter.write(fieldIndex, valueArray, mapIndex);
                structVector.setIndexDefined(fieldIndex);
            }
            mapVector.endValue(rowIndex, map.size());
        }
    }

    private static class ArrayChildWriteInfo {
        @Nullable final int[] pickedInColumn;
        final int startIndex;
        final int batchRows;

        ArrayChildWriteInfo(@Nullable int[] pickedInColumn, int startIndex, int batchRows) {
            this.pickedInColumn = pickedInColumn;
            this.startIndex = startIndex;
            this.batchRows = batchRows;
        }
    }

    private static ArrayChildWriteInfo getArrayChildWriteInfo(
            @Nullable int[] pickedInParentColumn, int parentStartIndex, int[] parentLengths) {
        return pickedInParentColumn == null
                ? getArrayChildWriteInfoWithoutDelete(parentStartIndex, parentLengths)
                : getArrayChildWriteInfoWithDelete(
                        pickedInParentColumn, parentStartIndex, parentLengths);
    }

    private static ArrayChildWriteInfo getArrayChildWriteInfoWithoutDelete(
            int parentStartIndex, int[] parentLengths) {
        // the first element index which is to be written
        int firstElementIndex = 0;
        // batchRows of child column vector
        int childBatchRows = 0;
        for (int i = 0; i < parentLengths.length; i++) {
            if (i < parentStartIndex) {
                firstElementIndex += parentLengths[i];
            } else {
                childBatchRows += parentLengths[i];
            }
        }
        return new ArrayChildWriteInfo(null, firstElementIndex, childBatchRows);
    }

    private static ArrayChildWriteInfo getArrayChildWriteInfoWithDelete(
            int[] pickedInParentColumn, int parentStartIndex, int[] parentLengths) {
        // the first element index which is to be written
        int firstElementIndex = 0;
        // objects to calculate child pickedInColumn
        IntArrayList childPicked = new IntArrayList(1024);
        int offset = 0;
        int currentParentPickedIndex = parentStartIndex;
        for (int i = 0; i < parentLengths.length; i++) {
            if (i < pickedInParentColumn[parentStartIndex]) {
                firstElementIndex += parentLengths[i];
                offset = firstElementIndex;
            } else {
                if (i == pickedInParentColumn[currentParentPickedIndex]) {
                    for (int pick = 0; pick < parentLengths[i]; pick++) {
                        childPicked.add(pick + offset);
                    }
                    currentParentPickedIndex += 1;
                }
                offset += parentLengths[i];
            }
        }
        return new ArrayChildWriteInfo(childPicked.toArray(), 0, childPicked.size());
    }

    /** Writer for ROW. */
    public static class RowWriter extends ArrowFieldWriter {

        private final ArrowFieldWriter[] fieldWriters;

        public RowWriter(FieldVector fieldVector, ArrowFieldWriter[] fieldWriters) {
            super(fieldVector);
            this.fieldWriters = fieldWriters;
        }

        @Override
        protected void doWrite(
                ColumnVector columnVector,
                @Nullable int[] pickedInColumn,
                int startIndex,
                int batchRows) {
            boolean[] isNull = new boolean[batchRows];
            boolean allNull = true;
            for (int i = 0; i < batchRows; i++) {
                int row = getRowNumber(startIndex, i, pickedInColumn);
                if (columnVector.isNullAt(row)) {
                    isNull[i] = true;
                } else {
                    allNull = false;
                }
            }

            RowColumnVector rowColumnVector = (RowColumnVector) columnVector;
            VectorizedColumnBatch batch = rowColumnVector.getBatch();
            int nestedBatchRows = allNull ? 0 : batchRows;
            for (int i = 0; i < fieldWriters.length; i++) {
                fieldWriters[i].write(
                        batch.columns[i], pickedInColumn, startIndex, nestedBatchRows);
            }

            StructVector structVector = (StructVector) fieldVector;
            for (int i = 0; i < batchRows; i++) {
                if (isNull[i]) {
                    structVector.setNull(i);
                } else {
                    structVector.setIndexDefined(i);
                }
            }
        }

        @Override
        protected void doWrite(int rowIndex, DataGetters getters, int pos) {
            int fieldCount = fieldWriters.length;
            InternalRow row = getters.getRow(pos, fieldCount);
            StructVector structVector = (StructVector) fieldVector;
            for (int i = 0; i < fieldWriters.length; i++) {
                fieldWriters[i].write(rowIndex, row, i);
            }
            structVector.setIndexDefined(rowIndex);
        }
    }
}
