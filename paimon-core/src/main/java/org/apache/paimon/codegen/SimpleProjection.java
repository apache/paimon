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

package org.apache.paimon.codegen;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalArraySerializer;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
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

import java.util.ArrayList;
import java.util.List;

/** Projection without codegen. */
public class SimpleProjection implements Projection {

    private static final FieldWriterVisitor FIELD_WRITER_VISITOR = new FieldWriterVisitor();

    private final BinaryRow binaryRow;
    private final BinaryRowWriter binaryRowWriter;
    private final Writer[] writers;

    public SimpleProjection(RowType all, List<String> projectedFields) {

        this.binaryRow = new BinaryRow(projectedFields.size());
        this.binaryRowWriter = new BinaryRowWriter(binaryRow);

        List<Writer> writers = new ArrayList<>();
        List<DataField> fields = all.getFields();

        for (int i = 0; i < all.getFieldCount(); i++) {
            int index = projectedFields.indexOf(fields.get(i).name());
            if (index != -1) {
                writers.add(
                        new Writer(
                                i,
                                index,
                                binaryRowWriter,
                                fields.get(i).type().accept(FIELD_WRITER_VISITOR)));
            }
        }

        this.writers = writers.toArray(new Writer[0]);
    }

    @Override
    public BinaryRow apply(InternalRow internalRow) {

        binaryRowWriter.reset();
        for (Writer writer : writers) {
            writer.write(internalRow);
        }
        binaryRowWriter.complete();
        return binaryRow.copy();
    }

    /** Writer binary row. */
    public static class Writer {

        private final int i;
        private final int j;
        private final BinaryRowWriter binaryRowWriter;
        private final FieldWriter fieldWriter;

        public Writer(int i, int j, BinaryRowWriter binaryRowWriter, FieldWriter fieldWriter) {
            this.i = i;
            this.j = j;

            this.binaryRowWriter = binaryRowWriter;
            this.fieldWriter = fieldWriter;
        }

        public void write(InternalRow row) {
            if (row.isNullAt(i)) {
                binaryRowWriter.setNullAt(i);
            } else {
                fieldWriter.write(row, i, binaryRowWriter, j);
            }
        }
    }

    /** Write fields. */
    public interface FieldWriter {
        void write(InternalRow internalRow, int i, BinaryRowWriter binaryRowWriter, int j);
    }

    /** Visitor. */
    public static class FieldWriterVisitor implements DataTypeVisitor<FieldWriter> {

        @Override
        public FieldWriter visit(CharType charType) {
            return (row, i, writer, j) -> writer.writeString(j, row.getString(i));
        }

        @Override
        public FieldWriter visit(VarCharType varCharType) {
            return (row, i, writer, j) -> writer.writeString(j, row.getString(i));
        }

        @Override
        public FieldWriter visit(BooleanType booleanType) {
            return (row, i, writer, j) -> writer.writeBoolean(j, row.getBoolean(i));
        }

        @Override
        public FieldWriter visit(BinaryType binaryType) {
            return (row, i, writer, j) -> writer.writeBinary(j, row.getBinary(i));
        }

        @Override
        public FieldWriter visit(VarBinaryType varBinaryType) {
            return (row, i, writer, j) -> writer.writeBinary(j, row.getBinary(i));
        }

        @Override
        public FieldWriter visit(DecimalType decimalType) {
            return (row, i, writer, j) ->
                    writer.writeDecimal(
                            j,
                            row.getDecimal(i, decimalType.getPrecision(), decimalType.getScale()),
                            decimalType.getPrecision());
        }

        @Override
        public FieldWriter visit(TinyIntType tinyIntType) {
            return (row, i, writer, j) -> writer.writeByte(j, row.getByte(i));
        }

        @Override
        public FieldWriter visit(SmallIntType smallIntType) {
            return (row, i, writer, j) -> writer.writeShort(j, row.getShort(i));
        }

        @Override
        public FieldWriter visit(IntType intType) {
            return (row, i, writer, j) -> writer.writeInt(j, row.getInt(i));
        }

        @Override
        public FieldWriter visit(BigIntType bigIntType) {
            return (row, i, writer, j) -> writer.writeLong(j, row.getLong(i));
        }

        @Override
        public FieldWriter visit(FloatType floatType) {
            return (row, i, writer, j) -> writer.writeFloat(j, row.getFloat(i));
        }

        @Override
        public FieldWriter visit(DoubleType doubleType) {
            return (row, i, writer, j) -> writer.writeDouble(j, row.getDouble(i));
        }

        @Override
        public FieldWriter visit(DateType dateType) {
            return (row, i, writer, j) -> writer.writeInt(j, row.getInt(i));
        }

        @Override
        public FieldWriter visit(TimeType timeType) {
            return (row, i, writer, j) -> writer.writeInt(j, row.getInt(i));
        }

        @Override
        public FieldWriter visit(TimestampType timestampType) {
            return (row, i, writer, j) ->
                    writer.writeTimestamp(
                            j,
                            row.getTimestamp(i, timestampType.getPrecision()),
                            timestampType.getPrecision());
        }

        @Override
        public FieldWriter visit(LocalZonedTimestampType localZonedTimestampType) {
            return (row, i, writer, j) ->
                    writer.writeTimestamp(
                            j,
                            row.getTimestamp(i, localZonedTimestampType.getPrecision()),
                            localZonedTimestampType.getPrecision());
        }

        @Override
        public FieldWriter visit(ArrayType arrayType) {
            return (row, i, writer, j) ->
                    writer.writeArray(
                            j,
                            row.getArray(i),
                            new InternalArraySerializer(arrayType.getElementType()));
        }

        @Override
        public FieldWriter visit(MultisetType multisetType) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FieldWriter visit(MapType mapType) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FieldWriter visit(RowType rowType) {
            throw new UnsupportedOperationException();
        }
    }
}
