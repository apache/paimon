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

package org.apache.paimon.sort.zorder;

import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
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
import org.apache.paimon.utils.Bytes;
import org.apache.paimon.utils.ZOrderByteUtils;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;

import static org.apache.paimon.utils.ZOrderByteUtils.NULL_BYTES;
import static org.apache.paimon.utils.ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE;

/** Z-indexer for responsibility to generate z-index. */
public class ZIndexer implements Serializable {

    private final Set<RowProcessor> functionSet;
    private final int[] fieldsIndex;
    private final int totalBytes;
    private transient ByteBuffer reuse;

    public ZIndexer(RowType rowType, List<String> orderColumns) {
        List<String> fields = rowType.getFieldNames();
        fieldsIndex = new int[orderColumns.size()];
        for (int i = 0; i < fieldsIndex.length; i++) {
            int index = fields.indexOf(orderColumns.get(i));
            if (index == -1) {
                throw new IllegalArgumentException(
                        "Can't find column: "
                                + orderColumns.get(i)
                                + " in row type fields: "
                                + fields);
            }
            fieldsIndex[i] = index;
        }
        this.functionSet = constructFunctionMap(rowType.getFields());
        this.totalBytes = PRIMITIVE_BUFFER_SIZE * this.fieldsIndex.length;
    }

    public void open() {
        this.reuse = ByteBuffer.allocate(totalBytes);
        functionSet.forEach(RowProcessor::open);
    }

    public int size() {
        return totalBytes;
    }

    public byte[] index(InternalRow row) {
        byte[][] columnBytes = new byte[fieldsIndex.length][];

        int index = 0;
        for (RowProcessor f : functionSet) {
            columnBytes[index++] = f.zvalue(row);
        }

        return ZOrderByteUtils.interleaveBits(columnBytes, totalBytes, reuse);
    }

    public Set<RowProcessor> constructFunctionMap(List<DataField> fields) {
        Set<RowProcessor> zorderFunctionSet = new LinkedHashSet<>();
        // Construct zorderFunctionSet and fill dataTypes, rowFields
        for (int fieldIndex = 0; fieldIndex < fieldsIndex.length; fieldIndex++) {
            int index = fieldsIndex[fieldIndex];
            DataField field = fields.get(index);
            zorderFunctionSet.add(zmapColumnToCalculator(field, index));
        }
        return zorderFunctionSet;
    }

    public static RowProcessor zmapColumnToCalculator(DataField field, int index) {
        DataType type = field.type();
        return type.accept(new TypeVisitor(index));
    }

    /** Type Visitor to generate function map from row column to z-index. */
    public static class TypeVisitor implements DataTypeVisitor<RowProcessor> {

        private final int fieldIndex;

        public TypeVisitor(int index) {
            this.fieldIndex = index;
        }

        @Override
        public RowProcessor visit(CharType charType) {
            final InternalRow.FieldGetter fieldGetter =
                    InternalRow.createFieldGetter(charType, fieldIndex);
            return new RowProcessor(
                    (row, reuse) -> {
                        Object o = fieldGetter.getFieldOrNull(row);
                        return o == null
                                ? NULL_BYTES
                                : ZOrderByteUtils.stringToOrderedBytes(
                                                o.toString(), PRIMITIVE_BUFFER_SIZE, reuse)
                                        .array();
                    });
        }

        @Override
        public RowProcessor visit(VarCharType varCharType) {
            final InternalRow.FieldGetter fieldGetter =
                    InternalRow.createFieldGetter(varCharType, fieldIndex);
            return new RowProcessor(
                    (row, reuse) -> {
                        Object o = fieldGetter.getFieldOrNull(row);
                        return o == null
                                ? NULL_BYTES
                                : ZOrderByteUtils.stringToOrderedBytes(
                                                o.toString(), PRIMITIVE_BUFFER_SIZE, reuse)
                                        .array();
                    });
        }

        @Override
        public RowProcessor visit(BooleanType booleanType) {
            final InternalRow.FieldGetter fieldGetter =
                    InternalRow.createFieldGetter(booleanType, fieldIndex);
            return new RowProcessor(
                    (row, reuse) -> {
                        Object o = fieldGetter.getFieldOrNull(row);
                        Bytes.reuse(reuse, PRIMITIVE_BUFFER_SIZE);
                        reuse.put(0, (byte) ((boolean) o ? -127 : 0));
                        return reuse.array();
                    });
        }

        @Override
        public RowProcessor visit(BinaryType binaryType) {
            final InternalRow.FieldGetter fieldGetter =
                    InternalRow.createFieldGetter(binaryType, fieldIndex);
            return new RowProcessor(
                    (row, reuse) -> {
                        Object o = fieldGetter.getFieldOrNull(row);
                        return o == null
                                ? NULL_BYTES
                                : ZOrderByteUtils.byteTruncateOrFill(
                                                (byte[]) o, PRIMITIVE_BUFFER_SIZE, reuse)
                                        .array();
                    });
        }

        @Override
        public RowProcessor visit(VarBinaryType varBinaryType) {
            final InternalRow.FieldGetter fieldGetter =
                    InternalRow.createFieldGetter(varBinaryType, fieldIndex);
            return new RowProcessor(
                    (row, reuse) -> {
                        Object o = fieldGetter.getFieldOrNull(row);
                        return o == null
                                ? NULL_BYTES
                                : ZOrderByteUtils.byteTruncateOrFill(
                                                (byte[]) o, PRIMITIVE_BUFFER_SIZE, reuse)
                                        .array();
                    });
        }

        @Override
        public RowProcessor visit(DecimalType decimalType) {
            final InternalRow.FieldGetter fieldGetter =
                    InternalRow.createFieldGetter(decimalType, fieldIndex);
            return new RowProcessor(
                    (row, reuse) -> {
                        Object o = fieldGetter.getFieldOrNull(row);
                        return o == null
                                ? NULL_BYTES
                                : ZOrderByteUtils.byteTruncateOrFill(
                                                ((Decimal) o).toUnscaledBytes(),
                                                PRIMITIVE_BUFFER_SIZE,
                                                reuse)
                                        .array();
                    });
        }

        @Override
        public RowProcessor visit(TinyIntType tinyIntType) {
            final InternalRow.FieldGetter fieldGetter =
                    InternalRow.createFieldGetter(tinyIntType, fieldIndex);
            return new RowProcessor(
                    (row, reuse) -> {
                        Object o = fieldGetter.getFieldOrNull(row);
                        return o == null
                                ? NULL_BYTES
                                : ZOrderByteUtils.tinyintToOrderedBytes((byte) o, reuse).array();
                    });
        }

        @Override
        public RowProcessor visit(SmallIntType smallIntType) {
            final InternalRow.FieldGetter fieldGetter =
                    InternalRow.createFieldGetter(smallIntType, fieldIndex);
            return new RowProcessor(
                    (row, reuse) -> {
                        Object o = fieldGetter.getFieldOrNull(row);
                        return o == null
                                ? NULL_BYTES
                                : ZOrderByteUtils.shortToOrderedBytes((short) o, reuse).array();
                    });
        }

        @Override
        public RowProcessor visit(IntType intType) {
            final InternalRow.FieldGetter fieldGetter =
                    InternalRow.createFieldGetter(intType, fieldIndex);
            return new RowProcessor(
                    (row, reuse) -> {
                        Object o = fieldGetter.getFieldOrNull(row);
                        return o == null
                                ? NULL_BYTES
                                : ZOrderByteUtils.intToOrderedBytes((int) o, reuse).array();
                    });
        }

        @Override
        public RowProcessor visit(BigIntType bigIntType) {
            final InternalRow.FieldGetter fieldGetter =
                    InternalRow.createFieldGetter(bigIntType, fieldIndex);
            return new RowProcessor(
                    (row, reuse) -> {
                        Object o = fieldGetter.getFieldOrNull(row);
                        return o == null
                                ? NULL_BYTES
                                : ZOrderByteUtils.longToOrderedBytes((long) o, reuse).array();
                    });
        }

        @Override
        public RowProcessor visit(FloatType floatType) {
            final InternalRow.FieldGetter fieldGetter =
                    InternalRow.createFieldGetter(floatType, fieldIndex);
            return new RowProcessor(
                    (row, reuse) -> {
                        Object o = fieldGetter.getFieldOrNull(row);
                        return o == null
                                ? NULL_BYTES
                                : ZOrderByteUtils.floatToOrderedBytes((float) o, reuse).array();
                    });
        }

        @Override
        public RowProcessor visit(DoubleType doubleType) {
            final InternalRow.FieldGetter fieldGetter =
                    InternalRow.createFieldGetter(doubleType, fieldIndex);
            return new RowProcessor(
                    (row, reuse) -> {
                        Object o = fieldGetter.getFieldOrNull(row);
                        return o == null
                                ? NULL_BYTES
                                : ZOrderByteUtils.doubleToOrderedBytes((double) o, reuse).array();
                    });
        }

        @Override
        public RowProcessor visit(DateType dateType) {
            final InternalRow.FieldGetter fieldGetter =
                    InternalRow.createFieldGetter(dateType, fieldIndex);
            return new RowProcessor(
                    (row, reuse) -> {
                        Object o = fieldGetter.getFieldOrNull(row);
                        return o == null
                                ? NULL_BYTES
                                : ZOrderByteUtils.intToOrderedBytes((int) o, reuse).array();
                    });
        }

        @Override
        public RowProcessor visit(TimeType timeType) {
            final InternalRow.FieldGetter fieldGetter =
                    InternalRow.createFieldGetter(timeType, fieldIndex);
            return new RowProcessor(
                    (row, reuse) -> {
                        Object o = fieldGetter.getFieldOrNull(row);
                        return o == null
                                ? NULL_BYTES
                                : ZOrderByteUtils.intToOrderedBytes((int) o, reuse).array();
                    });
        }

        @Override
        public RowProcessor visit(TimestampType timestampType) {
            final InternalRow.FieldGetter fieldGetter =
                    InternalRow.createFieldGetter(timestampType, fieldIndex);
            return new RowProcessor(
                    (row, reuse) -> {
                        Object o = fieldGetter.getFieldOrNull(row);
                        return o == null
                                ? NULL_BYTES
                                : ZOrderByteUtils.longToOrderedBytes(
                                                ((Timestamp) o).getMillisecond(), reuse)
                                        .array();
                    });
        }

        @Override
        public RowProcessor visit(LocalZonedTimestampType localZonedTimestampType) {
            final InternalRow.FieldGetter fieldGetter =
                    InternalRow.createFieldGetter(localZonedTimestampType, fieldIndex);
            return new RowProcessor(
                    (row, reuse) -> {
                        Object o = fieldGetter.getFieldOrNull(row);
                        return o == null
                                ? NULL_BYTES
                                : ZOrderByteUtils.longToOrderedBytes(
                                                ((Timestamp) o).getMillisecond(), reuse)
                                        .array();
                    });
        }

        @Override
        public RowProcessor visit(ArrayType arrayType) {
            throw new RuntimeException("Unsupported type");
        }

        @Override
        public RowProcessor visit(MultisetType multisetType) {
            throw new RuntimeException("Unsupported type");
        }

        @Override
        public RowProcessor visit(MapType mapType) {
            throw new RuntimeException("Unsupported type");
        }

        @Override
        public RowProcessor visit(RowType rowType) {
            throw new RuntimeException("Unsupported type");
        }
    }

    /** BaseFunction to convert row field record to devoted bytes. */
    public static class RowProcessor implements Serializable {

        private transient ByteBuffer reuse;
        private final ZProcessFunction process;

        public RowProcessor(ZProcessFunction process) {
            this.process = process;
        }

        public void open() {
            reuse = ByteBuffer.allocate(PRIMITIVE_BUFFER_SIZE);
        }

        public byte[] zvalue(InternalRow o) {
            return process.apply(o, reuse);
        }
    }

    interface ZProcessFunction extends BiFunction<InternalRow, ByteBuffer, byte[]>, Serializable {}
}
