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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.memory.MemorySegmentUtils;
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
import org.apache.paimon.types.VariantType;
import org.apache.paimon.utils.ZOrderByteUtils;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
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
        this(rowType, orderColumns, PRIMITIVE_BUFFER_SIZE);
    }

    public ZIndexer(RowType rowType, List<String> orderColumns, int varTypeSize) {
        List<String> fields = rowType.getFieldNames();
        fieldsIndex = new int[orderColumns.size()];
        int varTypeCount = 0;
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

            if (isVarType(rowType.getFieldTypes().get(index))) {
                varTypeCount++;
            }
        }
        this.functionSet = constructFunctionMap(rowType.getFields(), varTypeSize);
        this.totalBytes =
                PRIMITIVE_BUFFER_SIZE * (this.fieldsIndex.length - varTypeCount)
                        + varTypeSize * varTypeCount;
    }

    private static boolean isVarType(DataType dataType) {
        return dataType instanceof CharType
                || dataType instanceof VarCharType
                || dataType instanceof BinaryType
                || dataType instanceof VarBinaryType;
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

    public Set<RowProcessor> constructFunctionMap(List<DataField> fields, int varTypeSize) {
        Set<RowProcessor> zorderFunctionSet = new LinkedHashSet<>();
        // Construct zorderFunctionSet and fill dataTypes, rowFields
        for (int index : fieldsIndex) {
            DataField field = fields.get(index);
            zorderFunctionSet.add(zmapColumnToCalculator(field, index, varTypeSize));
        }
        return zorderFunctionSet;
    }

    public static RowProcessor zmapColumnToCalculator(DataField field, int index, int varTypeSize) {
        DataType type = field.type();
        return new RowProcessor(
                type.accept(new TypeVisitor(index, varTypeSize)),
                isVarType(type) ? varTypeSize : PRIMITIVE_BUFFER_SIZE);
    }

    /** Type Visitor to generate function map from row column to z-index. */
    public static class TypeVisitor implements DataTypeVisitor<ZProcessFunction>, Serializable {

        private final int fieldIndex;
        private final int varTypeSize;

        private final byte[] nullVarBytes;

        public TypeVisitor(int index, int varTypeSize) {
            this.fieldIndex = index;
            this.varTypeSize = varTypeSize;

            if (varTypeSize == PRIMITIVE_BUFFER_SIZE) {
                nullVarBytes = NULL_BYTES;
            } else {
                nullVarBytes = new byte[varTypeSize];
                Arrays.fill(nullVarBytes, (byte) 0x00);
            }
        }

        @Override
        public ZProcessFunction visit(CharType charType) {
            return (row, reuse) -> {
                if (row.isNullAt(fieldIndex)) {
                    return nullVarBytes;
                } else {
                    BinaryString binaryString = row.getString(fieldIndex);

                    return ZOrderByteUtils.byteTruncateOrFill(
                                    MemorySegmentUtils.getBytes(
                                            binaryString.getSegments(),
                                            binaryString.getOffset(),
                                            Math.min(varTypeSize, binaryString.getSizeInBytes())),
                                    varTypeSize,
                                    reuse)
                            .array();
                }
            };
        }

        @Override
        public ZProcessFunction visit(VarCharType varCharType) {
            return (row, reuse) -> {
                if (row.isNullAt(fieldIndex)) {
                    return nullVarBytes;
                } else {
                    BinaryString binaryString = row.getString(fieldIndex);

                    return ZOrderByteUtils.byteTruncateOrFill(
                                    MemorySegmentUtils.getBytes(
                                            binaryString.getSegments(),
                                            binaryString.getOffset(),
                                            Math.min(varTypeSize, binaryString.getSizeInBytes())),
                                    varTypeSize,
                                    reuse)
                            .array();
                }
            };
        }

        @Override
        public ZProcessFunction visit(BooleanType booleanType) {
            return (row, reuse) -> {
                if (row.isNullAt(fieldIndex)) {
                    return NULL_BYTES;
                }
                ZOrderByteUtils.reuse(reuse, PRIMITIVE_BUFFER_SIZE);
                reuse.put(0, (byte) (row.getBoolean(fieldIndex) ? -127 : 0));
                return reuse.array();
            };
        }

        @Override
        public ZProcessFunction visit(BinaryType binaryType) {
            return (row, reuse) ->
                    row.isNullAt(fieldIndex)
                            ? nullVarBytes
                            : ZOrderByteUtils.byteTruncateOrFill(
                                            row.getBinary(fieldIndex), varTypeSize, reuse)
                                    .array();
        }

        @Override
        public ZProcessFunction visit(VarBinaryType varBinaryType) {
            return (row, reuse) ->
                    row.isNullAt(fieldIndex)
                            ? nullVarBytes
                            : ZOrderByteUtils.byteTruncateOrFill(
                                            row.getBinary(fieldIndex), varTypeSize, reuse)
                                    .array();
        }

        @Override
        public ZProcessFunction visit(DecimalType decimalType) {
            final InternalRow.FieldGetter fieldGetter =
                    InternalRow.createFieldGetter(decimalType, fieldIndex);
            return (row, reuse) -> {
                Object o = fieldGetter.getFieldOrNull(row);
                return o == null
                        ? NULL_BYTES
                        : ZOrderByteUtils.byteTruncateOrFill(
                                        ((Decimal) o).toUnscaledBytes(),
                                        PRIMITIVE_BUFFER_SIZE,
                                        reuse)
                                .array();
            };
        }

        @Override
        public ZProcessFunction visit(TinyIntType tinyIntType) {
            return (row, reuse) ->
                    row.isNullAt(fieldIndex)
                            ? NULL_BYTES
                            : ZOrderByteUtils.tinyintToOrderedBytes(row.getByte(fieldIndex), reuse)
                                    .array();
        }

        @Override
        public ZProcessFunction visit(SmallIntType smallIntType) {
            return (row, reuse) ->
                    row.isNullAt(fieldIndex)
                            ? NULL_BYTES
                            : ZOrderByteUtils.shortToOrderedBytes(row.getShort(fieldIndex), reuse)
                                    .array();
        }

        @Override
        public ZProcessFunction visit(IntType intType) {
            return (row, reuse) ->
                    row.isNullAt(fieldIndex)
                            ? NULL_BYTES
                            : ZOrderByteUtils.intToOrderedBytes(row.getInt(fieldIndex), reuse)
                                    .array();
        }

        @Override
        public ZProcessFunction visit(BigIntType bigIntType) {
            return (row, reuse) ->
                    row.isNullAt(fieldIndex)
                            ? NULL_BYTES
                            : ZOrderByteUtils.longToOrderedBytes(row.getLong(fieldIndex), reuse)
                                    .array();
        }

        @Override
        public ZProcessFunction visit(FloatType floatType) {
            return (row, reuse) ->
                    row.isNullAt(fieldIndex)
                            ? NULL_BYTES
                            : ZOrderByteUtils.floatToOrderedBytes(row.getFloat(fieldIndex), reuse)
                                    .array();
        }

        @Override
        public ZProcessFunction visit(DoubleType doubleType) {
            return (row, reuse) ->
                    row.isNullAt(fieldIndex)
                            ? NULL_BYTES
                            : ZOrderByteUtils.doubleToOrderedBytes(row.getDouble(fieldIndex), reuse)
                                    .array();
        }

        @Override
        public ZProcessFunction visit(DateType dateType) {
            return (row, reuse) ->
                    row.isNullAt(fieldIndex)
                            ? NULL_BYTES
                            : ZOrderByteUtils.intToOrderedBytes(row.getInt(fieldIndex), reuse)
                                    .array();
        }

        @Override
        public ZProcessFunction visit(TimeType timeType) {
            return (row, reuse) ->
                    row.isNullAt(fieldIndex)
                            ? NULL_BYTES
                            : ZOrderByteUtils.intToOrderedBytes(row.getInt(fieldIndex), reuse)
                                    .array();
        }

        @Override
        public ZProcessFunction visit(TimestampType timestampType) {
            final InternalRow.FieldGetter fieldGetter =
                    InternalRow.createFieldGetter(timestampType, fieldIndex);
            return (row, reuse) -> {
                Object o = fieldGetter.getFieldOrNull(row);
                return o == null
                        ? NULL_BYTES
                        : ZOrderByteUtils.longToOrderedBytes(
                                        ((Timestamp) o).getMillisecond(), reuse)
                                .array();
            };
        }

        @Override
        public ZProcessFunction visit(LocalZonedTimestampType localZonedTimestampType) {
            final InternalRow.FieldGetter fieldGetter =
                    InternalRow.createFieldGetter(localZonedTimestampType, fieldIndex);
            return (row, reuse) -> {
                Object o = fieldGetter.getFieldOrNull(row);
                return o == null
                        ? NULL_BYTES
                        : ZOrderByteUtils.longToOrderedBytes(
                                        ((Timestamp) o).getMillisecond(), reuse)
                                .array();
            };
        }

        @Override
        public ZProcessFunction visit(VariantType variantType) {
            throw new RuntimeException("Unsupported type");
        }

        @Override
        public ZProcessFunction visit(ArrayType arrayType) {
            throw new RuntimeException("Unsupported type");
        }

        @Override
        public ZProcessFunction visit(MultisetType multisetType) {
            throw new RuntimeException("Unsupported type");
        }

        @Override
        public ZProcessFunction visit(MapType mapType) {
            throw new RuntimeException("Unsupported type");
        }

        @Override
        public ZProcessFunction visit(RowType rowType) {
            throw new RuntimeException("Unsupported type");
        }
    }

    /** Be used as converting row field record to devoted bytes. */
    public static class RowProcessor implements Serializable {

        private transient ByteBuffer reuse;
        private final ZProcessFunction process;
        private final int byteSize;

        public RowProcessor(ZProcessFunction process, int byteSize) {
            this.process = process;
            this.byteSize = byteSize;
        }

        public void open() {
            reuse = ByteBuffer.allocate(byteSize);
        }

        public byte[] zvalue(InternalRow o) {
            return process.apply(o, reuse);
        }
    }

    /** Process function interface. */
    public interface ZProcessFunction
            extends BiFunction<InternalRow, ByteBuffer, byte[]>, Serializable {}
}
