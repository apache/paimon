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

package org.apache.paimon.sort.hilbert;

import org.apache.paimon.data.BinaryString;
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
import org.apache.paimon.types.VariantType;
import org.apache.paimon.utils.ConvertBinaryUtil;

import org.davidmoten.hilbert.HilbertCurve;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Hilbert indexer for responsibility to generate hilbert-index. */
public class HilbertIndexer implements Serializable {

    private static final long PRIMITIVE_EMPTY = Long.MAX_VALUE;
    private static final int BITS_NUM = 63;

    private final Set<RowProcessor> functionSet;
    private final int[] fieldsIndex;

    public HilbertIndexer(RowType rowType, List<String> orderColumns) {
        checkArgument(orderColumns.size() > 1, "Hilbert sort needs at least two columns.");
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
    }

    public void open() {
        functionSet.forEach(RowProcessor::open);
    }

    public byte[] index(InternalRow row) {
        Long[] columnLongs = new Long[fieldsIndex.length];

        int index = 0;
        for (RowProcessor f : functionSet) {
            columnLongs[index++] = f.hilbertValue(row);
        }
        return hilbertCurvePosBytes(columnLongs);
    }

    public Set<RowProcessor> constructFunctionMap(List<DataField> fields) {
        Set<RowProcessor> hilbertFunctionSet = new LinkedHashSet<>();

        // Construct hilbertFunctionSet and fill dataTypes, rowFields
        for (int index : fieldsIndex) {
            DataField field = fields.get(index);
            hilbertFunctionSet.add(hmapColumnToCalculator(field, index));
        }
        return hilbertFunctionSet;
    }

    public static RowProcessor hmapColumnToCalculator(DataField field, int index) {
        DataType type = field.type();
        return new RowProcessor(type.accept(new TypeVisitor(index)));
    }

    /** Type Visitor to generate function map from row column to hilbert-index. */
    public static class TypeVisitor implements DataTypeVisitor<HProcessFunction>, Serializable {

        private final int fieldIndex;

        public TypeVisitor(int index) {
            this.fieldIndex = index;
        }

        @Override
        public HProcessFunction visit(CharType charType) {
            return (row) -> {
                if (row.isNullAt(fieldIndex)) {
                    return PRIMITIVE_EMPTY;
                } else {
                    BinaryString binaryString = row.getString(fieldIndex);

                    return ConvertBinaryUtil.convertBytesToLong(binaryString.toBytes());
                }
            };
        }

        @Override
        public HProcessFunction visit(VarCharType varCharType) {
            return (row) -> {
                if (row.isNullAt(fieldIndex)) {
                    return PRIMITIVE_EMPTY;
                } else {
                    BinaryString binaryString = row.getString(fieldIndex);

                    return ConvertBinaryUtil.convertBytesToLong(binaryString.toBytes());
                }
            };
        }

        @Override
        public HProcessFunction visit(BooleanType booleanType) {
            return (row) -> {
                if (row.isNullAt(fieldIndex)) {
                    return PRIMITIVE_EMPTY;
                }
                return row.getBoolean(fieldIndex) ? PRIMITIVE_EMPTY : 0;
            };
        }

        @Override
        public HProcessFunction visit(BinaryType binaryType) {
            return (row) ->
                    row.isNullAt(fieldIndex)
                            ? PRIMITIVE_EMPTY
                            : ConvertBinaryUtil.convertBytesToLong(row.getBinary(fieldIndex));
        }

        @Override
        public HProcessFunction visit(VarBinaryType varBinaryType) {
            return (row) ->
                    row.isNullAt(fieldIndex)
                            ? PRIMITIVE_EMPTY
                            : ConvertBinaryUtil.convertBytesToLong(row.getBinary(fieldIndex));
        }

        @Override
        public HProcessFunction visit(DecimalType decimalType) {
            final InternalRow.FieldGetter fieldGetter =
                    InternalRow.createFieldGetter(decimalType, fieldIndex);
            return (row) -> {
                Object o = fieldGetter.getFieldOrNull(row);
                return o == null ? PRIMITIVE_EMPTY : ((Decimal) o).toBigDecimal().longValue();
            };
        }

        @Override
        public HProcessFunction visit(TinyIntType tinyIntType) {
            return (row) ->
                    row.isNullAt(fieldIndex)
                            ? PRIMITIVE_EMPTY
                            : ConvertBinaryUtil.convertBytesToLong(
                                    new byte[] {row.getByte(fieldIndex)});
        }

        @Override
        public HProcessFunction visit(SmallIntType smallIntType) {
            return (row) ->
                    row.isNullAt(fieldIndex) ? PRIMITIVE_EMPTY : (long) row.getShort(fieldIndex);
        }

        @Override
        public HProcessFunction visit(IntType intType) {
            return (row) ->
                    row.isNullAt(fieldIndex) ? PRIMITIVE_EMPTY : (long) row.getInt(fieldIndex);
        }

        @Override
        public HProcessFunction visit(BigIntType bigIntType) {
            return (row) -> row.isNullAt(fieldIndex) ? PRIMITIVE_EMPTY : row.getLong(fieldIndex);
        }

        @Override
        public HProcessFunction visit(FloatType floatType) {
            return (row) ->
                    row.isNullAt(fieldIndex)
                            ? PRIMITIVE_EMPTY
                            : Double.doubleToLongBits(row.getFloat(fieldIndex));
        }

        @Override
        public HProcessFunction visit(DoubleType doubleType) {
            return (row) ->
                    row.isNullAt(fieldIndex)
                            ? PRIMITIVE_EMPTY
                            : Double.doubleToLongBits(row.getDouble(fieldIndex));
        }

        @Override
        public HProcessFunction visit(DateType dateType) {
            return (row) -> row.isNullAt(fieldIndex) ? PRIMITIVE_EMPTY : row.getInt(fieldIndex);
        }

        @Override
        public HProcessFunction visit(TimeType timeType) {
            return (row) -> row.isNullAt(fieldIndex) ? PRIMITIVE_EMPTY : row.getInt(fieldIndex);
        }

        @Override
        public HProcessFunction visit(TimestampType timestampType) {
            final InternalRow.FieldGetter fieldGetter =
                    InternalRow.createFieldGetter(timestampType, fieldIndex);
            return (row) -> {
                Object o = fieldGetter.getFieldOrNull(row);
                return o == null ? PRIMITIVE_EMPTY : ((Timestamp) o).getMillisecond();
            };
        }

        @Override
        public HProcessFunction visit(LocalZonedTimestampType localZonedTimestampType) {
            final InternalRow.FieldGetter fieldGetter =
                    InternalRow.createFieldGetter(localZonedTimestampType, fieldIndex);
            return (row) -> {
                Object o = fieldGetter.getFieldOrNull(row);
                return o == null ? PRIMITIVE_EMPTY : ((Timestamp) o).getMillisecond();
            };
        }

        @Override
        public HProcessFunction visit(VariantType variantType) {
            throw new RuntimeException("Unsupported type");
        }

        @Override
        public HProcessFunction visit(ArrayType arrayType) {
            throw new RuntimeException("Unsupported type");
        }

        @Override
        public HProcessFunction visit(MultisetType multisetType) {
            throw new RuntimeException("Unsupported type");
        }

        @Override
        public HProcessFunction visit(MapType mapType) {
            throw new RuntimeException("Unsupported type");
        }

        @Override
        public HProcessFunction visit(RowType rowType) {
            throw new RuntimeException("Unsupported type");
        }
    }

    /** Be used as converting row field record to devoted bytes. */
    public static class RowProcessor implements Serializable {
        private final HProcessFunction process;

        public RowProcessor(HProcessFunction process) {
            this.process = process;
        }

        public void open() {}

        public Long hilbertValue(InternalRow o) {
            return process.apply(o);
        }
    }

    public static byte[] hilbertCurvePosBytes(Long[] points) {
        long[] data = Arrays.stream(points).mapToLong(Long::longValue).toArray();
        HilbertCurve hilbertCurve = HilbertCurve.bits(BITS_NUM).dimensions(points.length);
        BigInteger index = hilbertCurve.index(data);
        return ConvertBinaryUtil.paddingToNByte(index.toByteArray(), BITS_NUM);
    }

    /** Process function interface. */
    public interface HProcessFunction extends Function<InternalRow, Long>, Serializable {}
}
