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
import org.apache.paimon.utils.ByteBuffers;
import org.apache.paimon.utils.ZOrderByteUtils;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static org.apache.paimon.utils.ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE;

/** Z-indexer for responsibility to generate z-index. */
public class ZIndexer implements Serializable {

    private final Set<ZorderBaseFunction> functionSet;
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
        functionSet.forEach(ZorderBaseFunction::open);
    }

    public int size() {
        return totalBytes;
    }

    public byte[] index(InternalRow row) {
        byte[][] columnBytes = new byte[fieldsIndex.length][];

        int index = 0;
        for (ZorderBaseFunction f : functionSet) {
            columnBytes[index++] = f.apply(row);
        }

        return ZOrderByteUtils.interleaveBits(columnBytes, totalBytes, reuse);
    }

    public Set<ZorderBaseFunction> constructFunctionMap(List<DataField> fields) {
        Set<ZorderBaseFunction> zorderFunctionSet = new LinkedHashSet<>();
        // Construct zorderFunctionSet and fill dataTypes, rowFields
        for (int fieldIndex = 0; fieldIndex < fieldsIndex.length; fieldIndex++) {
            int index = fieldsIndex[fieldIndex];
            DataField field = fields.get(index);
            zorderFunctionSet.add(zmapColumnToCalculator(field).setPosition(index));
        }
        return zorderFunctionSet;
    }

    public static ZorderBaseFunction zmapColumnToCalculator(DataField field) {
        DataType type = field.type();
        return type.accept(new TypeVisitor());
    }

    /** Type Visitor to generate function map from row column to z-index. */
    public static class TypeVisitor implements DataTypeVisitor<ZorderBaseFunction> {

        @Override
        public ZorderBaseFunction visit(CharType charType) {
            return new ZorderStringFunction();
        }

        @Override
        public ZorderBaseFunction visit(VarCharType varCharType) {
            return new ZorderStringFunction();
        }

        @Override
        public ZorderBaseFunction visit(BooleanType booleanType) {
            return new ZorderBooleanFunction();
        }

        @Override
        public ZorderBaseFunction visit(BinaryType binaryType) {
            return new ZorderBytesFunction();
        }

        @Override
        public ZorderBaseFunction visit(VarBinaryType varBinaryType) {
            return new ZorderBytesFunction();
        }

        @Override
        public ZorderBaseFunction visit(DecimalType decimalType) {
            return new ZorderDecimalFunction(decimalType.getPrecision(), decimalType.getScale());
        }

        @Override
        public ZorderBaseFunction visit(TinyIntType tinyIntType) {
            return new ZorderTinyIntFunction();
        }

        @Override
        public ZorderBaseFunction visit(SmallIntType smallIntType) {
            return new ZorderShortFunction();
        }

        @Override
        public ZorderBaseFunction visit(IntType intType) {
            return new ZorderIntFunction();
        }

        @Override
        public ZorderBaseFunction visit(BigIntType bigIntType) {
            return new ZorderLongFunction();
        }

        @Override
        public ZorderBaseFunction visit(FloatType floatType) {
            return new ZorderFloatFunction();
        }

        @Override
        public ZorderBaseFunction visit(DoubleType doubleType) {
            return new ZorderDoubleFunction();
        }

        @Override
        public ZorderBaseFunction visit(DateType dateType) {
            return new ZorderDateFunction();
        }

        @Override
        public ZorderBaseFunction visit(TimeType timeType) {
            return new ZorderTimeFunction();
        }

        @Override
        public ZorderBaseFunction visit(TimestampType timestampType) {
            return new ZorderTimestampFunction(timestampType.getPrecision());
        }

        @Override
        public ZorderBaseFunction visit(LocalZonedTimestampType localZonedTimestampType) {
            return new ZorderTimestampFunction(localZonedTimestampType.getPrecision());
        }

        @Override
        public ZorderBaseFunction visit(ArrayType arrayType) {
            throw new RuntimeException("Unsupported type");
        }

        @Override
        public ZorderBaseFunction visit(MultisetType multisetType) {
            throw new RuntimeException("Unsupported type");
        }

        @Override
        public ZorderBaseFunction visit(MapType mapType) {
            throw new RuntimeException("Unsupported type");
        }

        @Override
        public ZorderBaseFunction visit(RowType rowType) {
            throw new RuntimeException("Unsupported type");
        }
    }

    /** BaseFunction to convert row field record to devoted bytes. */
    public abstract static class ZorderBaseFunction
            implements Function<InternalRow, byte[]>, Serializable {

        protected int position;
        protected transient ByteBuffer reuse;

        public void open() {
            reuse = ByteBuffer.allocate(PRIMITIVE_BUFFER_SIZE);
        }

        public ZorderBaseFunction setPosition(int position) {
            this.position = position;
            return this;
        }
    }

    /** Function used for Type Byte(TinyInt). */
    public static class ZorderTinyIntFunction extends ZorderBaseFunction {

        public byte[] apply(InternalRow b) {
            return ZOrderByteUtils.tinyintToOrderedBytes(b.getByte(position), reuse).array();
        }
    }

    /** Function used for Type Short. */
    public static class ZorderShortFunction extends ZorderBaseFunction {

        public byte[] apply(InternalRow s) {
            return ZOrderByteUtils.shortToOrderedBytes(s.getShort(position), reuse).array();
        }
    }

    /** Function used for Type Int. */
    public static class ZorderIntFunction extends ZorderBaseFunction {

        public byte[] apply(InternalRow i) {
            return ZOrderByteUtils.intToOrderedBytes(i.getInt(position), reuse).array();
        }
    }

    /** Function used for Type String. */
    public static class ZorderStringFunction extends ZorderBaseFunction {

        public byte[] apply(InternalRow s) {
            return ZOrderByteUtils.stringToOrderedBytes(
                            s.getString(position).toString(),
                            PRIMITIVE_BUFFER_SIZE,
                            reuse,
                            StandardCharsets.UTF_8.newEncoder())
                    .array();
        }
    }

    /** Function used for Type Long. */
    public static class ZorderLongFunction extends ZorderBaseFunction {

        public byte[] apply(InternalRow val) {
            return ZOrderByteUtils.longToOrderedBytes(val.getLong(position), reuse).array();
        }
    }

    /** Function used for Type Date. */
    public static class ZorderDateFunction extends ZorderBaseFunction {

        public byte[] apply(InternalRow val) {
            Long date = val.getLong(position);
            return ZOrderByteUtils.longToOrderedBytes(date, reuse).array();
        }
    }

    /** Function used for Type Time. */
    public static class ZorderTimeFunction extends ZorderBaseFunction {

        public byte[] apply(InternalRow val) {
            Long date = val.getLong(position);
            return ZOrderByteUtils.longToOrderedBytes(date, reuse).array();
        }
    }

    /** Function used for Type Timestamp. */
    public static class ZorderTimestampFunction extends ZorderBaseFunction {

        private final int precision;

        public ZorderTimestampFunction(int precision) {
            this.precision = precision;
        }

        public byte[] apply(InternalRow val) {
            Timestamp time = val.getTimestamp(position, precision);
            return ZOrderByteUtils.longToOrderedBytes(time.getMillisecond(), reuse).array();
        }
    }

    /** Function used for Type Float. */
    public static class ZorderFloatFunction extends ZorderBaseFunction {

        public byte[] apply(InternalRow f) {
            return ZOrderByteUtils.floatToOrderedBytes(f.getFloat(position), reuse).array();
        }
    }

    /** Function used for Type Double. */
    public static class ZorderDoubleFunction extends ZorderBaseFunction {

        public byte[] apply(InternalRow d) {
            return ZOrderByteUtils.doubleToOrderedBytes(d.getDouble(position), reuse).array();
        }
    }

    /** Function used for Type Decimal. */
    public static class ZorderDecimalFunction extends ZorderBaseFunction {

        private int precise;
        private int scale;

        public ZorderDecimalFunction(int precise, int scale) {
            this.precise = precise;
            this.scale = scale;
        }

        public byte[] apply(InternalRow b) {
            return ZOrderByteUtils.doubleToOrderedBytes(
                            b.getDecimal(position, precise, scale).toUnscaledLong(), reuse)
                    .array();
        }
    }

    /** Function used for Type Boolean. */
    public static class ZorderBooleanFunction extends ZorderBaseFunction {

        public byte[] apply(InternalRow b) {
            reuse = ByteBuffers.reuse(reuse, PRIMITIVE_BUFFER_SIZE);
            reuse.put(0, (byte) (b.getBoolean(position) ? -127 : 0));
            return reuse.array();
        }
    }

    /** Function used for Type Bytes. */
    public static class ZorderBytesFunction extends ZorderBaseFunction {

        public byte[] apply(InternalRow bytes) {
            return ZOrderByteUtils.byteTruncateOrFill(
                            bytes.getBinary(position), PRIMITIVE_BUFFER_SIZE, reuse)
                    .array();
        }
    }
}
