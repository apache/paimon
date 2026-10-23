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

package org.apache.paimon.format.vortex;

import org.apache.paimon.arrow.converter.Arrow2PaimonVectorConverter;
import org.apache.paimon.data.columnar.ByteColumnVector;
import org.apache.paimon.data.columnar.BytesColumnVector;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.DoubleColumnVector;
import org.apache.paimon.data.columnar.FloatColumnVector;
import org.apache.paimon.data.columnar.IntColumnVector;
import org.apache.paimon.data.columnar.LongColumnVector;
import org.apache.paimon.data.columnar.ShortColumnVector;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BitVector;

import java.util.function.Function;

/**
 * Converts the Arrow vectors of Vortex scans to Paimon column vectors.
 *
 * <p>The default converters copy every string and binary value into a new array when it is read.
 * Here, the first read of a column copies its whole data buffer to the heap once, and each value is
 * a slice of that copy. Columns that are never read are never copied.
 *
 * <p>Fixed-width numbers are read straight from the data buffer, without boxing, and their null
 * bits are only checked when the vector holds a null.
 */
final class VortexArrowConverters
        extends Arrow2PaimonVectorConverter.Arrow2PaimonVectorConvertorVisitor {

    static final VortexArrowConverters INSTANCE = new VortexArrowConverters();

    private VortexArrowConverters() {}

    @Override
    public Arrow2PaimonVectorConverter visit(CharType charType) {
        return withHeapBytes(super.visit(charType));
    }

    @Override
    public Arrow2PaimonVectorConverter visit(VarCharType varCharType) {
        return withHeapBytes(super.visit(varCharType));
    }

    @Override
    public Arrow2PaimonVectorConverter visit(BinaryType binaryType) {
        return withHeapBytes(super.visit(binaryType));
    }

    @Override
    public Arrow2PaimonVectorConverter visit(VarBinaryType varBinaryType) {
        return withHeapBytes(super.visit(varBinaryType));
    }

    @Override
    public Arrow2PaimonVectorConverter visit(TinyIntType tinyIntType) {
        return fixedWidth(super.visit(tinyIntType), ByteColumn::new);
    }

    @Override
    public Arrow2PaimonVectorConverter visit(SmallIntType smallIntType) {
        return fixedWidth(super.visit(smallIntType), ShortColumn::new);
    }

    @Override
    public Arrow2PaimonVectorConverter visit(IntType intType) {
        return fixedWidth(super.visit(intType), IntColumn::new);
    }

    @Override
    public Arrow2PaimonVectorConverter visit(BigIntType bigIntType) {
        return fixedWidth(super.visit(bigIntType), LongColumn::new);
    }

    @Override
    public Arrow2PaimonVectorConverter visit(FloatType floatType) {
        return fixedWidth(super.visit(floatType), FloatColumn::new);
    }

    @Override
    public Arrow2PaimonVectorConverter visit(DoubleType doubleType) {
        return fixedWidth(super.visit(doubleType), DoubleColumn::new);
    }

    /**
     * Uses {@code direct} for plain fixed-width vectors and {@code fallback} for anything else,
     * such as dictionary-encoded or extension vectors.
     */
    private static Arrow2PaimonVectorConverter fixedWidth(
            Arrow2PaimonVectorConverter fallback,
            Function<BaseFixedWidthVector, ColumnVector> direct) {
        return vector ->
                vector instanceof BaseFixedWidthVector && !(vector instanceof BitVector)
                        ? direct.apply((BaseFixedWidthVector) vector)
                        : fallback.convertVector(vector);
    }

    /** Null checks of a fixed-width vector that skip the validity bits when there are no nulls. */
    private abstract static class NullAware implements ColumnVector {

        protected final ArrowBuf data;
        private final BaseFixedWidthVector vector;
        private final boolean hasNulls;

        NullAware(BaseFixedWidthVector vector) {
            this.vector = vector;
            this.data = vector.getDataBuffer();
            this.hasNulls = vector.getNullCount() > 0;
        }

        @Override
        public boolean isNullAt(int index) {
            return hasNulls && vector.isNull(index);
        }
    }

    private static Arrow2PaimonVectorConverter withHeapBytes(Arrow2PaimonVectorConverter fallback) {
        return vector ->
                vector instanceof BaseVariableWidthVector
                        ? new HeapBytesColumn((BaseVariableWidthVector) vector)
                        : fallback.convertVector(vector);
    }

    /** A string or binary column whose values are slices of one heap copy of its data. */
    private static final class HeapBytesColumn implements BytesColumnVector {

        private final BaseVariableWidthVector vector;
        private final ArrowBuf offsets;
        private byte[] data;
        private int base;

        private HeapBytesColumn(BaseVariableWidthVector vector) {
            this.vector = vector;
            this.offsets = vector.getOffsetBuffer();
        }

        @Override
        public boolean isNullAt(int index) {
            return vector.isNull(index);
        }

        @Override
        public Bytes getBytes(int index) {
            if (data == null) {
                copyData();
            }
            int start = offsets.getInt((long) index * BaseVariableWidthVector.OFFSET_WIDTH);
            int end = offsets.getInt((long) (index + 1) * BaseVariableWidthVector.OFFSET_WIDTH);
            return new Bytes(data, start - base, end - start);
        }

        private void copyData() {
            int valueCount = vector.getValueCount();
            if (valueCount == 0) {
                data = new byte[0];
                return;
            }
            base = offsets.getInt(0);
            int end = offsets.getInt((long) valueCount * BaseVariableWidthVector.OFFSET_WIDTH);
            byte[] copy = new byte[end - base];
            vector.getDataBuffer().getBytes(base, copy);
            data = copy;
        }
    }

    private static final class ByteColumn extends NullAware implements ByteColumnVector {

        private ByteColumn(BaseFixedWidthVector vector) {
            super(vector);
        }

        @Override
        public byte getByte(int index) {
            return data.getByte(index);
        }
    }

    private static final class ShortColumn extends NullAware implements ShortColumnVector {

        private ShortColumn(BaseFixedWidthVector vector) {
            super(vector);
        }

        @Override
        public short getShort(int index) {
            return data.getShort((long) index * Short.BYTES);
        }
    }

    private static final class IntColumn extends NullAware implements IntColumnVector {

        private IntColumn(BaseFixedWidthVector vector) {
            super(vector);
        }

        @Override
        public int getInt(int index) {
            return data.getInt((long) index * Integer.BYTES);
        }
    }

    private static final class LongColumn extends NullAware implements LongColumnVector {

        private LongColumn(BaseFixedWidthVector vector) {
            super(vector);
        }

        @Override
        public long getLong(int index) {
            return data.getLong((long) index * Long.BYTES);
        }
    }

    private static final class FloatColumn extends NullAware implements FloatColumnVector {

        private FloatColumn(BaseFixedWidthVector vector) {
            super(vector);
        }

        @Override
        public float getFloat(int index) {
            return data.getFloat((long) index * Float.BYTES);
        }
    }

    private static final class DoubleColumn extends NullAware implements DoubleColumnVector {

        private DoubleColumn(BaseFixedWidthVector vector) {
            super(vector);
        }

        @Override
        public double getDouble(int index) {
            return data.getDouble((long) index * Double.BYTES);
        }
    }
}
