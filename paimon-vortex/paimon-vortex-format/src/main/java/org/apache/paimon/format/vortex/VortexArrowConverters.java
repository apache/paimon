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
import org.apache.paimon.data.columnar.BytesColumnVector;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BaseVariableWidthVector;

/**
 * Converts the Arrow vectors of Vortex scans to Paimon column vectors.
 *
 * <p>The default converters copy every string and binary value into a new array when it is read.
 * Here, the first read of a column copies its whole data buffer to the heap once, and each value is
 * a slice of that copy. Columns that are never read are never copied.
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
}
