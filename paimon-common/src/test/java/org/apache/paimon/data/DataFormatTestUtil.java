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

package org.apache.paimon.data;

import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.StringUtils;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Utils for testing data formats. */
public class DataFormatTestUtil {

    /** Stringify the given {@link InternalRow}. */
    public static String toStringNoRowKind(InternalRow row, RowType type) {
        StringBuilder build = new StringBuilder();
        for (int i = 0; i < type.getFieldCount(); i++) {
            if (i != 0) {
                build.append(", ");
            }
            if (row.isNullAt(i)) {
                build.append("NULL");
            } else {
                InternalRow.FieldGetter fieldGetter =
                        InternalRow.createFieldGetter(type.getTypeAt(i), i);
                Object field = fieldGetter.getFieldOrNull(row);
                if (field instanceof byte[]) {
                    build.append(Arrays.toString((byte[]) field));
                } else if (field instanceof InternalArray) {
                    InternalArray internalArray = (InternalArray) field;
                    ArrayType arrayType = (ArrayType) type.getTypeAt(i);
                    InternalArray.ElementGetter elementGetter =
                            InternalArray.createElementGetter(arrayType.getElementType());
                    String[] result = new String[internalArray.size()];
                    for (int j = 0; j < internalArray.size(); j++) {
                        Object object = elementGetter.getElementOrNull(internalArray, j);
                        result[j] = null == object ? null : object.toString();
                    }
                    build.append(Arrays.toString(result));
                } else {
                    build.append(field);
                }
            }
        }
        return build.toString();
    }

    /** Stringify the given {@link InternalRow}. */
    public static String internalRowToString(InternalRow row, RowType type) {
        return row.getRowKind().shortString() + "[" + toStringNoRowKind(row, type) + ']';
    }

    /** Get a binary row of 24 bytes long. */
    public static BinaryRow get24BytesBinaryRow() {
        // header (8 bytes) + 2 * string in fixed-length part (8 bytes each)
        BinaryRow row = new BinaryRow(2);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeString(0, BinaryString.fromString(StringUtils.randomNumericString(2)));
        writer.writeString(1, BinaryString.fromString(StringUtils.randomNumericString(2)));
        writer.complete();
        return row;
    }

    /** Get a binary row of 160 bytes long. */
    public static BinaryRow get160BytesBinaryRow() {
        // header (8 bytes) +
        // 72 byte length string (8 bytes in fixed-length, 72 bytes in variable-length) +
        // 64 byte length string (8 bytes in fixed-length, 64 bytes in variable-length)
        BinaryRow row = new BinaryRow(2);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeString(0, BinaryString.fromString(StringUtils.randomNumericString(72)));
        writer.writeString(1, BinaryString.fromString(StringUtils.randomNumericString(64)));
        writer.complete();
        return row;
    }

    /**
     * Get a binary row consisting of 6 segments. The bytes of the returned row is the same with the
     * given input binary row.
     */
    public static BinaryRow getMultiSeg160BytesBinaryRow(BinaryRow row160) {
        BinaryRow multiSegRow160 = new BinaryRow(2);
        MemorySegment[] segments = new MemorySegment[6];
        int baseOffset = 8;
        int posInSeg = baseOffset;
        int remainSize = 160;
        for (int i = 0; i < segments.length; i++) {
            segments[i] = MemorySegment.wrap(new byte[32]);
            int copy = Math.min(32 - posInSeg, remainSize);
            row160.getSegments()[0].copyTo(160 - remainSize, segments[i], posInSeg, copy);
            remainSize -= copy;
            posInSeg = 0;
        }
        multiSegRow160.pointTo(segments, baseOffset, 160);
        assertThat(multiSegRow160).isEqualTo(row160);
        return multiSegRow160;
    }

    /**
     * Get a binary row consisting of 2 segments. Its first segment is the same with the given input
     * binary row, while its second segment is empty.
     */
    public static BinaryRow getMultiSeg160BytesInOneSegRow(BinaryRow row160) {
        MemorySegment[] segments = new MemorySegment[2];
        segments[0] = row160.getSegments()[0];
        segments[1] = MemorySegment.wrap(new byte[row160.getSegments()[0].size()]);
        row160.pointTo(segments, 0, row160.getSizeInBytes());
        return row160;
    }

    /** Split the given byte array into two memory segments. */
    public static MemorySegment[] splitBytes(byte[] bytes, int baseOffset) {
        int newSize = (bytes.length + 1) / 2 + baseOffset;
        MemorySegment[] ret = new MemorySegment[2];
        ret[0] = MemorySegment.wrap(new byte[newSize]);
        ret[1] = MemorySegment.wrap(new byte[newSize]);

        ret[0].put(baseOffset, bytes, 0, newSize - baseOffset);
        ret[1].put(0, bytes, newSize - baseOffset, bytes.length - (newSize - baseOffset));
        return ret;
    }

    /** A simple class for testing generic type getting / setting on data formats. */
    public static class MyObj {
        public int i;
        public double j;

        public MyObj(int i, double j) {
            this.i = i;
            this.j = j;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            MyObj myObj = (MyObj) o;

            return i == myObj.i && Double.compare(myObj.j, j) == 0;
        }

        @Override
        public String toString() {
            return "MyObj{" + "i=" + i + ", j=" + j + '}';
        }
    }
}
