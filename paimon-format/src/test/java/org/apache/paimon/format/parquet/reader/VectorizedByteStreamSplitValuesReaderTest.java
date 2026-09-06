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

package org.apache.paimon.format.parquet.reader;

import org.apache.paimon.data.columnar.heap.HeapBytesVector;
import org.apache.paimon.data.columnar.heap.HeapDoubleVector;
import org.apache.paimon.data.columnar.heap.HeapFloatVector;
import org.apache.paimon.data.columnar.heap.HeapIntVector;
import org.apache.paimon.data.columnar.heap.HeapLongVector;

import org.apache.parquet.bytes.ByteBufferInputStream;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link VectorizedByteStreamSplitValuesReader}. */
public class VectorizedByteStreamSplitValuesReaderTest {

    @Test
    public void testIntegers() throws IOException {
        int[] values = new int[] {Integer.MIN_VALUE, -1, 0, 42, Integer.MAX_VALUE};
        VectorizedByteStreamSplitValuesReader reader = reader(4, encodeIntegers(values));

        assertThat(reader.readInteger()).isEqualTo(values[0]);
        reader.skipIntegers(1);
        HeapIntVector vector = new HeapIntVector(3);
        reader.readIntegers(3, vector, 0);
        assertThat(vector.getInt(0)).isEqualTo(values[2]);
        assertThat(vector.getInt(1)).isEqualTo(values[3]);
        assertThat(vector.getInt(2)).isEqualTo(values[4]);

        reader = reader(4, encodeIntegers(values));
        HeapLongVector longVector = new HeapLongVector(values.length);
        reader.readIntegersAsLongs(values.length, longVector, 0);
        for (int i = 0; i < values.length; i++) {
            assertThat(longVector.getLong(i)).isEqualTo(values[i]);
        }
    }

    @Test
    public void testLongs() throws IOException {
        long[] values = new long[] {Long.MIN_VALUE, -1L, 0L, 42L, Long.MAX_VALUE};
        VectorizedByteStreamSplitValuesReader reader = reader(8, encodeLongs(values));

        assertThat(reader.readLong()).isEqualTo(values[0]);
        reader.skipLongs(1);
        HeapLongVector vector = new HeapLongVector(3);
        reader.readLongs(3, vector, 0);
        assertThat(vector.getLong(0)).isEqualTo(values[2]);
        assertThat(vector.getLong(1)).isEqualTo(values[3]);
        assertThat(vector.getLong(2)).isEqualTo(values[4]);
    }

    @Test
    public void testFloats() throws IOException {
        float[] values =
                new float[] {
                    Float.NEGATIVE_INFINITY,
                    -0.0f,
                    1.25f,
                    Float.POSITIVE_INFINITY,
                    Float.intBitsToFloat(0x7fc00001)
                };
        VectorizedByteStreamSplitValuesReader reader = reader(4, encodeFloats(values));
        HeapFloatVector vector = new HeapFloatVector(values.length);
        reader.readFloats(values.length, vector, 0);
        for (int i = 0; i < values.length; i++) {
            assertThat(Float.floatToRawIntBits(vector.getFloat(i)))
                    .isEqualTo(Float.floatToRawIntBits(values[i]));
        }

        reader = reader(4, encodeFloats(values));
        HeapDoubleVector doubleVector = new HeapDoubleVector(values.length);
        reader.readFloatsAsDoubles(values.length, doubleVector, 0);
        for (int i = 0; i < values.length; i++) {
            assertThat(Double.doubleToLongBits(doubleVector.getDouble(i)))
                    .isEqualTo(Double.doubleToLongBits((double) values[i]));
        }
    }

    @Test
    public void testDoubles() throws IOException {
        double[] values =
                new double[] {
                    Double.NEGATIVE_INFINITY,
                    -0.0d,
                    1.25d,
                    Double.POSITIVE_INFINITY,
                    Double.longBitsToDouble(0x7ff8000000000001L)
                };
        VectorizedByteStreamSplitValuesReader reader = reader(8, encodeDoubles(values));
        HeapDoubleVector vector = new HeapDoubleVector(values.length);
        reader.readDoubles(values.length, vector, 0);
        for (int i = 0; i < values.length; i++) {
            assertThat(Double.doubleToRawLongBits(vector.getDouble(i)))
                    .isEqualTo(Double.doubleToRawLongBits(values[i]));
        }
    }

    @Test
    public void testFixedLenByteArrays() throws IOException {
        byte[][] values =
                new byte[][] {
                    new byte[] {0, 1, 2},
                    new byte[] {3, 4, 5},
                    new byte[] {6, 7, 8},
                    new byte[] {9, 10, 11}
                };
        VectorizedByteStreamSplitValuesReader reader = reader(3, encode(values));

        assertThat(reader.readBinary(3).getBytes()).isEqualTo(values[0]);
        reader.skipFixedLenByteArray(1, 3);
        HeapBytesVector vector = new HeapBytesVector(2);
        reader.readFixedLenByteArray(2, 3, vector, 0);
        assertThat(vector.getBytes(0).getBytes()).isEqualTo(values[2]);
        assertThat(vector.getBytes(1).getBytes()).isEqualTo(values[3]);
    }

    private static VectorizedByteStreamSplitValuesReader reader(int width, byte[] encoded)
            throws IOException {
        VectorizedByteStreamSplitValuesReader reader =
                new VectorizedByteStreamSplitValuesReader(width);
        reader.initFromPage(
                encoded.length / width, ByteBufferInputStream.wrap(ByteBuffer.wrap(encoded)));
        return reader;
    }

    private static byte[] encodeIntegers(int[] values) {
        byte[][] bytes = new byte[values.length][];
        for (int i = 0; i < values.length; i++) {
            bytes[i] =
                    ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(values[i]).array();
        }
        return encode(bytes);
    }

    private static byte[] encodeLongs(long[] values) {
        byte[][] bytes = new byte[values.length][];
        for (int i = 0; i < values.length; i++) {
            bytes[i] =
                    ByteBuffer.allocate(8)
                            .order(ByteOrder.LITTLE_ENDIAN)
                            .putLong(values[i])
                            .array();
        }
        return encode(bytes);
    }

    private static byte[] encodeFloats(float[] values) {
        int[] bits = new int[values.length];
        for (int i = 0; i < values.length; i++) {
            bits[i] = Float.floatToRawIntBits(values[i]);
        }
        return encodeIntegers(bits);
    }

    private static byte[] encodeDoubles(double[] values) {
        long[] bits = new long[values.length];
        for (int i = 0; i < values.length; i++) {
            bits[i] = Double.doubleToRawLongBits(values[i]);
        }
        return encodeLongs(bits);
    }

    private static byte[] encode(byte[][] values) {
        int width = values[0].length;
        byte[] encoded = new byte[width * values.length];
        for (int b = 0; b < width; b++) {
            for (int i = 0; i < values.length; i++) {
                encoded[b * values.length + i] = values[i][b];
            }
        }
        return encoded;
    }
}
