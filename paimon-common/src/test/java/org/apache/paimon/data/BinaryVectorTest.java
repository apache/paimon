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

import org.apache.paimon.data.columnar.ColumnarArray;
import org.apache.paimon.data.columnar.heap.HeapFloatVector;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests for {@link BinaryVector}. */
public class BinaryVectorTest {

    @Test
    public void testFromPrimitiveArray() {
        int[] ints = new int[] {1, 2, 3};
        BinaryVector intVector = BinaryVector.fromPrimitiveArray(ints);
        assertThat(intVector.size()).isEqualTo(ints.length);
        assertThat(intVector.getInt(1)).isEqualTo(2);
        assertThat(intVector.toIntArray()).isEqualTo(ints);

        float[] floats = new float[] {1.0f, 2.5f};
        BinaryVector floatVector = BinaryVector.fromPrimitiveArray(floats);
        assertThat(floatVector.size()).isEqualTo(floats.length);
        assertThat(floatVector.getFloat(0)).isEqualTo(1.0f);
        assertThat(floatVector.toFloatArray()).isEqualTo(floats);

        boolean[] booleans = new boolean[] {true, false, true};
        BinaryVector booleanVector = BinaryVector.fromPrimitiveArray(booleans);
        assertThat(booleanVector.size()).isEqualTo(booleans.length);
        assertThat(booleanVector.getBoolean(2)).isTrue();
        assertThat(booleanVector.toBooleanArray()).isEqualTo(booleans);

        byte[] bytes = new byte[] {1, 2, 3};
        BinaryVector byteVector = BinaryVector.fromPrimitiveArray(bytes);
        assertThat(byteVector.size()).isEqualTo(bytes.length);
        assertThat(byteVector.getByte(2)).isEqualTo((byte) 3);
        assertThat(byteVector.toByteArray()).isEqualTo(bytes);

        short[] shorts = new short[] {4, 5, 6};
        BinaryVector shortVector = BinaryVector.fromPrimitiveArray(shorts);
        assertThat(shortVector.size()).isEqualTo(shorts.length);
        assertThat(shortVector.getShort(1)).isEqualTo((short) 5);
        assertThat(shortVector.toShortArray()).isEqualTo(shorts);

        long[] longs = new long[] {7L, 8L, 9L};
        BinaryVector longVector = BinaryVector.fromPrimitiveArray(longs);
        assertThat(longVector.size()).isEqualTo(longs.length);
        assertThat(longVector.getLong(0)).isEqualTo(7L);
        assertThat(longVector.toLongArray()).isEqualTo(longs);

        double[] doubles = new double[] {1.2d, 3.4d};
        BinaryVector doubleVector = BinaryVector.fromPrimitiveArray(doubles);
        assertThat(doubleVector.size()).isEqualTo(doubles.length);
        assertThat(doubleVector.getDouble(1)).isEqualTo(3.4d);
        assertThat(doubleVector.toDoubleArray()).isEqualTo(doubles);
    }

    @Test
    public void testFromGenericArray() {
        float[] values = new float[] {1.0f, -2.0f, 3.5f};
        GenericArray array = new GenericArray(values);
        BinaryVector vector = BinaryVector.fromInternalArray(array, DataTypes.FLOAT());
        assertThat(vector.size()).isEqualTo(values.length);
        assertThat(vector.toFloatArray()).isEqualTo(values);
    }

    @Test
    public void testFromBinaryArray() {
        float[] values = new float[] {1.0f, -2.0f, 3.5f};
        BinaryArray array = BinaryArray.fromPrimitiveArray(values);
        BinaryVector vector = BinaryVector.fromInternalArray(array, DataTypes.FLOAT());
        assertThat(vector.size()).isEqualTo(values.length);
        assertThat(vector.toFloatArray()).isEqualTo(values);
    }

    @Test
    public void testCopiedBinaryVector() {
        float[] values = new float[] {1.0f, -2.0f, 3.5f};
        BinaryVector vector = BinaryVector.fromPrimitiveArray(values);
        BinaryVector copied = vector.copy();

        // Assert that the copied vector is a new object
        assertThat(copied == vector).isFalse();
        assertThat(copied.getSegments() == vector.getSegments()).isFalse();
        assertThat(copied.getSegments()[0] == vector.getSegments()[0]).isFalse();

        // Assert that the copied vector has the same values as the original vector
        assertThat(copied.toFloatArray()).isEqualTo(values);
        assertThat(copied.equals(vector)).isTrue();
    }

    @Test
    public void testRefuseArrayWithNullElements() {
        float[] values = new float[] {1.0f, -2.0f, 3.5f};

        // From binary array with nulls
        BinaryArray binaryArray = BinaryArray.fromPrimitiveArray(values);
        binaryArray.setNullAt(2);
        try {
            BinaryVector vector = BinaryVector.fromInternalArray(binaryArray, DataTypes.FLOAT());
            fail("Should throw exception when array has null elements");
        } catch (Exception e) {
            assertThat(e).hasMessageContaining("Primitive array must not contain a null value.");
        }

        // From columnar array with nulls
        HeapFloatVector heapFloats = new HeapFloatVector(3);
        heapFloats.setFloat(0, values[0]);
        heapFloats.setFloat(1, values[1]);
        heapFloats.setNullAt(2);
        ColumnarArray columnarArray = new ColumnarArray(heapFloats, 0, 3);
        try {
            BinaryVector vector = BinaryVector.fromInternalArray(columnarArray, DataTypes.FLOAT());
            fail("Should throw exception when array has null elements");
        } catch (Exception e) {
            assertThat(e).hasMessageContaining("BinaryVector refuse null elements.");
        }
    }

    @Test
    public void testPrimitiveElementSize() {
        assertThat(BinaryVector.getPrimitiveElementSize(DataTypes.BOOLEAN())).isEqualTo(1);
        assertThat(BinaryVector.getPrimitiveElementSize(DataTypes.TINYINT())).isEqualTo(1);
        assertThat(BinaryVector.getPrimitiveElementSize(DataTypes.SMALLINT())).isEqualTo(2);
        assertThat(BinaryVector.getPrimitiveElementSize(DataTypes.INT())).isEqualTo(4);
        assertThat(BinaryVector.getPrimitiveElementSize(DataTypes.BIGINT())).isEqualTo(8);
        assertThat(BinaryVector.getPrimitiveElementSize(DataTypes.FLOAT())).isEqualTo(4);
        assertThat(BinaryVector.getPrimitiveElementSize(DataTypes.DOUBLE())).isEqualTo(8);
    }
}
