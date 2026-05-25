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

package org.apache.paimon.data.serializer;

import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.InternalVector;
import org.apache.paimon.io.DataOutputSerializer;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link InternalVectorSerializer}. */
class InternalVectorSerializerTest extends SerializerTestBase<InternalVector> {

    @Override
    protected InternalVectorSerializer createSerializer() {
        return new InternalVectorSerializer(DataTypes.FLOAT(), 3);
    }

    @Override
    protected boolean deepEquals(InternalVector vector1, InternalVector vector2) {
        if (vector1.size() != vector2.size()) {
            return false;
        }
        float[] left = vector1.toFloatArray();
        float[] right = vector2.toFloatArray();
        if (left.length != right.length) {
            return false;
        }
        for (int i = 0; i < left.length; i++) {
            if (Float.compare(left[i], right[i]) != 0) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected InternalVector[] getTestData() {
        return new InternalVector[] {
            BinaryVector.fromPrimitiveArray(new float[] {1.0f, 2.0f, 3.0f}),
            BinaryVector.fromPrimitiveArray(new float[] {-1.0f, 0.5f, 2.0f}),
            createCustomVector(new float[] {0.0f, -2.0f, 4.5f})
        };
    }

    @Override
    protected InternalVector[] getSerializableTestData() {
        InternalVector[] testData = getTestData();
        return Arrays.copyOfRange(testData, 0, testData.length - 1);
    }

    @Test
    public void testSerializeWithInvalidSize() {
        InternalVectorSerializer serializer = new InternalVectorSerializer(DataTypes.FLOAT(), 2);
        InternalVector vector = BinaryVector.fromPrimitiveArray(new float[] {1.0f, 2.0f, 3.0f});
        DataOutputSerializer out = new DataOutputSerializer(32);
        assertThatThrownBy(() -> serializer.serialize(vector, out)).isInstanceOf(IOException.class);
    }

    private static InternalVector createCustomVector(float[] values) {
        BinaryVector vector = BinaryVector.fromPrimitiveArray(values);
        Object proxy =
                Proxy.newProxyInstance(
                        InternalVectorSerializerTest.class.getClassLoader(),
                        new Class[] {InternalVector.class},
                        (obj, method, args) -> method.invoke(vector, args));
        return (InternalVector) proxy;
    }
}
