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

import org.apache.paimon.data.serializer.InternalArraySerializer;
import org.apache.paimon.io.DataInputViewStreamWrapper;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.VectorType;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that ARRAY&lt;VECTOR&gt; is supported by {@link InternalArray} accessors: the {@link
 * InternalArraySerializer} eagerly builds an element getter, so a missing VECTOR case fails
 * serializer construction for an accepted DDL type.
 */
class InternalArrayVectorGetterTest {

    private static final VectorType VECTOR_TYPE = new VectorType(3, new FloatType());

    @Test
    void arraySerializerOverVectorConstructs() {
        InternalArraySerializer serializer = new InternalArraySerializer(VECTOR_TYPE);
        assertThat(serializer).isNotNull();
    }

    @Test
    void elementGetterReadsVector() {
        BinaryVector vector = BinaryVector.fromPrimitiveArray(new float[] {1.0f, 2.0f, 3.0f});
        GenericArray array = new GenericArray(new Object[] {vector});

        InternalArray.ElementGetter getter = InternalArray.createElementGetter(VECTOR_TYPE);

        assertThat(getter.getElementOrNull(array, 0)).isEqualTo(vector);
    }

    @Test
    void elementGetterReturnsNullForNullElement() {
        GenericArray array = new GenericArray(new Object[] {null});

        InternalArray.ElementGetter getter = InternalArray.createElementGetter(VECTOR_TYPE);

        assertThat(getter.getElementOrNull(array, 0)).isNull();
    }

    @Test
    void vectorArrayRoundTripsThroughSerializer() throws Exception {
        InternalArraySerializer serializer = new InternalArraySerializer(VECTOR_TYPE);
        BinaryVector vector = BinaryVector.fromPrimitiveArray(new float[] {4.0f, 5.0f, 6.0f});
        GenericArray array = new GenericArray(new Object[] {vector});

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        serializer.serialize(array, new DataOutputViewStreamWrapper(out));
        InternalArray readBack =
                serializer.deserialize(
                        new DataInputViewStreamWrapper(
                                new ByteArrayInputStream(out.toByteArray())));

        assertThat(readBack.getVector(0).toFloatArray()).isEqualTo(new float[] {4.0f, 5.0f, 6.0f});
    }
}
