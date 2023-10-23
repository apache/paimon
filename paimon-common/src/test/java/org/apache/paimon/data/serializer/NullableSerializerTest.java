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

import org.junit.jupiter.api.Test;

import java.util.Objects;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link NullableSerializer}. */
public class NullableSerializerTest extends SerializerTestBase<Integer> {

    @Override
    protected Serializer<Integer> createSerializer() {
        return NullableSerializer.wrap(IntSerializer.INSTANCE);
    }

    @Override
    protected boolean deepEquals(Integer t1, Integer t2) {
        return Objects.equals(t1, t2);
    }

    @Override
    protected Integer[] getTestData() {
        Random rnd = new Random();
        int rndInt = rnd.nextInt();

        return new Integer[] {
            0, 1, -1, null, Integer.MAX_VALUE, Integer.MIN_VALUE, rndInt, -rndInt
        };
    }

    @Test
    void testWrappingNeeded() {
        Serializer<Integer> nullableSerializer = createSerializer();
        assertThat(nullableSerializer)
                .isInstanceOf(NullableSerializer.class)
                .isEqualTo(NullableSerializer.wrapIfNullIsNotSupported(nullableSerializer));
    }
}
