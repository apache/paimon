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

import org.apache.paimon.utils.Pair;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/** Test for {@link FloatSerializer}. */
public class FloatSerializerTest extends SerializerTestBase<Float> {

    @Override
    protected Serializer<Float> createSerializer() {
        return FloatSerializer.INSTANCE;
    }

    @Override
    protected boolean deepEquals(Float t1, Float t2) {
        return t1.equals(t2);
    }

    @Override
    protected Float[] getTestData() {
        Random rnd = new Random();
        float rndFloat = rnd.nextFloat() * Float.MAX_VALUE;

        return new Float[] {
            0F,
            1F,
            -1F,
            Float.MAX_VALUE,
            Float.MIN_VALUE,
            rndFloat,
            -rndFloat,
            Float.NaN,
            Float.NEGATIVE_INFINITY,
            Float.POSITIVE_INFINITY
        };
    }

    @Override
    protected List<Pair<Float, String>> getSerializableToStringTestData() {
        return Arrays.asList(
                Pair.of(0F, "0.0"),
                Pair.of(1F, "1.0"),
                Pair.of(-1F, "-1.0"),
                Pair.of(Float.MAX_VALUE, "3.4028235E38"),
                Pair.of(Float.MIN_VALUE, "1.4E-45"),
                Pair.of(Float.NaN, "NaN"),
                Pair.of(Float.NEGATIVE_INFINITY, "-Infinity"),
                Pair.of(Float.POSITIVE_INFINITY, "Infinity"));
    }
}
