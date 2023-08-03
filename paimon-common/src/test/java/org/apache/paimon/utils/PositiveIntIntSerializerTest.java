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

package org.apache.paimon.utils;

import org.apache.paimon.data.serializer.Serializer;
import org.apache.paimon.data.serializer.SerializerTestBase;

import java.util.Random;

/** Test for {@link PositiveIntIntSerializer}. */
public class PositiveIntIntSerializerTest extends SerializerTestBase<PositiveIntInt> {

    @Override
    protected Serializer<PositiveIntInt> createSerializer() {
        return new PositiveIntIntSerializer();
    }

    @Override
    protected boolean deepEquals(PositiveIntInt t1, PositiveIntInt t2) {
        return t1.equals(t2);
    }

    @Override
    protected PositiveIntInt[] getTestData() {
        Random rnd = new Random();
        int rndInt = Math.abs(rnd.nextInt());

        int[] ints = new int[] {0, 1, Integer.MAX_VALUE, rndInt};
        PositiveIntInt[] intInts = new PositiveIntInt[ints.length];
        for (int i = 0; i < ints.length; i++) {
            intInts[i] =
                    new PositiveIntInt(
                            ints[rnd.nextInt(ints.length)], ints[rnd.nextInt(ints.length)]);
        }
        return intInts;
    }
}
