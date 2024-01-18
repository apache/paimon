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

/** Test for {@link ByteSerializer}. */
public class ByteSerializerTest extends SerializerTestBase<Byte> {

    @Override
    protected Serializer<Byte> createSerializer() {
        return ByteSerializer.INSTANCE;
    }

    @Override
    protected boolean deepEquals(Byte t1, Byte t2) {
        return t1.equals(t2);
    }

    @Override
    protected Byte[] getTestData() {
        Random rnd = new Random();
        byte rndShort = (byte) rnd.nextInt();

        return new Byte[] {0, 1, -1, Byte.MAX_VALUE, Byte.MIN_VALUE, rndShort, (byte) -rndShort};
    }

    @Override
    protected List<Pair<Byte, String>> getSerializableToStringTestData() {
        return Arrays.asList(
                Pair.of((byte) 0, "0"),
                Pair.of((byte) 1, "1"),
                Pair.of((byte) -1, "-1"),
                Pair.of(Byte.MAX_VALUE, "127"),
                Pair.of(Byte.MIN_VALUE, "-128"));
    }
}
