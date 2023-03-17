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

import java.util.Arrays;
import java.util.Random;

/** Test for {@link BinarySerializer}. */
public class BinarySerializerTest extends SerializerTestBase<byte[]> {

    private final Random rnd = new Random();

    @Override
    protected Serializer<byte[]> createSerializer() {
        return BinarySerializer.INSTANCE;
    }

    @Override
    protected boolean deepEquals(byte[] t1, byte[] t2) {
        return Arrays.equals(t1, t2);
    }

    @Override
    protected byte[][] getTestData() {
        return new byte[][] {
            randomByteArray(),
            randomByteArray(),
            new byte[] {},
            randomByteArray(),
            randomByteArray(),
            randomByteArray(),
            new byte[] {},
            randomByteArray(),
            randomByteArray(),
            randomByteArray(),
            new byte[] {}
        };
    }

    private byte[] randomByteArray() {
        int len = rnd.nextInt(1024 * 1024);
        byte[] data = new byte[len];
        for (int i = 0; i < len; i++) {
            data[i] = (byte) rnd.nextInt();
        }
        return data;
    }
}
