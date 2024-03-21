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

package org.apache.paimon.fileindex;

import org.apache.paimon.utils.Pair;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/** Test for {@link FileIndexPredicateUtil}. */
public class FileIndexPredicateUtilTest {

    private static final Random RANDOM = new Random();

    @Test
    public void testSeder() {
        String type = "testType";

        Map<String, byte[]> map = new HashMap<>();

        int loop = RANDOM.nextInt(100);
        for (int i = 0; i < loop; i++) {
            map.put(randomString(RANDOM.nextInt(100)), randomBytes(RANDOM.nextInt(100)));
        }

        Pair<String, Map<String, byte[]>> deObject =
                FileIndexPredicateUtil.deserializeIndexString(
                        new DataInputStream(
                                new ByteArrayInputStream(
                                        FileIndexPredicateUtil.serializeIndexMap(type, map))),
                        map.keySet());

        Assertions.assertThat(deObject.getLeft()).isEqualTo(type);
        for (String name : map.keySet()) {
            Assertions.assertThat(map.get(name)).containsExactly(deObject.getRight().get(name));
        }
    }

    private static String randomString(int length) {
        byte[] buffer = new byte[length];
        for (int i = 0; i < length; i += 1) {
            buffer[i] = (byte) ('a' + RANDOM.nextInt(26));
        }
        return new String(buffer);
    }

    private static byte[] randomBytes(int length) {
        byte[] b = new byte[length];
        RANDOM.nextBytes(b);
        return b;
    }
}
