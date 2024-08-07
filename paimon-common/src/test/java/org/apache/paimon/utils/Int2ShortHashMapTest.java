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

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Test for {@link Int2ShortHashMap}. */
public class Int2ShortHashMapTest {

    @Test
    public void testRandom() {
        Map<Integer, Short> values = new HashMap<>();
        Random rnd = new Random();
        for (int i = 0; i < rnd.nextInt(100); i++) {
            values.put(rnd.nextInt(), (short) rnd.nextInt());
        }
        if (rnd.nextBoolean()) {
            values.put(0, (short) 0);
            values.put(-1, (short) -1);
            values.put(1, (short) 1);
        }

        Int2ShortHashMap map = new Int2ShortHashMap();
        values.forEach(map::put);

        assertThat(map.size()).isEqualTo(values.size());

        values.forEach(
                (k, v) -> {
                    assertThat(map.containsKey(k)).isTrue();
                    assertThat(map.get(k)).isEqualTo(v);
                });
    }

    @Test
    public void testCapacity() {
        assertThrows(RuntimeException.class, () -> new Int2ShortHashMap(1073741824));
    }
}
