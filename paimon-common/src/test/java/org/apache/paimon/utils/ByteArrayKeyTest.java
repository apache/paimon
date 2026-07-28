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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ByteArrayKey} and {@link ByteArrayLookupKey}. */
class ByteArrayKeyTest {

    @Test
    void testCrossTypeEquality() {
        ByteArrayKey key = new ByteArrayKey(new byte[] {1, 2, 3});
        ByteArrayKey equalKey = new ByteArrayKey(new byte[] {1, 2, 3});
        ByteArrayLookupKey lookup = new ByteArrayLookupKey(new byte[] {1, 2, 3});

        assertThat(key).isEqualTo(equalKey).isEqualTo(lookup);
        assertThat(lookup).isEqualTo(key);
        assertThat(key.hashCode()).isEqualTo(equalKey.hashCode()).isEqualTo(lookup.hashCode());
        assertThat(key).isNotEqualTo(new ByteArrayKey(new byte[] {1, 2, 4}));
    }

    @Test
    void testReusableMapLookup() {
        Map<ByteArrayKey, String> values = new HashMap<>();
        values.put(new ByteArrayKey(new byte[] {1}), "one");
        values.put(new ByteArrayKey(new byte[] {2}), "two");
        ByteArrayLookupKey lookup = new ByteArrayLookupKey();

        lookup.reset(new byte[] {1});
        assertThat(values.get(lookup)).isEqualTo("one");

        lookup.reset(new byte[] {2});
        assertThat(values.get(lookup)).isEqualTo("two");

        lookup.clear();
        assertThat(values.get(lookup)).isNull();
        assertThat(lookup.hashCode()).isZero();
    }

    @Test
    void testLookupEqualityLifecycle() {
        ByteArrayLookupKey first = new ByteArrayLookupKey(new byte[] {1});
        ByteArrayLookupKey second = new ByteArrayLookupKey(new byte[] {1});

        assertThat(first).isEqualTo(second);

        first.clear();
        assertThat(first).isNotEqualTo(second);
        assertThat(first).isEqualTo(first);
    }

    @Test
    void testRejectsNullArray() {
        assertThatThrownBy(() -> new ByteArrayKey(null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ByteArrayLookupKey(null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ByteArrayLookupKey().reset(null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
