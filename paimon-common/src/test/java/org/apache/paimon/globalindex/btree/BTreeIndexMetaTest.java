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

package org.apache.paimon.globalindex.btree;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/** Test for {@link BTreeIndexMeta} serialization/deserialization. */
public class BTreeIndexMetaTest {

    @Test
    public void testSerializeDeserializeNormal() {
        byte[] firstKey = new byte[] {1, 2, 3};
        byte[] lastKey = new byte[] {4, 5, 6};
        BTreeIndexMeta meta = new BTreeIndexMeta(firstKey, lastKey, true);

        BTreeIndexMeta deserialized = BTreeIndexMeta.deserialize(meta.serialize());
        Assertions.assertThat(deserialized.getFirstKey()).isEqualTo(firstKey);
        Assertions.assertThat(deserialized.getLastKey()).isEqualTo(lastKey);
        Assertions.assertThat(deserialized.hasNulls()).isTrue();
        Assertions.assertThat(deserialized.onlyNulls()).isFalse();
    }

    @Test
    public void testSerializeDeserializeNullKeys() {
        BTreeIndexMeta meta = new BTreeIndexMeta(null, null, true);

        BTreeIndexMeta deserialized = BTreeIndexMeta.deserialize(meta.serialize());
        Assertions.assertThat(deserialized.getFirstKey()).isNull();
        Assertions.assertThat(deserialized.getLastKey()).isNull();
        Assertions.assertThat(deserialized.hasNulls()).isTrue();
        Assertions.assertThat(deserialized.onlyNulls()).isTrue();
    }

    @Test
    public void testSerializeDeserializeEmptyKey() {
        // This is the core bug scenario: empty byte array (e.g. from empty string)
        // should NOT be confused with null
        byte[] emptyKey = new byte[0];
        byte[] lastKey = new byte[] {4, 5, 6};
        BTreeIndexMeta meta = new BTreeIndexMeta(emptyKey, lastKey, false);

        BTreeIndexMeta deserialized = BTreeIndexMeta.deserialize(meta.serialize());
        Assertions.assertThat(deserialized.getFirstKey()).isNotNull();
        Assertions.assertThat(deserialized.getFirstKey()).isEmpty();
        Assertions.assertThat(deserialized.getLastKey()).isEqualTo(lastKey);
        Assertions.assertThat(deserialized.hasNulls()).isFalse();
        Assertions.assertThat(deserialized.onlyNulls()).isFalse();
    }

    @Test
    public void testSerializeDeserializeBothEmptyKeys() {
        byte[] emptyKey = new byte[0];
        BTreeIndexMeta meta = new BTreeIndexMeta(emptyKey, emptyKey, false);

        BTreeIndexMeta deserialized = BTreeIndexMeta.deserialize(meta.serialize());
        Assertions.assertThat(deserialized.getFirstKey()).isNotNull();
        Assertions.assertThat(deserialized.getFirstKey()).isEmpty();
        Assertions.assertThat(deserialized.getLastKey()).isNotNull();
        Assertions.assertThat(deserialized.getLastKey()).isEmpty();
        Assertions.assertThat(deserialized.onlyNulls()).isFalse();
    }

    @Test
    public void testSerializeDeserializeFirstKeyNull() {
        byte[] lastKey = new byte[] {4, 5, 6};
        BTreeIndexMeta meta = new BTreeIndexMeta(null, lastKey, true);

        BTreeIndexMeta deserialized = BTreeIndexMeta.deserialize(meta.serialize());
        Assertions.assertThat(deserialized.getFirstKey()).isNull();
        Assertions.assertThat(deserialized.getLastKey()).isEqualTo(lastKey);
        Assertions.assertThat(deserialized.hasNulls()).isTrue();
        Assertions.assertThat(deserialized.onlyNulls()).isFalse();
    }

    @Test
    public void testSerializeDeserializeLastKeyNull() {
        byte[] firstKey = new byte[] {1, 2, 3};
        BTreeIndexMeta meta = new BTreeIndexMeta(firstKey, null, true);

        BTreeIndexMeta deserialized = BTreeIndexMeta.deserialize(meta.serialize());
        Assertions.assertThat(deserialized.getFirstKey()).isEqualTo(firstKey);
        Assertions.assertThat(deserialized.getLastKey()).isNull();
        Assertions.assertThat(deserialized.hasNulls()).isTrue();
        Assertions.assertThat(deserialized.onlyNulls()).isFalse();
    }

    @Test
    public void testRoundTripWithStringSerializer() {
        // Simulate the real scenario: StringSerializer.serialize(BinaryString.EMPTY_UTF8)
        // returns byte[0], which must survive the BTreeIndexMeta round-trip
        KeySerializer serializer = KeySerializer.create(new org.apache.paimon.types.VarCharType());
        byte[] emptyStrKey = serializer.serialize(org.apache.paimon.data.BinaryString.EMPTY_UTF8);
        Assertions.assertThat(emptyStrKey).isEmpty();

        byte[] normalKey =
                serializer.serialize(org.apache.paimon.data.BinaryString.fromString("abc"));
        BTreeIndexMeta meta = new BTreeIndexMeta(emptyStrKey, normalKey, false);

        BTreeIndexMeta deserialized = BTreeIndexMeta.deserialize(meta.serialize());
        Assertions.assertThat(deserialized.getFirstKey()).isNotNull();
        Assertions.assertThat(deserialized.getFirstKey()).isEmpty();
        Assertions.assertThat(deserialized.getLastKey()).isEqualTo(normalKey);

        // Verify the deserialized keys can be read back by KeySerializer
        Object firstKeyObj =
                serializer.deserialize(
                        org.apache.paimon.memory.MemorySlice.wrap(deserialized.getFirstKey()));
        Object lastKeyObj =
                serializer.deserialize(
                        org.apache.paimon.memory.MemorySlice.wrap(deserialized.getLastKey()));
        Assertions.assertThat(firstKeyObj)
                .isEqualTo(org.apache.paimon.data.BinaryString.EMPTY_UTF8);
        Assertions.assertThat(lastKeyObj)
                .isEqualTo(org.apache.paimon.data.BinaryString.fromString("abc"));
    }
}
