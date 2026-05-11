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

import org.apache.paimon.memory.MemorySliceOutput;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/** Test for {@link BTreeIndexMeta} serialization/deserialization. */
public class BTreeIndexMetaTest {

    // ---- V1 format tests (current version) ----

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
        // Core bug scenario: empty byte array (e.g. from empty string key)
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

    @Test
    public void testV1SerializedEndsWithVersionByte() {
        // Verify that V1 serialized data ends with the VERSION byte (>= 2)
        BTreeIndexMeta meta = new BTreeIndexMeta(new byte[] {1}, new byte[] {2}, false);
        byte[] data = meta.serialize();
        Assertions.assertThat(data[data.length - 1]).isEqualTo((byte) 2);
    }

    // ---- V0 format backward compatibility tests ----

    @Test
    public void testDeserializeV0FormatAllNullKeys() {
        // V0 format: no version byte, 0 means null
        // firstKey=null (int 0), lastKey=null (int 0), hasNulls=true (byte 1)
        MemorySliceOutput out = new MemorySliceOutput(9);
        out.writeInt(0); // firstKeyLength = 0 means null in V0
        out.writeInt(0); // lastKeyLength = 0 means null in V0
        out.writeByte(1); // hasNulls = true, also serves as last byte (1 < VERSION)
        byte[] v0Data = out.toSlice().copyBytes();

        BTreeIndexMeta deserialized = BTreeIndexMeta.deserialize(v0Data);
        Assertions.assertThat(deserialized.getFirstKey()).isNull();
        Assertions.assertThat(deserialized.getLastKey()).isNull();
        Assertions.assertThat(deserialized.hasNulls()).isTrue();
        Assertions.assertThat(deserialized.onlyNulls()).isTrue();
    }

    @Test
    public void testDeserializeV0FormatAllNullKeysHasNullsFalse() {
        // V0 format: all null keys with hasNulls=false (last byte is 0, still < VERSION)
        MemorySliceOutput out = new MemorySliceOutput(9);
        out.writeInt(0);
        out.writeInt(0);
        out.writeByte(0); // hasNulls = false, last byte is 0 < VERSION
        byte[] v0Data = out.toSlice().copyBytes();

        BTreeIndexMeta deserialized = BTreeIndexMeta.deserialize(v0Data);
        Assertions.assertThat(deserialized.getFirstKey()).isNull();
        Assertions.assertThat(deserialized.getLastKey()).isNull();
        Assertions.assertThat(deserialized.hasNulls()).isFalse();
        Assertions.assertThat(deserialized.onlyNulls()).isTrue();
    }

    @Test
    public void testDeserializeV0FormatNormalKeys() {
        // V0 format: positive length means key data
        // firstKey=[1,2,3] (length=3), lastKey=[4,5,6] (length=3), hasNulls=false (byte 0)
        MemorySliceOutput out = new MemorySliceOutput(15);
        out.writeInt(3);
        out.writeBytes(new byte[] {1, 2, 3});
        out.writeInt(3);
        out.writeBytes(new byte[] {4, 5, 6});
        out.writeByte(0);
        byte[] v0Data = out.toSlice().copyBytes();

        BTreeIndexMeta deserialized = BTreeIndexMeta.deserialize(v0Data);
        Assertions.assertThat(deserialized.getFirstKey()).isEqualTo(new byte[] {1, 2, 3});
        Assertions.assertThat(deserialized.getLastKey()).isEqualTo(new byte[] {4, 5, 6});
        Assertions.assertThat(deserialized.hasNulls()).isFalse();
        Assertions.assertThat(deserialized.onlyNulls()).isFalse();
    }

    @Test
    public void testDeserializeV0FormatFirstKeyNullLastKeyNormal() {
        // V0 format: firstKey=null (int 0), lastKey=[4,5,6] (length=3)
        MemorySliceOutput out = new MemorySliceOutput(12);
        out.writeInt(0);
        out.writeInt(3);
        out.writeBytes(new byte[] {4, 5, 6});
        out.writeByte(1);
        byte[] v0Data = out.toSlice().copyBytes();

        BTreeIndexMeta deserialized = BTreeIndexMeta.deserialize(v0Data);
        Assertions.assertThat(deserialized.getFirstKey()).isNull();
        Assertions.assertThat(deserialized.getLastKey()).isEqualTo(new byte[] {4, 5, 6});
        Assertions.assertThat(deserialized.hasNulls()).isTrue();
        Assertions.assertThat(deserialized.onlyNulls()).isFalse();
    }
}
