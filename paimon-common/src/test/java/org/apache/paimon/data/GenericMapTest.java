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

package org.apache.paimon.data;

import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.InternalRowUtils;

import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link GenericMap}. */
class GenericMapTest {

    // Serialized by GenericMap with serialVersionUID 1L and only the map field.
    private static final String LEGACY_DUPLICATE_BINARY_KEY_MAP =
            "rO0ABXNyACFvcmcuYXBhY2hlLnBhaW1vbi5kYXRhLkdlbmVyaWNNYXAAAAAAAAAAAQIAAUwAA21hcHQA"
                    + "D0xqYXZhL3V0aWwvTWFwO3hwc3IAF2phdmEudXRpbC5MaW5rZWRIYXNoTWFwNMBOXBBswPsCAAFaAAth"
                    + "Y2Nlc3NPcmRlcnhyABFqYXZhLnV0aWwuSGFzaE1hcAUH2sHDFmDRAwACRgAKbG9hZEZhY3RvckkACXRo"
                    + "cmVzaG9sZHhwP0AAAAAAAAx3CAAAABAAAAACdXIAAltCrPMX+AYIVOACAAB4cAAAAAEBc3IAEWphdmEu"
                    + "bGFuZy5JbnRlZ2VyEuKgpPeBhzgCAAFJAAV2YWx1ZXhyABBqYXZhLmxhbmcuTnVtYmVyhqyVHQuU4IsC"
                    + "AAB4cAAAAAF1cQB+AAYAAAABAXNxAH4ACAAAAAJ4AA==";

    @Test
    void testBinarySemanticsDoNotDependOnInitialContents() throws Exception {
        Map<Object, Object> initiallyEmptyEntries = new LinkedHashMap<>();
        GenericMap initiallyEmpty = new GenericMap(initiallyEmptyEntries);
        byte[] initiallyEmptyKey = new byte[] {1};
        initiallyEmptyEntries.put(initiallyEmptyKey, 2);

        Map<Object, Object> populatedEntries = new LinkedHashMap<>();
        byte[] populatedKey = new byte[] {1};
        populatedEntries.put(populatedKey, 2);
        GenericMap initiallyPopulated = new GenericMap(populatedEntries);
        GenericMap binary = GenericMap.fromBinaryKeyMap(populatedEntries);

        assertThat(initiallyEmpty.contains(initiallyEmptyKey)).isTrue();
        assertThat(initiallyEmpty.contains(new byte[] {1})).isFalse();
        assertThat(initiallyPopulated.contains(populatedKey)).isTrue();
        assertThat(initiallyPopulated.contains(new byte[] {1})).isFalse();

        assertThat(initiallyEmpty).isEqualTo(initiallyPopulated).isEqualTo(binary);
        assertThat(initiallyPopulated).isEqualTo(initiallyEmpty).isEqualTo(binary);
        assertThat(binary).isEqualTo(initiallyEmpty).isEqualTo(initiallyPopulated);
        assertThat(initiallyEmpty.hashCode())
                .isEqualTo(initiallyPopulated.hashCode())
                .isEqualTo(binary.hashCode());

        GenericMap restored = InstantiationUtil.clone(initiallyPopulated);
        assertThat(restored.contains(new byte[] {1})).isFalse();
        assertThat(restored).isEqualTo(binary);
        assertThat(binary).isEqualTo(restored);
        assertThat(restored.hashCode()).isEqualTo(binary.hashCode());
    }

    @Test
    void testOrdinaryDuplicateBinaryKeysPreservePhysicalEntries() {
        Map<Object, Object> entries = new LinkedHashMap<>();
        entries.put(new byte[] {1}, 1);
        entries.put(new byte[] {1}, 2);
        GenericMap ordinary = new GenericMap(entries);

        Map<Object, Object> sameEntries = new LinkedHashMap<>();
        sameEntries.put(new byte[] {1}, 2);
        sameEntries.put(new byte[] {1}, 1);
        GenericMap same = new GenericMap(sameEntries);

        GenericMap normalized = GenericMap.fromBinaryKeyMap(entries);

        assertThat(ordinary.size()).isEqualTo(2);
        assertThat(ordinary.keyArray().size()).isEqualTo(2);
        assertThat(ordinary.contains(new byte[] {1})).isFalse();
        assertThat(normalized.size()).isOne();
        assertThat(normalized.keyArray().size()).isOne();
        assertThat(normalized.contains(new byte[] {1})).isTrue();

        assertThat(ordinary).isEqualTo(same);
        assertThat(same).isEqualTo(ordinary);
        assertThat(ordinary.hashCode()).isEqualTo(same.hashCode());
        assertThat(ordinary).isNotEqualTo(normalized);
        assertThat(normalized).isNotEqualTo(ordinary);
        assertThat(ordinary.hashCode()).isNotEqualTo(normalized.hashCode());
        assertThat(GenericRow.of(ordinary)).isNotEqualTo(GenericRow.of(normalized));
        assertThat(GenericRow.of(normalized)).isNotEqualTo(GenericRow.of(ordinary));
        assertThat(
                        InternalRowUtils.equals(
                                ordinary,
                                normalized,
                                DataTypes.MAP(DataTypes.BYTES(), DataTypes.INT())))
                .isFalse();
    }

    @Test
    void testDeserializeLegacyMapPreservesDuplicateBinaryKeys() throws Exception {
        GenericMap legacy =
                InstantiationUtil.deserializeObject(
                        Base64.getDecoder().decode(LEGACY_DUPLICATE_BINARY_KEY_MAP),
                        GenericMap.class.getClassLoader());

        assertThat(legacy.size()).isEqualTo(2);
        assertThat(legacy.keyArray().size()).isEqualTo(2);
        assertThat(legacy.keyArray().getBinary(0)).isEqualTo(new byte[] {1});
        assertThat(legacy.keyArray().getBinary(1)).isEqualTo(new byte[] {1});
        assertThat(legacy.valueArray().getInt(0)).isEqualTo(1);
        assertThat(legacy.valueArray().getInt(1)).isEqualTo(2);
        assertThat(legacy.contains(new byte[] {1})).isFalse();
        assertThat(legacy.get(new byte[] {1})).isNull();
    }

    @Test
    void testDuplicateBinaryKeysPreserveEqualsContract() {
        GenericMap left =
                binaryMap(new byte[][] {new byte[] {1}, new byte[] {1}}, new Object[] {1, 2});
        GenericMap right =
                binaryMap(new byte[][] {new byte[] {1}, new byte[] {1}}, new Object[] {2, 2});
        GenericMap canonical = binaryMap(new byte[][] {new byte[] {1}}, new Object[] {2});

        assertThat(left.size()).isOne();
        assertThat(left.get(new byte[] {1})).isEqualTo(2);
        assertThat(left).isEqualTo(right);
        assertThat(right).isEqualTo(left);
        assertThat(right).isEqualTo(canonical);
        assertThat(canonical).isEqualTo(right);
        assertThat(left).isEqualTo(canonical);
        assertThat(left.hashCode()).isEqualTo(right.hashCode()).isEqualTo(canonical.hashCode());

        GenericRow leftRow = GenericRow.of(left);
        GenericRow rightRow = GenericRow.of(right);
        GenericRow canonicalRow = GenericRow.of(canonical);
        assertThat(leftRow).isEqualTo(rightRow);
        assertThat(rightRow).isEqualTo(leftRow);
        assertThat(rightRow).isEqualTo(canonicalRow);
        assertThat(leftRow).isEqualTo(canonicalRow);
        assertThat(leftRow.hashCode())
                .isEqualTo(rightRow.hashCode())
                .isEqualTo(canonicalRow.hashCode());
    }

    @Test
    void testBinaryKeyOwnershipIsIsolated() throws Exception {
        byte[] key = new byte[] {1};
        GenericMap map = binaryMap(new byte[][] {key}, new Object[] {2});

        key[0] = 2;
        assertThat(map.contains(new byte[] {1})).isTrue();
        assertThat(map.contains(new byte[] {2})).isFalse();

        byte[] exposed = map.keyArray().getBinary(0);
        exposed[0] = 3;
        assertThat(map.contains(new byte[] {1})).isTrue();
        assertThat(map.contains(new byte[] {3})).isFalse();
        assertThat(map.keyArray().getBinary(0)).isEqualTo(new byte[] {1});

        GenericMap restored = InstantiationUtil.clone(map);
        assertThat(restored.contains(new byte[] {1})).isTrue();
        assertThat(restored).isEqualTo(map);
        assertThat(restored.hashCode()).isEqualTo(map.hashCode());
    }

    private static GenericMap binaryMap(byte[][] keys, Object[] values) {
        Map<Object, Object> entries = new LinkedHashMap<>();
        for (int i = 0; i < keys.length; i++) {
            entries.put(keys[i], values[i]);
        }
        return GenericMap.fromBinaryKeyMap(entries);
    }
}
