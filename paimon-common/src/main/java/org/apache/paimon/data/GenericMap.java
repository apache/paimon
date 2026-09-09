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

import org.apache.paimon.annotation.Public;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * An internal data structure representing data of {@link MapType} or {@link MultisetType}.
 *
 * <p>{@link GenericMap} is a generic implementation of {@link InternalMap} which wraps regular Java
 * maps.
 *
 * <p>Note: All keys and values of this data structure must be internal data structures. All keys
 * must be of the same type; same for values. See {@link InternalRow} for more information about
 * internal data structures.
 *
 * <p>Both keys and values can contain null for representing nullability.
 *
 * @since 0.4.0
 */
@Public
public final class GenericMap implements InternalMap, Serializable {

    private static final long serialVersionUID = 1L;

    private final Map<?, ?> map;
    private final boolean binaryKeys;

    /**
     * Creates an instance of {@link GenericMap} using the given Java map.
     *
     * <p>Note: All keys and values of the map must be internal data structures.
     */
    public GenericMap(Map<?, ?> map) {
        this(map, false);
    }

    private GenericMap(Map<?, ?> map, boolean binaryKeys) {
        this.binaryKeys = binaryKeys;
        this.map = binaryKeys ? normalizeBinaryKeys(map) : map;
    }

    /**
     * Creates a map whose binary keys use content equality.
     *
     * @since 2.1
     */
    public static GenericMap fromBinaryKeyMap(Map<?, ?> map) {
        return new GenericMap(map, true);
    }

    private static Map<BinaryKey, Object> normalizeBinaryKeys(Map<?, ?> map) {
        Map<BinaryKey, Object> binaryMap = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            Object key = entry.getKey();
            if (key != null && !(key instanceof byte[])) {
                throw new IllegalArgumentException("Binary key must be byte[].");
            }
            binaryMap.put(copyBinaryKey(key), entry.getValue());
        }
        return binaryMap;
    }

    /**
     * Returns the value to which the specified key is mapped, or {@code null} if this map contains
     * no mapping for the key. The returned value is in internal data structure.
     */
    public Object get(Object key) {
        if (binaryKeys) {
            return isBinaryKey(key) ? map.get(lookupBinaryKey(key)) : null;
        }
        return map.get(key);
    }

    public boolean contains(Object key) {
        if (binaryKeys) {
            return isBinaryKey(key) && map.containsKey(lookupBinaryKey(key));
        }
        return map.containsKey(key);
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public InternalArray keyArray() {
        Object[] keys = new Object[map.size()];
        int index = 0;
        for (Object key : map.keySet()) {
            keys[index++] = copyUnwrappedBinaryKey(key);
        }
        return new GenericArray(keys);
    }

    @Override
    public InternalArray valueArray() {
        Object[] values = map.values().toArray();
        return new GenericArray(values);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof GenericMap)) {
            return false;
        }
        // deepEquals for values of byte[]
        return deepEquals(this, (GenericMap) o);
    }

    private static boolean deepEquals(GenericMap m1, GenericMap m2) {
        if (m1.map.size() != m2.map.size()) {
            return false;
        }
        if ((m1.binaryKeys && m2.binaryKeys) || (!m1.hasBinaryKeys() && !m2.hasBinaryKeys())) {
            return deepEquals(m1.map, m2.map);
        }

        List<Map.Entry<?, ?>> entries2 = new ArrayList<>(m2.map.entrySet());
        boolean[] matched = new boolean[entries2.size()];
        for (Map.Entry<?, ?> entry1 : m1.map.entrySet()) {
            boolean found = false;
            for (int i = 0; i < entries2.size(); i++) {
                if (matched[i]) {
                    continue;
                }
                Map.Entry<?, ?> entry2 = entries2.get(i);
                if (Objects.deepEquals(
                                unwrapBinaryKey(entry1.getKey()), unwrapBinaryKey(entry2.getKey()))
                        && Objects.deepEquals(entry1.getValue(), entry2.getValue())) {
                    matched[i] = true;
                    found = true;
                    break;
                }
            }
            if (!found) {
                return false;
            }
        }
        return true;
    }

    private static boolean deepEquals(Map<?, ?> m1, Map<?, ?> m2) {
        // copied from HashMap.equals but with deepEquals comparison
        if (m1.size() != m2.size()) {
            return false;
        }
        try {
            for (Map.Entry<?, ?> entry : m1.entrySet()) {
                Object key = entry.getKey();
                Object value = entry.getValue();
                if (value == null) {
                    if (!(m2.get(key) == null && m2.containsKey(key))) {
                        return false;
                    }
                } else {
                    if (!Objects.deepEquals(value, m2.get(key))) {
                        return false;
                    }
                }
            }
        } catch (ClassCastException | NullPointerException unused) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = 0;
        for (Object key : map.keySet()) {
            key = unwrapBinaryKey(key);
            // only include key because values can contain byte[]
            result +=
                    31
                            * (key instanceof byte[]
                                    ? Arrays.hashCode((byte[]) key)
                                    : Objects.hashCode(key));
        }
        return result;
    }

    private boolean hasBinaryKeys() {
        return binaryKeys || hasBinaryKey(map);
    }

    private static Object unwrapBinaryKey(Object key) {
        return key instanceof BinaryKey ? ((BinaryKey) key).bytes : key;
    }

    private static Object copyUnwrappedBinaryKey(Object key) {
        return key instanceof BinaryKey ? ((BinaryKey) key).copyBytes() : key;
    }

    private static boolean isBinaryKey(Object key) {
        return key == null || key instanceof byte[];
    }

    private static BinaryKey copyBinaryKey(Object key) {
        return key == null ? null : new BinaryKey((byte[]) key, true);
    }

    private static BinaryKey lookupBinaryKey(Object key) {
        return key == null ? null : new BinaryKey((byte[]) key, false);
    }

    private static boolean hasBinaryKey(Map<?, ?> map) {
        for (Object key : map.keySet()) {
            if (key instanceof byte[]) {
                return true;
            }
        }
        return false;
    }

    private static final class BinaryKey implements Serializable {

        private static final long serialVersionUID = 1L;

        private final byte[] bytes;
        private final int hash;

        private BinaryKey(byte[] bytes, boolean copy) {
            this.bytes = copy ? Arrays.copyOf(bytes, bytes.length) : bytes;
            this.hash = Arrays.hashCode(this.bytes);
        }

        private byte[] copyBytes() {
            return Arrays.copyOf(bytes, bytes.length);
        }

        @Override
        public boolean equals(Object object) {
            return object == this
                    || (object instanceof BinaryKey
                            && Arrays.equals(bytes, ((BinaryKey) object).bytes));
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }
}
