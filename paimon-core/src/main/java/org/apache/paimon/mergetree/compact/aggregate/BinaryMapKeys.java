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

package org.apache.paimon.mergetree.compact.aggregate;

import org.apache.paimon.data.GenericMap;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.utils.ByteArrayKey;

import java.util.HashMap;
import java.util.Map;

/**
 * Value semantics for map keys of type {@code BINARY}, {@code VARBINARY}, {@code GEOMETRY} or
 * {@code GEOGRAPHY}.
 *
 * <p>Such a key arrives as a {@code byte[]}, which inherits identity equality from {@link Object}.
 * Used directly as a hash key, two keys with the same content occupy two entries, a lookup never
 * finds an existing entry and a removal never matches. Keys are therefore held in a {@link
 * ByteArrayKey} for as long as they are in a hash collection and unwrapped when the result map is
 * built.
 */
final class BinaryMapKeys {

    private BinaryMapKeys() {}

    /**
     * Whether values of this type are held as a {@code byte[]}. This is the set of roots that
     * {@link org.apache.paimon.data.InternalArray#createElementGetter} reads with {@code
     * getBinary}: {@code BINARY} and {@code VARBINARY}, plus {@code GEOMETRY} and {@code GEOGRAPHY}
     * whose in-memory value is WKB.
     */
    static boolean isBinary(DataType type) {
        return type.isAnyOf(
                DataTypeRoot.BINARY,
                DataTypeRoot.VARBINARY,
                DataTypeRoot.GEOMETRY,
                DataTypeRoot.GEOGRAPHY);
    }

    /** Wrap a key for storage in a hash collection; a no-op for every non-binary key type. */
    static Object hashKey(boolean binaryKey, Object key) {
        return binaryKey && key != null ? new ByteArrayKey((byte[]) key) : key;
    }

    /** Build the result map, restoring the original {@code byte[]} of any wrapped key. */
    static GenericMap toGenericMap(boolean binaryKey, Map<Object, Object> map) {
        if (!binaryKey) {
            return new GenericMap(map);
        }
        Map<Object, Object> unwrapped = new HashMap<>(map.size());
        map.forEach(
                (key, value) ->
                        unwrapped.put(
                                key instanceof ByteArrayKey ? ((ByteArrayKey) key).bytes() : key,
                                value));
        return new GenericMap(unwrapped);
    }
}
