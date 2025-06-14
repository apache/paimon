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

package org.apache.paimon.lookup.memory;

import org.apache.paimon.data.serializer.Serializer;
import org.apache.paimon.lookup.ByteArray;
import org.apache.paimon.lookup.SetState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.apache.paimon.lookup.ByteArray.wrapBytes;

/** In-memory set state. */
public class InMemorySetState<K, V> extends InMemoryState<K, V> implements SetState<K, V> {

    private final Map<ByteArray, TreeSet<ByteArray>> values;

    public InMemorySetState(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        super(keySerializer, valueSerializer);
        this.values = new HashMap<>();
    }

    @Override
    public List<V> get(K key) throws IOException {
        Set<ByteArray> set = values.get(wrapBytes(serializeKey(key)));
        List<V> result = new ArrayList<>();
        if (set != null) {
            for (ByteArray value : set) {
                result.add(deserializeValue(value.bytes));
            }
        }
        return result;
    }

    @Override
    public void retract(K key, V value) throws IOException {
        values.get(wrapBytes(serializeKey(key))).remove(wrapBytes(serializeValue(value)));
    }

    @Override
    public void add(K key, V value) throws IOException {
        byte[] keyBytes = serializeKey(key);
        byte[] valueBytes = serializeValue(value);
        values.computeIfAbsent(wrapBytes(keyBytes), k -> new TreeSet<>())
                .add(wrapBytes(valueBytes));
    }
}
