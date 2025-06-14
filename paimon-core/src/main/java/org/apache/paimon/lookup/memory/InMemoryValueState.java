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
import org.apache.paimon.lookup.ValueBulkLoader;
import org.apache.paimon.lookup.ValueState;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.paimon.lookup.ByteArray.wrapBytes;

/** In-memory value state. */
public class InMemoryValueState<K, V> extends InMemoryState<K, V> implements ValueState<K, V> {

    private final Map<ByteArray, byte[]> values;

    public InMemoryValueState(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        super(keySerializer, valueSerializer);
        this.values = new HashMap<>();
    }

    @Override
    public @Nullable V get(K key) throws IOException {
        byte[] bytes = values.get(wrapBytes(serializeKey(key)));
        if (bytes == null) {
            return null;
        }
        return deserializeValue(bytes);
    }

    @Override
    public void put(K key, V value) throws IOException {
        values.put(wrapBytes(serializeKey(key)), serializeValue(value));
    }

    @Override
    public void delete(K key) throws IOException {
        values.remove(wrapBytes(serializeKey(key)));
    }

    @Override
    public ValueBulkLoader createBulkLoader() {
        return new ValueBulkLoader() {

            @Override
            public void write(byte[] key, byte[] value) {
                values.put(wrapBytes(key), value);
            }

            @Override
            public void finish() {}
        };
    }
}
