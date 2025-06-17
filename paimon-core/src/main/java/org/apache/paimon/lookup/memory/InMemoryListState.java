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
import org.apache.paimon.lookup.ListBulkLoader;
import org.apache.paimon.lookup.ListState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.lookup.ByteArray.wrapBytes;

/** In-memory list state. */
public class InMemoryListState<K, V> extends InMemoryState<K, V> implements ListState<K, V> {

    private final Map<ByteArray, List<byte[]>> values;

    public InMemoryListState(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        super(keySerializer, valueSerializer);
        this.values = new HashMap<>();
    }

    @Override
    public void add(K key, V value) throws IOException {
        byte[] keyBytes = serializeKey(key);
        byte[] valueBytes = serializeValue(value);
        values.computeIfAbsent(wrapBytes(keyBytes), k -> new ArrayList<>()).add(valueBytes);
    }

    @Override
    public List<V> get(K key) throws IOException {
        List<byte[]> list = this.values.get(wrapBytes(serializeKey(key)));
        List<V> result = new ArrayList<>();
        if (list != null) {
            for (byte[] value : list) {
                result.add(deserializeValue(value));
            }
        }
        return result;
    }

    @Override
    public ListBulkLoader createBulkLoader() {
        return new ListBulkLoader() {

            @Override
            public void write(byte[] key, List<byte[]> value) {
                // copy the list, outside will reuse list
                values.put(wrapBytes(key), new ArrayList<>(value));
            }

            @Override
            public void finish() {}
        };
    }
}
