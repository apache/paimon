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
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataOutputSerializer;
import org.apache.paimon.lookup.State;

import java.io.IOException;

/** In-memory state. */
public abstract class InMemoryState<K, V> implements State<K, V> {

    protected final Serializer<K> keySerializer;
    protected final Serializer<V> valueSerializer;
    protected final DataOutputSerializer keyOutView;
    protected final DataInputDeserializer valueInputView;
    protected final DataOutputSerializer valueOutputView;

    public InMemoryState(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.keyOutView = new DataOutputSerializer(32);
        this.valueInputView = new DataInputDeserializer();
        this.valueOutputView = new DataOutputSerializer(32);
    }

    @Override
    public byte[] serializeKey(K key) throws IOException {
        keyOutView.clear();
        keySerializer.serialize(key, keyOutView);
        return keyOutView.getCopyOfBuffer();
    }

    @Override
    public byte[] serializeValue(V value) throws IOException {
        valueOutputView.clear();
        valueSerializer.serialize(value, valueOutputView);
        return valueOutputView.getCopyOfBuffer();
    }

    @Override
    public V deserializeValue(byte[] valueBytes) throws IOException {
        valueInputView.setBuffer(valueBytes);
        return valueSerializer.deserialize(valueInputView);
    }
}
