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

package org.apache.paimon.lookup.local;

import org.apache.paimon.data.serializer.Serializer;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataOutputSerializer;
import org.apache.paimon.lookup.ByteArray;
import org.apache.paimon.lookup.State;
import org.apache.paimon.lookup.sort.db.LocalKvDb;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;

import javax.annotation.Nullable;

import java.io.IOException;

/** Base class for states backed by {@link LocalKvDb}. */
abstract class LocalKvState<K, V, CacheV> implements State<K, V> {

    protected final LocalKvDb db;
    protected final Serializer<K> keySerializer;
    protected final Serializer<V> valueSerializer;
    protected final DataOutputSerializer keyOutput;
    protected final DataOutputSerializer valueOutput;
    protected final DataInputDeserializer valueInput;
    protected final Cache<ByteArray, CacheV> cache;
    protected final LocalKvValueCodec valueCodec;

    LocalKvState(
            LocalKvDb db,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer,
            long lruCacheSize,
            LocalKvValueCodec valueCodec) {
        this.db = db;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.keyOutput = new DataOutputSerializer(32);
        this.valueOutput = new DataOutputSerializer(32);
        this.valueInput = new DataInputDeserializer();
        this.valueCodec = valueCodec;
        this.cache =
                Caffeine.newBuilder()
                        .softValues()
                        .maximumSize(lruCacheSize)
                        .executor(Runnable::run)
                        .build();
    }

    @Override
    public byte[] serializeKey(K key) throws IOException {
        keyOutput.clear();
        keySerializer.serialize(key, keyOutput);
        return keyOutput.getCopyOfBuffer();
    }

    @Override
    public byte[] serializeValue(V value) throws IOException {
        valueOutput.clear();
        valueSerializer.serialize(value, valueOutput);
        return valueOutput.getCopyOfBuffer();
    }

    @Override
    public V deserializeValue(byte[] valueBytes) throws IOException {
        valueInput.setBuffer(valueBytes);
        return valueSerializer.deserialize(valueInput);
    }

    @Nullable
    protected byte[] getRaw(byte[] key) throws IOException {
        byte[] stored = db.get(key);
        if (stored == null) {
            return null;
        }
        return valueCodec.decode(stored);
    }

    protected void putRaw(byte[] key, byte[] value) throws IOException {
        db.put(key, valueCodec.encode(value));
    }

    protected ByteArray wrap(byte[] bytes) {
        return new ByteArray(bytes);
    }

    @Nullable
    protected CacheV getCached(ByteArray key) {
        return valueCodec.ttlEnabled() ? null : cache.getIfPresent(key);
    }

    protected void putCached(ByteArray key, CacheV value) {
        if (valueCodec.ttlEnabled()) {
            cache.invalidate(key);
        } else {
            cache.put(key, value);
        }
    }
}
