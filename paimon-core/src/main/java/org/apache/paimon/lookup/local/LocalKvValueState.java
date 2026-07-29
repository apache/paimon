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
import org.apache.paimon.lookup.ByteArray;
import org.apache.paimon.lookup.ValueBulkLoader;
import org.apache.paimon.lookup.ValueState;
import org.apache.paimon.lookup.sort.db.LocalKvDb;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Local KV state for one value per key. */
public class LocalKvValueState<K, V> extends LocalKvState<K, V, LocalKvValueState.Reference>
        implements ValueState<K, V> {

    LocalKvValueState(
            LocalKvDb db,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer,
            long lruCacheSize,
            LocalKvValueCodec valueCodec) {
        super(db, keySerializer, valueSerializer, lruCacheSize, valueCodec);
    }

    @Nullable
    @Override
    public V get(K key) throws IOException {
        Reference reference = getReference(wrap(serializeKey(key)));
        return reference.value == null ? null : deserializeValue(reference.value);
    }

    private Reference getReference(ByteArray key) throws IOException {
        Reference reference = getCached(key);
        if (reference == null) {
            reference = new Reference(getRaw(key.bytes));
            putCached(key, reference);
        }
        return reference;
    }

    @Override
    public void put(K key, V value) throws IOException {
        checkArgument(value != null, "Value must not be null.");
        byte[] keyBytes = serializeKey(key);
        byte[] valueBytes = serializeValue(value);
        putRaw(keyBytes, valueBytes);
        putCached(wrap(keyBytes), new Reference(valueBytes));
    }

    @Override
    public void delete(K key) throws IOException {
        byte[] keyBytes = serializeKey(key);
        ByteArray wrappedKey = wrap(keyBytes);
        if (getReference(wrappedKey).value != null) {
            db.delete(keyBytes);
            putCached(wrappedKey, new Reference(null));
        }
    }

    @Override
    public ValueBulkLoader createBulkLoader() {
        return new LocalKvBulkLoader(db, valueCodec, key -> cache.invalidate(wrap(key)));
    }

    /** Nullable value wrapper used for negative cache entries. */
    static final class Reference {

        @Nullable private final byte[] value;

        private Reference(@Nullable byte[] value) {
            this.value = value;
        }
    }
}
