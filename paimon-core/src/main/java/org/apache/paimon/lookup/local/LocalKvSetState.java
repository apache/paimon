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
import org.apache.paimon.lookup.SetState;
import org.apache.paimon.lookup.sort.db.LocalKvDb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Local KV state for bytewise-sorted unique values per key. */
public class LocalKvSetState<K, V> extends LocalKvState<K, V, List<byte[]>>
        implements SetState<K, V> {

    private static final byte[] PRESENT = new byte[0];

    LocalKvSetState(
            LocalKvDb db,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer,
            long lruCacheSize,
            LocalKvValueCodec valueCodec) {
        super(db, keySerializer, valueSerializer, lruCacheSize, valueCodec);
    }

    @Override
    public List<V> get(K key) throws IOException {
        List<byte[]> values = getSerializedValues(serializeKey(key));
        List<V> result = new ArrayList<>(values.size());
        for (byte[] value : values) {
            result.add(deserializeValue(value));
        }
        return result;
    }

    @Override
    public void retract(K key, V value) throws IOException {
        checkArgument(value != null, "Value must not be null.");
        byte[] keyBytes = serializeKey(key);
        byte[] compositeKey =
                LocalKvCompositeKey.append(
                        LocalKvCompositeKey.prefix(keyBytes), serializeValue(value));
        if (db.get(compositeKey) != null) {
            db.delete(compositeKey);
        }
        cache.invalidate(wrap(keyBytes));
    }

    @Override
    public void add(K key, V value) throws IOException {
        checkArgument(value != null, "Value must not be null.");
        byte[] keyBytes = serializeKey(key);
        byte[] compositeKey =
                LocalKvCompositeKey.append(
                        LocalKvCompositeKey.prefix(keyBytes), serializeValue(value));
        db.put(compositeKey, valueCodec.encode(PRESENT));
        cache.invalidate(wrap(keyBytes));
    }

    private List<byte[]> getSerializedValues(byte[] keyBytes) throws IOException {
        ByteArray key = wrap(keyBytes);
        List<byte[]> values = getCached(key);
        if (values == null) {
            byte[] prefix = LocalKvCompositeKey.prefix(keyBytes);
            List<byte[]> scanned = new ArrayList<>();
            db.forEachInRange(
                    prefix,
                    LocalKvCompositeKey.upperBound(prefix),
                    (compositeKey, storedSlice) -> {
                        byte[] stored = storedSlice.getHeapMemory();
                        int storedOffset = storedSlice.offset();
                        if (stored == null) {
                            stored = storedSlice.copyBytes();
                            storedOffset = 0;
                        }
                        valueCodec.valueOffset(stored, storedOffset, storedSlice.length());
                        scanned.add(LocalKvCompositeKey.suffix(compositeKey, prefix.length));
                    });
            values =
                    scanned.isEmpty()
                            ? Collections.emptyList()
                            : Collections.unmodifiableList(scanned);
            putCached(key, values);
        }
        return values;
    }
}
