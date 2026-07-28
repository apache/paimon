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
import org.apache.paimon.lookup.ListBulkLoader;
import org.apache.paimon.lookup.ListState;
import org.apache.paimon.lookup.sort.db.LocalKvDb;
import org.apache.paimon.memory.MemorySlice;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Local KV state for an insertion-ordered list per key. */
public class LocalKvListState<K, V> extends LocalKvState<K, V, List<V>> implements ListState<K, V> {

    private final LocalKvListValueCodec listValueCodec;
    private long nextSequence;

    LocalKvListState(
            LocalKvDb db,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer,
            long lruCacheSize,
            LocalKvValueCodec valueCodec) {
        super(db, keySerializer, valueSerializer, lruCacheSize, valueCodec);
        this.listValueCodec = new LocalKvListValueCodec();
        this.nextSequence = 0;
    }

    @Override
    public void add(K key, V value) throws IOException {
        checkArgument(value != null, "Value must not be null.");
        byte[] keyBytes = serializeKey(key);
        byte[] valueBytes = serializeValue(value);
        db.put(
                nextCompositeKey(LocalKvCompositeKey.prefix(keyBytes)),
                valueCodec.encode(listValueCodec.encodeSingle(valueBytes)));
        ByteArray cacheKey = wrap(keyBytes);
        if (cache.getIfPresent(cacheKey) != null) {
            cache.invalidate(cacheKey);
        }
    }

    @Override
    public List<V> get(K key) throws IOException {
        byte[] keyBytes = serializeKey(key);
        ByteArray cacheKey = wrap(keyBytes);
        List<V> values = getCached(cacheKey);
        if (values == null) {
            byte[] prefix = LocalKvCompositeKey.prefix(keyBytes);
            List<V> scanned = new ArrayList<>();
            db.forEachInRange(
                    prefix,
                    LocalKvCompositeKey.upperBound(prefix),
                    (ignored, stored) -> decodeValues(stored, scanned));
            values =
                    scanned.isEmpty()
                            ? Collections.emptyList()
                            : Collections.unmodifiableList(scanned);
            putCached(cacheKey, values);
        }
        return values;
    }

    private void decodeValues(MemorySlice storedSlice, List<V> target) throws IOException {
        byte[] stored = storedSlice.getHeapMemory();
        int storedOffset = storedSlice.offset();
        if (stored == null) {
            stored = storedSlice.copyBytes();
            storedOffset = 0;
        }

        int valueOffset = valueCodec.valueOffset(stored, storedOffset, storedSlice.length());
        listValueCodec.decode(
                stored,
                valueOffset,
                storedOffset + storedSlice.length() - valueOffset,
                valueSerializer,
                target);
    }

    @Override
    public ListBulkLoader createBulkLoader() {
        return new LocalKvListBulkLoader(
                db,
                valueCodec,
                listValueCodec,
                this::nextCompositeKey,
                key -> cache.invalidate(wrap(key)));
    }

    private byte[] nextCompositeKey(byte[] key) {
        if (nextSequence < 0) {
            throw new IllegalStateException("Local KV list sequence has overflowed.");
        }
        return LocalKvCompositeKey.appendLong(key, nextSequence++);
    }
}
