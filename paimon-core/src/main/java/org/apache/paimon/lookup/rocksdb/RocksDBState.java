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

package org.apache.paimon.lookup.rocksdb;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.serializer.Serializer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataOutputSerializer;
import org.apache.paimon.lookup.ByteArray;
import org.apache.paimon.lookup.State;
import org.apache.paimon.sort.BinaryExternalSortBuffer;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteOptions;

import javax.annotation.Nullable;

import java.io.IOException;

/** Rocksdb state for key value. */
public abstract class RocksDBState<K, V, CacheV> implements State<K, V> {

    protected final RocksDBStateFactory stateFactory;

    protected final RocksDB db;

    protected final WriteOptions writeOptions;

    protected final ColumnFamilyHandle columnFamily;

    protected final Serializer<K> keySerializer;

    protected final Serializer<V> valueSerializer;

    protected final DataOutputSerializer keyOutView;

    protected final DataInputDeserializer valueInputView;

    protected final DataOutputSerializer valueOutputView;

    protected final Cache<ByteArray, CacheV> cache;

    public RocksDBState(
            RocksDBStateFactory stateFactory,
            ColumnFamilyHandle columnFamily,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer,
            long lruCacheSize) {
        this.stateFactory = stateFactory;
        this.db = stateFactory.db();
        this.columnFamily = columnFamily;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.keyOutView = new DataOutputSerializer(32);
        this.valueInputView = new DataInputDeserializer();
        this.valueOutputView = new DataOutputSerializer(32);
        this.writeOptions = new WriteOptions().setDisableWAL(true);
        this.cache =
                Caffeine.newBuilder()
                        .softValues()
                        .maximumSize(lruCacheSize)
                        .executor(Runnable::run)
                        .build();
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

    protected ByteArray wrap(byte[] bytes) {
        return new ByteArray(bytes);
    }

    protected Reference ref(byte[] bytes) {
        return new Reference(bytes);
    }

    public RocksDBBulkLoader createBulkLoader() {
        return new RocksDBBulkLoader(db, stateFactory.options(), columnFamily, stateFactory.path());
    }

    public static BinaryExternalSortBuffer createBulkLoadSorter(
            IOManager ioManager, CoreOptions options) {
        return BinaryExternalSortBuffer.create(
                ioManager,
                RowType.of(DataTypes.BYTES(), DataTypes.BYTES()),
                new int[] {0},
                options.writeBufferSize() / 2,
                options.pageSize(),
                options.localSortMaxNumFileHandles(),
                options.spillCompressOptions(),
                options.writeBufferSpillDiskSize(),
                options.sequenceFieldSortOrderIsAscending());
    }

    /** A class wraps byte[] to indicate contain or not contain. */
    protected static class Reference {

        @Nullable protected final byte[] bytes;

        protected Reference(@Nullable byte[] bytes) {
            this.bytes = bytes;
        }

        public boolean isPresent() {
            return bytes != null;
        }
    }
}
