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

package org.apache.paimon.lookup;

import org.apache.paimon.data.serializer.Serializer;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.TtlDB;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/** Factory to create state. */
public class RocksDBStateFactory implements Closeable {

    public static final String MERGE_OPERATOR_NAME = "stringappendtest";

    private final Options options;
    private final String path;
    private final ColumnFamilyOptions columnFamilyOptions;

    private RocksDB db;

    public RocksDBStateFactory(
            String path, org.apache.paimon.options.Options conf, @Nullable Duration ttlSecs)
            throws IOException {
        DBOptions dbOptions =
                RocksDBOptions.createDBOptions(
                        new DBOptions()
                                .setUseFsync(false)
                                .setStatsDumpPeriodSec(0)
                                .setCreateIfMissing(true),
                        conf);
        this.path = path;
        this.columnFamilyOptions =
                RocksDBOptions.createColumnOptions(new ColumnFamilyOptions(), conf)
                        .setMergeOperatorName(MERGE_OPERATOR_NAME);

        this.options = new Options(dbOptions, columnFamilyOptions);
        try {
            this.db =
                    ttlSecs == null
                            ? RocksDB.open(options, path)
                            : TtlDB.open(options, path, (int) ttlSecs.getSeconds(), false);
        } catch (RocksDBException e) {
            throw new IOException("Error while opening RocksDB instance.", e);
        }
    }

    public RocksDB db() {
        return db;
    }

    public Options options() {
        return options;
    }

    public String path() {
        return path;
    }

    public <K, V> RocksDBValueState<K, V> valueState(
            String name,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer,
            long lruCacheSize)
            throws IOException {
        return new RocksDBValueState<>(
                this, createColumnFamily(name), keySerializer, valueSerializer, lruCacheSize);
    }

    public <K, V> RocksDBSetState<K, V> setState(
            String name,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer,
            long lruCacheSize)
            throws IOException {
        return new RocksDBSetState<>(
                this, createColumnFamily(name), keySerializer, valueSerializer, lruCacheSize);
    }

    public <K, V> RocksDBListState<K, V> listState(
            String name,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer,
            long lruCacheSize)
            throws IOException {

        return new RocksDBListState<>(
                this, createColumnFamily(name), keySerializer, valueSerializer, lruCacheSize);
    }

    private ColumnFamilyHandle createColumnFamily(String name) throws IOException {
        try {
            return db.createColumnFamily(
                    new ColumnFamilyDescriptor(
                            name.getBytes(StandardCharsets.UTF_8), columnFamilyOptions));
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() throws IOException {
        if (db != null) {
            db.close();
            db = null;
        }
    }
}
