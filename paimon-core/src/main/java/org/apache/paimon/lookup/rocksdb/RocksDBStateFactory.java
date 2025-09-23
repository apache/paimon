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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.serializer.Serializer;
import org.apache.paimon.lookup.StateFactory;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.NativeLibraryLoader;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.TtlDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/** Factory to create state. */
public class RocksDBStateFactory implements StateFactory {

    private static final Logger LOG = LoggerFactory.getLogger(RocksDBStateFactory.class);

    public static final String MERGE_OPERATOR_NAME = "stringappendtest";
    private static final int ROCKSDB_LIB_LOADING_ATTEMPTS = 3;

    private static boolean rocksDbInitialized = false;

    private final Options options;
    private final String path;
    private final ColumnFamilyOptions columnFamilyOptions;

    private RocksDB db;

    public RocksDBStateFactory(
            String path, org.apache.paimon.options.Options conf, @Nullable Duration ttlSecs)
            throws IOException {
        try {
            ensureRocksDBIsLoaded();
        } catch (Throwable e) {
            throw new IOException("Could not load the native RocksDB library", e);
        }
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

    @Override
    public <K, V> RocksDBValueState<K, V> valueState(
            String name,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer,
            long lruCacheSize)
            throws IOException {
        return new RocksDBValueState<>(
                this, createColumnFamily(name), keySerializer, valueSerializer, lruCacheSize);
    }

    @Override
    public <K, V> RocksDBSetState<K, V> setState(
            String name,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer,
            long lruCacheSize)
            throws IOException {
        return new RocksDBSetState<>(
                this, createColumnFamily(name), keySerializer, valueSerializer, lruCacheSize);
    }

    @Override
    public <K, V> RocksDBListState<K, V> listState(
            String name,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer,
            long lruCacheSize)
            throws IOException {

        return new RocksDBListState<>(
                this, createColumnFamily(name), keySerializer, valueSerializer, lruCacheSize);
    }

    @Override
    public boolean preferBulkLoad() {
        return true;
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

    // ------------------------------------------------------------------------
    //  static library loading utilities
    // ------------------------------------------------------------------------

    @VisibleForTesting
    static void ensureRocksDBIsLoaded() throws IOException {
        synchronized (RocksDBStateFactory.class) {
            if (!rocksDbInitialized) {
                LOG.info("Attempting to load RocksDB native library");

                Throwable lastException = null;
                for (int attempt = 1; attempt <= ROCKSDB_LIB_LOADING_ATTEMPTS; attempt++) {
                    try {
                        // keep same with RocksDB.loadLibrary
                        final String tmpDir = System.getenv("ROCKSDB_SHAREDLIB_DIR");
                        // explicitly load the JNI dependency if it has not been loaded before
                        NativeLibraryLoader.getInstance().loadLibrary(tmpDir);

                        // this initialization here should validate that the loading succeeded
                        RocksDB.loadLibrary();

                        // seems to have worked
                        LOG.info("Successfully loaded RocksDB native library");
                        rocksDbInitialized = true;
                        return;
                    } catch (Throwable t) {
                        lastException = t;
                        LOG.debug("RocksDB JNI library loading attempt {} failed", attempt, t);
                        // try to force RocksDB to attempt reloading the library
                        try {
                            resetRocksDBLoadedFlag();
                        } catch (Throwable tt) {
                            LOG.debug(
                                    "Failed to reset 'initialized' flag in RocksDB native code loader",
                                    tt);
                        }
                    }
                }
                throw new IOException("Could not load the native RocksDB library", lastException);
            }
        }
    }

    @VisibleForTesting
    static void resetRocksDBLoadedFlag() throws Exception {
        final Field initField =
                org.rocksdb.NativeLibraryLoader.class.getDeclaredField("initialized");
        initField.setAccessible(true);
        initField.setBoolean(null, false);
    }

    public static boolean rocksDbInitialized() {
        return rocksDbInitialized;
    }
}
