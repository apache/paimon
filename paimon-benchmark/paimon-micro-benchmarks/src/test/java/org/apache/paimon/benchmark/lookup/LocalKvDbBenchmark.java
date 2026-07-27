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

package org.apache.paimon.benchmark.lookup;

import org.apache.paimon.benchmark.Benchmark;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.lookup.rocksdb.RocksDBBulkLoader;
import org.apache.paimon.lookup.rocksdb.RocksDBOptions;
import org.apache.paimon.lookup.rocksdb.RocksDBStateFactory;
import org.apache.paimon.lookup.sort.db.LocalKvDb;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.FileIOUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.CompressionType;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Benchmark for {@link LocalKvDb}. */
public class LocalKvDbBenchmark {

    private static final int RECORD_COUNT = intProperty("local-kv-db.benchmark.records", 300_000);
    private static final int OPERATION_COUNT =
            intProperty("local-kv-db.benchmark.operations", 1_000_000);
    private static final int VALUE_SIZE = intProperty("local-kv-db.benchmark.value-size", 64);
    private static final int CACHE_SIZE_MB =
            intProperty("local-kv-db.benchmark.cache-size-mb", 128);
    private static final int MEMTABLE_SIZE_MB =
            intProperty("local-kv-db.benchmark.memtable-size-mb", 64);
    private static final int SST_FILE_SIZE_MB =
            intProperty("local-kv-db.benchmark.sst-file-size-mb", 64);
    private static final int BLOCK_SIZE_KB = intProperty("local-kv-db.benchmark.block-size-kb", 4);
    private static final String COMPRESSION =
            System.getProperties().getProperty("local-kv-db.benchmark.compression", "lz4");
    private static final double BLOOM_FILTER_FPP =
            doubleProperty("local-kv-db.benchmark.bloom-filter-fpp", -1);

    private static volatile long checksum;

    @TempDir Path tempDir;

    @Test
    public void testBulkLoad() {
        Benchmark benchmark =
                new Benchmark("local-kv-db-bulk-load-" + benchmarkDescription(), RECORD_COUNT)
                        .setNumWarmupIters(1)
                        .setOutputPerIteration(true);
        benchmark.addCase(
                "bulk-load",
                3,
                () -> {
                    File directory = new File(tempDir.toFile(), "bulk-load-" + UUID.randomUUID());
                    try (LocalKvDb db = createDb(directory)) {
                        db.bulkLoad(entries(RECORD_COUNT), RECORD_COUNT);
                        byte[] result = db.get(key(RECORD_COUNT / 2 * 2));
                        if (result == null) {
                            throw new IllegalStateException("Expected lookup hit.");
                        }
                        checksum += result.length;
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    } finally {
                        FileIOUtils.deleteDirectoryQuietly(directory);
                    }
                });
        benchmark.run();
        assertThat(checksum).isNotZero();
    }

    @Test
    public void testPointLookup() throws IOException {
        File directory = new File(tempDir.toFile(), "point-lookup");
        byte[][] hitKeys = queryKeys(false);
        byte[][] missKeys = queryKeys(true);

        try (LocalKvDb db = createDb(directory)) {
            db.bulkLoad(entries(RECORD_COUNT), RECORD_COUNT);

            Benchmark benchmark =
                    new Benchmark(
                                    "local-kv-db-point-lookup-" + benchmarkDescription(),
                                    OPERATION_COUNT)
                            .setNumWarmupIters(2)
                            .setOutputPerIteration(true);
            benchmark.addCase("hit", 5, () -> lookup(db, hitKeys, false));
            benchmark.addCase("miss", 5, () -> lookup(db, missKeys, true));
            benchmark.run();
        } finally {
            FileIOUtils.deleteDirectoryQuietly(directory);
        }
        assertThat(checksum).isNotZero();
    }

    @Test
    public void testPointLookupComparison() throws IOException {
        File localDirectory = new File(tempDir.toFile(), "point-lookup-local-kv-db");
        File rocksDirectory = new File(tempDir.toFile(), "point-lookup-rocks");
        byte[][] hitKeys = queryKeys(false);
        byte[][] missKeys = queryKeys(true);

        try (LocalKvDb localDb = createDb(localDirectory);
                RocksDbHandle rocksDb = createRocksDb(rocksDirectory)) {
            long localLoadStart = System.nanoTime();
            localDb.bulkLoad(entries(RECORD_COUNT), RECORD_COUNT);
            long localLoadNanos = System.nanoTime() - localLoadStart;
            long rocksLoadStart = System.nanoTime();
            rocksDb.bulkLoad(entries(RECORD_COUNT));
            long rocksLoadNanos = System.nanoTime() - rocksLoadStart;
            System.out.printf(
                    Locale.ROOT,
                    "Build: local-kv-db=%.1f ms / %.2f MB, rocks=%.1f ms / %.2f MB%n",
                    localLoadNanos / 1_000_000.0,
                    directorySize(localDirectory) / (1024.0 * 1024.0),
                    rocksLoadNanos / 1_000_000.0,
                    directorySize(rocksDirectory) / (1024.0 * 1024.0));

            Benchmark benchmark =
                    new Benchmark(
                                    "local-kv-db-vs-rocks-point-lookup-" + benchmarkDescription(),
                                    OPERATION_COUNT)
                            .setNumWarmupIters(2)
                            .setOutputPerIteration(true);
            benchmark.addCase("local-kv-db-hit", 5, () -> lookup(localDb, hitKeys, false));
            benchmark.addCase("rocks-hit", 5, () -> lookup(rocksDb.db, hitKeys, false));
            benchmark.addCase("local-kv-db-miss", 5, () -> lookup(localDb, missKeys, true));
            benchmark.addCase("rocks-miss", 5, () -> lookup(rocksDb.db, missKeys, true));
            benchmark.run();
        } finally {
            FileIOUtils.deleteDirectoryQuietly(localDirectory);
            FileIOUtils.deleteDirectoryQuietly(rocksDirectory);
        }
        assertThat(checksum).isNotZero();
    }

    @Test
    public void testMixedReadWrite() throws IOException {
        File directory = new File(tempDir.toFile(), "mixed-read-write");
        byte[][] hitKeys = queryKeys(false);
        byte[][] values = updateValues();

        try (LocalKvDb db = createDb(directory)) {
            db.bulkLoad(entries(RECORD_COUNT), RECORD_COUNT);

            Benchmark benchmark =
                    new Benchmark(
                                    "local-kv-db-mixed-read-write-" + benchmarkDescription(),
                                    OPERATION_COUNT)
                            .setNumWarmupIters(2)
                            .setOutputPerIteration(true);
            benchmark.addCase("50-percent-put", 5, () -> mixedReadWrite(db, hitKeys, values));
            benchmark.run();
        } finally {
            FileIOUtils.deleteDirectoryQuietly(directory);
        }
        assertThat(checksum).isNotZero();
    }

    @Test
    public void testMixedReadWriteComparison() throws IOException {
        File localDirectory = new File(tempDir.toFile(), "mixed-read-write-local-kv-db");
        File rocksDirectory = new File(tempDir.toFile(), "mixed-read-write-rocks");
        byte[][] hitKeys = queryKeys(false);
        byte[][] values = updateValues();

        try (LocalKvDb localDb = createDb(localDirectory);
                RocksDbHandle rocksDb = createRocksDb(rocksDirectory)) {
            localDb.bulkLoad(entries(RECORD_COUNT), RECORD_COUNT);
            rocksDb.bulkLoad(entries(RECORD_COUNT));

            Benchmark benchmark =
                    new Benchmark(
                                    "local-kv-db-vs-rocks-mixed-read-write-"
                                            + benchmarkDescription(),
                                    OPERATION_COUNT)
                            .setNumWarmupIters(2)
                            .setOutputPerIteration(true);
            benchmark.addCase(
                    "local-kv-db-50-percent-put",
                    5,
                    () -> mixedReadWrite(localDb, hitKeys, values));
            benchmark.addCase(
                    "rocks-50-percent-put", 5, () -> mixedReadWrite(rocksDb, hitKeys, values));
            benchmark.run();
        } finally {
            FileIOUtils.deleteDirectoryQuietly(localDirectory);
            FileIOUtils.deleteDirectoryQuietly(rocksDirectory);
        }
        assertThat(checksum).isNotZero();
    }

    private void mixedReadWrite(LocalKvDb db, byte[][] keys, byte[][] values) {
        try {
            long localChecksum = 0;
            for (int i = 0; i < OPERATION_COUNT; i++) {
                byte[] key = keys[i];
                if ((i & 1) == 0) {
                    db.put(key, values[i & (values.length - 1)]);
                } else {
                    byte[] result = db.get(key);
                    if (result != null) {
                        localChecksum += result[0] & 0xff;
                    }
                }
            }
            checksum += localChecksum;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void mixedReadWrite(RocksDbHandle handle, byte[][] keys, byte[][] values) {
        try {
            long localChecksum = 0;
            for (int i = 0; i < OPERATION_COUNT; i++) {
                byte[] key = keys[i];
                if ((i & 1) == 0) {
                    handle.db.put(handle.writeOptions, key, values[i & (values.length - 1)]);
                } else {
                    byte[] result = handle.db.get(key);
                    if (result != null) {
                        localChecksum += result[0] & 0xff;
                    }
                }
            }
            checksum += localChecksum;
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    private static byte[][] updateValues() {
        byte[][] values = new byte[1024][];
        for (int i = 0; i < values.length; i++) {
            values[i] = value(i);
        }
        return values;
    }

    private void lookup(LocalKvDb db, byte[][] keys, boolean expectMiss) {
        try {
            long localChecksum = 0;
            for (byte[] key : keys) {
                byte[] value = db.get(key);
                if (expectMiss) {
                    if (value != null) {
                        throw new IllegalStateException("Expected lookup miss.");
                    }
                } else if (value == null) {
                    throw new IllegalStateException("Expected lookup hit.");
                } else {
                    localChecksum += value[0] & 0xff;
                }
            }
            checksum += localChecksum;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void lookup(RocksDB db, byte[][] keys, boolean expectMiss) {
        try {
            long localChecksum = 0;
            for (byte[] key : keys) {
                byte[] value = db.get(key);
                if (expectMiss) {
                    if (value != null) {
                        throw new IllegalStateException("Expected lookup miss.");
                    }
                } else if (value == null) {
                    throw new IllegalStateException("Expected lookup hit.");
                } else {
                    localChecksum += value[0] & 0xff;
                }
            }
            checksum += localChecksum;
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    private LocalKvDb createDb(File directory) {
        LocalKvDb.Builder builder =
                LocalKvDb.builder(directory)
                        .memTableFlushThreshold(MEMTABLE_SIZE_MB * 1024L * 1024L)
                        .maxSstFileSize(SST_FILE_SIZE_MB * 1024L * 1024L)
                        .blockSize(BLOCK_SIZE_KB * 1024)
                        .cacheManager(new CacheManager(MemorySize.ofMebiBytes(CACHE_SIZE_MB), 0))
                        .compressOptions(new CompressOptions(COMPRESSION, 1))
                        .bloomFilterEnabled(BLOOM_FILTER_FPP > 0);
        if (BLOOM_FILTER_FPP > 0) {
            builder.bloomFilterFpp(BLOOM_FILTER_FPP);
        }
        return builder.build();
    }

    private RocksDbHandle createRocksDb(File directory) throws IOException {
        Options options = new Options();
        options.set(RocksDBOptions.WRITE_BUFFER_SIZE, MemorySize.ofMebiBytes(MEMTABLE_SIZE_MB));
        options.set(RocksDBOptions.TARGET_FILE_SIZE_BASE, MemorySize.ofMebiBytes(SST_FILE_SIZE_MB));
        options.set(RocksDBOptions.BLOCK_SIZE, MemorySize.ofKibiBytes(BLOCK_SIZE_KB));
        options.set(RocksDBOptions.BLOCK_CACHE_SIZE, MemorySize.ofMebiBytes(CACHE_SIZE_MB));
        options.set(RocksDBOptions.COMPRESSION_TYPE, rocksCompression());
        if (BLOOM_FILTER_FPP > 0) {
            options.set(RocksDBOptions.USE_BLOOM_FILTER, true);
            options.set(
                    RocksDBOptions.BLOOM_FILTER_BITS_PER_KEY,
                    -Math.log(BLOOM_FILTER_FPP) / (Math.log(2) * Math.log(2)));
        }
        return new RocksDbHandle(
                new RocksDBStateFactory(directory.getAbsolutePath(), options, null));
    }

    private static CompressionType rocksCompression() {
        switch (COMPRESSION.toLowerCase()) {
            case "none":
                return CompressionType.NO_COMPRESSION;
            case "lz4":
                return CompressionType.LZ4_COMPRESSION;
            case "zstd":
                return CompressionType.ZSTD_COMPRESSION;
            default:
                throw new IllegalArgumentException(
                        "Unsupported RocksDB compression for benchmark: " + COMPRESSION);
        }
    }

    private byte[][] queryKeys(boolean missing) {
        byte[][] keys = new byte[OPERATION_COUNT][];
        long random = missing ? 0xcafebabeL : 0x5deece66dL;
        for (int i = 0; i < keys.length; i++) {
            random = nextRandom(random);
            int index = (int) ((random & Long.MAX_VALUE) % RECORD_COUNT);
            keys[i] = key(index * 2 + (missing ? 1 : 0));
        }
        return keys;
    }

    private Iterator<Map.Entry<byte[], byte[]>> entries(final int count) {
        return new Iterator<Map.Entry<byte[], byte[]>>() {
            private int index;

            @Override
            public boolean hasNext() {
                return index < count;
            }

            @Override
            public Map.Entry<byte[], byte[]> next() {
                int current = index++;
                return new AbstractMap.SimpleImmutableEntry<>(key(current * 2), value(current));
            }
        };
    }

    private static byte[] key(int value) {
        byte[] bytes = new byte[8];
        long current = value;
        for (int i = bytes.length - 1; i >= 0; i--) {
            bytes[i] = (byte) current;
            current >>>= 8;
        }
        return bytes;
    }

    private static byte[] value(int seed) {
        byte[] bytes = new byte[VALUE_SIZE];
        long random = seed * 0x9e3779b97f4a7c15L + 0x632be59bd9b4e019L;
        for (int i = 0; i < bytes.length; i++) {
            random = nextRandom(random);
            bytes[i] = (byte) (random >>> 56);
        }
        return bytes;
    }

    private static long nextRandom(long value) {
        return value * 6364136223846793005L + 1442695040888963407L;
    }

    private static int intProperty(String key, int defaultValue) {
        return Integer.parseInt(
                System.getProperties().getProperty(key, Integer.toString(defaultValue)));
    }

    private static double doubleProperty(String key, double defaultValue) {
        return Double.parseDouble(
                System.getProperties().getProperty(key, Double.toString(defaultValue)));
    }

    private static long directorySize(File directory) {
        File[] files = directory.listFiles();
        if (files == null) {
            return 0;
        }

        long size = 0;
        for (File file : files) {
            size += file.isDirectory() ? directorySize(file) : file.length();
        }
        return size;
    }

    private static String benchmarkDescription() {
        return String.format(
                Locale.ROOT,
                "%d-records-%dB-value-%dMB-caffeine-cache-%dMB-memtable-%dMB-sst-%dKB-block-%s-bloom-%s",
                RECORD_COUNT,
                VALUE_SIZE,
                CACHE_SIZE_MB,
                MEMTABLE_SIZE_MB,
                SST_FILE_SIZE_MB,
                BLOCK_SIZE_KB,
                COMPRESSION,
                BLOOM_FILTER_FPP > 0 ? Double.toString(BLOOM_FILTER_FPP) : "disabled");
    }

    private static class RocksDbHandle implements Closeable {

        private final RocksDBStateFactory factory;
        private final RocksDB db;
        private final WriteOptions writeOptions;

        private RocksDbHandle(RocksDBStateFactory factory) {
            this.factory = factory;
            this.db = factory.db();
            this.writeOptions = new WriteOptions().setDisableWAL(true);
        }

        private void bulkLoad(Iterator<Map.Entry<byte[], byte[]>> entries) throws IOException {
            RocksDBBulkLoader loader =
                    new RocksDBBulkLoader(
                            db, factory.options(), db.getDefaultColumnFamily(), factory.path());
            try {
                while (entries.hasNext()) {
                    Map.Entry<byte[], byte[]> entry = entries.next();
                    loader.write(entry.getKey(), entry.getValue());
                }
            } catch (RocksDBBulkLoader.WriteException e) {
                throw new IOException(e);
            }
            loader.finish();
        }

        @Override
        public void close() throws IOException {
            writeOptions.close();
            factory.close();
        }
    }
}
