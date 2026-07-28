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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.benchmark.Benchmark;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.serializer.IntSerializer;
import org.apache.paimon.data.serializer.RowCompactedSerializer;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.lookup.BulkLoader;
import org.apache.paimon.lookup.ListBulkLoader;
import org.apache.paimon.lookup.ListState;
import org.apache.paimon.lookup.SetState;
import org.apache.paimon.lookup.StateFactory;
import org.apache.paimon.lookup.local.LocalKvStateFactory;
import org.apache.paimon.lookup.sort.db.LocalKvDb;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileIOUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import static org.apache.paimon.utils.VarLengthIntUtils.encodeInt;
import static org.assertj.core.api.Assertions.assertThat;

/** Benchmark for {@link LocalKvDb}. */
public class LocalKvDbBenchmark {

    private static final int RECORD_COUNT = intProperty("local-kv-db.benchmark.records", 300_000);
    private static final int OPERATION_COUNT =
            intProperty("local-kv-db.benchmark.operations", 1_000_000);
    private static final int VALUE_SIZE = intProperty("local-kv-db.benchmark.value-size", 64);
    private static final int CACHE_SIZE_MB =
            intProperty("local-kv-db.benchmark.cache-size-mb", 128);
    private static final boolean CACHE_OFF_HEAP =
            Boolean.parseBoolean(
                    System.getProperties()
                            .getProperty("local-kv-db.benchmark.cache-off-heap", "false"));
    private static final int MEMTABLE_SIZE_MB =
            intProperty("local-kv-db.benchmark.memtable-size-mb", 64);
    private static final int SST_FILE_SIZE_MB =
            intProperty("local-kv-db.benchmark.sst-file-size-mb", 64);
    private static final int BLOCK_SIZE_KB = intProperty("local-kv-db.benchmark.block-size-kb", 4);
    private static final String COMPRESSION =
            System.getProperties()
                    .getProperty(
                            "local-kv-db.benchmark.compression",
                            CompressOptions.defaultOptions().compress());
    private static final double BLOOM_FILTER_FPP =
            doubleProperty("local-kv-db.benchmark.bloom-filter-fpp", -1);
    private static final int STATE_KEY_COUNT =
            intProperty("local-kv-db.benchmark.state-keys", 2_000);
    private static final int STATE_FAN_OUT = intProperty("local-kv-db.benchmark.state-fan-out", 64);
    private static final int STATE_CACHE_ROWS =
            intProperty("local-kv-db.benchmark.state-cache-rows", 10_000);
    private static final RowType CLUSTERING_KEY_TYPE =
            RowType.of(DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.STRING());
    private static final BinaryString[] REGIONS = createRegions();

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
    public void testClusteringBlockSizeComparison() throws IOException {
        int cacheSizeMb = intProperty("local-kv-db.benchmark.clustering-cache-size-mb", 32);
        File warmupDirectory = new File(tempDir.toFile(), "clustering-warmup");
        File fourKbDirectory = new File(tempDir.toFile(), "clustering-4kb");
        File eightKbDirectory = new File(tempDir.toFile(), "clustering-8kb");
        File sixteenKbDirectory = new File(tempDir.toFile(), "clustering-16kb");
        byte[][] uniformKeys = clusteringQueryKeys(false);
        byte[][] hotKeys = clusteringQueryKeys(true);

        int warmupRecords = Math.min(RECORD_COUNT, 500_000);
        try (LocalKvDb warmupDb = createClusteringDb(warmupDirectory, 4, cacheSizeMb)) {
            warmupDb.bulkLoad(clusteringEntries(warmupRecords), warmupRecords);
        } finally {
            FileIOUtils.deleteDirectoryQuietly(warmupDirectory);
        }

        try (LocalKvDb fourKbDb = createClusteringDb(fourKbDirectory, 4, cacheSizeMb);
                LocalKvDb eightKbDb = createClusteringDb(eightKbDirectory, 8, cacheSizeMb);
                LocalKvDb sixteenKbDb = createClusteringDb(sixteenKbDirectory, 16, cacheSizeMb)) {
            long sixteenKbLoadStart = System.nanoTime();
            sixteenKbDb.bulkLoad(clusteringEntries(RECORD_COUNT), RECORD_COUNT);
            long sixteenKbLoadNanos = System.nanoTime() - sixteenKbLoadStart;
            long eightKbLoadStart = System.nanoTime();
            eightKbDb.bulkLoad(clusteringEntries(RECORD_COUNT), RECORD_COUNT);
            long eightKbLoadNanos = System.nanoTime() - eightKbLoadStart;
            long fourKbLoadStart = System.nanoTime();
            fourKbDb.bulkLoad(clusteringEntries(RECORD_COUNT), RECORD_COUNT);
            long fourKbLoadNanos = System.nanoTime() - fourKbLoadStart;

            byte[] sampleKey =
                    clusteringKey(new RowCompactedSerializer(CLUSTERING_KEY_TYPE), 0, false);
            long sampleValueBytes = 0;
            int sampleCount = Math.min(RECORD_COUNT, 10_000);
            for (int i = 0; i < sampleCount; i++) {
                sampleValueBytes += clusteringValue(i).length;
            }
            System.out.printf(
                    Locale.ROOT,
                    "Clustering data: key=%d B, avg-value=%.1f B, cache=%d MB%n",
                    sampleKey.length,
                    sampleValueBytes / (double) sampleCount,
                    cacheSizeMb);
            System.out.printf(
                    Locale.ROOT,
                    "Build: 4KB=%.1f ms / %.2f MB, 8KB=%.1f ms / %.2f MB, 16KB=%.1f ms / %.2f MB%n",
                    fourKbLoadNanos / 1_000_000.0,
                    directorySize(fourKbDirectory) / (1024.0 * 1024.0),
                    eightKbLoadNanos / 1_000_000.0,
                    directorySize(eightKbDirectory) / (1024.0 * 1024.0),
                    sixteenKbLoadNanos / 1_000_000.0,
                    directorySize(sixteenKbDirectory) / (1024.0 * 1024.0));

            Benchmark benchmark =
                    new Benchmark(
                                    String.format(
                                            Locale.ROOT,
                                            "local-kv-db-clustering-block-size-%d-records-%d-ops-%dMB-cache-zstd-bloom-0.1",
                                            RECORD_COUNT,
                                            OPERATION_COUNT,
                                            cacheSizeMb),
                                    OPERATION_COUNT)
                            .setNumWarmupIters(2)
                            .setOutputPerIteration(true);
            benchmark.addCase("4KB-uniform-hit", 5, () -> lookup(fourKbDb, uniformKeys, false));
            benchmark.addCase("8KB-uniform-hit", 5, () -> lookup(eightKbDb, uniformKeys, false));
            benchmark.addCase("16KB-uniform-hit", 5, () -> lookup(sixteenKbDb, uniformKeys, false));
            benchmark.addCase("4KB-hot-hit", 5, () -> lookup(fourKbDb, hotKeys, false));
            benchmark.addCase("8KB-hot-hit", 5, () -> lookup(eightKbDb, hotKeys, false));
            benchmark.addCase("16KB-hot-hit", 5, () -> lookup(sixteenKbDb, hotKeys, false));
            benchmark.run();
        } finally {
            FileIOUtils.deleteDirectoryQuietly(fourKbDirectory);
            FileIOUtils.deleteDirectoryQuietly(eightKbDirectory);
            FileIOUtils.deleteDirectoryQuietly(sixteenKbDirectory);
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
    public void testStateFanOut() throws IOException {
        benchmarkListState("list-warmup");
        benchmarkListBulkLoad("list-bulk-warmup");
        benchmarkSetState("set-warmup");

        StateBenchmarkResult list = benchmarkListState("list");
        StateBenchmarkResult listBulk = benchmarkListBulkLoad("list-bulk");
        StateBenchmarkResult set = benchmarkSetState("set");

        System.out.printf(
                Locale.ROOT,
                "ListState (%d keys x %d values, local cache=%s):%n"
                        + "  add=%.1f ms, get=%.1f ms, cached=%.1f ms, close=%.1f ms, size=%.2f MB%n",
                STATE_KEY_COUNT,
                STATE_FAN_OUT,
                CACHE_OFF_HEAP ? "off-heap" : "heap",
                list.updateNanos / 1_000_000.0,
                list.readNanos / 1_000_000.0,
                list.cachedReadNanos / 1_000_000.0,
                list.closeNanos / 1_000_000.0,
                list.directoryBytes / (1024.0 * 1024.0));
        System.out.printf(
                Locale.ROOT,
                "ListState bulk load (%d keys x %d values):%n"
                        + "  load=%.1f ms, get=%.1f ms, cached=%.1f ms, close=%.1f ms, size=%.2f MB%n",
                STATE_KEY_COUNT,
                STATE_FAN_OUT,
                listBulk.updateNanos / 1_000_000.0,
                listBulk.readNanos / 1_000_000.0,
                listBulk.cachedReadNanos / 1_000_000.0,
                listBulk.closeNanos / 1_000_000.0,
                listBulk.directoryBytes / (1024.0 * 1024.0));
        System.out.printf(
                Locale.ROOT,
                "SetState (%d keys x %d values):%n"
                        + "  add=%.1f ms, get=%.1f ms, cached=%.1f ms, close=%.1f ms, size=%.2f MB%n",
                STATE_KEY_COUNT,
                STATE_FAN_OUT,
                set.updateNanos / 1_000_000.0,
                set.readNanos / 1_000_000.0,
                set.cachedReadNanos / 1_000_000.0,
                set.closeNanos / 1_000_000.0,
                set.directoryBytes / (1024.0 * 1024.0));

        checksum += list.checksum + listBulk.checksum + set.checksum;
        assertThat(checksum).isNotZero();
    }

    private StateBenchmarkResult benchmarkListState(String name) throws IOException {
        File directory = new File(tempDir.toFile(), name);
        StateFactory factory = createStateFactory(directory);
        long updateNanos = 0;
        long readNanos = 0;
        long cachedReadNanos = 0;
        long closeNanos = 0;
        long directoryBytes = 0;
        long localChecksum = 0;
        try {
            ListState<Integer, Integer> state =
                    factory.listState(
                            "list",
                            IntSerializer.INSTANCE,
                            IntSerializer.INSTANCE,
                            STATE_CACHE_ROWS);
            long updateStart = System.nanoTime();
            for (int key = 0; key < STATE_KEY_COUNT; key++) {
                for (int value = 0; value < STATE_FAN_OUT; value++) {
                    state.add(key, value);
                }
            }
            updateNanos = System.nanoTime() - updateStart;

            long readStart = System.nanoTime();
            for (int key = 0; key < STATE_KEY_COUNT; key++) {
                List<Integer> values = state.get(key);
                if (values.size() != STATE_FAN_OUT) {
                    throw new IllegalStateException("Unexpected ListState fan-out.");
                }
                localChecksum += values.size() + values.get(values.size() - 1);
            }
            readNanos = System.nanoTime() - readStart;

            long cachedReadStart = System.nanoTime();
            for (int key = 0; key < STATE_KEY_COUNT; key++) {
                List<Integer> values = state.get(key);
                if (values.size() != STATE_FAN_OUT) {
                    throw new IllegalStateException("Unexpected cached ListState fan-out.");
                }
                localChecksum += values.size() + values.get(values.size() - 1);
            }
            cachedReadNanos = System.nanoTime() - cachedReadStart;
        } finally {
            long closeStart = System.nanoTime();
            try {
                factory.close();
            } finally {
                closeNanos = System.nanoTime() - closeStart;
                directoryBytes = directorySize(directory);
                FileIOUtils.deleteDirectoryQuietly(directory);
            }
        }
        return new StateBenchmarkResult(
                updateNanos, readNanos, cachedReadNanos, closeNanos, directoryBytes, localChecksum);
    }

    private StateBenchmarkResult benchmarkSetState(String name) throws IOException {
        File directory = new File(tempDir.toFile(), name);
        StateFactory factory = createStateFactory(directory);
        long updateNanos = 0;
        long readNanos = 0;
        long cachedReadNanos = 0;
        long closeNanos = 0;
        long directoryBytes = 0;
        long localChecksum = 0;
        try {
            SetState<Integer, Integer> state =
                    factory.setState(
                            "set",
                            IntSerializer.INSTANCE,
                            IntSerializer.INSTANCE,
                            STATE_CACHE_ROWS);
            long updateStart = System.nanoTime();
            for (int key = 0; key < STATE_KEY_COUNT; key++) {
                for (int value = 0; value < STATE_FAN_OUT; value++) {
                    state.add(key, value);
                }
            }
            updateNanos = System.nanoTime() - updateStart;

            long readStart = System.nanoTime();
            for (int key = 0; key < STATE_KEY_COUNT; key++) {
                List<Integer> values = state.get(key);
                if (values.size() != STATE_FAN_OUT) {
                    throw new IllegalStateException("Unexpected SetState fan-out.");
                }
                localChecksum += values.size() + values.get(values.size() - 1);
            }
            readNanos = System.nanoTime() - readStart;

            long cachedReadStart = System.nanoTime();
            for (int key = 0; key < STATE_KEY_COUNT; key++) {
                List<Integer> values = state.get(key);
                if (values.size() != STATE_FAN_OUT) {
                    throw new IllegalStateException("Unexpected cached SetState fan-out.");
                }
                localChecksum += values.size() + values.get(values.size() - 1);
            }
            cachedReadNanos = System.nanoTime() - cachedReadStart;
        } finally {
            long closeStart = System.nanoTime();
            try {
                factory.close();
            } finally {
                closeNanos = System.nanoTime() - closeStart;
                directoryBytes = directorySize(directory);
                FileIOUtils.deleteDirectoryQuietly(directory);
            }
        }
        return new StateBenchmarkResult(
                updateNanos, readNanos, cachedReadNanos, closeNanos, directoryBytes, localChecksum);
    }

    private StateBenchmarkResult benchmarkListBulkLoad(String name) throws IOException {
        File directory = new File(tempDir.toFile(), name);
        StateFactory factory = createStateFactory(directory);
        long loadNanos = 0;
        long readNanos = 0;
        long cachedReadNanos = 0;
        long closeNanos = 0;
        long directoryBytes = 0;
        long localChecksum = 0;
        try {
            ListState<Integer, Integer> state =
                    factory.listState(
                            "list",
                            IntSerializer.INSTANCE,
                            IntSerializer.INSTANCE,
                            STATE_CACHE_ROWS);
            ListBulkLoader loader = state.createBulkLoader();
            long loadStart = System.nanoTime();
            for (int key = 0; key < STATE_KEY_COUNT; key++) {
                List<byte[]> values = new ArrayList<>(STATE_FAN_OUT);
                for (int value = 0; value < STATE_FAN_OUT; value++) {
                    values.add(state.serializeValue(value));
                }
                loader.write(state.serializeKey(key), values);
            }
            loader.finish();
            loadNanos = System.nanoTime() - loadStart;

            long readStart = System.nanoTime();
            for (int key = 0; key < STATE_KEY_COUNT; key++) {
                List<Integer> values = state.get(key);
                if (values.size() != STATE_FAN_OUT) {
                    throw new IllegalStateException("Unexpected bulk-loaded ListState fan-out.");
                }
                localChecksum += values.size() + values.get(values.size() - 1);
            }
            readNanos = System.nanoTime() - readStart;

            long cachedReadStart = System.nanoTime();
            for (int key = 0; key < STATE_KEY_COUNT; key++) {
                List<Integer> values = state.get(key);
                if (values.size() != STATE_FAN_OUT) {
                    throw new IllegalStateException(
                            "Unexpected cached bulk-loaded ListState fan-out.");
                }
                localChecksum += values.size() + values.get(values.size() - 1);
            }
            cachedReadNanos = System.nanoTime() - cachedReadStart;
        } catch (BulkLoader.WriteException e) {
            throw new IOException(e);
        } finally {
            long closeStart = System.nanoTime();
            try {
                factory.close();
            } finally {
                closeNanos = System.nanoTime() - closeStart;
                directoryBytes = directorySize(directory);
                FileIOUtils.deleteDirectoryQuietly(directory);
            }
        }
        return new StateBenchmarkResult(
                loadNanos, readNanos, cachedReadNanos, closeNanos, directoryBytes, localChecksum);
    }

    private StateFactory createStateFactory(File directory) {
        Options options = new Options();
        options.set(
                CoreOptions.LOOKUP_CACHE_MAX_MEMORY_SIZE, MemorySize.ofMebiBytes(CACHE_SIZE_MB));
        options.set(CoreOptions.LOOKUP_CACHE_SPILL_COMPRESSION, COMPRESSION);
        return new LocalKvStateFactory(
                directory.getAbsolutePath(), options, null, null, CACHE_OFF_HEAP);
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

    private LocalKvDb createDb(File directory) {
        LocalKvDb.Builder builder =
                LocalKvDb.builder(directory)
                        .memTableFlushThreshold(MEMTABLE_SIZE_MB * 1024L * 1024L)
                        .maxSstFileSize(SST_FILE_SIZE_MB * 1024L * 1024L)
                        .blockSize(BLOCK_SIZE_KB * 1024)
                        .cacheManager(createCacheManager(CACHE_SIZE_MB))
                        .compressOptions(new CompressOptions(COMPRESSION, 1))
                        .bloomFilterEnabled(BLOOM_FILTER_FPP > 0);
        if (BLOOM_FILTER_FPP > 0) {
            builder.bloomFilterFpp(BLOOM_FILTER_FPP);
        }
        return builder.build();
    }

    private LocalKvDb createClusteringDb(File directory, int blockSizeKb, int cacheSizeMb) {
        return LocalKvDb.builder(directory)
                .memTableFlushThreshold(MEMTABLE_SIZE_MB * 1024L * 1024L)
                .maxSstFileSize(SST_FILE_SIZE_MB * 1024L * 1024L)
                .blockSize(blockSizeKb * 1024)
                .cacheManager(createCacheManager(cacheSizeMb))
                .compressOptions(CompressOptions.defaultOptions())
                .bloomFilterEnabled(true)
                .bloomFilterFpp(0.1)
                .keyComparator(
                        new RowCompactedSerializer(CLUSTERING_KEY_TYPE).createSliceComparator())
                .build();
    }

    private CacheManager createCacheManager(int cacheSizeMb) {
        MemorySize cacheSize = MemorySize.ofMebiBytes(cacheSizeMb);
        return CACHE_OFF_HEAP
                ? CacheManager.createOffHeap(cacheSize, 0)
                : new CacheManager(cacheSize, 0);
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

    private byte[][] clusteringQueryKeys(boolean hot) {
        byte[][] keys = new byte[OPERATION_COUNT][];
        RowCompactedSerializer serializer = new RowCompactedSerializer(CLUSTERING_KEY_TYPE);
        int hotRecords = Math.max(1, RECORD_COUNT / 100);
        long random = hot ? 0x1234abcdL : 0x5deece66dL;
        for (int i = 0; i < keys.length; i++) {
            random = nextRandom(random);
            long positive = random & Long.MAX_VALUE;
            boolean useHotSet = hot && positive % 10 != 0;
            int index = (int) (positive % (useHotSet ? hotRecords : RECORD_COUNT));
            keys[i] = clusteringKey(serializer, index, false);
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

    private Iterator<Map.Entry<byte[], byte[]>> clusteringEntries(final int count) {
        return new Iterator<Map.Entry<byte[], byte[]>>() {
            private final RowCompactedSerializer serializer =
                    new RowCompactedSerializer(CLUSTERING_KEY_TYPE);
            private int index;

            @Override
            public boolean hasNext() {
                return index < count;
            }

            @Override
            public Map.Entry<byte[], byte[]> next() {
                int current = index++;
                return new AbstractMap.SimpleImmutableEntry<>(
                        clusteringKey(serializer, current, false), clusteringValue(current));
            }
        };
    }

    private static byte[] clusteringKey(
            RowCompactedSerializer serializer, int index, boolean missing) {
        long tenantId = index / 100_000;
        long orderId = ((long) index << 1) + (missing ? 1 : 0);
        return serializer.serializeToBytes(
                GenericRow.of(tenantId, orderId, REGIONS[index & (REGIONS.length - 1)]));
    }

    private static byte[] clusteringValue(int index) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(8);
        try {
            encodeInt(out, index / 100_000);
            encodeInt(out, index % 100_000);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return out.toByteArray();
    }

    private static BinaryString[] createRegions() {
        BinaryString[] regions = new BinaryString[16];
        for (int i = 0; i < regions.length; i++) {
            regions[i] = BinaryString.fromString(String.format(Locale.ROOT, "region-%02d", i));
        }
        return regions;
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
                "%d-records-%dB-value-%dMB-%s-caffeine-cache-%dMB-memtable-%dMB-sst-%dKB-block-%s-bloom-%s",
                RECORD_COUNT,
                VALUE_SIZE,
                CACHE_SIZE_MB,
                CACHE_OFF_HEAP ? "off-heap" : "heap",
                MEMTABLE_SIZE_MB,
                SST_FILE_SIZE_MB,
                BLOCK_SIZE_KB,
                COMPRESSION,
                BLOOM_FILTER_FPP > 0 ? Double.toString(BLOOM_FILTER_FPP) : "disabled");
    }

    private static class StateBenchmarkResult {

        private final long updateNanos;
        private final long readNanos;
        private final long cachedReadNanos;
        private final long closeNanos;
        private final long directoryBytes;
        private final long checksum;

        private StateBenchmarkResult(
                long updateNanos,
                long readNanos,
                long cachedReadNanos,
                long closeNanos,
                long directoryBytes,
                long checksum) {
            this.updateNanos = updateNanos;
            this.readNanos = readNanos;
            this.cachedReadNanos = cachedReadNanos;
            this.closeNanos = closeNanos;
            this.directoryBytes = directoryBytes;
            this.checksum = checksum;
        }
    }
}
