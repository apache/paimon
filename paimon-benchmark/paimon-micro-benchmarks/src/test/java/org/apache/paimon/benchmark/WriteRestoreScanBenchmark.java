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

package org.apache.paimon.benchmark;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.BucketEntry;
import org.apache.paimon.operation.FileSystemWriteRestore;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.SegmentsCache;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Benchmark for the {@link FileSystemWriteRestore#restoreFiles} hot loop, instrumented to surface
 * the manifest-cache memory spike that writers can pay during cold cache population.
 *
 * <p>Builds a primary-key table with many partitions and a small number of rows per partition, then
 * enumerates every (partition, bucket) pair and invokes {@code restoreFiles} on each — the same
 * call pattern a writer pays during restore. The arms isolate the contribution of the {@link
 * SegmentsCache} (the byte-level manifest cache) and of its {@code soft-values} configuration:
 *
 * <ul>
 *   <li>{@code segmentsCacheDisabled} — no {@code SegmentsCache}. Cold disk reads every iteration;
 *       upper bound.
 *   <li>{@code segmentsCacheEnabled} — catalog manifest cache on. Each {@code restoreFiles} call
 *       goes through {@code ManifestFile.read} which consults {@code SegmentsCache}. With
 *       per-iteration cache resets (see below) every measured iteration pays cold-populate cost.
 * </ul>
 *
 * <p>Spike-reproduction characteristics, applied uniformly to all arms:
 *
 * <ul>
 *   <li>Whichever {@code SegmentsCache} is in play is <b>always</b> reset between iterations. See
 *       {@link #resetCachesForIteration(FileStoreTable)} — derived from {@code
 *       fst.getManifestCache() != null}, so no extra config flags are carried.
 *   <li>Restore is driven by an {@link ExecutorService} with {@link #NUM_RESTORE_THREADS} threads,
 *       each holding its own {@link FileSystemWriteRestore}. This is required because {@code
 *       AbstractFileStoreScan} is stateful, and it matches a real Flink TM packed with multiple
 *       writer subtasks restoring concurrently.
 *   <li>Manifests are intentionally fragmented (small commit batches) and rows are widened (many
 *       columns × bigger values) to make per-manifest stats sizes realistic.
 *   <li>Per iteration we sample heap usage before the restore task without forcing GC, heap peak
 *       via {@link MemoryPoolMXBean#getPeakUsage()} after the restore task, and post-{@code
 *       System.gc()} heap usage via {@link java.lang.management.MemoryMXBean#getHeapMemoryUsage()}.
 *       The full {@code Manifest cache footprint} block — including {@code Peak/After-GC} (the
 *       "spike multiplier") — is printed at the end of each iteration. A one-line aggregate over
 *       the measured iterations is printed after the benchmark completes.
 * </ul>
 */
public class WriteRestoreScanBenchmark extends TableBenchmark {

    /**
     * Default parallelism for the restore worker pool. Bumping this approximates packing more Flink
     * writer subtasks onto a single TM.
     */
    private static final int NUM_RESTORE_THREADS = 4;

    /** All tunables for one benchmark run. */
    private static final class BenchParams {
        int numPartitions = 2_000;
        int rowsPerPartition = 16;
        int numBuckets = 4;

        /** Smaller -> more, smaller manifest files (fragmentation). */
        int commitBatchPartitions = 10;

        /** Number of value columns; widens DataFileMeta stats per manifest entry. */
        int valueCount = 10;

        /** Length of each random hex value string; widens per-stat min/max blobs. */
        int valueCharCount = 64;

        /** Parallelism for the restore worker pool. */
        int numRestoreThreads = NUM_RESTORE_THREADS;

        int numWarmupIters = 1;
        int numMeasuredIters = 3;
    }

    /**
     * Manifest-directory bytes on disk, split by file-name prefix. Constant across iterations (the
     * table is populated once), but captured per-iteration so each {@link FootprintSample} is
     * self-contained.
     */
    private static final class DiskFootprint {
        final long manifestBytes;
        final int manifestCount;
        final long manifestListBytes;
        final int manifestListCount;
        final long indexManifestBytes;
        final int indexManifestCount;
        final long total;

        private DiskFootprint(
                long manifestBytes,
                int manifestCount,
                long manifestListBytes,
                int manifestListCount,
                long indexManifestBytes,
                int indexManifestCount) {
            this.manifestBytes = manifestBytes;
            this.manifestCount = manifestCount;
            this.manifestListBytes = manifestListBytes;
            this.manifestListCount = manifestListCount;
            this.indexManifestBytes = indexManifestBytes;
            this.indexManifestCount = indexManifestCount;
            this.total = manifestBytes + manifestListBytes + indexManifestBytes;
        }

        /** List the table's manifest directory and classify each file by name prefix. */
        static DiskFootprint scan(FileStoreTable fst) throws Exception {
            Path manifestDir = new Path(fst.snapshotManager().tablePath(), "manifest");
            FileStatus[] statuses = fst.snapshotManager().fileIO().listStatus(manifestDir);
            long manifestBytes = 0;
            long manifestListBytes = 0;
            long indexManifestBytes = 0;
            int manifestCount = 0;
            int manifestListCount = 0;
            int indexManifestCount = 0;
            for (FileStatus s : statuses) {
                String fileName = s.getPath().getName();
                // INDEX_MANIFEST_PREFIX and MANIFEST_LIST_PREFIX both start with "manifest-",
                // so the more specific prefixes must be checked first.
                if (fileName.startsWith(FileStorePathFactory.INDEX_MANIFEST_PREFIX)) {
                    indexManifestBytes += s.getLen();
                    indexManifestCount++;
                } else if (fileName.startsWith(FileStorePathFactory.MANIFEST_LIST_PREFIX)) {
                    manifestListBytes += s.getLen();
                    manifestListCount++;
                } else if (fileName.startsWith(FileStorePathFactory.MANIFEST_PREFIX)) {
                    manifestBytes += s.getLen();
                    manifestCount++;
                }
            }
            return new DiskFootprint(
                    manifestBytes,
                    manifestCount,
                    manifestListBytes,
                    manifestListCount,
                    indexManifestBytes,
                    indexManifestCount);
        }
    }

    /**
     * All numbers captured during a single iteration's footprint print. The aggregate at the end of
     * {@link #innerTest} consumes one of these per iteration so it can report SegmentsCache and
     * Heap dimensions side by side.
     */
    private static final class FootprintSample {
        final DiskFootprint disk;

        /** {@code null} when no {@link SegmentsCache} is attached to the table. */
        final Long segmentsCacheBytes;

        final long beforeHeap;
        final long peakHeap;
        final long afterGcHeap;

        FootprintSample(
                DiskFootprint disk,
                Long segmentsCacheBytes,
                long beforeHeap,
                long peakHeap,
                long afterGcHeap) {
            this.disk = disk;
            this.segmentsCacheBytes = segmentsCacheBytes;
            this.beforeHeap = beforeHeap;
            this.peakHeap = peakHeap;
            this.afterGcHeap = afterGcHeap;
        }
    }

    /** Sum/min/max/count accumulator for the per-metric aggregate over measured iterations. */
    private static final class LongStats {
        long sum = 0;
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        int count = 0;

        void accept(long value) {
            sum += value;
            min = Math.min(min, value);
            max = Math.max(max, value);
            count++;
        }

        long avg() {
            return count == 0 ? 0 : sum / count;
        }
    }

    @Test
    public void testRestoreFiles_segmentsCacheDisabled() throws Exception {
        Options catalogOptions = new Options();
        catalogOptions.set(CatalogOptions.CACHE_ENABLED, false);
        Options tableOptions = new Options();

        BenchParams p = new BenchParams();
        innerTest("segmentsCacheDisabled", catalogOptions, tableOptions, p);

        /*
        OpenJDK 64-Bit Server VM 11.0.28+0 on Mac OS X 26.5
        Apple M4 Pro
        segmentsCacheDisabled:                      Best/Avg Time(ms)    Row Rate(K/s)      Per Row(ns)   Relative
        -----------------------------------------------------------------------------------------------------------
        OPERATORTEST_segmentsCacheDisabled_restore     20299 / 20792              0.4        2537410.2       1.0X

        Manifest cache footprint aggregate (segmentsCacheDisabled, 3 measured iters):
          Disk          manifests=1,703,457 bytes (26 files), manifest-lists=28,363 bytes (20 files), index-manifests=0 bytes (0 files); total=1,731,820 bytes
          SegmentsCache n/a (no manifest cache attached to table — cache disabled)
          Heap bytes    before   avg=54,524,533, min=54,460,872, max=54,557,016
          Heap bytes    peak     avg=470,198,938, min=396,855,096, max=560,410,880
          Heap bytes    after-gc avg=54,507,109, min=54,459,880, max=54,556,952
         */
    }

    @Test
    public void testRestoreFiles_segmentsCacheEnabled() throws Exception {
        Options catalogOptions = new Options();
        Options tableOptions = new Options();
        catalogOptions.set(
                CatalogOptions.CACHE_MANIFEST_SMALL_FILE_MEMORY, MemorySize.ofMebiBytes(2048));
        catalogOptions.set(CatalogOptions.CACHE_MANIFEST_MAX_MEMORY, MemorySize.ofMebiBytes(4096));
        catalogOptions.set(CatalogOptions.CACHE_MANIFEST_SOFT_VALUES, false);

        BenchParams p = new BenchParams();
        innerTest("segmentsCacheEnabled", catalogOptions, tableOptions, p);
        /*
        OpenJDK 64-Bit Server VM 11.0.28+0 on Mac OS X 26.5
        Apple M4 Pro
        segmentsCacheEnabled:                      Best/Avg Time(ms)    Row Rate(K/s)      Per Row(ns)   Relative
        ----------------------------------------------------------------------------------------------------------
        OPERATORTEST_segmentsCacheEnabled_restore        675 /  679             11.9          84382.8       1.0X

        Manifest cache footprint aggregate (segmentsCacheEnabled, 3 measured iters):
          Disk          manifests=1,773,080 bytes (26 files), manifest-lists=28,406 bytes (20 files), index-manifests=0 bytes (0 files); total=1,801,486 bytes
          SegmentsCache bytes    avg=16,422,496, min=16,422,496, max=16,422,496
          Heap bytes    before   avg=71,640,053, min=70,333,608, max=73,453,168
          Heap bytes    peak     avg=460,976,730, min=443,345,432, max=480,326,824
          Heap bytes    after-gc avg=72,115,114, min=71,131,888, max=73,451,672
         */
    }

    private void innerTest(String name, Options catalogOptions, Options tableOptions, BenchParams p)
            throws Exception {
        Table table = createPartitionedTable(catalogOptions, tableOptions, "T", p);
        populateTable(table, p);

        FileStoreTable fst = (FileStoreTable) table;
        List<BucketEntry> bucketEntries = fst.newSnapshotReader().bucketEntries();
        System.out.printf(
                "Populated table has %d (partition, bucket) pairs across %d partitions "
                        + "(%d restore threads, %dx value cols, %d-char values, commit batch=%d).%n",
                bucketEntries.size(),
                p.numPartitions,
                p.numRestoreThreads,
                p.valueCount,
                p.valueCharCount,
                p.commitBatchPartitions);

        long valuesPerIteration = bucketEntries.size();
        ExecutorService executor = Executors.newFixedThreadPool(p.numRestoreThreads);
        AtomicInteger iterCounter = new AtomicInteger(0);
        List<FootprintSample> perIterSamples =
                new ArrayList<>(p.numWarmupIters + p.numMeasuredIters);

        try {
            Benchmark benchmark =
                    new Benchmark(name, valuesPerIteration)
                            .setNumWarmupIters(p.numWarmupIters)
                            .setOutputPerIteration(true);
            benchmark.addCase(
                    "restore",
                    p.numMeasuredIters,
                    () -> {
                        int iter = iterCounter.getAndIncrement();
                        String iterLabel =
                                iter < p.numWarmupIters
                                        ? "warmup-" + iter
                                        : "iter-" + (iter - p.numWarmupIters);
                        try {
                            perIterSamples.add(
                                    runMeasuredIteration(
                                            name + " " + iterLabel, fst, executor, bucketEntries));
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
            benchmark.run();
        } finally {
            executor.shutdownNow();
        }

        printAggregateFootprint(name, p, perIterSamples);
    }

    /**
     * Run one iteration end to end: reset the cache to its cold state, sample heap before, drive
     * {@code restoreFiles} across every (partition, bucket) pair on the worker pool, sample heap
     * peak and post-GC usage, then print and return the footprint.
     */
    private FootprintSample runMeasuredIteration(
            String label,
            FileStoreTable fst,
            ExecutorService executor,
            List<BucketEntry> bucketEntries)
            throws Exception {
        resetCachesForIteration(fst);
        // Fully run gc before starting the iteration.
        fullGc();

        // Fresh ThreadLocal each iteration so the first worker access constructs a fresh
        // FileSystemWriteRestore + scan that observes the just-reset cache. (AbstractFileStoreScan
        // is stateful, so one FSWR per thread is required.)
        ThreadLocal<FileSystemWriteRestore> threadLocalRestore =
                ThreadLocal.withInitial(
                        () ->
                                new FileSystemWriteRestore(
                                        fst.coreOptions(),
                                        fst.snapshotManager(),
                                        fst.store().newScan(),
                                        fst.store().newIndexFileHandler()));

        resetHeapPeak();
        long before = currentHeapUsage();

        runRestoreAcrossBuckets(executor, threadLocalRestore, bucketEntries);

        long peak = peakHeapUsage();
        fullGc();
        long afterGc = currentHeapUsage();

        return printCacheFootprint(label, fst, before, peak, afterGc);
    }

    /** Submit a {@code restoreFiles} task per (partition, bucket) pair and wait for all of them. */
    private static void runRestoreAcrossBuckets(
            ExecutorService executor,
            ThreadLocal<FileSystemWriteRestore> threadLocalRestore,
            List<BucketEntry> bucketEntries) {
        List<Future<?>> futures = new ArrayList<>(bucketEntries.size());
        for (BucketEntry entry : bucketEntries) {
            futures.add(
                    executor.submit(
                            () ->
                                    threadLocalRestore
                                            .get()
                                            .restoreFiles(
                                                    entry.partition(),
                                                    entry.bucket(),
                                                    false,
                                                    false)));
        }
        for (Future<?> f : futures) {
            try {
                f.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private Table createPartitionedTable(
            Options catalogOptions, Options tableOptions, String tableName, BenchParams p)
            throws Exception {
        catalogOptions.set(CatalogOptions.WAREHOUSE, tempFile.toUri().toString());
        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(catalogOptions));
        String database = "default";
        catalog.createDatabase(database, true);

        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(0, "pt", new IntType()));
        fields.add(new DataField(1, "k", new IntType()));
        for (int i = 0; i < p.valueCount; i++) {
            fields.add(new DataField(2 + i, "f" + i, DataTypes.STRING()));
        }

        tableOptions.set(CoreOptions.BUCKET, p.numBuckets);
        tableOptions.set(CoreOptions.WRITE_ONLY, false);
        tableOptions.set(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX, 10);

        // Primary keys must include all partition keys, so PK = (pt, k).
        Schema schema =
                new Schema(
                        fields,
                        Collections.singletonList("pt"),
                        Arrays.asList("pt", "k"),
                        tableOptions.toMap(),
                        "");
        Identifier identifier = Identifier.create(database, tableName);
        catalog.createTable(identifier, schema, false);
        return catalog.getTable(identifier);
    }

    private void populateTable(Table table, BenchParams p) throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        RandomDataGenerator random = new RandomDataGenerator();
        for (int batchStart = 0;
                batchStart < p.numPartitions;
                batchStart += p.commitBatchPartitions) {
            int batchEnd = Math.min(batchStart + p.commitBatchPartitions, p.numPartitions);
            try (BatchTableWrite write = writeBuilder.newWrite();
                    BatchTableCommit commit = writeBuilder.newCommit()) {
                for (int pt = batchStart; pt < batchEnd; pt++) {
                    for (int k = 0; k < p.rowsPerPartition; k++) {
                        write.write(makeRow(pt, k, random, p));
                    }
                }
                commit.commit(write.prepareCommit());
            }
        }
    }

    private InternalRow makeRow(int pt, int k, RandomDataGenerator random, BenchParams p) {
        GenericRow row = new GenericRow(2 + p.valueCount);
        row.setField(0, pt);
        row.setField(1, k);
        for (int i = 0; i < p.valueCount; i++) {
            row.setField(2 + i, BinaryString.fromString(random.nextHexString(p.valueCharCount)));
        }
        return row;
    }

    /**
     * Reset the manifest {@link SegmentsCache} in play for this table. Always called at the start
     * of every iteration so each measured iteration pays the cold-populate cost (the
     * production-onset condition we're trying to reproduce).
     *
     * <p>If a {@link SegmentsCache} is attached (per-table, attached by {@code
     * CachingCatalog.putTableCache} when {@code CACHE_ENABLED=true}), it is replaced with a fresh
     * instance preserving {@code pageSize} / {@code maxMemorySize} / {@code maxElementSize} /
     * {@code ttl} / {@code softValues}. Replacing (rather than {@code invalidateAll()}-ing)
     * sidesteps Caffeine's asynchronous eviction so the cold state is deterministic. A no-op when
     * no cache is attached.
     */
    private static void resetCachesForIteration(FileStoreTable fst) {
        SegmentsCache<Path> original = fst.getManifestCache();
        if (original != null) {
            fst.setManifestCache(
                    SegmentsCache.create(
                            original.pageSize(),
                            original.maxMemorySize(),
                            original.maxElementSize(),
                            original.ttl(),
                            original.softValues()));
        }
    }

    /**
     * Print a per-iteration footprint summary: total manifest directory bytes on disk (split by
     * file-name prefix), the table's {@link SegmentsCache} accounting bytes, the just-sampled heap
     * before/peak and post-GC usage, and memory-to-disk plus {@code Peak/After-GC} (spike
     * multiplier) ratios.
     *
     * <p>Caveats:
     *
     * <ul>
     *   <li>{@link SegmentsCache#totalCacheBytes()} walks {@code cache.asMap()} and re-applies the
     *       weigher per entry — it's an O(N) snapshot, fine here but not a free read.
     *   <li>Peak is per-pool sum: {@code MemoryPoolMXBean.getPeakUsage()} is per-pool and peaks
     *       don't necessarily coincide across pools; summing slightly overcounts. Accurate enough
     *       for order-of-magnitude spike comparison.
     * </ul>
     */
    private FootprintSample printCacheFootprint(
            String label, FileStoreTable fst, long before, long peak, long afterGc)
            throws Exception {
        DiskFootprint disk = DiskFootprint.scan(fst);

        SegmentsCache<Path> sc = fst.getManifestCache();
        Long segmentsCacheBytes = sc == null ? null : sc.totalCacheBytes();
        String segmentsCacheLine;
        if (sc == null) {
            segmentsCacheLine =
                    "SegmentsCache n/a (no manifest cache attached to table — cache disabled)";
        } else {
            segmentsCacheLine =
                    String.format(
                            "SegmentsCache bytes=%,d (estimatedSize=%d, maxMemory=%s, maxElementSize=%d)",
                            segmentsCacheBytes,
                            sc.estimatedSize(),
                            sc.maxMemorySize(),
                            sc.maxElementSize());
        }

        System.out.println();
        System.out.println("Manifest cache footprint (" + label + "):");
        printDiskLine(disk);
        System.out.println("  " + segmentsCacheLine);
        System.out.printf(
                "  Heap          before=%,d bytes, peak=%,d bytes, after-gc=%,d bytes%n",
                before, peak, afterGc);
        System.out.println();

        return new FootprintSample(disk, segmentsCacheBytes, before, peak, afterGc);
    }

    private static void printDiskLine(DiskFootprint disk) {
        System.out.printf(
                "  Disk          manifests=%,d bytes (%d files), manifest-lists=%,d bytes (%d files), index-manifests=%,d bytes (%d files); total=%,d bytes%n",
                disk.manifestBytes,
                disk.manifestCount,
                disk.manifestListBytes,
                disk.manifestListCount,
                disk.indexManifestBytes,
                disk.indexManifestCount,
                disk.total);
    }

    /**
     * Print one-block aggregate over the <b>measured</b> iterations (warmup iterations skipped).
     * Reports Disk (constant — printed once), {@link SegmentsCache} bytes (avg/min/max + avg ratio
     * to disk), and Heap before/peak/after-GC (avg/min/max + Peak/After-GC spike multiplier +
     * heap/disk ratios).
     */
    private void printAggregateFootprint(
            String name, BenchParams p, List<FootprintSample> samples) {
        int start = p.numWarmupIters;
        int end = samples.size();
        if (start >= end) {
            return;
        }
        int n = end - start;
        DiskFootprint disk = samples.get(start).disk;

        // SegmentsCache: aggregate non-null sample bytes; treat as absent if every sample is null.
        LongStats sc = new LongStats();
        LongStats before = new LongStats();
        LongStats peak = new LongStats();
        LongStats afterGc = new LongStats();

        for (int i = start; i < end; i++) {
            FootprintSample s = samples.get(i);
            if (s.segmentsCacheBytes != null) {
                sc.accept(s.segmentsCacheBytes);
            }
            before.accept(s.beforeHeap);
            peak.accept(s.peakHeap);
            afterGc.accept(s.afterGcHeap);
        }

        System.out.println(
                "Manifest cache footprint aggregate (" + name + ", " + n + " measured iters):");
        printDiskLine(disk);
        if (sc.count > 0) {
            System.out.printf(
                    "  SegmentsCache bytes    avg=%,d, min=%,d, max=%,d%n", sc.avg(), sc.min, sc.max);
        } else {
            System.out.println(
                    "  SegmentsCache n/a (no manifest cache attached to table — cache disabled)");
        }
        System.out.printf(
                "  Heap bytes    before   avg=%,d, min=%,d, max=%,d%n",
                before.avg(), before.min, before.max);
        System.out.printf(
                "  Heap bytes    peak     avg=%,d, min=%,d, max=%,d%n",
                peak.avg(), peak.min, peak.max);
        System.out.printf(
                "  Heap bytes    after-gc avg=%,d, min=%,d, max=%,d%n",
                afterGc.avg(), afterGc.min, afterGc.max);
        System.out.println();
    }

    private static long currentHeapUsage() {
        return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
    }

    private static void fullGc() {
        System.gc();
        System.runFinalization();
        System.gc();
    }

    private static void resetHeapPeak() {
        for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
            if (pool.getType() == MemoryType.HEAP) {
                pool.resetPeakUsage();
            }
        }
    }

    private static long peakHeapUsage() {
        long peak = 0;
        for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
            if (pool.getType() == MemoryType.HEAP) {
                peak += pool.getPeakUsage().getUsed();
            }
        }
        return peak;
    }
}
