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

package org.apache.paimon.benchmark.spatial;

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.VectoredReadable;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.geo.BoundingBox;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.globalindex.rtree.RTreeGlobalIndexWriter;
import org.apache.paimon.globalindex.rtree.RTreeGlobalIndexer;
import org.apache.paimon.globalindex.rtree.RTreeIndexOptions;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.RoaringNavigableMap64;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.index.SpatialIndex;
import org.locationtech.jts.index.hprtree.HPRtree;
import org.locationtech.jts.index.strtree.STRtree;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.paimon.shade.guava30.com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;

/**
 * One isolated benchmark case. Use run-spatial-benchmark.py for JVM forks and aggregation.
 *
 * <p>This measures the production persisted-page path, not a replacement in-memory tree.
 */
public class RTreeIndexBenchmark {

    private static final DataField FIELD =
            new DataField(0, "geom", DataTypes.GEOMETRY("EPSG:3857"));
    private static final FieldRef REF = new FieldRef(0, FIELD.name(), FIELD.type());
    private static final MemoryMXBean HEAP = ManagementFactory.getMemoryMXBean();
    private static volatile long blackhole;

    private static int ioLatencyMs;
    private static int fileConcurrency;

    private static void rssMarker(String marker) throws IOException {
        if ("1".equals(System.getenv("PAIMON_BENCHMARK_QUERY_RSS"))) {
            System.out.println(marker);
            if (System.in.read() != '\n') {
                throw new IOException("Missing benchmark sampler acknowledgement");
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if ("--write-fixture".equals(args[0])) {
            writeFixture(java.nio.file.Paths.get(args[1]));
            return;
        }
        if ("--verify-fixture".equals(args[0])) {
            verifyFixture(java.nio.file.Paths.get(args[1]));
            return;
        }
        if ("--export".equals(args[0])) {
            Data data = new Data(args[1], Integer.parseInt(args[2]));
            data.export(java.nio.file.Paths.get(args[4]), Integer.parseInt(args[3]));
            return;
        }
        String engine = args[0];
        String distribution = args[1];
        int rows = Integer.parseInt(args[2]);
        int queryCount = Integer.parseInt(args[3]);
        int rounds = Integer.parseInt(args[4]);
        int capacity = Integer.parseInt(args[5]);
        int cacheMiB = Integer.parseInt(args[6]);
        if (rows < 1 || queryCount < 1 || rounds < 1 || capacity < 0 || cacheMiB < 0) {
            throw new IllegalArgumentException("Invalid benchmark arguments");
        }
        fileConcurrency =
                args.length > 8
                        ? Integer.parseInt(args[8])
                        : RTreeIndexOptions.READ_FILE_CONCURRENCY.defaultValue();
        if (fileConcurrency < 1) {
            throw new IllegalArgumentException("File concurrency must be positive");
        }
        // Warm class loading, WKB parsing, sorting and query code in this same fork.
        for (int i = 0; i < 2; i++) {
            warmup(engine, distribution, Math.min(rows, 65536), capacity, cacheMiB);
        }
        ioLatencyMs = args.length >= 8 ? Integer.parseInt(args[7]) : 0;
        if (ioLatencyMs < 0 || (ioLatencyMs > 0 && !"paimon-file".equals(engine))) {
            throw new IllegalArgumentException("IO latency requires a file backend");
        }
        Data data = new Data(distribution, rows);
        collectGarbage();
        retainedHeap(); // Initialize GC introspection before taking the baseline.
        collectGarbage();
        long baseline = retainedHeap();
        BenchIndex index;
        long buildNanos;
        long peak;
        HeapSampler sampler = new HeapSampler();
        try (HeapSampler measuring = sampler) {
            long start = System.nanoTime();
            index = build(engine, data, capacity, cacheMiB);
            buildNanos = System.nanoTime() - start;
            sampler.sample();
        }
        peak = sampler.peak.get();
        try (BenchIndex owned = index) {
            collectGarbage();
            long retained = Math.max(0, retainedHeap() - baseline);
            Map<String, Object> output = new LinkedHashMap<>();
            output.put("engine", engine);
            output.put("distribution", distribution);
            output.put("rows", rows);
            output.put("capacity", capacity == 0 ? defaultCapacity(engine) : capacity);
            output.put("cache_mib", cacheMiB);
            output.put("io_latency_ms", ioLatencyMs);
            output.put("file_concurrency", fileConcurrency);
            output.put(
                    "files_written",
                    index instanceof PaimonIndex ? ((PaimonIndex) index).files.size() : 0);
            output.put("build_ms", buildNanos / 1e6);
            output.put("baseline_heap_mib", baseline / 1048576.0);
            output.put("build_peak_heap_mib", peak / 1048576.0);
            output.put("build_peak_delta_mib", Math.max(0, peak - baseline) / 1048576.0);
            output.put("retained_heap_delta_mib", retained / 1048576.0);
            output.put("index_file_bytes", index.fileBytes());
            List<Map<String, Object>> queries = new ArrayList<>();
            String[] sizes = {"small", "medium", "large"};
            double[] sides = {10000, 100000, 500000};
            for (int s = 0; s < sizes.length; s++) {
                Envelope[] windows = data.windows(queryCount, sides[s]);
                // Exact row-ID comparison with an independent primitive-coordinate scan.
                for (Envelope window : windows) {
                    RoaringNavigableMap64 actual = index.query(window);
                    if (!actual.equals(data.scan(window))) {
                        throw new AssertionError("Wrong query rows: " + engine + "/" + sizes[s]);
                    }
                }
                if (index instanceof PaimonIndex) {
                    queries.add(measure(index, windows, rounds, sizes[s], "cache-cold"));
                    ((PaimonIndex) index).reopen();
                }
                for (int warm = 0; warm < 2; warm++) {
                    for (Envelope window : windows) {
                        consume(index.query(window));
                    }
                }
                queries.add(
                        measure(
                                index,
                                windows,
                                rounds,
                                sizes[s],
                                index instanceof PaimonIndex ? "cache-warm" : "resident"));
            }
            output.put("queries", queries);
            output.put("blackhole", blackhole);
            System.out.println("RESULT " + JsonSerdeUtil.toFlatJson(output));
        }
    }

    private static void writeFixture(java.nio.file.Path path) throws Exception {
        Options options = new Options();
        options.set(RTreeIndexOptions.PAGE_SIZE, 3);
        RTreeGlobalIndexer indexer = new RTreeGlobalIndexer(FIELD, options);
        try (Store store = new Store(false);
                RTreeGlobalIndexWriter writer = indexer.createWriter(store)) {
            for (int i = 0; i < 64; i++) {
                byte[] value =
                        i % 7 == 0
                                ? null
                                : ByteBuffer.allocate(21)
                                        .order(ByteOrder.LITTLE_ENDIAN)
                                        .put((byte) 1)
                                        .putInt(1)
                                        .putDouble(i % 11 == 0 ? Double.NaN : i % 9 - 4)
                                        .putDouble(i % 11 == 0 ? Double.NaN : i * i % 13 - 6)
                                        .array();
                writer.write(value, (1L << 33) + i * 13);
            }
            Files.write(path, store.bodies.get(writer.finish().get(0).fileName()));
        }
    }

    private static void verifyFixture(java.nio.file.Path path) throws Exception {
        byte[] bytes = Files.readAllBytes(path);
        GlobalIndexIOMeta file =
                new GlobalIndexIOMeta(
                        new Path(path.toUri()), bytes.length, 64, Arrays.copyOf(bytes, 80));
        RTreeGlobalIndexer indexer = new RTreeGlobalIndexer(FIELD, new Options());
        try (GlobalIndexReader reader =
                indexer.createReader(
                        meta -> LocalFileIO.create().newInputStream(meta.filePath()),
                        Collections.singletonList(file),
                        (1L << 33) + 64 * 13,
                        null,
                        newDirectExecutorService())) {
            RoaringNavigableMap64 expected = new RoaringNavigableMap64();
            RoaringNavigableMap64 nulls = new RoaringNavigableMap64();
            RoaringNavigableMap64 nonNulls = new RoaringNavigableMap64();
            for (long row = 0; row < 64; row++) {
                long id = (1L << 33) + row * 13;
                if (row % 7 == 0) {
                    nulls.add(id);
                } else {
                    nonNulls.add(id);
                    if (row % 11 != 0) {
                        expected.add(id);
                    }
                }
            }
            if (!reader.visitBBoxIntersects(REF, -4, -6, 4, 6)
                            .get()
                            .get()
                            .results()
                            .equals(expected)
                    || !reader.visitIsNull(REF).get().get().results().equals(nulls)
                    || !reader.visitIsNotNull(REF).get().get().results().equals(nonNulls)) {
                throw new AssertionError("Rust fixture differs from expected spatial/null rows");
            }
            System.out.println("INTEROP_OK: 49 spatial, 10 null, 54 non-null rows");
        }
    }

    private static void warmup(
            String engine, String distribution, int rows, int capacity, int cacheMiB)
            throws Exception {
        Data data = new Data(distribution, rows);
        try (BenchIndex index = build(engine, data, capacity, cacheMiB)) {
            for (Envelope query : data.windows(16, 100000)) {
                consume(index.query(query));
            }
        }
    }

    private static Map<String, Object> measure(
            BenchIndex index, Envelope[] windows, int rounds, String size, String phase)
            throws Exception {
        long[] times = new long[windows.length * rounds];
        long matches = 0;
        if (index instanceof PaimonIndex) {
            ((PaimonIndex) index).store.readDelayMs = ioLatencyMs;
        }
        index.resetIo();
        Random order = new Random(73);
        int[] permutation = new int[windows.length];
        for (int i = 0; i < permutation.length; i++) {
            permutation[i] = i;
        }
        int next = 0;
        rssMarker("QUERY_BEGIN " + size + " " + phase);
        for (int round = 0; round < rounds; round++) {
            for (int i = permutation.length - 1; i > 0; i--) {
                int j = order.nextInt(i + 1);
                int old = permutation[i];
                permutation[i] = permutation[j];
                permutation[j] = old;
            }
            for (int id : permutation) {
                long start = System.nanoTime();
                if ("cache-cold".equals(phase)) {
                    ((PaimonIndex) index).reopen();
                }
                RoaringNavigableMap64 result = index.query(windows[id]);
                // Both JTS and Paimon materialize the same bitmap. Cardinality is consumed;
                // brute-force validation and row iteration are outside the timed path.
                long count = result.getLongCardinality();
                times[next++] = System.nanoTime() - start;
                matches += count;
                blackhole ^= count;
            }
        }
        rssMarker("QUERY_END");
        if (index instanceof PaimonIndex) {
            ((PaimonIndex) index).store.readDelayMs = 0;
        }
        long total = 0;
        for (long nanos : times) {
            total += nanos;
        }
        Arrays.sort(times);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("window", size);
        result.put("phase", phase);
        result.put("samples", times.length);
        result.put("mean_us", total / (times.length * 1000.0));
        result.put("p50_us", times[(times.length - 1) / 2] / 1000.0);
        result.put("p95_us", times[(int) Math.ceil(times.length * 0.95) - 1] / 1000.0);
        result.put("qps", times.length * 1e9 / total);
        result.put("mean_matches", matches / (double) times.length);
        result.put("reads_per_query", index.reads() / (double) times.length);
        result.put("bytes_per_query", index.readBytes() / (double) times.length);
        return result;
    }

    private static void consume(RoaringNavigableMap64 result) {
        blackhole ^= result.getLongCardinality();
    }

    private static BenchIndex build(String engine, Data data, int capacity, int cacheMiB)
            throws Exception {
        int nodeCapacity = capacity == 0 ? defaultCapacity(engine) : capacity;
        if ("paimon-memory".equals(engine) || "paimon-file".equals(engine)) {
            PaimonIndex index =
                    new PaimonIndex(
                            "paimon-file".equals(engine), data.size, nodeCapacity, cacheMiB);
            try {
                try (RTreeGlobalIndexWriter writer = index.indexer.createWriter(index.store)) {
                    for (int i = 0; i < data.size; i++) {
                        writer.write(data.wkb(i), i);
                    }
                    for (ResultEntry entry : writer.finish()) {
                        Path path = index.store.path(entry.fileName());
                        index.files.add(
                                new GlobalIndexIOMeta(
                                        path,
                                        index.store.length(path),
                                        entry.rowCount(),
                                        entry.meta()));
                    }
                }
                index.open();
                return index;
            } catch (Throwable t) {
                index.close();
                throw t;
            }
        }
        SpatialIndex tree;
        if ("jts-hpr".equals(engine)) {
            tree = new HPRtree(nodeCapacity);
        } else if ("jts-str".equals(engine)) {
            tree = new STRtree(nodeCapacity);
        } else {
            throw new IllegalArgumentException("Unknown engine: " + engine);
        }
        for (int i = 0; i < data.size; i++) {
            // Identical input WKB generation and envelope parser in all implementations.
            BoundingBox box = BoundingBox.fromWkb(data.wkb(i));
            tree.insert(new Envelope(box.minX(), box.maxX(), box.minY(), box.maxY()), (long) i);
        }
        // JTS otherwise builds lazily in the first query.
        if (tree instanceof HPRtree) {
            ((HPRtree) tree).build();
        } else {
            ((STRtree) tree).build();
        }
        return new JtsIndex(tree);
    }

    private static int defaultCapacity(String engine) {
        return "jts-hpr".equals(engine) ? 16 : "jts-str".equals(engine) ? 10 : 256;
    }

    private static long usedHeap() {
        return HEAP.getHeapMemoryUsage().getUsed();
    }

    private static long retainedHeap() {
        com.sun.management.GcInfo latest = null;
        for (java.lang.management.GarbageCollectorMXBean collector :
                ManagementFactory.getGarbageCollectorMXBeans()) {
            if (collector instanceof com.sun.management.GarbageCollectorMXBean) {
                com.sun.management.GcInfo info =
                        ((com.sun.management.GarbageCollectorMXBean) collector).getLastGcInfo();
                if (info != null && (latest == null || info.getEndTime() > latest.getEndTime())) {
                    latest = info;
                }
            }
        }
        if (latest == null) {
            throw new IllegalStateException(
                    "Benchmark requires HotSpot GC information and enabled explicit GC.");
        }
        // A direct getHeapMemoryUsage() after GC also charges newly reserved TLABs and
        // observer allocations. The collector's snapshot excludes that measurement bias.
        long bytes = 0;
        for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
            if (pool.getType() == MemoryType.HEAP) {
                bytes += latest.getMemoryUsageAfterGc().get(pool.getName()).getUsed();
            }
        }
        return bytes;
    }

    private static void collectGarbage() throws InterruptedException {
        System.gc();
        Thread.sleep(40);
        System.gc();
        Thread.sleep(40);
    }

    private interface BenchIndex extends Closeable {
        RoaringNavigableMap64 query(Envelope window) throws Exception;

        default long fileBytes() {
            return 0;
        }

        default void resetIo() {}

        default long reads() {
            return 0;
        }

        default long readBytes() {
            return 0;
        }
    }

    private static final class JtsIndex implements BenchIndex {
        private final SpatialIndex tree;

        private JtsIndex(SpatialIndex tree) {
            this.tree = tree;
        }

        @Override
        public RoaringNavigableMap64 query(Envelope window) {
            RoaringNavigableMap64 rows = new RoaringNavigableMap64();
            tree.query(window, item -> rows.add((Long) item));
            return rows;
        }

        @Override
        public void close() {}
    }

    private static final class PaimonIndex implements BenchIndex {
        private final Store store;
        private final ExecutorService executor = Executors.newFixedThreadPool(32);
        private final long rows;
        private final Options options = new Options();
        private final List<GlobalIndexIOMeta> files = new ArrayList<>();
        private RTreeGlobalIndexer indexer;
        private GlobalIndexReader reader;

        private PaimonIndex(boolean disk, long rows, int capacity, int cacheMiB)
                throws IOException {
            this.rows = rows;
            store = new Store(disk);
            options.set(RTreeIndexOptions.PAGE_SIZE, capacity);
            options.set(RTreeIndexOptions.READ_FILE_CONCURRENCY, fileConcurrency);
            options.set(RTreeIndexOptions.READ_CACHE_SIZE, MemorySize.ofMebiBytes(cacheMiB));
            indexer = new RTreeGlobalIndexer(FIELD, options);
        }

        private void open() {
            reader = indexer.createReader(store, files, rows, null, executor);
        }

        private void reopen() throws IOException {
            reader.close();
            indexer = new RTreeGlobalIndexer(FIELD, options);
            open();
        }

        @Override
        public RoaringNavigableMap64 query(Envelope window) throws Exception {
            return reader.visitBBoxIntersects(
                            REF,
                            window.getMinX(),
                            window.getMinY(),
                            window.getMaxX(),
                            window.getMaxY())
                    .get()
                    .get()
                    .results();
        }

        @Override
        public long fileBytes() {
            return files.stream().mapToLong(GlobalIndexIOMeta::fileSize).sum();
        }

        @Override
        public void resetIo() {
            store.reads.set(0);
            store.bytes.set(0);
        }

        @Override
        public long reads() {
            return store.reads.get();
        }

        @Override
        public long readBytes() {
            return store.bytes.get();
        }

        @Override
        public void close() throws IOException {
            if (reader != null) {
                reader.close();
            }
            executor.shutdown();
            store.close();
        }
    }

    /** In-memory serialized files or actual local files; both use the production reader. */
    private static final class Store
            implements GlobalIndexFileWriter, GlobalIndexFileReader, Closeable {
        private int readDelayMs;
        private final LocalFileIO io = LocalFileIO.create();
        private final Path directory;
        private final Map<String, byte[]> bodies = new HashMap<>();
        private final AtomicLong reads = new AtomicLong();
        private final AtomicLong bytes = new AtomicLong();
        private int next;

        private Store(boolean disk) throws IOException {
            directory =
                    disk ? new Path(Files.createTempDirectory("rtree-benchmark-").toUri()) : null;
        }

        private Path path(String name) {
            return directory == null ? new Path(name) : new Path(directory, name);
        }

        private long length(Path path) throws IOException {
            return directory == null ? bodies.get(path.getName()).length : io.getFileSize(path);
        }

        @Override
        public String newFileName(String prefix) {
            return prefix + "-" + next++;
        }

        @Override
        public PositionOutputStream newOutputStream(String name) throws IOException {
            if (directory != null) {
                delayIo(ioLatencyMs);
                return io.newOutputStream(path(name), false);
            }
            return new PositionOutputStream() {
                private final ByteArrayOutputStream output = new ByteArrayOutputStream();

                @Override
                public long getPos() {
                    return output.size();
                }

                @Override
                public void write(int value) {
                    output.write(value);
                }

                @Override
                public void write(byte[] value) {
                    output.write(value, 0, value.length);
                }

                @Override
                public void write(byte[] value, int offset, int length) {
                    output.write(value, offset, length);
                }

                @Override
                public void flush() {}

                @Override
                public void close() {
                    bodies.put(name, output.toByteArray());
                }
            };
        }

        @Override
        public SeekableInputStream getInputStream(GlobalIndexIOMeta file) throws IOException {
            SeekableInputStream raw =
                    directory == null
                            ? new MemoryInput(bodies.get(file.filePath().getName()))
                            : io.newInputStream(file.filePath());
            return new CountedInput(raw, reads, bytes, readDelayMs);
        }

        @Override
        public void close() throws IOException {
            bodies.clear();
            if (directory != null) {
                io.delete(directory, true);
            }
        }
    }

    private static void delayIo(int millis) throws IOException {
        if (millis > 0) {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted during simulated FileIO latency", e);
            }
        }
    }

    private static final class CountedInput extends SeekableInputStream
            implements VectoredReadable {
        private final SeekableInputStream input;
        private final AtomicLong reads;
        private final AtomicLong bytes;

        private final int readDelayMs;

        private CountedInput(
                SeekableInputStream input, AtomicLong reads, AtomicLong bytes, int readDelayMs) {
            this.readDelayMs = readDelayMs;
            this.input = input;
            this.reads = reads;
            this.bytes = bytes;
        }

        private int count(int length) {
            reads.incrementAndGet();
            bytes.addAndGet(Math.max(0, length));
            return length;
        }

        @Override
        public int pread(long position, byte[] target, int offset, int length) throws IOException {
            delayIo(readDelayMs);
            return count(((VectoredReadable) input).pread(position, target, offset, length));
        }

        @Override
        public void seek(long position) throws IOException {
            input.seek(position);
        }

        @Override
        public long getPos() throws IOException {
            return input.getPos();
        }

        @Override
        public int read(byte[] target, int offset, int length) throws IOException {
            delayIo(readDelayMs);
            return count(input.read(target, offset, length));
        }

        @Override
        public int read() throws IOException {
            delayIo(readDelayMs);
            int result = input.read();
            count(result < 0 ? -1 : 1);
            return result;
        }

        @Override
        public void close() throws IOException {
            input.close();
        }
    }

    private static final class MemoryInput extends SeekableInputStream implements VectoredReadable {
        private final byte[] bytes;
        private int position;

        private MemoryInput(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public int pread(long position, byte[] target, int offset, int length) {
            int count = Math.min(length, bytes.length - (int) position);
            if (count <= 0) {
                return -1;
            }
            System.arraycopy(bytes, (int) position, target, offset, count);
            return count;
        }

        @Override
        public void seek(long position) {
            this.position = (int) position;
        }

        @Override
        public long getPos() {
            return position;
        }

        @Override
        public int read(byte[] target, int offset, int length) {
            int count = pread(position, target, offset, length);
            if (count > 0) {
                position += count;
            }
            return count;
        }

        @Override
        public int read() {
            return position < bytes.length ? bytes[position++] & 255 : -1;
        }

        @Override
        public void close() {}
    }

    /** Input is common primitive bounds; WKB and index-owned envelopes are built inside timing. */
    private static final class Data {
        private static final double DOMAIN = 1000000;
        private final int size;
        private final double[] bounds;
        private final boolean points;

        private Data(String distribution, int size) {
            this.size = size;
            bounds = new double[Math.multiplyExact(size, 4)];
            points = "uniform-points".equals(distribution);
            Random random = new Random(20261117);
            for (int i = 0; i < size; i++) {
                double x = random.nextDouble() * DOMAIN;
                double y = random.nextDouble() * DOMAIN;
                double width = 0;
                double height = 0;
                if ("clustered-boxes".equals(distribution)) {
                    int cluster = random.nextInt(16);
                    x = (cluster % 4 + 0.5) * DOMAIN / 4 + random.nextGaussian() * 15000;
                    y = (cluster / 4 + 0.5) * DOMAIN / 4 + random.nextGaussian() * 15000;
                    width = random.nextDouble() * 1000;
                    height = random.nextDouble() * 1000;
                } else if ("long-thin-boxes".equals(distribution)) {
                    x *= 0.8;
                    width = 50000 + random.nextDouble() * 150000;
                    height = 1 + random.nextDouble() * 49;
                } else if (!points) {
                    throw new IllegalArgumentException("Unknown distribution: " + distribution);
                }
                bounds[4 * i] = x;
                bounds[4 * i + 1] = y;
                bounds[4 * i + 2] = x + width;
                bounds[4 * i + 3] = y + height;
            }
        }

        private byte[] wkb(int row) {
            int offset = row * 4;
            ByteBuffer buffer =
                    ByteBuffer.allocate(points ? 21 : 41).order(ByteOrder.LITTLE_ENDIAN);
            buffer.put((byte) 1).putInt(points ? 1 : 2);
            if (!points) {
                buffer.putInt(2);
            }
            buffer.putDouble(bounds[offset]).putDouble(bounds[offset + 1]);
            if (!points) {
                buffer.putDouble(bounds[offset + 2]).putDouble(bounds[offset + 3]);
            }
            return buffer.array();
        }

        private Envelope[] windows(int count, double side) {
            Envelope[] queries = new Envelope[count];
            Random random = new Random(1987);
            for (int i = 0; i < count; i++) {
                int row = random.nextInt(size) * 4;
                double x =
                        i % 2 == 0
                                ? (bounds[row] + bounds[row + 2]) / 2
                                : random.nextDouble() * DOMAIN;
                double y =
                        i % 2 == 0
                                ? (bounds[row + 1] + bounds[row + 3]) / 2
                                : random.nextDouble() * DOMAIN;
                queries[i] = new Envelope(x - side / 2, x + side / 2, y - side / 2, y + side / 2);
            }
            return queries;
        }

        private RoaringNavigableMap64 scan(Envelope query) {
            RoaringNavigableMap64 result = new RoaringNavigableMap64();
            for (int i = 0; i < size; i++) {
                int at = i * 4;
                if (bounds[at] <= query.getMaxX()
                        && bounds[at + 2] >= query.getMinX()
                        && bounds[at + 1] <= query.getMaxY()
                        && bounds[at + 3] >= query.getMinY()) {
                    result.add(i);
                }
            }
            return result;
        }

        private void export(java.nio.file.Path path, int queries) throws IOException {
            try (DataOutputStream output =
                    new DataOutputStream(
                            new java.io.BufferedOutputStream(Files.newOutputStream(path)))) {
                output.writeInt(0x53504231);
                output.writeInt(size);
                output.writeInt(queries);
                output.writeInt(points ? 1 : 0);
                for (double value : bounds) {
                    output.writeDouble(value);
                }
                for (double side : new double[] {10000, 100000, 500000}) {
                    for (Envelope window : windows(queries, side)) {
                        output.writeDouble(window.getMinX());
                        output.writeDouble(window.getMinY());
                        output.writeDouble(window.getMaxX());
                        output.writeDouble(window.getMaxY());
                    }
                }
            }
        }
    }

    /** Observed used-heap high-water mark, sampled every millisecond; excludes native/OS memory. */
    private static final class HeapSampler implements AutoCloseable {
        private final AtomicLong peak = new AtomicLong(usedHeap());
        private final Thread thread;
        private volatile boolean running = true;

        private HeapSampler() {
            thread =
                    new Thread(
                            () -> {
                                while (running) {
                                    sample();
                                    try {
                                        Thread.sleep(1);
                                    } catch (InterruptedException e) {
                                        return;
                                    }
                                }
                            },
                            "rtree-benchmark-heap-sampler");
            thread.setDaemon(true);
            thread.start();
        }

        private void sample() {
            peak.accumulateAndGet(usedHeap(), Math::max);
        }

        @Override
        public void close() throws InterruptedException {
            sample();
            running = false;
            thread.interrupt();
            thread.join();
        }
    }
}
