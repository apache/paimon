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

package org.apache.paimon.spark.benchmark;

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.VectoredReadable;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.CentroidVectorIndexUtils;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.OffsetGlobalIndexReader;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.spark.SparkCatalog;
import org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;

import org.apache.spark.sql.SparkSession;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * Explicit opt-in benchmark of the actual Spark build and native Paimon index readers.
 *
 * <p>Arguments: data-directory, NEW output-directory, row-count, measured-query-count, repetitions,
 * nlist, rows-per-range-shard, threads, comma-separated nprobes. The input directory contains
 * base.fvecs, query.fvecs and groundtruth.ivecs. Ground truth must describe exactly the requested
 * base rows; no prefix ground truth is inferred.
 *
 * <p>Import and row-ID mapping are excluded from build time. Query time includes fresh reader
 * creation, routing/model loads, all shard searches, top-K merge and close. It excludes manifest
 * planning, source-row fetch and SQL scheduling. Every configuration uses the same held-out query
 * IDs, top-K, warm OS page cache and no refinement. I/O counters measure logical bytes actually
 * returned by Paimon streams, including metadata and routing, and preserve vectored reads.
 *
 * <p>Run serially, after compilation, with RAYON_NUM_THREADS set to the thread argument. Output
 * contains every query and every build; there are no timing assertions or automatic test runs.
 *
 * <p>System properties: {@code benchmark.range-based.options} and {@code
 * benchmark.centroid-based.options} add comma-separated procedure options for each mode. {@code
 * benchmark.range-based.nprobes} and {@code benchmark.centroid-based.nprobes} override the probe
 * sweep separately. Use the value {@code auto} to omit the probe option. {@code
 * benchmark.query-offset} skips that many entries in the fixed shuffled query order before warmup,
 * allowing disjoint calibration and validation sets. {@code benchmark.source-warehouse} reuses this
 * benchmark's existing source table; index generations are replaced but source rows are unchanged.
 * {@code benchmark.modes} selects one or both modes. With one mode, {@code
 * benchmark.skip-build=true} reads the current generation and records build time as -1
 * (unmeasured). {@code benchmark.source-ranges} splits the source partition into this many disjoint
 * row-ID ranges by inserting a row in a different partition between commits. The default is one.
 * {@code benchmark.index-type} selects any native vector index family. {@code
 * benchmark.read-snapshot} selects a retained generation for query-only sweeps. DiskANN
 * independently sweeps {@code benchmark.<mode>.l-searches} and outer centroid probes; range mode
 * has no outer probe. {@code benchmark.reuse-readers=true} is exclusively for recall calibration:
 * it records I/O as -1 and its query timing must not be compared with fresh-reader measurements.
 * {@code benchmark.top-k} defaults to 10. {@code benchmark.allowed-ids} points to a text file with
 * one eligible base vector ID per line. Supply exact ground truth over those eligible vectors when
 * using a filter.
 */
public final class VectorShardingBenchmark {
    private static final int WARMUP = 16;
    private static final long SEED = 20261117L;

    private final java.nio.file.Path data;
    private final java.nio.file.Path output;
    private final int rows;
    private final int queryCount;
    private final int repetitions;
    private final int nlist;
    private final int rangeRows;
    private final int threads;
    private final List<Integer> probes;
    private final List<String> configuredModes;
    private final int topK = Integer.parseInt(System.getProperty("benchmark.top-k", "10"));
    private final String allowedIds = System.getProperty("benchmark.allowed-ids");
    private RoaringNavigableMap64 includeRowIds;
    private final int sourceRanges =
            Integer.parseInt(System.getProperty("benchmark.source-ranges", "1"));
    private final int queryOffset =
            Integer.parseInt(System.getProperty("benchmark.query-offset", "0"));
    private final String sourceWarehouse = System.getProperty("benchmark.source-warehouse");
    private final boolean skipBuild =
            Boolean.parseBoolean(System.getProperty("benchmark.skip-build", "false"));
    private final String indexType = System.getProperty("benchmark.index-type", "ivf-pq");
    private final boolean reuseReaders =
            Boolean.parseBoolean(System.getProperty("benchmark.reuse-readers", "false"));
    private final String readSnapshot = System.getProperty("benchmark.read-snapshot");
    private final LocalFileIO io = new LocalFileIO();
    private SparkSession spark;
    private Path tablePath;
    private int dimension;
    private long[] logicalIds;
    private float[][] queries;
    private int[][] truth;
    private List<Integer> queryIds;
    private ExecutorService executor;

    private VectorShardingBenchmark(String[] args) {
        if (args.length != 9) {
            throw new IllegalArgumentException("Expected 9 arguments; see class documentation.");
        }
        data = java.nio.file.Paths.get(args[0]);
        output = java.nio.file.Paths.get(args[1]);
        rows = Integer.parseInt(args[2]);
        queryCount = Integer.parseInt(args[3]);
        repetitions = Integer.parseInt(args[4]);
        nlist = Integer.parseInt(args[5]);
        rangeRows = Integer.parseInt(args[6]);
        threads = Integer.parseInt(args[7]);
        configuredModes =
                Arrays.asList(
                        System.getProperty("benchmark.modes", "range-based,centroid-based")
                                .split(","));
        if (!Arrays.asList("ivf-flat", "ivf-pq", "ivf-sq", "ivf-rq", "diskann").contains(indexType)
                || (readSnapshot != null && !skipBuild)
                || configuredModes.isEmpty()
                || new HashSet<>(configuredModes).size() != configuredModes.size()
                || !Arrays.asList("range-based", "centroid-based").containsAll(configuredModes)
                || queryOffset < 0
                || (skipBuild && (sourceWarehouse == null || configuredModes.size() != 1))) {
            throw new IllegalArgumentException(
                    "Invalid modes, query offset or existing-index configuration");
        }
        probes = new ArrayList<>();
        for (String value : args[8].split(",")) {
            int probe = Integer.parseInt(value);
            if (probe < 1 || probe > nlist) {
                throw new IllegalArgumentException("nprobe must be in [1, nlist]");
            }
            probes.add(probe);
        }
        if (sourceRanges < 1
                || sourceRanges > rows
                || topK < 1
                || rows < topK
                || queryCount < 1
                || repetitions < 1
                || threads < 1
                || rangeRows < 1) {
            throw new IllegalArgumentException("Invalid workload size");
        }
        if (!String.valueOf(threads).equals(System.getenv("RAYON_NUM_THREADS"))) {
            throw new IllegalArgumentException("Set RAYON_NUM_THREADS=" + threads);
        }
    }

    public static void main(String[] args) throws Exception {
        Locale.setDefault(Locale.ROOT);
        new VectorShardingBenchmark(args).run();
    }

    private void run() throws Exception {
        Files.createDirectory(output);
        try (DataInputStream in = input("base.fvecs")) {
            dimension = littleInt(in);
        }
        if (dimension < 4
                || dimension % 4 != 0
                || Files.size(data.resolve("base.fvecs")) != (long) rows * (dimension + 1) * 4) {
            throw new IllegalArgumentException("Base shape differs from requested rows/dimension");
        }
        queries = loadQueries();
        truth = loadTruth();
        if (truth.length != queries.length
                || (long) queries.length < (long) queryOffset + queryCount + WARMUP) {
            throw new IllegalArgumentException(
                    "Need aligned ground truth and enough held-out queries");
        }
        queryIds = new ArrayList<>();
        for (int i = 0; i < queries.length; i++) {
            queryIds.add(i);
        }
        Collections.shuffle(queryIds, new Random(SEED));
        String excludedIds = System.getProperty("benchmark.exclude-query-ids");
        if (excludedIds != null) {
            Set<Integer> excluded = new HashSet<>();
            for (String id : Files.readAllLines(java.nio.file.Paths.get(excludedIds))) {
                excluded.add(Integer.parseInt(id));
            }
            queryIds.removeIf(excluded::contains);
            if (queryIds.size() < queryOffset + queryCount + WARMUP) {
                throw new IllegalArgumentException("Too few queries after exclusions");
            }
        }
        queryIds = new ArrayList<>(queryIds.subList(queryOffset, queryIds.size()));
        Files.write(
                output.resolve("workload.txt"),
                Arrays.asList(
                        "indexType=" + indexType,
                        "rows=" + rows,
                        "dimension=" + dimension,
                        "topK=" + topK,
                        "allowedIds=" + allowedIds,
                        "queries=" + queryCount,
                        "warmup=" + WARMUP,
                        "repetitions=" + repetitions,
                        "nlist=" + nlist,
                        "defaultPQm=" + (dimension / 4),
                        "metric=l2",
                        "train.sample-ratio=1.0",
                        "rangeRows=" + rangeRows,
                        "sourceRanges=" + sourceRanges,
                        "threads=" + threads,
                        "nprobes=" + probes,
                        "rangeNprobes=" + probesForMode("range-based"),
                        "centroidNprobes=" + probesForMode("centroid-based"),
                        "seed=" + SEED,
                        "queryOffset=" + queryOffset,
                        "buildSkipped=" + skipBuild,
                        "sourceWarehouse=" + sourceWarehouse,
                        "rangeOptions=" + buildOptions("range-based"),
                        "centroidOptions=" + buildOptions("centroid-based"),
                        "readSnapshot=" + readSnapshot,
                        "rangeLSearches=" + lSearchesForMode("range-based"),
                        "centroidLSearches=" + lSearchesForMode("centroid-based"),
                        "readerLifetime="
                                + (reuseReaders ? "quality-only-reused" : "fresh-per-query"),
                        "pageCache=warm",
                        "refinement=false",
                        "java=" + System.getProperty("java.version"),
                        "os=" + System.getProperty("os.name") + " " + System.getProperty("os.arch"),
                        "queryIds=" + queryIds.subList(WARMUP, WARMUP + queryCount)));
        executor = Executors.newFixedThreadPool(threads);
        spark =
                SparkSession.builder()
                        .master("local[" + threads + "]")
                        .appName("VectorShardingBenchmark")
                        .config("spark.ui.enabled", false)
                        .config("spark.sql.catalog.paimon", SparkCatalog.class.getName())
                        .config(
                                "spark.sql.catalog.paimon.warehouse",
                                sourceWarehouse == null
                                        ? output.resolve("warehouse").toString()
                                        : sourceWarehouse)
                        .config("spark.sql.defaultCatalog", "paimon")
                        .config(
                                "spark.sql.extensions",
                                PaimonSparkSessionExtensions.class.getName())
                        .config("spark.sql.shuffle.partitions", threads)
                        .config("spark.default.parallelism", threads)
                        .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        try (PrintWriter builds =
                        csv(
                                "builds.csv",
                                "repetition,mode,rows,build_ns,index_files,index_bytes,snapshot_id,index_type");
                PrintWriter measurements =
                        csv(
                                "queries.csv",
                                "repetition,mode,nprobe,query_id,latency_ns,hits_at_"
                                        + topK
                                        + ",result_count,files,opens,read_calls,read_bytes,routing_bytes,l_search,index_type")) {
            spark.sql("CREATE DATABASE IF NOT EXISTS bench").collect();
            spark.sql("USE bench").collect();
            tablePath =
                    new Path(
                            (sourceWarehouse == null
                                            ? output.resolve("warehouse")
                                            : java.nio.file.Paths.get(sourceWarehouse))
                                    .resolve("bench.db/vectors")
                                    .toString());
            if (sourceWarehouse == null) {
                spark.sql(
                                "CREATE TABLE vectors (id INT, v ARRAY<FLOAT>"
                                        + (sourceRanges == 1 ? ")" : ", p INT) PARTITIONED BY (p)")
                                        + " TBLPROPERTIES ("
                                        + "'bucket'='-1','row-tracking.enabled'='true','data-evolution.enabled'='true',"
                                        + "'fields.v.dimension'='"
                                        + dimension
                                        + "','fields.v.metric'='l2',"
                                        + "'global-index.row-count-per-shard'='"
                                        + rangeRows
                                        + "',"
                                        + "'global-index.build.max-parallelism'='"
                                        + threads
                                        + "')")
                        .collect();
                importSource();
            }
            mapRowIds();
            for (int repeat = 0; repeat < repetitions; repeat++) {
                List<String> modes = new ArrayList<>(configuredModes);
                if (repeat % 2 != 0) {
                    Collections.reverse(modes);
                }
                for (String mode : modes) {
                    // Every family can reuse the immutable source table.
                    spark.sql(
                                    "ALTER TABLE vectors SET TBLPROPERTIES ('fields.v.dimension'='"
                                            + dimension
                                            + "','fields.v.metric'='l2')")
                            .collect();
                    long elapsed = -1;
                    if (!skipBuild) {
                        Map<String, String> options = buildOptions(mode);
                        List<String> pairs = new ArrayList<>();
                        options.forEach((key, value) -> pairs.add(key + "=" + value));
                        for (String type :
                                Arrays.asList(
                                        "ivf-flat",
                                        "ivf-pq",
                                        "ivf-sq",
                                        "ivf-rq",
                                        "diskann",
                                        "ivf-flat-centroid",
                                        "ivf-pq-centroid",
                                        "ivf-sq-centroid",
                                        "ivf-rq-centroid",
                                        "diskann-centroid")) {
                            spark.sql(
                                            "CALL sys.drop_global_index(table=>'bench.vectors',"
                                                    + "index_column=>'v',index_type=>'"
                                                    + type
                                                    + "')")
                                    .collect();
                        }
                        long started = System.nanoTime();
                        spark.sql(
                                        "CALL sys.create_global_index(table=>'bench.vectors',"
                                                + "index_column=>'v',index_type=>'"
                                                + typeForMode(mode)
                                                + "',"
                                                + (sourceRanges == 1 ? "" : "partitions=>'p=0',")
                                                + "options=>'"
                                                + String.join(",", pairs)
                                                + "')")
                                .collect();
                        elapsed = System.nanoTime() - started;
                    }
                    FileStoreTable table = table();
                    Snapshot snapshot =
                            readSnapshot == null
                                    ? table.snapshotManager().latestSnapshot()
                                    : table.snapshotManager()
                                            .snapshot(Long.parseLong(readSnapshot));
                    List<IndexFileMeta> files = new ArrayList<>();
                    for (IndexManifestEntry entry :
                            table.store().newIndexFileHandler().scan(snapshot, typeForMode(mode))) {
                        files.add(entry.indexFile());
                    }
                    if (files.isEmpty()) {
                        throw new IllegalStateException("Build produced no index");
                    }
                    boolean centroid =
                            files.stream().anyMatch(CentroidVectorIndexUtils::isRoutingModel);
                    if (centroid != "centroid-based".equals(mode)) {
                        throw new IllegalStateException(
                                "Existing index does not match the requested mode");
                    }
                    long bytes = files.stream().mapToLong(IndexFileMeta::fileSize).sum();
                    builds.printf(
                            "%d,%s,%d,%d,%d,%d,%d,%s%n",
                            repeat,
                            mode,
                            rows,
                            elapsed,
                            files.size(),
                            bytes,
                            snapshot.id(),
                            typeForMode(mode));
                    builds.flush();
                    System.out.printf(
                            "BUILD repeat=%d mode=%s seconds=%.3f files=%d MiB=%.2f%n",
                            repeat, mode, elapsed / 1e9, files.size(), bytes / 1048576.0);
                    // Retain metadata for reproducibility even after the next index generation
                    // replaces it.
                    List<String> fileNames = new ArrayList<>();
                    List<GlobalIndexIOMeta> metas = new ArrayList<>();
                    byte[] warm = new byte[1024 * 1024];
                    for (IndexFileMeta file : files) {
                        Path path =
                                table.store().pathFactory().globalIndexFileFactory().toPath(file);
                        fileNames.add(path + "," + file.fileSize() + "," + file.rowCount());
                        metas.add(
                                new GlobalIndexIOMeta(
                                        path,
                                        file.fileSize(),
                                        file.rowCount(),
                                        file.globalIndexMeta().indexMeta()));
                        try (SeekableInputStream in = io.newInputStream(path)) {
                            while (in.read(warm) >= 0) {
                                // Warm every index file before measuring either mode.
                            }
                        }
                    }
                    Files.write(output.resolve("files-" + repeat + "-" + mode + ".csv"), fileNames);
                    List<Integer> widths = probesForMode(mode);
                    Collections.shuffle(widths, new Random(SEED + repeat));
                    List<GlobalIndexReader> cached =
                            reuseReaders
                                    ? openReaders(
                                            mode,
                                            files,
                                            metas,
                                            meta -> io.newInputStream(meta.filePath()))
                                    : null;
                    try {
                        for (int probe : widths) {
                            for (int lSearch : lSearchesForMode(mode)) {
                                for (int i = 0; i < WARMUP + queryCount; i++) {
                                    int id = queryIds.get(i);
                                    Observation observation =
                                            query(
                                                    mode,
                                                    probe,
                                                    lSearch,
                                                    queries[id],
                                                    files,
                                                    metas,
                                                    cached);
                                    int hits = 0;
                                    for (long rowId : observation.result.results()) {
                                        if (rowId < 0
                                                || rowId >= logicalIds.length
                                                || logicalIds[(int) rowId] < 0) {
                                            throw new IllegalStateException(
                                                    "Unknown absolute row ID " + rowId);
                                        }
                                        if (includeRowIds != null
                                                && !includeRowIds.contains(rowId)) {
                                            throw new IllegalStateException(
                                                    "Search returned an excluded row ID " + rowId);
                                        }
                                        if (Arrays.binarySearch(
                                                        truth[id], (int) logicalIds[(int) rowId])
                                                >= 0) {
                                            hits++;
                                        }
                                    }
                                    if (observation.result.results().getLongCardinality() != topK) {
                                        throw new IllegalStateException("Incomplete top-K");
                                    }
                                    if (i >= WARMUP) {
                                        Stats stats = observation.stats;
                                        measurements.printf(
                                                "%d,%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%s%n",
                                                repeat,
                                                mode,
                                                probe,
                                                id,
                                                observation.nanos,
                                                hits,
                                                topK,
                                                reuseReaders ? -1 : stats.files.size(),
                                                reuseReaders ? -1 : stats.opens.sum(),
                                                reuseReaders ? -1 : stats.calls.sum(),
                                                reuseReaders ? -1 : stats.bytes.sum(),
                                                reuseReaders ? -1 : stats.routingBytes.sum(),
                                                lSearch,
                                                typeForMode(mode));
                                    }
                                }
                                measurements.flush();
                                System.out.printf(
                                        "QUERIES repeat=%d mode=%s nprobe=%d l_search=%d count=%d%n",
                                        repeat, mode, probe, lSearch, queryCount);
                            }
                        }
                    } finally {
                        if (cached != null) {
                            for (GlobalIndexReader reader : cached) {
                                reader.close();
                            }
                        }
                    }
                }
            }
        } finally {
            spark.stop();
            executor.shutdownNow();
        }
    }

    private FileStoreTable table() {
        return FileStoreTableFactory.create(io, tablePath);
    }

    private Map<String, String> buildOptions(String mode) {
        Map<String, String> options = new LinkedHashMap<>();
        String type = typeForMode(mode);
        if (!"diskann".equals(indexType) || "centroid-based".equals(mode)) {
            options.put(type + ".nlist", String.valueOf(nlist));
        }
        if ("ivf-pq".equals(indexType) || "diskann".equals(indexType)) {
            options.put(type + ".pq.m", String.valueOf(dimension / 4));
        }
        options.put(type + ".train.sample-ratio", "1.0");
        String extra = System.getProperty("benchmark." + mode + ".options", "");
        if (!extra.isEmpty()) {
            for (String pair : extra.split(",")) {
                String[] entry = pair.split("=", 2);
                if (entry.length != 2
                        || !entry[0].startsWith(indexType + ".")
                        || entry[1].isEmpty()) {
                    throw new IllegalArgumentException("Invalid benchmark option: " + pair);
                }
                options.put(type + entry[0].substring(indexType.length()), entry[1]);
            }
        }
        return options;
    }

    private String typeForMode(String mode) {
        return "centroid-based".equals(mode) ? indexType + "-centroid" : indexType;
    }

    private List<Integer> probesForMode(String mode) {
        if ("diskann".equals(indexType) && "range-based".equals(mode)) {
            return new ArrayList<>(Collections.singletonList(0));
        }
        String override = System.getProperty("benchmark." + mode + ".nprobes");
        if (override == null) {
            return new ArrayList<>(probes);
        }
        List<Integer> widths = new ArrayList<>();
        int modeNlist = Integer.parseInt(buildOptions(mode).get(typeForMode(mode) + ".nlist"));
        for (String value : override.split(",")) {
            boolean automatic = "auto".equals(value);
            int width = automatic ? 0 : Integer.parseInt(value);
            if ((!automatic && width < 1) || width > modeNlist || widths.contains(width)) {
                throw new IllegalArgumentException("Invalid nprobe override: " + override);
            }
            widths.add(width);
        }
        return widths;
    }

    private void importSource() throws Exception {
        try (DataInputStream in = input("base.fvecs")) {
            int start = 0;
            for (int range = 0; range < sourceRanges; range++) {
                int end = (int) ((long) (range + 1) * rows / sourceRanges);
                BatchWriteBuilder builder = table().newBatchWriteBuilder();
                try (BatchTableWrite writer = builder.newWrite();
                        BatchTableCommit commit = builder.newCommit()) {
                    for (int id = start; id < end; id++) {
                        GenericArray v = new GenericArray(vector(in));
                        writer.write(
                                sourceRanges == 1 ? GenericRow.of(id, v) : GenericRow.of(id, v, 0));
                    }
                    commit.commit(writer.prepareCommit());
                }
                if (range + 1 < sourceRanges) {
                    spark.sql("INSERT INTO vectors VALUES (-1, CAST(NULL AS ARRAY<FLOAT>), 1)")
                            .collect();
                }
                start = end;
            }
        }
        System.out.println("Imported source rows=" + rows);
    }

    private void mapRowIds() throws Exception {
        FileStoreTable table = table();
        ReadBuilder builder =
                table.newReadBuilder()
                        .withReadType(
                                new RowType(
                                        true,
                                        Arrays.asList(
                                                table.rowType().getField("id"),
                                                SpecialFields.ROW_ID)));
        logicalIds = new long[Math.addExact(rows, sourceRanges - 1)];
        Arrays.fill(logicalIds, -1);
        Set<Integer> ids = new HashSet<>();
        try (RecordReader<InternalRow> reader =
                builder.newRead().createReader(builder.newScan().plan())) {
            reader.forEachRemaining(
                    row -> {
                        int id = row.getInt(0);
                        if (id == -1 && sourceRanges > 1) {
                            return;
                        }
                        long rowId = row.getLong(1);
                        if (rowId < 0
                                || rowId >= logicalIds.length
                                || logicalIds[(int) rowId] != -1
                                || !ids.add(id)) {
                            throw new IllegalStateException("Non-bijective source ID mapping");
                        }
                        logicalIds[(int) rowId] = id;
                    });
        }
        if (ids.size() != rows) {
            throw new IllegalStateException("Incomplete source mapping");
        }
        if (allowedIds != null) {
            Set<Integer> allowed = new HashSet<>();
            for (String id : Files.readAllLines(java.nio.file.Paths.get(allowedIds))) {
                allowed.add(Integer.parseInt(id));
            }
            if (!ids.containsAll(allowed) || allowed.size() < topK) {
                throw new IllegalArgumentException(
                        "Filter must contain at least top-K valid base IDs");
            }
            includeRowIds = new RoaringNavigableMap64();
            for (int rowId = 0; rowId < logicalIds.length; rowId++) {
                if (logicalIds[rowId] >= 0 && allowed.contains((int) logicalIds[rowId])) {
                    includeRowIds.add(rowId);
                }
            }
            for (int[] neighbors : truth) {
                for (int id : neighbors) {
                    if (!allowed.contains(id)) {
                        throw new IllegalArgumentException(
                                "Ground truth contains an excluded base ID");
                    }
                }
            }
        }
        System.out.println("Validated absolute row-ID mapping rows=" + ids.size());
    }

    private List<Integer> lSearchesForMode(String mode) {
        if (!"diskann".equals(indexType)) {
            return Collections.singletonList(0);
        }
        List<Integer> result = new ArrayList<>();
        for (String value :
                System.getProperty(
                                "benchmark." + mode + ".l-searches",
                                String.valueOf(Math.max(64, topK)))
                        .split(",")) {
            int width = Integer.parseInt(value);
            if (width < topK || result.contains(width)) {
                throw new IllegalArgumentException("Invalid DiskANN search width: " + value);
            }
            result.add(width);
        }
        return result;
    }

    private Observation query(
            String mode,
            int probe,
            int lSearch,
            float[] vector,
            List<IndexFileMeta> files,
            List<GlobalIndexIOMeta> metas,
            List<GlobalIndexReader> cached)
            throws Exception {
        Stats stats = new Stats();
        GlobalIndexFileReader input =
                meta -> {
                    stats.opens.increment();
                    stats.files.add(meta.filePath().toString());
                    SeekableInputStream delegate = io.newInputStream(meta.filePath());
                    if (!(delegate instanceof VectoredReadable)) {
                        delegate.close();
                        throw new IOException("Local benchmark requires vectored reads");
                    }
                    return new CountingInput(
                            delegate, stats, meta.filePath().getName().contains("training-model"));
                };
        Map<String, String> options = new LinkedHashMap<>();
        if (probe > 0) {
            options.put(
                    "centroid-based".equals(mode) ? "centroid.nprobe" : "ivf.nprobe",
                    String.valueOf(probe));
        }
        String inner = System.getProperty("benchmark.local-nprobe");
        if (inner != null && "centroid-based".equals(mode)) {
            options.put("ivf.nprobe", inner);
        }
        if (lSearch > 0) {
            options.put("diskann.l_search", String.valueOf(lSearch));
        }
        VectorSearch search =
                new VectorSearch(vector, topK, "v", options).withIncludeRowIds(includeRowIds);
        long started = System.nanoTime();
        List<GlobalIndexReader> readers =
                cached == null ? openReaders(mode, files, metas, input) : cached;
        ScoredGlobalIndexResult result;
        try {
            List<CompletableFuture<Optional<ScoredGlobalIndexResult>>> futures = new ArrayList<>();
            for (GlobalIndexReader reader : readers) {
                futures.add(reader.visitVectorSearch(search));
            }
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .get(120, TimeUnit.SECONDS);
            List<ScoredGlobalIndexResult> results = new ArrayList<>();
            for (CompletableFuture<Optional<ScoredGlobalIndexResult>> future : futures) {
                future.get().ifPresent(results::add);
            }
            result = ScoredGlobalIndexResult.merge(results).topK(topK);
        } finally {
            if (cached == null) {
                for (GlobalIndexReader reader : readers) {
                    reader.close();
                }
            }
        }
        return new Observation(result, System.nanoTime() - started, stats);
    }

    private List<GlobalIndexReader> openReaders(
            String mode,
            List<IndexFileMeta> files,
            List<GlobalIndexIOMeta> metas,
            GlobalIndexFileReader input)
            throws IOException {
        GlobalIndexer indexer =
                GlobalIndexer.create(
                        typeForMode(mode),
                        Collections.singletonList(
                                new DataField(0, "v", new ArrayType(new FloatType()))),
                        new Options());
        List<GlobalIndexReader> readers = new ArrayList<>();
        try {
            Map<Range, List<GlobalIndexIOMeta>> groups = new LinkedHashMap<>();
            for (int i = 0; i < files.size(); i++) {
                groups.computeIfAbsent(
                                files.get(i).globalIndexMeta().rowRange(), k -> new ArrayList<>())
                        .add(metas.get(i));
            }
            for (Map.Entry<Range, List<GlobalIndexIOMeta>> group : groups.entrySet()) {
                Range range = group.getKey();
                readers.add(
                        new OffsetGlobalIndexReader(
                                indexer.createReader(
                                        input, group.getValue(), range.count(), null, executor),
                                range.from,
                                range.to));
            }
            return readers;
        } catch (RuntimeException e) {
            for (GlobalIndexReader reader : readers) {
                try {
                    reader.close();
                } catch (IOException close) {
                    e.addSuppressed(close);
                }
            }
            throw e;
        }
    }

    private DataInputStream input(String name) throws IOException {
        return new DataInputStream(
                new BufferedInputStream(Files.newInputStream(data.resolve(name)), 1 << 20));
    }

    private static int littleInt(DataInputStream in) throws IOException {
        return Integer.reverseBytes(in.readInt());
    }

    private float[] vector(DataInputStream in) throws IOException {
        if (littleInt(in) != dimension) {
            throw new IOException("Inconsistent vector dimension");
        }
        float[] vector = new float[dimension];
        for (int i = 0; i < dimension; i++) {
            vector[i] = Float.intBitsToFloat(littleInt(in));
            if (!Float.isFinite(vector[i])) {
                throw new IOException("Non-finite vector");
            }
        }
        return vector;
    }

    private float[][] loadQueries() throws IOException {
        int count =
                Math.toIntExact(Files.size(data.resolve("query.fvecs")) / ((dimension + 1L) * 4));
        float[][] vectors = new float[count][];
        try (DataInputStream in = input("query.fvecs")) {
            for (int i = 0; i < count; i++) {
                vectors[i] = vector(in);
            }
        }
        return vectors;
    }

    private int[][] loadTruth() throws IOException {
        int width;
        try (DataInputStream in = input("groundtruth.ivecs")) {
            width = littleInt(in);
        }
        if (width < topK) {
            throw new IOException("Ground truth is shorter than top-K");
        }
        int count =
                Math.toIntExact(Files.size(data.resolve("groundtruth.ivecs")) / ((width + 1L) * 4));
        int[][] neighbors = new int[count][topK];
        try (DataInputStream in = input("groundtruth.ivecs")) {
            for (int i = 0; i < count; i++) {
                if (littleInt(in) != width) {
                    throw new IOException("Inconsistent ground truth width");
                }
                for (int j = 0; j < width; j++) {
                    int id = littleInt(in);
                    if (id < 0 || id >= rows) {
                        throw new IOException("Ground truth must refer to the exact source corpus");
                    }
                    if (j < topK) {
                        neighbors[i][j] = id;
                    }
                }
                Arrays.sort(neighbors[i]);
            }
        }
        return neighbors;
    }

    private PrintWriter csv(String name, String header) throws IOException {
        PrintWriter writer =
                new PrintWriter(
                        Files.newBufferedWriter(
                                output.resolve(name), StandardOpenOption.CREATE_NEW));
        writer.println(header);
        writer.flush();
        return writer;
    }

    private static class Observation {
        final ScoredGlobalIndexResult result;
        final long nanos;
        final Stats stats;

        Observation(ScoredGlobalIndexResult result, long nanos, Stats stats) {
            this.result = result;
            this.nanos = nanos;
            this.stats = stats;
        }
    }

    private static class Stats {
        final Set<String> files = ConcurrentHashMap.newKeySet();
        final LongAdder opens = new LongAdder();
        final LongAdder calls = new LongAdder();
        final LongAdder bytes = new LongAdder();
        final LongAdder routingBytes = new LongAdder();

        int record(int count, boolean routing) {
            calls.increment();
            if (count > 0) {
                bytes.add(count);
                if (routing) {
                    routingBytes.add(count);
                }
            }
            return count;
        }
    }

    private static class CountingInput extends SeekableInputStream implements VectoredReadable {
        private final SeekableInputStream delegate;
        private final Stats stats;
        private final boolean routing;

        CountingInput(SeekableInputStream delegate, Stats stats, boolean routing) {
            this.delegate = delegate;
            this.stats = stats;
            this.routing = routing;
        }

        @Override
        public int read() throws IOException {
            int value = delegate.read();
            stats.record(value < 0 ? 0 : 1, routing);
            return value;
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
            return stats.record(delegate.read(buffer, offset, length), routing);
        }

        @Override
        public int pread(long position, byte[] buffer, int offset, int length) throws IOException {
            return stats.record(
                    ((VectoredReadable) delegate).pread(position, buffer, offset, length), routing);
        }

        @Override
        public void seek(long desired) throws IOException {
            delegate.seek(desired);
        }

        @Override
        public long getPos() throws IOException {
            return delegate.getPos();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
