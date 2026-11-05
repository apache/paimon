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
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.operation.AbstractFileStoreScan;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.operation.ManifestsReader;
import org.apache.paimon.operation.PartitionEntryScanner;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.stats.SimpleStatsConverter;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.SegmentsCache;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Compares the generic partition scan with the partition maintenance scanner on identical
 * manifests. Manifest-list I/O and data-file I/O are excluded. "Cold" refers to Paimon's cache, not
 * the OS cache.
 *
 * <pre>
 * mvn -pl paimon-benchmark/paimon-micro-benchmarks -am -Pfast-build \
 *   -DwildcardSuites=none -DfailIfNoTests=false -Dtest=PartitionEntryScanBenchmark test
 * </pre>
 *
 * <p>Use {@code partition.scan.benchmark.manifests}, {@code entries}, {@code partitions}, {@code
 * stats-columns}, {@code index-bytes}, {@code parallelism}, {@code warmups}, {@code iterations},
 * {@code cache-mib}, and {@code caches} (a comma-separated list) to configure the experiment. The
 * stats-columns property counts string columns in addition to the integer partition column.
 */
public class PartitionEntryScanBenchmark {

    private static final RowType PARTITION_TYPE = RowType.of(new IntType());

    @TempDir java.nio.file.Path tempDir;

    @Test
    void compareScanners() throws Exception {
        int warmups = property("warmups", 2);
        int iterations = property("iterations", 5);
        System.out.printf(
                "PARTITION_SCAN_CONFIG manifests=%d entries=%d partitions=%d statsColumns=%d stringColumns=%d "
                        + "indexBytes=%d parallelism=%d warmups=%d iterations=%d cacheMiB=%d%n",
                property("manifests", 8),
                property("entries", 2000),
                property("partitions", 1000),
                createRowType().getFieldCount(),
                property("stats-columns", 32),
                property("index-bytes", 512),
                property("parallelism", 1),
                warmups,
                iterations,
                property("cache-mib", 512));
        for (String cache :
                System.getProperty(
                                "partition.scan.benchmark.caches",
                                "none,whole-cold,whole-warm,blocks-cold,blocks-warm")
                        .split(",")) {
            for (boolean deletes : new boolean[] {false, true}) {
                Fixture fixture = new Fixture(cache, deletes);
                for (int selectivity : new int[] {100, 10}) {
                    List<Sample> baseline = new ArrayList<>();
                    List<Sample> projected = new ArrayList<>();
                    for (int iteration = -warmups; iteration < iterations; iteration++) {
                        // Alternate execution order to reduce the effect of JIT and machine drift.
                        Sample full;
                        Sample narrow;
                        if ((iteration & 1) == 0) {
                            full = fixture.measure(false, selectivity);
                            narrow = fixture.measure(true, selectivity);
                        } else {
                            narrow = fixture.measure(true, selectivity);
                            full = fixture.measure(false, selectivity);
                        }
                        assertThat(narrow.entries)
                                .containsExactlyInAnyOrderElementsOf(full.entries);
                        if (iteration >= 0) {
                            baseline.add(full);
                            projected.add(narrow);
                        }
                    }
                    report(cache, deletes, selectivity, "full", baseline);
                    report(cache, deletes, selectivity, "projected", projected);
                }
            }
        }
    }

    private class Fixture {
        private final String cacheMode;
        private final FileStoreTable table;
        private final FileSystemSchemaManager schemaManager;
        private final List<ManifestFileMeta> manifests = new ArrayList<>();

        private Fixture(String cacheMode, boolean deletes) throws Exception {
            this.cacheMode = cacheMode;
            Path path = new Path(tempDir.resolve(cacheMode + "-" + deletes).toString());
            Options options = new Options();
            options.set(CoreOptions.BUCKET, -1);
            options.set(CoreOptions.MANIFEST_SIDECAR_ENABLED, cacheMode.startsWith("blocks"));
            options.set("manifest.target-file-size", "1GB");
            LocalFileIO io = LocalFileIO.create();
            schemaManager = new FileSystemSchemaManager(io, path);
            RowType rowType = createRowType();
            TableSchema schema =
                    schemaManager.createTable(
                            new Schema(
                                    rowType.getFields(),
                                    Collections.singletonList("pt"),
                                    Collections.emptyList(),
                                    options.toMap(),
                                    ""));
            table =
                    FileStoreTableFactory.create(
                            io, path, schema, options, CatalogEnvironment.empty());
            ManifestFile file = table.store().manifestFileFactory().create();
            List<SimpleStats> partitionStats = new ArrayList<>();
            for (int partition = 0; partition < property("partitions", 1000); partition++) {
                SimpleStats stats = createStats(rowType, partition);
                assertThat(stats.minValues().getFieldCount()).isEqualTo(schema.fields().size());
                assertThat(stats.maxValues().getFieldCount()).isEqualTo(schema.fields().size());
                assertThat(stats.nullCounts().size()).isEqualTo(schema.fields().size());
                assertThat(stats.minValues().getInt(0)).isEqualTo(partition);
                assertThat(stats.maxValues().getInt(0)).isEqualTo(partition);
                partitionStats.add(stats);
            }
            byte[] index = null;
            if (property("index-bytes", 512) > 0) {
                index = new byte[property("index-bytes", 512)];
                new Random(42).nextBytes(index);
            }
            for (int manifest = 0; manifest < property("manifests", 8); manifest++) {
                List<ManifestEntry> entries = new ArrayList<>();
                List<ManifestEntry> deleted = new ArrayList<>();
                for (int row = 0; row < property("entries", 2000); row++) {
                    BinaryRow partition =
                            new InternalRowSerializer(PARTITION_TYPE)
                                    .toBinaryRow(GenericRow.of(row % property("partitions", 1000)));
                    DataFileMeta data =
                            DataFileMeta.forAppend(
                                    "data-" + manifest + "-" + row + ".parquet",
                                    131072,
                                    1000,
                                    partitionStats.get(partition.getInt(0)),
                                    0,
                                    999,
                                    schema.id(),
                                    Collections.emptyList(),
                                    index,
                                    FileSource.APPEND,
                                    null,
                                    null,
                                    null,
                                    null);
                    entries.add(ManifestEntry.create(FileKind.ADD, partition, 0, -1, data));
                    if (deletes && row % 4 == 0) {
                        deleted.add(ManifestEntry.create(FileKind.DELETE, partition, 0, -1, data));
                    }
                }
                manifests.addAll(file.write(entries));
                if (!deleted.isEmpty()) {
                    manifests.addAll(file.write(deleted));
                }
            }
            System.out.printf(
                    "PARTITION_SCAN_INPUT cache=%s deletes=%s manifests=%d bytes=%d%n",
                    cacheMode,
                    deletes,
                    manifests.size(),
                    manifests.stream().mapToLong(ManifestFileMeta::fileSize).sum());
        }

        private Sample measure(boolean projected, int selectivity) {
            SegmentsCache<Path> cache =
                    cacheMode.equals("none")
                            ? null
                            : new SegmentsCache<>(
                                    32768,
                                    MemorySize.ofMebiBytes(property("cache-mib", 512)),
                                    Long.MAX_VALUE,
                                    null,
                                    false);
            table.store().setManifestCache(cache);
            ManifestFile.Factory factory = table.store().manifestFileFactory();
            ManifestsReader reader =
                    new ManifestsReader(
                            PARTITION_TYPE,
                            "default",
                            table.snapshotManager(),
                            table.store().manifestListFactory()) {
                        @Override
                        public Result read(Snapshot snapshot, ScanMode scanMode) {
                            return new Result(null, manifests, manifests);
                        }
                    };
            if (selectivity < 100) {
                reader.withPartitionFilter(
                        PartitionPredicate.fromPredicate(
                                PARTITION_TYPE,
                                new PredicateBuilder(PARTITION_TYPE)
                                        .lessThan(
                                                0,
                                                property("partitions", 1000) * selectivity / 100)));
            }
            Supplier<List<PartitionEntry>> action;
            if (projected) {
                PartitionEntryScanner scanner =
                        new PartitionEntryScanner(reader, factory, property("parallelism", 1));
                action = scanner::scan;
            } else {
                FileStoreScan scan =
                        new AbstractFileStoreScan(
                                reader,
                                table.snapshotManager(),
                                schemaManager,
                                table.schema(),
                                factory,
                                property("parallelism", 1)) {
                            @Override
                            protected boolean filterByStats(ManifestEntry entry) {
                                return true;
                            }

                            @Override
                            public FileStoreScan withCompleteFilter(Predicate predicate) {
                                throw new UnsupportedOperationException();
                            }
                        };
                action = scan::readPartitionEntries;
            }
            if (cacheMode.endsWith("warm")) {
                action.get();
                if (cacheMode.equals("whole-warm")) {
                    for (ManifestFileMeta manifest : manifests) {
                        assertThat(
                                        cache.getIfPresents(
                                                new Path(
                                                        table.location(),
                                                        "manifest/" + manifest.fileName())))
                                .as("The whole-manifest cache must fit the warm working set")
                                .isNotNull();
                    }
                }
            }
            Map<Long, Long> before = allocatedBytes();
            long start = System.nanoTime();
            List<PartitionEntry> entries = action.get();
            long elapsed = System.nanoTime() - start;
            long allocated = 0;
            for (Map.Entry<Long, Long> allocation : allocatedBytes().entrySet()) {
                allocated +=
                        Math.max(
                                0,
                                allocation.getValue()
                                        - before.getOrDefault(allocation.getKey(), 0L));
            }
            return new Sample(elapsed, allocated, entries);
        }
    }

    private static RowType createRowType() {
        int columns = property("stats-columns", 32);
        DataType[] types = new DataType[columns + 1];
        String[] names = new String[columns + 1];
        types[0] = new IntType();
        names[0] = "pt";
        for (int column = 0; column < columns; column++) {
            types[column + 1] = new VarCharType();
            names[column + 1] = "value_" + column;
        }
        return RowType.of(types, names);
    }

    private static SimpleStats createStats(RowType rowType, int partition) {
        SimpleColStats[] stats = new SimpleColStats[rowType.getFieldCount()];
        stats[0] = new SimpleColStats(partition, partition, 0L);
        for (int column = 1; column < stats.length; column++) {
            BinaryString value =
                    BinaryString.fromString(
                            "stat-column-" + column + "-abcdefghijklmnopqrstuvwxyz");
            stats[column] = new SimpleColStats(value, value, 0L);
        }
        return new SimpleStatsConverter(rowType).toBinaryAllMode(stats);
    }

    private static Map<Long, Long> allocatedBytes() {
        java.lang.management.ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        if (!(bean instanceof com.sun.management.ThreadMXBean)) {
            throw new IllegalStateException(
                    "Thread allocation measurement requires a HotSpot JVM.");
        }
        com.sun.management.ThreadMXBean allocationBean = (com.sun.management.ThreadMXBean) bean;
        if (!allocationBean.isThreadAllocatedMemoryEnabled()) {
            allocationBean.setThreadAllocatedMemoryEnabled(true);
        }
        long[] ids = bean.getAllThreadIds();
        long[] bytes = allocationBean.getThreadAllocatedBytes(ids);
        Map<Long, Long> result = new HashMap<>();
        for (int thread = 0; thread < ids.length; thread++) {
            if (bytes[thread] >= 0) {
                result.put(ids[thread], bytes[thread]);
            }
        }
        return result;
    }

    private static void report(
            String cache,
            boolean deletes,
            int selectivity,
            String implementation,
            List<Sample> samples) {
        long[] times = samples.stream().mapToLong(sample -> sample.nanos).sorted().toArray();
        long[] allocations =
                samples.stream().mapToLong(sample -> sample.allocated).sorted().toArray();
        System.out.printf(
                "PARTITION_SCAN_RESULT cache=%s deletes=%s selected=%d%% implementation=%s "
                        + "medianMs=%.3f medianMiB=%.3f partitions=%d%n",
                cache,
                deletes,
                selectivity,
                implementation,
                times[times.length / 2] / 1000000.0,
                allocations[allocations.length / 2] / 1048576.0,
                samples.get(0).entries.size());
    }

    private static int property(String name, int defaultValue) {
        return Integer.parseInt(
                System.getProperty(
                        "partition.scan.benchmark." + name, Integer.toString(defaultValue)));
    }

    private static class Sample {
        private final long nanos;
        private final long allocated;
        private final List<PartitionEntry> entries;

        private Sample(long nanos, long allocated, List<PartitionEntry> entries) {
            this.nanos = nanos;
            this.allocated = allocated;
            this.entries = entries;
        }
    }
}
