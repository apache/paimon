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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.operation.ManifestFileMerger;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * End-to-end allocation and throughput benchmark for full and minor manifest sort compaction.
 *
 * <p>Run with:
 *
 * <pre>
 * mvn -pl paimon-benchmark/paimon-micro-benchmarks -am -Pfast-build \
 *   -DskipTests package
 * mvn -pl paimon-benchmark/paimon-micro-benchmarks -am -Pfast-build \
 *   -DfailIfNoTests=false -Dtest=ManifestFileSorterBenchmark test
 * </pre>
 *
 * <p>The data size can be changed with {@code manifest.sort.benchmark.manifests}, {@code
 * manifest.sort.benchmark.entries-per-manifest}, {@code manifest.sort.benchmark.iterations}, and
 * {@code manifest.sort.benchmark.parallelism}. The external sort buffer can be changed with {@code
 * manifest.sort.benchmark.sort-buffer-size}.
 */
public class ManifestFileSorterBenchmark {

    private static final RowType PARTITION_TYPE = RowType.of(new IntType());
    private static final long TARGET_MANIFEST_SIZE = 1024L * 1024L;

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void benchmarkFullCompaction() throws Exception {
        int manifestCount = integerProperty("manifest.sort.benchmark.manifests", 24);
        int entriesPerManifest =
                integerProperty("manifest.sort.benchmark.entries-per-manifest", 2_000);
        int iterations = integerProperty("manifest.sort.benchmark.iterations", 4);
        int totalEntries = Math.multiplyExact(manifestCount, entriesPerManifest);

        ManifestFile manifestFile = createManifestFile();
        List<ManifestFileMeta> input = createInput(manifestFile, manifestCount, entriesPerManifest);
        runBenchmark("full", manifestFile, input, totalEntries, totalEntries, iterations, "1B");
    }

    @Test
    public void benchmarkMinorCompactionWithDeletes() throws Exception {
        int manifestCount = integerProperty("manifest.sort.benchmark.manifests", 24);
        int entriesPerManifest =
                integerProperty("manifest.sort.benchmark.entries-per-manifest", 2_000);
        int iterations = integerProperty("manifest.sort.benchmark.iterations", 4);
        int addCount = Math.multiplyExact(manifestCount, entriesPerManifest);

        ManifestFile manifestFile = createManifestFile();
        List<ManifestFileMeta> input = createInput(manifestFile, manifestCount, entriesPerManifest);
        int matchedDeletes = 0;
        int unmatchedDeletes = 0;
        int partitionCount = partitionCount(entriesPerManifest);
        List<ManifestEntry> deletes = new ArrayList<>(entriesPerManifest);
        for (int manifest = 0; manifest < manifestCount; manifest++) {
            for (int entry = 0; entry < entriesPerManifest; entry += 4) {
                int partition = partition(manifest, entry, partitionCount);
                deletes.add(entry(FileKind.DELETE, fileName(manifest, entry), partition));
                matchedDeletes++;
                if ((entry & 7) == 0) {
                    deletes.add(
                            entry(
                                    FileKind.DELETE,
                                    "unmatched-" + fileName(manifest, entry),
                                    partition));
                    unmatchedDeletes++;
                }
                if (deletes.size() >= entriesPerManifest) {
                    input.add(manifestFile.write(deletes).get(0));
                    deletes.clear();
                }
            }
        }
        if (!deletes.isEmpty()) {
            input.add(manifestFile.write(deletes).get(0));
        }

        runBenchmark(
                "minor-delete",
                manifestFile,
                input,
                addCount + matchedDeletes + unmatchedDeletes,
                addCount - matchedDeletes + unmatchedDeletes,
                iterations,
                Long.MAX_VALUE + "B");
    }

    private void runBenchmark(
            String name,
            ManifestFile manifestFile,
            List<ManifestFileMeta> input,
            int processedEntries,
            int expectedOutputEntries,
            int iterations,
            String fullCompactionThreshold) {
        Set<String> inputNames = new HashSet<>();
        for (ManifestFileMeta meta : input) {
            inputNames.add(meta.fileName());
        }

        Options options = new Options();
        options.set("manifest-sort.enabled", "true");
        options.set("manifest.target-file-size", TARGET_MANIFEST_SIZE + "B");
        options.set("manifest.full-compaction-threshold-size", fullCompactionThreshold);
        options.set("manifest-sort.max-rewrite-size", Long.MAX_VALUE + "B");
        options.set(
                "scan.manifest.parallelism",
                Integer.toString(integerProperty("manifest.sort.benchmark.parallelism", 1)));
        options.set(
                "sort-spill-buffer-size",
                System.getProperty("manifest.sort.benchmark.sort-buffer-size", "64MB"));
        CoreOptions coreOptions = CoreOptions.fromMap(options.toMap());

        long bestNanos = Long.MAX_VALUE;
        long totalNanos = 0;
        long bestAllocatedBytes = Long.MAX_VALUE;
        for (int iteration = -1; iteration < iterations; iteration++) {
            System.gc();
            Map<Long, Long> allocatedBefore = allocatedBytesByThread();
            long start = System.nanoTime();
            List<ManifestFileMeta> output =
                    ManifestFileMerger.merge(input, manifestFile, PARTITION_TYPE, coreOptions);
            long elapsed = System.nanoTime() - start;
            long allocated = allocatedBytesSince(allocatedBefore);

            long outputEntries = 0;
            for (ManifestFileMeta meta : output) {
                outputEntries += meta.numAddedFiles() + meta.numDeletedFiles();
                if (!inputNames.contains(meta.fileName())) {
                    manifestFile.delete(meta.fileName());
                }
            }
            if (outputEntries != expectedOutputEntries) {
                throw new AssertionError(
                        "Expected "
                                + expectedOutputEntries
                                + " output entries, but got "
                                + outputEntries);
            }

            if (iteration >= 0) {
                bestNanos = Math.min(bestNanos, elapsed);
                totalNanos += elapsed;
                bestAllocatedBytes = Math.min(bestAllocatedBytes, allocated);
                System.out.printf(
                        "ManifestFileSorter %s iteration %d: %.1f ms, %.1f MiB allocated%n",
                        name, iteration + 1, elapsed / 1_000_000.0, allocated / 1024.0 / 1024.0);
            }
        }

        System.out.printf(
                "ManifestFileSorter %s result: entries=%d, manifests=%d, "
                        + "best/avg=%.1f/%.1f ms, "
                        + "best allocation=%.1f MiB, best rate=%.1f K entries/s%n",
                name,
                processedEntries,
                input.size(),
                bestNanos / 1_000_000.0,
                totalNanos / iterations / 1_000_000.0,
                bestAllocatedBytes / 1024.0 / 1024.0,
                processedEntries / (bestNanos / 1_000_000_000.0) / 1_000.0);
    }

    private List<ManifestFileMeta> createInput(
            ManifestFile manifestFile, int manifestCount, int entriesPerManifest) {
        List<ManifestFileMeta> manifests = new ArrayList<>(manifestCount);
        int partitionCount = partitionCount(entriesPerManifest);
        for (int manifest = 0; manifest < manifestCount; manifest++) {
            List<ManifestEntry> entries = new ArrayList<>(entriesPerManifest);
            for (int entry = 0; entry < entriesPerManifest; entry++) {
                int partition = partition(manifest, entry, partitionCount);
                entries.add(entry(FileKind.ADD, fileName(manifest, entry), partition));
            }
            manifests.add(manifestFile.write(entries).get(0));
        }
        return manifests;
    }

    private static int partitionCount(int entriesPerManifest) {
        return Math.max(128, entriesPerManifest / 2);
    }

    private static int partition(int manifest, int entry, int partitionCount) {
        return Math.floorMod(entry * 104_729 + manifest * 32_749, partitionCount);
    }

    private static String fileName(int manifest, int entry) {
        return String.format(
                "data-m%03d-e%06d-padding-0123456789abcdef0123456789abcdef.parquet",
                manifest, entry);
    }

    private static ManifestEntry entry(FileKind kind, String fileName, int partition) {
        BinaryRow partitionRow = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(partitionRow);
        writer.writeInt(0, partition);
        writer.complete();
        return ManifestEntry.create(
                kind,
                partitionRow,
                0,
                1,
                DataFileMeta.create(
                        fileName,
                        128L * 1024L,
                        1_000L,
                        partitionRow,
                        partitionRow,
                        SimpleStats.EMPTY_STATS,
                        SimpleStats.EMPTY_STATS,
                        0,
                        0,
                        1,
                        0,
                        Collections.emptyList(),
                        0L,
                        null,
                        FileSource.APPEND,
                        null,
                        null,
                        null,
                        null));
    }

    private ManifestFile createManifestFile() {
        Path tablePath = new Path(tempDir.toString());
        FileIO fileIO = LocalFileIO.create();
        FileStorePathFactory pathFactory =
                new FileStorePathFactory(
                        tablePath,
                        PARTITION_TYPE,
                        "default",
                        CoreOptions.FILE_FORMAT.defaultValue(),
                        CoreOptions.DATA_FILE_PREFIX.defaultValue(),
                        CoreOptions.CHANGELOG_FILE_PREFIX.defaultValue(),
                        CoreOptions.PARTITION_GENERATE_LEGACY_NAME.defaultValue(),
                        CoreOptions.FILE_SUFFIX_INCLUDE_COMPRESSION.defaultValue(),
                        CoreOptions.FILE_COMPRESSION.defaultValue(),
                        null,
                        null,
                        CoreOptions.ExternalPathStrategy.NONE,
                        null,
                        false,
                        null);
        return new ManifestFile.Factory(
                        fileIO,
                        new SchemaManager(fileIO, tablePath),
                        PARTITION_TYPE,
                        FileFormat.fromIdentifier("avro", new Options()),
                        "zstd",
                        pathFactory,
                        TARGET_MANIFEST_SIZE,
                        null)
                .create();
    }

    private static int integerProperty(String name, int defaultValue) {
        return Integer.parseInt(System.getProperty(name, Integer.toString(defaultValue)));
    }

    private static Map<Long, Long> allocatedBytesByThread() {
        java.lang.management.ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        if (!(bean instanceof com.sun.management.ThreadMXBean)) {
            return Collections.emptyMap();
        }
        com.sun.management.ThreadMXBean allocationBean = (com.sun.management.ThreadMXBean) bean;
        if (!allocationBean.isThreadAllocatedMemorySupported()) {
            return Collections.emptyMap();
        }
        if (!allocationBean.isThreadAllocatedMemoryEnabled()) {
            allocationBean.setThreadAllocatedMemoryEnabled(true);
        }
        long[] threadIds = bean.getAllThreadIds();
        long[] allocated = allocationBean.getThreadAllocatedBytes(threadIds);
        Map<Long, Long> result = new HashMap<>();
        for (int i = 0; i < threadIds.length; i++) {
            if (allocated[i] >= 0) {
                result.put(threadIds[i], allocated[i]);
            }
        }
        return result;
    }

    private static long allocatedBytesSince(Map<Long, Long> allocatedBefore) {
        long total = 0;
        for (Map.Entry<Long, Long> entry : allocatedBytesByThread().entrySet()) {
            Long before = allocatedBefore.get(entry.getKey());
            long delta = entry.getValue() - (before == null ? 0 : before);
            if (delta > 0) {
                total += delta;
            }
        }
        return total;
    }
}
