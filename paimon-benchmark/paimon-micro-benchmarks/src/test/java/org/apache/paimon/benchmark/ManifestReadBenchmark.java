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
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.avro.AvroBulkFormat;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.BinaryManifestEntry;
import org.apache.paimon.manifest.BinaryManifestEntry.Projection;
import org.apache.paimon.manifest.BucketFilter;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestEntrySerializer;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.FileUtils;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.ObjectsFile;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * End-to-end throughput and allocation benchmark for generic and block-based manifest readers.
 *
 * <p>Run with:
 *
 * <pre>
 * mvn -pl paimon-benchmark/paimon-micro-benchmarks -am -Pfast-build \
 *   -DskipTests package
 * mvn -pl paimon-benchmark/paimon-micro-benchmarks -am -Pfast-build \
 *   -DfailIfNoTests=false -Dtest=ManifestReadBenchmark test
 * </pre>
 *
 * <p>The data size can be changed with {@code manifest.read.benchmark.entries}, {@code
 * manifest.read.benchmark.partitions}, {@code manifest.read.benchmark.buckets}, {@code
 * manifest.read.benchmark.warmups}, and {@code manifest.read.benchmark.iterations}.
 */
public class ManifestReadBenchmark {

    private static final RowType PARTITION_TYPE = RowType.of(new IntType());
    private static final RowType DELETE_ENTRY_TYPE = createDeleteEntryType();

    private static volatile long blackHole;

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void benchmarkManifestRead() {
        int entryCount = integerProperty("manifest.read.benchmark.entries", 30_000);
        int partitionCount = integerProperty("manifest.read.benchmark.partitions", 2_048);
        int bucketCount = integerProperty("manifest.read.benchmark.buckets", 16);
        int warmups = integerProperty("manifest.read.benchmark.warmups", 2);
        int iterations = integerProperty("manifest.read.benchmark.iterations", 5);

        BenchmarkState state = createState(entryCount, partitionCount, bucketCount);
        runScenario(
                "full",
                entryCount,
                entryCount,
                state.expectedFullChecksum,
                warmups,
                iterations,
                state::readGenericFull,
                state::readFastFull);
        runScenario(
                "partition-and-bucket",
                entryCount,
                state.selectiveEntryCount,
                state.expectedSelectiveChecksum,
                warmups,
                iterations,
                state::readGenericSelective,
                state::readFastSelective);
        runScenario(
                "scan-full-projection",
                entryCount,
                entryCount,
                state.expectedScanChecksum,
                warmups,
                iterations,
                state::scanGenericFull,
                state::scanFastFull);
        runScenario(
                "scan-delete-projection",
                entryCount,
                entryCount,
                state.expectedScanChecksum,
                warmups,
                iterations,
                state::scanGenericDelete,
                state::scanFastDelete);
    }

    private BenchmarkState createState(int entryCount, int partitionCount, int bucketCount) {
        Path tablePath = new Path(tempDir.toString());
        FileIO fileIO = LocalFileIO.create();
        FileStorePathFactory pathFactory = createPathFactory(tablePath);
        ManifestFile manifestFile =
                new ManifestFile.Factory(
                                fileIO,
                                new SchemaManager(fileIO, tablePath),
                                PARTITION_TYPE,
                                FileFormat.fromIdentifier("avro", new Options()),
                                "zstd",
                                pathFactory,
                                Long.MAX_VALUE,
                                null)
                        .create();

        List<ManifestEntry> entries = new ArrayList<>(entryCount);
        for (int i = 0; i < entryCount; i++) {
            int partition = Math.floorMod(i * 104_729, partitionCount);
            int bucket = Math.floorMod(i * 32_749, bucketCount);
            entries.add(entry(i, partition, bucket, bucketCount));
        }

        List<ManifestFileMeta> files = manifestFile.write(entries);
        if (files.size() != 1) {
            throw new AssertionError("Expected one manifest file, but got " + files.size());
        }

        BinaryRow selectedPartition = partitionRow(0);
        int selectedBucket = 0;
        List<ManifestEntry> selected = new ArrayList<>();
        for (ManifestEntry entry : entries) {
            if (entry.partition().equals(selectedPartition) && entry.bucket() == selectedBucket) {
                selected.add(entry);
            }
        }

        return new BenchmarkState(
                manifestFile,
                files.get(0),
                pathFactory.toManifestFilePath(files.get(0).fileName()),
                selectedPartition,
                selectedBucket,
                selected.size(),
                checksum(entries),
                checksum(selected),
                scanChecksum(entries));
    }

    private static RowType createDeleteEntryType() {
        RowType manifestType = ManifestEntry.MANIFEST_ROW_TYPE;
        return new RowType(
                false,
                Arrays.asList(
                        manifestType.getField(ManifestEntry.KIND),
                        manifestType.getField(ManifestEntry.PARTITION),
                        manifestType.getField(ManifestEntry.BUCKET),
                        manifestType
                                .getField(ManifestEntry.FILE)
                                .newType(
                                        DataFileMeta.SCHEMA.project(
                                                DataFileMeta.FILE_NAME,
                                                DataFileMeta.LEVEL,
                                                DataFileMeta.EXTRA_FILES,
                                                DataFileMeta.EMBEDDED_FILE_INDEX,
                                                DataFileMeta.EXTERNAL_PATH))));
    }

    private static FileStorePathFactory createPathFactory(Path tablePath) {
        return new FileStorePathFactory(
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
    }

    private static ManifestEntry entry(int id, int partition, int bucket, int bucketCount) {
        BinaryRow partitionRow = partitionRow(partition);
        return ManifestEntry.create(
                FileKind.ADD,
                partitionRow,
                bucket,
                bucketCount,
                DataFileMeta.create(
                        String.format(
                                "data-%08d-padding-0123456789abcdef0123456789abcdef.parquet", id),
                        128L * 1024L,
                        1_000L,
                        partitionRow,
                        partitionRow,
                        SimpleStats.EMPTY_STATS,
                        SimpleStats.EMPTY_STATS,
                        id * 1_000L,
                        id * 1_000L + 999L,
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

    private static BinaryRow partitionRow(int partition) {
        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeInt(0, partition);
        writer.complete();
        return row;
    }

    private static void runScenario(
            String name,
            int processedEntries,
            int matchedEntries,
            long expectedChecksum,
            int warmups,
            int iterations,
            ReadOperation generic,
            ReadOperation fast) {
        for (int i = 0; i < warmups; i++) {
            verify(name, "generic", expectedChecksum, generic.run());
            verify(name, "fast", expectedChecksum, fast.run());
        }

        Measurement genericMeasurement = new Measurement();
        Measurement fastMeasurement = new Measurement();
        for (int i = 0; i < iterations; i++) {
            if ((i & 1) == 0) {
                measure(name, "generic", expectedChecksum, generic, genericMeasurement);
                measure(name, "fast", expectedChecksum, fast, fastMeasurement);
            } else {
                measure(name, "fast", expectedChecksum, fast, fastMeasurement);
                measure(name, "generic", expectedChecksum, generic, genericMeasurement);
            }
        }

        System.out.printf(
                "ManifestRead %s: entries=%d, matched=%d, iterations=%d%n",
                name, processedEntries, matchedEntries, iterations);
        printResult("generic", processedEntries, iterations, genericMeasurement);
        printResult("fast", processedEntries, iterations, fastMeasurement);
        System.out.printf(
                "  speedup=%.2fx, allocation reduction=%.2fx%n",
                (double) genericMeasurement.bestNanos / fastMeasurement.bestNanos,
                (double) genericMeasurement.bestAllocatedBytes
                        / fastMeasurement.bestAllocatedBytes);
    }

    private static void measure(
            String scenario,
            String reader,
            long expectedChecksum,
            ReadOperation operation,
            Measurement measurement) {
        long allocatedBefore = currentThreadAllocatedBytes();
        long start = System.nanoTime();
        long actualChecksum = operation.run();
        long elapsed = System.nanoTime() - start;
        long allocated = currentThreadAllocatedBytes() - allocatedBefore;
        verify(scenario, reader, expectedChecksum, actualChecksum);
        blackHole = actualChecksum;
        measurement.add(elapsed, allocated);
    }

    private static void verify(
            String scenario, String reader, long expectedChecksum, long actualChecksum) {
        if (actualChecksum != expectedChecksum) {
            throw new AssertionError(
                    String.format(
                            "%s %s checksum mismatch: expected %d, got %d",
                            scenario, reader, expectedChecksum, actualChecksum));
        }
    }

    private static void printResult(
            String reader, int processedEntries, int iterations, Measurement measurement) {
        System.out.printf(
                "  %-7s best/avg=%.1f/%.1f ms, best allocation=%.1f MiB, "
                        + "best rate=%.1f K entries/s%n",
                reader,
                measurement.bestNanos / 1_000_000.0,
                measurement.totalNanos / iterations / 1_000_000.0,
                measurement.bestAllocatedBytes / 1024.0 / 1024.0,
                processedEntries / (measurement.bestNanos / 1_000_000_000.0) / 1_000.0);
    }

    private static long checksum(List<ManifestEntry> entries) {
        long checksum = entries.size();
        for (ManifestEntry entry : entries) {
            checksum = checksum * 31 + entry.file().fileName().hashCode();
            checksum = checksum * 31 + entry.bucket();
            checksum = checksum * 31 + entry.file().minSequenceNumber();
        }
        return checksum;
    }

    private static long scanChecksum(List<? extends ManifestEntry> entries) {
        long checksum = 1;
        for (ManifestEntry entry : entries) {
            checksum = scanChecksum(checksum, entry);
        }
        return checksum * 31 + entries.size();
    }

    private static long scanChecksum(long checksum, ManifestEntry entry) {
        checksum = checksum * 31 + entry.kind().ordinal();
        checksum = checksum * 31 + entry.bucket();
        checksum = checksum * 31 + entry.file().fileName().hashCode();
        return checksum * 31 + entry.file().level();
    }

    private static int integerProperty(String name, int defaultValue) {
        return Integer.parseInt(System.getProperty(name, Integer.toString(defaultValue)));
    }

    private static long currentThreadAllocatedBytes() {
        java.lang.management.ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        if (!(bean instanceof com.sun.management.ThreadMXBean)) {
            return 0L;
        }
        com.sun.management.ThreadMXBean allocationBean = (com.sun.management.ThreadMXBean) bean;
        if (!allocationBean.isThreadAllocatedMemorySupported()) {
            return 0L;
        }
        if (!allocationBean.isThreadAllocatedMemoryEnabled()) {
            allocationBean.setThreadAllocatedMemoryEnabled(true);
        }
        return allocationBean.getThreadAllocatedBytes(Thread.currentThread().getId());
    }

    private interface ReadOperation {
        long run();
    }

    private static class Measurement {

        private long bestNanos = Long.MAX_VALUE;
        private long totalNanos;
        private long bestAllocatedBytes = Long.MAX_VALUE;

        private void add(long nanos, long allocatedBytes) {
            bestNanos = Math.min(bestNanos, nanos);
            totalNanos += nanos;
            bestAllocatedBytes = Math.min(bestAllocatedBytes, allocatedBytes);
        }
    }

    private static class BenchmarkState {

        private final ManifestFile manifestFile;
        private final ManifestFileMeta manifest;
        private final Path manifestPath;
        private final PartitionPredicate partitionFilter;
        private final BucketFilter bucketFilter;
        private final Filter<InternalRow> selectiveRowFilter;
        private final ManifestEntrySerializer serializer = new ManifestEntrySerializer();
        private final int selectiveEntryCount;
        private final long expectedFullChecksum;
        private final long expectedSelectiveChecksum;
        private final long expectedScanChecksum;

        private BenchmarkState(
                ManifestFile manifestFile,
                ManifestFileMeta manifest,
                Path manifestPath,
                BinaryRow selectedPartition,
                int selectedBucket,
                int selectiveEntryCount,
                long expectedFullChecksum,
                long expectedSelectiveChecksum,
                long expectedScanChecksum) {
            this.manifestFile = manifestFile;
            this.manifest = manifest;
            this.manifestPath = manifestPath;
            this.partitionFilter =
                    new PartitionPredicate() {
                        @Override
                        public boolean test(BinaryRow partition) {
                            return selectedPartition.equals(partition);
                        }

                        @Override
                        public boolean test(
                                long rowCount,
                                InternalRow minValues,
                                InternalRow maxValues,
                                InternalArray nullCounts) {
                            return true;
                        }
                    };
            this.bucketFilter = new BucketFilter(false, selectedBucket, null, null);
            Function<InternalRow, BinaryRow> partitionGetter =
                    ManifestEntrySerializer.partitionGetter();
            Function<InternalRow, Integer> bucketGetter = ManifestEntrySerializer.bucketGetter();
            Function<InternalRow, Integer> totalBucketGetter =
                    ManifestEntrySerializer.totalBucketGetter();
            this.selectiveRowFilter =
                    row -> {
                        BinaryRow partition = partitionGetter.apply(row);
                        return partitionFilter.test(partition)
                                && bucketFilter.test(
                                        partition,
                                        bucketGetter.apply(row),
                                        totalBucketGetter.apply(row));
                    };
            this.selectiveEntryCount = selectiveEntryCount;
            this.expectedFullChecksum = expectedFullChecksum;
            this.expectedSelectiveChecksum = expectedSelectiveChecksum;
            this.expectedScanChecksum = expectedScanChecksum;
        }

        private long readGenericFull() {
            return readGeneric(Filter.alwaysTrue());
        }

        private long readFastFull() {
            return checksum(
                    manifestFile.read(
                            manifest.fileName(),
                            manifest.fileSize(),
                            null,
                            null,
                            Filter.alwaysTrue(),
                            Filter.alwaysTrue()));
        }

        private long readGenericSelective() {
            return readGeneric(selectiveRowFilter);
        }

        private long readFastSelective() {
            return checksum(
                    manifestFile.read(
                            manifest.fileName(),
                            manifest.fileSize(),
                            partitionFilter,
                            bucketFilter,
                            selectiveRowFilter,
                            Filter.alwaysTrue()));
        }

        private long readGeneric(Filter<InternalRow> rowFilter) {
            try {
                return checksum(
                        ObjectsFile.readFromIterator(
                                genericIterator(ManifestEntry.MANIFEST_ROW_TYPE),
                                serializer,
                                rowFilter,
                                Filter.alwaysTrue()));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private long scanGenericFull() {
            return scanGeneric(
                    ManifestEntry.MANIFEST_ROW_TYPE, BinaryManifestEntry.fullProjection());
        }

        private long scanFastFull() {
            return scanFast(BinaryManifestEntry.fullProjection());
        }

        private long scanGenericDelete() {
            return scanGeneric(DELETE_ENTRY_TYPE, BinaryManifestEntry.DELETE_ENTRY_PROJECTION);
        }

        private long scanFastDelete() {
            return scanFast(BinaryManifestEntry.DELETE_ENTRY_PROJECTION);
        }

        private long scanGeneric(RowType projectedType, Projection projection) {
            try (CloseableIterator<InternalRow> rows = genericIterator(projectedType)) {
                BinaryManifestEntry entry = projection.createEntry();
                long checksum = 1;
                int count = 0;
                while (rows.hasNext()) {
                    checksum = scanChecksum(checksum, entry.replace(rows.next()));
                    count++;
                }
                entry.clear();
                return checksum * 31 + count;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private long scanFast(Projection projection) {
            try (CloseableIterator<BinaryManifestEntry> entries =
                    manifestFile.scan(manifest.fileName(), manifest.fileSize(), projection)) {
                long checksum = 1;
                int count = 0;
                while (entries.hasNext()) {
                    checksum = scanChecksum(checksum, entries.next());
                    count++;
                }
                return checksum * 31 + count;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private CloseableIterator<InternalRow> genericIterator(RowType projectedType)
                throws Exception {
            return FileUtils.createFormatReader(
                            manifestFile.fileIO(),
                            new AvroBulkFormat(projectedType),
                            manifestPath,
                            manifest.fileSize())
                    .toCloseableIterator();
        }
    }
}
