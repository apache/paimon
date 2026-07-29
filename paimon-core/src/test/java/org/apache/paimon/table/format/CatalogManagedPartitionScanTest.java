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

package org.apache.paimon.table.format;

import org.apache.paimon.PagedList;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.PartitionPathUtils;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.FORMAT_TABLE_PARTITION_ONLY_VALUE_IN_PATH;
import static org.apache.paimon.CoreOptions.FORMAT_TABLE_SCAN_LIST_PARALLELISM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for Format Table scans whose partitions are catalog-managed. */
class CatalogManagedPartitionScanTest {

    private static final Identifier IDENTIFIER =
            Identifier.create("catalog_partition_db", "catalog_partition_table");

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testLeadingPatternResidualFilterAndUnregisteredDirectory() throws Exception {
        Catalog catalog = mock(Catalog.class);
        // The residual predicate beyond the prefix goes to the filter endpoint; the filter is a
        // hint, so returning a superset here is legal and the plan still filters per partition.
        when(catalog.listPartitionsByFilterPaged(
                        eq(IDENTIFIER),
                        any(Predicate.class),
                        eq(1000),
                        isNull(),
                        eq("year=2025/%")))
                .thenReturn(
                        new PagedList<>(
                                Arrays.asList(partition("2025", "10"), partition("2025", "11")),
                                null));
        TrackingLocalFileIO fileIO = new TrackingLocalFileIO();
        Path tablePath = new Path(tempDir.toUri());
        Path octoberFile = writeDataFile(fileIO, tablePath, "year=2025/month=10");
        Path novemberFile = writeDataFile(fileIO, tablePath, "year=2025/month=11");
        Path unregisteredFile = writeDataFile(fileIO, tablePath, "year=2025/month=12");
        FormatTable table = createTable(fileIO, tablePath, partitionManager(catalog), false);
        PredicateBuilder builder = new PredicateBuilder(table.partitionType());
        Predicate predicate =
                PredicateBuilder.and(builder.equal(0, 2025), builder.greaterThan(1, 10));
        PartitionPredicate filter =
                PartitionPredicate.fromPredicate(table.partitionType(), predicate);

        FormatTableScan scan = new FormatTableScan(table, filter, null);
        List<Path> plannedFiles = plannedFiles(scan.plan().splits());

        assertThat(plannedFiles).containsExactly(novemberFile);
        assertThat(plannedFiles).doesNotContain(octoberFile, unregisteredFile);
        assertThat(fileIO.listedPaths).containsExactly(new Path(tablePath, "year=2025/month=11"));
        ArgumentCaptor<Predicate> pushedFilter = ArgumentCaptor.forClass(Predicate.class);
        verify(catalog)
                .listPartitionsByFilterPaged(
                        eq(IDENTIFIER),
                        pushedFilter.capture(),
                        eq(1000),
                        isNull(),
                        eq("year=2025/%"));
        assertThat(pushedFilter.getValue().test(GenericRow.of(2025, 11))).isTrue();
        assertThat(pushedFilter.getValue().test(GenericRow.of(2025, 10))).isFalse();
    }

    @Test
    void testPushesFilterWithoutLeadingPrefixAndAppliesResidualFilter() throws Exception {
        TrackingLocalFileIO fileIO = new TrackingLocalFileIO();
        Path tablePath = new Path(tempDir.toUri());
        Path octoberFile = writeDataFile(fileIO, tablePath, "year=2025/month=10");
        Path novemberFile = writeDataFile(fileIO, tablePath, "year=2025/month=11");
        FormatTable table =
                createTable(
                        fileIO,
                        tablePath,
                        recordingCatalog(
                                Arrays.asList(partition("2025", "10"), partition("2025", "11"))),
                        false);
        Predicate predicate = new PredicateBuilder(table.partitionType()).greaterThan(1, 10);
        PartitionPredicate filter =
                PartitionPredicate.fromPredicate(table.partitionType(), predicate);

        List<Path> plannedFiles =
                plannedFiles(new FormatTableScan(table, filter, null).plan().splits());

        assertThat(plannedFiles).containsExactly(novemberFile);
        assertThat(plannedFiles).doesNotContain(octoberFile);
        assertThat(fileIO.listedPaths).containsExactly(new Path(tablePath, "year=2025/month=11"));
        assertThat(requestedPrefixes).containsExactly(Collections.emptyMap());
        assertThat(requestedFilters).hasSize(1);
        Predicate pushedFilter = requestedFilters.get(0);
        assertThat(pushedFilter).isNotNull();
        assertThat(pushedFilter.test(GenericRow.of(2025, 11))).isTrue();
        assertThat(pushedFilter.test(GenericRow.of(2025, 10))).isFalse();
    }

    @Test
    void testListPartitionEntriesUsesCatalogVisibility() throws Exception {
        Catalog catalog = mock(Catalog.class);
        Partition catalogPartition =
                new Partition(partition("2025", "11").spec(), 11L, 22L, 3L, 44L, 5, false);
        when(catalog.listPartitionsPaged(eq(IDENTIFIER), eq(1000), isNull(), isNull()))
                .thenReturn(new PagedList<>(Collections.singletonList(catalogPartition), null));
        TrackingLocalFileIO fileIO = new TrackingLocalFileIO();
        Path tablePath = new Path(tempDir.toUri());
        writeDataFile(fileIO, tablePath, "year=2025/month=11");
        writeDataFile(fileIO, tablePath, "year=2025/month=12");
        FormatTable table = createTable(fileIO, tablePath, partitionManager(catalog), false);

        List<PartitionEntry> entries =
                new FormatTableScan(table, null, null).listPartitionEntries();

        assertThat(entries)
                .extracting(
                        entry -> entry.partition().getInt(0), entry -> entry.partition().getInt(1))
                .containsExactly(org.assertj.core.groups.Tuple.tuple(2025, 11));
        assertThat(entries.get(0).recordCount()).isEqualTo(11L);
        assertThat(entries.get(0).fileSizeInBytes()).isEqualTo(22L);
        assertThat(entries.get(0).fileCount()).isEqualTo(3L);
        assertThat(entries.get(0).lastFileCreationTime()).isEqualTo(44L);
        assertThat(entries.get(0).totalBuckets()).isEqualTo(5);
        assertThat(fileIO.listedPaths).isEmpty();
    }

    @Test
    void testUnderscoreInPartitionNameRemainsLiteralPrefix() {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        FormatTable table =
                createStringPartitionTable(
                        fileIO, tablePath, recordingCatalog(Collections.emptyList()));
        Predicate predicate =
                new PredicateBuilder(table.partitionType())
                        .equal(0, BinaryString.fromString("a_b"));
        PartitionPredicate filter =
                PartitionPredicate.fromPredicate(table.partitionType(), predicate);

        List<Path> plannedFiles =
                plannedFiles(new FormatTableScan(table, filter, null).plan().splits());

        // '_' has no special meaning in the prefix contract; it must not widen the match.
        assertThat(plannedFiles).isEmpty();
        assertThat(requestedPrefixes).containsExactly(Collections.singletonMap("year", "a_b"));
    }

    @Test
    void testValueOnlyPartitionPath() throws Exception {
        Catalog catalog = mock(Catalog.class);
        when(catalog.listPartitionsPaged(eq(IDENTIFIER), eq(1000), isNull(), isNull()))
                .thenReturn(
                        new PagedList<>(Collections.singletonList(partition("2025", "11")), null));
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        Path dataFile = writeDataFile(fileIO, tablePath, "2025/11");
        FormatTable table = createTable(fileIO, tablePath, partitionManager(catalog), true);

        List<Path> plannedFiles =
                plannedFiles(new FormatTableScan(table, null, null).plan().splits());

        assertThat(plannedFiles).containsExactly(dataFile);
    }

    @Test
    void testDuplicateCatalogPartitionPlansSplitsOnce() throws Exception {
        Catalog catalog = mock(Catalog.class);
        when(catalog.listPartitionsPaged(eq(IDENTIFIER), eq(1000), isNull(), isNull()))
                .thenReturn(
                        new PagedList<>(
                                Arrays.asList(partition("2025", "11"), partition("2025", "11")),
                                null));
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        Path dataFile = writeDataFile(fileIO, tablePath, "year=2025/month=11");
        FormatTable table = createTable(fileIO, tablePath, partitionManager(catalog), false);

        List<Path> plannedFiles =
                plannedFiles(new FormatTableScan(table, null, null).plan().splits());

        assertThat(plannedFiles).containsExactly(dataFile);
    }

    @Test
    void testWhitespacePartitionValueIsVisible() throws Exception {
        Catalog catalog = mock(Catalog.class);
        Partition whitespacePartition = partition("   ", "11");
        when(catalog.listPartitionsPaged(eq(IDENTIFIER), eq(1000), isNull(), isNull()))
                .thenReturn(new PagedList<>(Collections.singletonList(whitespacePartition), null));
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        Path dataFile =
                writeDataFile(
                        fileIO,
                        tablePath,
                        PartitionPathUtils.generatePartitionPath(
                                new LinkedHashMap<>(whitespacePartition.spec())));
        FormatTable table =
                createStringPartitionTable(fileIO, tablePath, partitionManager(catalog));

        List<Path> plannedFiles =
                plannedFiles(new FormatTableScan(table, null, null).plan().splits());

        assertThat(plannedFiles).containsExactly(dataFile);
    }

    @Test
    void testEmptyPartitionValueReportsCorruptMetadata() throws Exception {
        Catalog catalog = mock(Catalog.class);
        Partition emptyValuePartition = partition("2025", "");
        when(catalog.listPartitionsPaged(eq(IDENTIFIER), eq(1000), isNull(), isNull()))
                .thenReturn(new PagedList<>(Collections.singletonList(emptyValuePartition), null));
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        FormatTable table =
                createStringPartitionTable(fileIO, tablePath, partitionManager(catalog));

        assertThatThrownBy(() -> new FormatTableScan(table, null, null).plan().splits())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("corrupt partition metadata")
                .hasMessageContaining(IDENTIFIER.getFullName());
    }

    @Test
    void testCatalogFailureDoesNotFallBackToFileSystem() throws Exception {
        Catalog catalog = mock(Catalog.class);
        RuntimeException catalogFailure =
                new RuntimeException("Catalog partition listing unavailable");
        when(catalog.listPartitionsPaged(eq(IDENTIFIER), eq(1000), isNull(), isNull()))
                .thenThrow(catalogFailure);
        TrackingLocalFileIO fileIO = new TrackingLocalFileIO();
        Path tablePath = new Path(tempDir.toUri());
        writeDataFile(fileIO, tablePath, "year=2025/month=11");
        FormatTable table = createTable(fileIO, tablePath, partitionManager(catalog), false);

        assertThatThrownBy(() -> new FormatTableScan(table, null, null).plan().splits())
                .isSameAs(catalogFailure);
        assertThat(fileIO.listedPaths).isEmpty();
    }

    @Test
    @DisplayName(
            "treats a registered partition with a missing directory as an empty partition "
                    + "(requires real object-store validation)")
    void testMissingCatalogRegisteredPartitionReadsAsEmpty() throws Exception {
        Catalog catalog = mock(Catalog.class);
        when(catalog.listPartitionsPaged(eq(IDENTIFIER), eq(1000), isNull(), isNull()))
                .thenReturn(
                        new PagedList<>(Collections.singletonList(partition("2025", "11")), null));
        Path tablePath = new Path(tempDir.toUri());
        Path missingPath = new Path(tablePath, "year=2025/month=11");
        LocalFileIO fileIO =
                new LocalFileIO() {
                    @Override
                    public FileStatus[] listStatus(Path path) throws IOException {
                        assertThat(path).isEqualTo(missingPath);
                        throw new FileNotFoundException(path.toString());
                    }
                };
        FormatTable table = createTable(fileIO, tablePath, partitionManager(catalog), false);

        // A registered partition whose directory is missing (e.g. ADD PARTITION before the first
        // INSERT) must not fail the whole scan; it reads as an empty partition, matching Hive.
        List<Path> plannedFiles =
                plannedFiles(new FormatTableScan(table, null, null).plan().splits());
        assertThat(plannedFiles).isEmpty();
    }

    @Test
    void testTraversalPartitionValueReportsCorruptMetadata() throws Exception {
        Catalog catalog = mock(Catalog.class);
        Partition traversalPartition = partition("2025", "..");
        when(catalog.listPartitionsPaged(eq(IDENTIFIER), eq(1000), isNull(), isNull()))
                .thenReturn(new PagedList<>(Collections.singletonList(traversalPartition), null));
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        FormatTable table =
                createStringPartitionTable(fileIO, tablePath, partitionManager(catalog), true);

        assertThatThrownBy(() -> new FormatTableScan(table, null, null).plan().splits())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("corrupt partition metadata")
                .hasMessageContaining(IDENTIFIER.getFullName());
    }

    @Test
    void testDotDotValueInKeyValueLayoutRemainsVisible() throws Exception {
        Catalog catalog = mock(Catalog.class);
        Partition partition = partition("2025", "..");
        when(catalog.listPartitionsPaged(eq(IDENTIFIER), eq(1000), isNull(), isNull()))
                .thenReturn(new PagedList<>(Collections.singletonList(partition), null));
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        Path dataFile = writeDataFile(fileIO, tablePath, "year=2025/month=..");
        FormatTable table =
                createStringPartitionTable(fileIO, tablePath, partitionManager(catalog));

        List<Path> plannedFiles =
                plannedFiles(new FormatTableScan(table, null, null).plan().splits());

        assertThat(plannedFiles).containsExactly(dataFile);
    }

    private final List<Map<String, String>> requestedPrefixes = new ArrayList<>();
    private final List<Predicate> requestedFilters = new ArrayList<>();

    private FormatTablePartitionManager partitionManager(Catalog catalog) {
        return FormatTablePartitionManager.create(
                IDENTIFIER, Arrays.asList("year", "month"), () -> catalog);
    }

    /** Records the prefix the scan pushes down and answers from a fixed partition list. */
    private FormatTablePartitionManager recordingCatalog(List<Partition> partitions) {
        List<Map<String, String>> prefixes = requestedPrefixes;
        List<Predicate> filters = requestedFilters;
        return new FormatTablePartitionManager() {
            @Override
            public List<Partition> listPartitions(
                    Map<String, String> prefix, @Nullable Predicate filter) {
                prefixes.add(new LinkedHashMap<>(prefix));
                filters.add(filter);
                List<Partition> matching = new ArrayList<>();
                for (Partition partition : partitions) {
                    boolean matches = true;
                    for (Map.Entry<String, String> entry : prefix.entrySet()) {
                        if (!entry.getValue().equals(partition.spec().get(entry.getKey()))) {
                            matches = false;
                            break;
                        }
                    }
                    if (matches) {
                        matching.add(partition);
                    }
                }
                return matching;
            }

            @Override
            public List<Partition> listPartitionsByNames(List<Map<String, String>> partitions) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void createPartitions(
                    List<Map<String, String>> partitions, boolean ignoreIfExists) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void dropPartitions(List<Map<String, String>> partitions) {
                throw new UnsupportedOperationException();
            }
        };
    }

    private FormatTable createTable(
            LocalFileIO fileIO,
            Path tablePath,
            FormatTablePartitionManager partitionManager,
            boolean valueOnlyPath) {
        RowType rowType =
                RowType.builder()
                        .field("year", DataTypes.INT())
                        .field("month", DataTypes.INT())
                        .field("id", DataTypes.INT())
                        .build();
        return FormatTable.builder()
                .fileIO(fileIO)
                .identifier(IDENTIFIER)
                .rowType(rowType)
                .partitionKeys(Arrays.asList("year", "month"))
                .location(tablePath.toString())
                .format(FormatTable.Format.CSV)
                .options(
                        Collections.singletonMap(
                                FORMAT_TABLE_PARTITION_ONLY_VALUE_IN_PATH.key(),
                                Boolean.toString(valueOnlyPath)))
                .partitionManager(partitionManager)
                .build();
    }

    private FormatTable createStringPartitionTable(
            LocalFileIO fileIO, Path tablePath, FormatTablePartitionManager partitionManager) {
        return createStringPartitionTable(fileIO, tablePath, partitionManager, false);
    }

    private FormatTable createStringPartitionTable(
            LocalFileIO fileIO,
            Path tablePath,
            FormatTablePartitionManager partitionManager,
            boolean valueOnlyPath) {
        RowType rowType =
                RowType.builder()
                        .field("year", DataTypes.STRING())
                        .field("month", DataTypes.STRING())
                        .field("id", DataTypes.INT())
                        .build();
        return FormatTable.builder()
                .fileIO(fileIO)
                .identifier(IDENTIFIER)
                .rowType(rowType)
                .partitionKeys(Arrays.asList("year", "month"))
                .location(tablePath.toString())
                .format(FormatTable.Format.CSV)
                .options(
                        Collections.singletonMap(
                                FORMAT_TABLE_PARTITION_ONLY_VALUE_IN_PATH.key(),
                                Boolean.toString(valueOnlyPath)))
                .partitionManager(partitionManager)
                .build();
    }

    private Path writeDataFile(LocalFileIO fileIO, Path tablePath, String partitionPath)
            throws IOException {
        Path file = new Path(new Path(tablePath, partitionPath), "data.csv");
        fileIO.mkdirs(file.getParent());
        try (PositionOutputStream out = fileIO.newOutputStream(file, false)) {
            out.write(1);
        }
        return file;
    }

    private static Partition partition(String year, String month) {
        Map<String, String> spec = new LinkedHashMap<>();
        spec.put("year", year);
        spec.put("month", month);
        return new Partition(spec, 0, 0, 0, 0, -1, false);
    }

    private static List<Path> plannedFiles(List<Split> splits) {
        return splits.stream()
                .map(FormatDataSplit.class::cast)
                .flatMap(split -> split.files().stream())
                .map(FormatDataSplit.FileMeta::filePath)
                .collect(Collectors.toList());
    }

    private static class TrackingLocalFileIO extends LocalFileIO {

        private final List<Path> listedPaths = new ArrayList<>();

        @Override
        public FileStatus[] listStatus(Path path) throws IOException {
            listedPaths.add(path);
            return super.listStatus(path);
        }
    }

    @Test
    void testCatalogPartitionListingRunsInParallelAndPreservesOrder() throws Exception {
        ParallelTrackingLocalFileIO fileIO = new ParallelTrackingLocalFileIO();
        Path tablePath = new Path(tempDir.toUri());
        List<Partition> partitions = new ArrayList<>();
        List<Path> expected = new ArrayList<>();
        for (int m = 10; m <= 15; m++) {
            partitions.add(partition("2025", Integer.toString(m)));
            expected.add(writeDataFile(fileIO, tablePath, "year=2025/month=" + m));
        }

        List<Path> parallel =
                plannedFiles(
                        new FormatTableScan(
                                        stringPartitionTable(
                                                fileIO, tablePath, recordingCatalog(partitions), 8),
                                        null,
                                        null)
                                .plan()
                                .splits());

        assertThat(parallel).containsExactlyElementsOf(expected);
        assertThat(fileIO.maxConcurrentListings()).isGreaterThan(1);
    }

    @Test
    void testParallelListingSkipsMissingCatalogRegisteredPartition() throws Exception {
        InjectingLocalFileIO fileIO = new InjectingLocalFileIO();
        Path tablePath = new Path(tempDir.toUri());
        List<Partition> partitions =
                Arrays.asList(
                        partition("2025", "10"), partition("2025", "11"), partition("2025", "12"));
        Path oct = writeDataFile(fileIO, tablePath, "year=2025/month=10");
        Path dec = writeDataFile(fileIO, tablePath, "year=2025/month=12");
        fileIO.failListingContaining("month=11", new FileNotFoundException("missing"));

        List<Path> files =
                plannedFiles(
                        new FormatTableScan(
                                        stringPartitionTable(
                                                fileIO, tablePath, recordingCatalog(partitions), 4),
                                        null,
                                        null)
                                .plan()
                                .splits());

        assertThat(files).containsExactlyInAnyOrder(oct, dec);
    }

    @Test
    void testCatalogPartitionListingIoErrorFailsWholeScan() throws Exception {
        InjectingLocalFileIO fileIO = new InjectingLocalFileIO();
        Path tablePath = new Path(tempDir.toUri());
        List<Partition> partitions =
                Arrays.asList(partition("2025", "10"), partition("2025", "11"));
        writeDataFile(fileIO, tablePath, "year=2025/month=10");
        writeDataFile(fileIO, tablePath, "year=2025/month=11");
        fileIO.failListingContaining("month=11", new IOException("boom"));

        FormatTable table =
                stringPartitionTable(fileIO, tablePath, recordingCatalog(partitions), 4);
        assertThatThrownBy(() -> new FormatTableScan(table, null, null).plan().splits())
                .isInstanceOf(RuntimeException.class)
                .hasRootCauseInstanceOf(IOException.class)
                .hasRootCauseMessage("boom");
    }

    @Test
    void testFilesystemDiscoveredMissingDirectoryStillFailsScan() throws Exception {
        InjectingLocalFileIO fileIO = new InjectingLocalFileIO();
        Path tablePath = new Path(tempDir.toUri());
        writeDataFile(fileIO, tablePath, "year=2025/month=10");
        writeDataFile(fileIO, tablePath, "year=2025/month=11");
        fileIO.failListingContaining("month=11", new FileNotFoundException("missing"));

        FormatTable table = createStringPartitionTable(fileIO, tablePath, null);
        assertThatThrownBy(() -> new FormatTableScan(table, null, null).plan().splits())
                .isInstanceOf(RuntimeException.class)
                .hasRootCauseInstanceOf(FileNotFoundException.class);
    }

    @Test
    void testListParallelismDoesNotChangeFilesystemDiscoveredScanning() throws Exception {
        SerialTrackingLocalFileIO fileIO = new SerialTrackingLocalFileIO();
        Path tablePath = new Path(tempDir.toUri());
        List<Path> expected = new ArrayList<>();
        for (int month = 10; month <= 12; month++) {
            expected.add(writeDataFile(fileIO, tablePath, "year=2025/month=" + month));
        }

        FormatTable table = stringPartitionTable(fileIO, tablePath, null, 8);
        List<Path> files = plannedFiles(new FormatTableScan(table, null, null).plan().splits());

        assertThat(files).containsExactlyInAnyOrderElementsOf(expected);
        assertThat(fileIO.maxConcurrentListings()).isOne();
    }

    @Test
    void testCatalogListingEstablishesFilesystemOnCallerThread() throws Exception {
        CallerBoundLocalFileIO fileIO = new CallerBoundLocalFileIO();
        Path tablePath = new Path(tempDir.toUri());
        List<Partition> partitions = new ArrayList<>();
        for (int month = 10; month <= 13; month++) {
            partitions.add(partition("2025", Integer.toString(month)));
            writeDataFile(fileIO, tablePath, "year=2025/month=" + month);
        }

        fileIO.resetBinding();
        CallerBoundLocalFileIO.CALLER_USER.set("caller-user");
        try {
            new FormatTableScan(
                            stringPartitionTable(
                                    fileIO, tablePath, recordingCatalog(partitions), 4),
                            null,
                            null)
                    .plan()
                    .splits();
        } finally {
            CallerBoundLocalFileIO.CALLER_USER.remove();
        }

        assertThat(fileIO.boundUser()).isEqualTo("caller-user");
        assertThat(fileIO.boundOnListingWorker()).isFalse();
    }

    private FormatTable stringPartitionTable(
            LocalFileIO fileIO,
            Path tablePath,
            @Nullable FormatTablePartitionManager partitionManager,
            int parallelism) {
        RowType rowType =
                RowType.builder()
                        .field("year", DataTypes.STRING())
                        .field("month", DataTypes.STRING())
                        .field("id", DataTypes.INT())
                        .build();
        Map<String, String> options = new LinkedHashMap<>();
        options.put(FORMAT_TABLE_PARTITION_ONLY_VALUE_IN_PATH.key(), "false");
        options.put(FORMAT_TABLE_SCAN_LIST_PARALLELISM.key(), Integer.toString(parallelism));
        return FormatTable.builder()
                .fileIO(fileIO)
                .identifier(IDENTIFIER)
                .rowType(rowType)
                .partitionKeys(Arrays.asList("year", "month"))
                .location(tablePath.toString())
                .format(FormatTable.Format.CSV)
                .options(options)
                .partitionManager(partitionManager)
                .build();
    }

    private static class ParallelTrackingLocalFileIO extends LocalFileIO {

        private final CountDownLatch concurrentListings = new CountDownLatch(2);
        private final AtomicInteger activeListings = new AtomicInteger();
        private final AtomicInteger maxConcurrentListings = new AtomicInteger();

        @Override
        public FileStatus[] listStatus(Path path) throws IOException {
            int active = activeListings.incrementAndGet();
            maxConcurrentListings.updateAndGet(current -> Math.max(current, active));
            concurrentListings.countDown();
            try {
                if (!concurrentListings.await(10, TimeUnit.SECONDS)) {
                    throw new IOException("Partition file listings did not run concurrently");
                }
                return super.listStatus(path);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for concurrent listings", e);
            } finally {
                activeListings.decrementAndGet();
            }
        }

        private int maxConcurrentListings() {
            return maxConcurrentListings.get();
        }
    }

    private static class SerialTrackingLocalFileIO extends LocalFileIO {

        private final AtomicInteger activeListings = new AtomicInteger();
        private final AtomicInteger maxConcurrentListings = new AtomicInteger();

        @Override
        public FileStatus[] listStatus(Path path) throws IOException {
            int active = activeListings.incrementAndGet();
            maxConcurrentListings.updateAndGet(current -> Math.max(current, active));
            try {
                Thread.sleep(50);
                return super.listStatus(path);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while tracking file listings", e);
            } finally {
                activeListings.decrementAndGet();
            }
        }

        private int maxConcurrentListings() {
            return maxConcurrentListings.get();
        }
    }

    private static class InjectingLocalFileIO extends LocalFileIO {

        private final Map<String, IOException> listFailures = new LinkedHashMap<>();

        void failListingContaining(String marker, IOException failure) {
            listFailures.put(marker, failure);
        }

        @Override
        public FileStatus[] listStatus(Path path) throws IOException {
            for (Map.Entry<String, IOException> entry : listFailures.entrySet()) {
                if (path.toString().contains(entry.getKey())) {
                    throw entry.getValue();
                }
            }
            return super.listStatus(path);
        }
    }

    /** Binds to the thread and thread-local user that first touches the filesystem. */
    private static class CallerBoundLocalFileIO extends LocalFileIO {

        static final ThreadLocal<String> CALLER_USER = new ThreadLocal<>();

        private final AtomicReference<Thread> boundThread = new AtomicReference<>();
        private final AtomicReference<String> boundUser = new AtomicReference<>();

        private void bindOnce() {
            if (boundThread.compareAndSet(null, Thread.currentThread())) {
                boundUser.set(CALLER_USER.get());
            }
        }

        void resetBinding() {
            boundThread.set(null);
            boundUser.set(null);
        }

        String boundUser() {
            return boundUser.get();
        }

        boolean boundOnListingWorker() {
            Thread thread = boundThread.get();
            return thread != null && thread.getName().contains("FORMAT-TABLE-LIST");
        }

        @Override
        public boolean exists(Path path) throws IOException {
            bindOnce();
            return super.exists(path);
        }

        @Override
        public FileStatus getFileStatus(Path path) throws IOException {
            bindOnce();
            return super.getFileStatus(path);
        }

        @Override
        public FileStatus[] listStatus(Path path) throws IOException {
            bindOnce();
            return super.listStatus(path);
        }

        @Override
        public FileStatus[] listFiles(Path path, boolean recursive) throws IOException {
            bindOnce();
            return super.listFiles(path, recursive);
        }
    }
}
