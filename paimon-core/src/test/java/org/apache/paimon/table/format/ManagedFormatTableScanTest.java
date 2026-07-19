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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.FORMAT_TABLE_PARTITION_ONLY_VALUE_IN_PATH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for managed format table partition discovery. */
class ManagedFormatTableScanTest {

    private static final Identifier IDENTIFIER = Identifier.create("managed_db", "managed_table");

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testProviderRegistersLargeBatchesInBoundedRequests() throws Exception {
        Catalog catalog = mock(Catalog.class);
        FormatTableCatalogProvider provider = provider(catalog);
        List<Map<String, String>> specs = new ArrayList<>();
        for (int index = 0; index < 2500; index++) {
            specs.add(Collections.singletonMap("year", String.format("y%04d", index)));
        }

        provider.createPartitions(specs);

        @SuppressWarnings({"unchecked", "rawtypes"})
        org.mockito.ArgumentCaptor<List<Map<String, String>>> batches =
                (org.mockito.ArgumentCaptor) org.mockito.ArgumentCaptor.forClass(List.class);
        verify(catalog, times(3)).createPartitions(eq(IDENTIFIER), batches.capture());
        assertThat(batches.getAllValues().get(0)).hasSize(1000);
        assertThat(batches.getAllValues().get(1)).hasSize(1000);
        assertThat(batches.getAllValues().get(2)).hasSize(500);
        assertThat(
                        batches.getAllValues().stream()
                                .flatMap(List::stream)
                                .collect(java.util.stream.Collectors.toList()))
                .containsExactlyElementsOf(specs);
    }

    @Test
    void testProviderPaginatesAndCachesMatchingPattern() throws Exception {
        Catalog catalog = mock(Catalog.class);
        Partition october = partition("2025", "10");
        Partition november = partition("2025", "11");
        when(catalog.listPartitionsPaged(eq(IDENTIFIER), eq(1000), isNull(), eq("year=2025/%")))
                .thenReturn(new PagedList<>(Collections.singletonList(october), "next"));
        when(catalog.listPartitionsPaged(eq(IDENTIFIER), eq(1000), eq("next"), eq("year=2025/%")))
                .thenReturn(new PagedList<>(Collections.singletonList(november), null));

        FormatTableCatalogProvider provider = provider(catalog);

        assertThat(provider.listPartitions("year=2025/%")).containsExactly(october, november);
        assertThat(provider.listPartitions("year=2025/%")).containsExactly(october, november);
        verify(catalog, times(1)).listPartitionsPaged(IDENTIFIER, 1000, null, "year=2025/%");
        verify(catalog, times(1)).listPartitionsPaged(IDENTIFIER, 1000, "next", "year=2025/%");
    }

    @Test
    void testProviderRejectsRepeatedPageTokenCycle() throws Exception {
        Catalog catalog = mock(Catalog.class);
        when(catalog.listPartitionsPaged(eq(IDENTIFIER), eq(1000), isNull(), isNull()))
                .thenReturn(new PagedList<>(Collections.emptyList(), "a"));
        when(catalog.listPartitionsPaged(eq(IDENTIFIER), eq(1000), eq("a"), isNull()))
                .thenReturn(new PagedList<>(Collections.emptyList(), "b"))
                .thenThrow(new AssertionError("provider requested page token 'a' twice"));
        when(catalog.listPartitionsPaged(eq(IDENTIFIER), eq(1000), eq("b"), isNull()))
                .thenReturn(new PagedList<>(Collections.emptyList(), "a"));

        assertThatThrownBy(() -> provider(catalog).listPartitions(null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("repeated partition page token 'a'")
                .hasMessageContaining(IDENTIFIER.getFullName());
    }

    @Test
    void testCreatePartitionsInvalidatesCachedPattern() throws Exception {
        Catalog catalog = mock(Catalog.class);
        Partition oldPartition = partition("2025", "10");
        Partition newPartition = partition("2025", "11");
        when(catalog.listPartitionsPaged(eq(IDENTIFIER), eq(1000), isNull(), eq("year=2025/%")))
                .thenReturn(
                        new PagedList<>(Collections.singletonList(oldPartition), null),
                        new PagedList<>(Arrays.asList(oldPartition, newPartition), null));

        FormatTableCatalogProvider provider = provider(catalog);
        assertThat(provider.listPartitions("year=2025/%")).containsExactly(oldPartition);
        assertThat(provider.listPartitions("year=2025/%")).containsExactly(oldPartition);

        List<Map<String, String>> created = Collections.singletonList(newPartition.spec());
        provider.createPartitions(created);

        assertThat(provider.listPartitions("year=2025/%"))
                .containsExactly(oldPartition, newPartition);
        verify(catalog).createPartitions(IDENTIFIER, created);
        verify(catalog, times(2)).listPartitionsPaged(IDENTIFIER, 1000, null, "year=2025/%");
    }

    @Test
    void testCreatePartitionsInvalidatesOtherProviderInSameProcess() throws Exception {
        Catalog catalog = mock(Catalog.class);
        Partition oldPartition = partition("2025", "10");
        Partition newPartition = partition("2025", "11");
        when(catalog.listPartitionsPaged(eq(IDENTIFIER), eq(1000), isNull(), eq("year=2025/%")))
                .thenReturn(
                        new PagedList<>(Collections.singletonList(oldPartition), null),
                        new PagedList<>(Arrays.asList(oldPartition, newPartition), null));
        FormatTableCatalogProvider readerProvider = provider(catalog);
        FormatTableCatalogProvider writerProvider = provider(catalog);

        assertThat(readerProvider.listPartitions("year=2025/%")).containsExactly(oldPartition);
        writerProvider.createPartitions(Collections.singletonList(newPartition.spec()));

        assertThat(readerProvider.listPartitions("year=2025/%"))
                .containsExactly(oldPartition, newPartition);
        verify(catalog, times(2)).listPartitionsPaged(IDENTIFIER, 1000, null, "year=2025/%");
    }

    @Test
    void testCreatePartitionsFailureInvalidatesOtherProviderCache() throws Exception {
        Catalog catalog = mock(Catalog.class);
        Partition oldPartition = partition("2025", "10");
        Partition newPartition = partition("2025", "11");
        List<Partition> storedPartitions = new ArrayList<>();
        storedPartitions.add(oldPartition);
        when(catalog.listPartitionsPaged(eq(IDENTIFIER), eq(1000), isNull(), eq("year=2025/%")))
                .thenAnswer(ignored -> new PagedList<>(new ArrayList<>(storedPartitions), null));
        List<Map<String, String>> created = Collections.singletonList(newPartition.spec());
        RuntimeException responseLost = new RuntimeException("response lost");
        doAnswer(
                        ignored -> {
                            storedPartitions.add(newPartition);
                            throw responseLost;
                        })
                .when(catalog)
                .createPartitions(IDENTIFIER, created);
        FormatTableCatalogProvider readerProvider = provider(catalog);
        FormatTableCatalogProvider writerProvider = provider(catalog);

        assertThat(readerProvider.listPartitions("year=2025/%")).containsExactly(oldPartition);
        assertThatThrownBy(() -> writerProvider.createPartitions(created)).isSameAs(responseLost);

        assertThat(readerProvider.listPartitions("year=2025/%"))
                .containsExactly(oldPartition, newPartition);
        verify(catalog, times(2)).listPartitionsPaged(IDENTIFIER, 1000, null, "year=2025/%");
    }

    @Test
    void testAdvanceGenerationInvalidatesProvidersInSameProcess() throws Exception {
        Catalog catalog = mock(Catalog.class);
        Partition oldPartition = partition("2025", "10");
        when(catalog.listPartitionsPaged(eq(IDENTIFIER), eq(1000), isNull(), eq("year=2025/%")))
                .thenReturn(
                        new PagedList<>(Arrays.asList(oldPartition, partition("2025", "11")), null),
                        new PagedList<>(Collections.singletonList(oldPartition), null));
        FormatTableCatalogProvider provider = provider(catalog);

        assertThat(provider.listPartitions("year=2025/%")).hasSize(2);
        // The catalog partition DDL path (e.g. RESTCatalog.dropPartitions) advances the
        // generation so same-process scans read their own writes within the cache TTL.
        FormatTableCatalogProvider.advanceGeneration(IDENTIFIER);

        assertThat(provider.listPartitions("year=2025/%")).containsExactly(oldPartition);
        verify(catalog, times(2)).listPartitionsPaged(IDENTIFIER, 1000, null, "year=2025/%");
    }

    @Test
    void testLeadingPatternResidualFilterAndUnregisteredDirectory() throws Exception {
        Catalog catalog = mock(Catalog.class);
        when(catalog.listPartitionsPaged(eq(IDENTIFIER), eq(1000), isNull(), eq("year=2025/%")))
                .thenReturn(
                        new PagedList<>(
                                Arrays.asList(partition("2025", "10"), partition("2025", "11")),
                                null));
        TrackingLocalFileIO fileIO = new TrackingLocalFileIO();
        Path tablePath = new Path(tempDir.toUri());
        Path octoberFile = writeDataFile(fileIO, tablePath, "year=2025/month=10");
        Path novemberFile = writeDataFile(fileIO, tablePath, "year=2025/month=11");
        Path unregisteredFile = writeDataFile(fileIO, tablePath, "year=2025/month=12");
        FormatTable table = createTable(fileIO, tablePath, provider(catalog), false);
        PredicateBuilder builder = new PredicateBuilder(table.partitionType());
        Predicate predicate =
                PredicateBuilder.and(builder.equal(0, 2025), builder.greaterThan(1, 10));
        PartitionPredicate filter =
                PartitionPredicate.fromPredicate(table.partitionType(), predicate);

        ManagedFormatTableScan scan = new ManagedFormatTableScan(table, filter, null);
        List<Path> plannedFiles = plannedFiles(scan.plan().splits());

        assertThat(scan.createPartitionNamePattern()).isEqualTo("year=2025/%");
        assertThat(plannedFiles).containsExactly(novemberFile);
        assertThat(plannedFiles).doesNotContain(octoberFile, unregisteredFile);
        assertThat(fileIO.listedPaths).containsExactly(new Path(tablePath, "year=2025/month=11"));
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
        FormatTable table = createTable(fileIO, tablePath, provider(catalog), false);

        List<PartitionEntry> entries =
                new ManagedFormatTableScan(table, null, null).listPartitionEntries();

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
        assertThat(fileIO.statusListedPaths).isEmpty();
    }

    @Test
    void testUnderscoreInPartitionNameRemainsLiteralPrefix() {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        FormatTable table =
                createStringPartitionTable(fileIO, tablePath, provider(mock(Catalog.class)));
        Predicate predicate =
                new PredicateBuilder(table.partitionType())
                        .equal(0, BinaryString.fromString("a_b"));
        PartitionPredicate filter =
                PartitionPredicate.fromPredicate(table.partitionType(), predicate);

        ManagedFormatTableScan scan = new ManagedFormatTableScan(table, filter, null);

        assertThat(scan.createPartitionNamePattern()).isEqualTo("year=a_b/%");
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
        FormatTable table = createTable(fileIO, tablePath, provider(catalog), true);

        List<Path> plannedFiles =
                plannedFiles(new ManagedFormatTableScan(table, null, null).plan().splits());

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
        FormatTable table = createTable(fileIO, tablePath, provider(catalog), false);

        List<Path> plannedFiles =
                plannedFiles(new ManagedFormatTableScan(table, null, null).plan().splits());

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
        FormatTable table = createStringPartitionTable(fileIO, tablePath, provider(catalog));

        List<Path> plannedFiles =
                plannedFiles(new ManagedFormatTableScan(table, null, null).plan().splits());

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
        FormatTable table = createStringPartitionTable(fileIO, tablePath, provider(catalog));

        assertThatThrownBy(() -> new ManagedFormatTableScan(table, null, null).plan().splits())
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
        FormatTable table = createTable(fileIO, tablePath, provider(catalog), false);

        assertThatThrownBy(() -> new ManagedFormatTableScan(table, null, null).plan().splits())
                .isSameAs(catalogFailure);
        assertThat(fileIO.listedPaths).isEmpty();
    }

    @Test
    @DisplayName(
            "treats a registered partition with a missing directory as an empty partition "
                    + "(requires real object-store validation)")
    void testMissingManagedPartitionReadsAsEmpty() throws Exception {
        Catalog catalog = mock(Catalog.class);
        when(catalog.listPartitionsPaged(eq(IDENTIFIER), eq(1000), isNull(), isNull()))
                .thenReturn(
                        new PagedList<>(Collections.singletonList(partition("2025", "11")), null));
        Path tablePath = new Path(tempDir.toUri());
        Path missingPath = new Path(tablePath, "year=2025/month=11");
        LocalFileIO fileIO =
                new LocalFileIO() {
                    @Override
                    public FileStatus[] listFiles(Path path, boolean recursive) throws IOException {
                        assertThat(path).isEqualTo(missingPath);
                        throw new FileNotFoundException(path.toString());
                    }
                };
        FormatTable table = createTable(fileIO, tablePath, provider(catalog), false);

        // A registered partition whose directory is missing (e.g. ADD PARTITION before the first
        // INSERT) must not fail the whole scan; it reads as an empty partition, matching Hive.
        List<Path> plannedFiles =
                plannedFiles(new ManagedFormatTableScan(table, null, null).plan().splits());
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
        FormatTable table = createStringPartitionTable(fileIO, tablePath, provider(catalog), true);

        assertThatThrownBy(() -> new ManagedFormatTableScan(table, null, null).plan().splits())
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
        FormatTable table = createStringPartitionTable(fileIO, tablePath, provider(catalog));

        List<Path> plannedFiles =
                plannedFiles(new ManagedFormatTableScan(table, null, null).plan().splits());

        assertThat(plannedFiles).containsExactly(dataFile);
    }

    private FormatTableCatalogProvider provider(Catalog catalog) {
        return new FormatTableCatalogProvider(IDENTIFIER, () -> catalog);
    }

    private FormatTable createTable(
            LocalFileIO fileIO,
            Path tablePath,
            FormatTableCatalogProvider provider,
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
                .catalogProvider(provider)
                .build();
    }

    private FormatTable createStringPartitionTable(
            LocalFileIO fileIO, Path tablePath, FormatTableCatalogProvider provider) {
        return createStringPartitionTable(fileIO, tablePath, provider, false);
    }

    private FormatTable createStringPartitionTable(
            LocalFileIO fileIO,
            Path tablePath,
            FormatTableCatalogProvider provider,
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
                .catalogProvider(provider)
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
        private final List<Path> statusListedPaths = new ArrayList<>();

        @Override
        public FileStatus[] listStatus(Path path) throws IOException {
            statusListedPaths.add(path);
            return super.listStatus(path);
        }

        @Override
        public FileStatus[] listFiles(Path path, boolean recursive) throws IOException {
            listedPaths.add(path);
            return super.listFiles(path, recursive);
        }
    }
}
