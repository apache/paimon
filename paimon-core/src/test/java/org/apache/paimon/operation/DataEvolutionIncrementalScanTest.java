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

package org.apache.paimon.operation;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.DataEvolutionMetadataMemoryMode;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.TableQueryAuthResult;
import org.apache.paimon.data.BinaryArray;
import org.apache.paimon.data.BinaryArrayWriter;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.disk.BufferFileWriter;
import org.apache.paimon.disk.FileIOChannel;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.metrics.Gauge;
import org.apache.paimon.metrics.MetricGroup;
import org.apache.paimon.metrics.TestMetricRegistry;
import org.apache.paimon.operation.metrics.ScanMetrics;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.source.AppendBatchTableScan;
import org.apache.paimon.table.source.DataEvolutionSplitGenerator;
import org.apache.paimon.table.source.DataFileMetaStore;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.QueryAuthSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.SplitSerializer;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.table.source.snapshot.SnapshotReaderImpl;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real-manifest semantic checks for incremental DE group production, not SQL capacity tests. */
class DataEvolutionIncrementalScanTest {

    @TempDir Path directory;

    @Test
    void testStreamingCommitEntriesCancelDeletesAndKeepDuplicateAdds() throws Exception {
        Fixture fixture = new Fixture();
        ManifestEntry removed = entry("same.parquet", 0L, 2, 1, "a", 1, 1, 1, 0, 0);
        ManifestEntry otherPartition = entry("same.parquet", 10L, 2, 1, "a", 1, 1, 2, 0, 0);
        ManifestEntry duplicate = entry("duplicate.parquet", 20L, 2, 1, "b", 2, 1, 1, 0, 0);
        fixture.write(removed, otherPartition, duplicate);
        fixture.write(
                ManifestEntry.create(
                        FileKind.DELETE,
                        removed.partition(),
                        removed.bucket(),
                        removed.totalBuckets(),
                        removed.file()),
                duplicate);
        List<SimpleFileEntry> expected = fixture.scan().readSimpleEntries();
        List<SimpleFileEntry> actual = new ArrayList<>();

        fixture.scan().forEachSimpleEntry(actual::add);

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
        assertThat(actual)
                .containsExactly(
                        SimpleFileEntry.from(otherPartition),
                        SimpleFileEntry.from(duplicate),
                        SimpleFileEntry.from(duplicate));
        assertThat(actual.get(0).identifier()).isEqualTo(otherPartition.identifier());
    }

    @Test
    void testStreamingCommitEntriesPreserveDeltaKindsAndOrder() throws Exception {
        Fixture fixture = new Fixture();
        ManifestEntry first = entry("first.parquet", 0L, 2, 1, "a", 1, 1, 1, 0, 0);
        ManifestEntry deleted =
                ManifestEntry.create(
                        FileKind.DELETE,
                        first.partition(),
                        first.bucket(),
                        first.totalBuckets(),
                        first.file());
        ManifestEntry last = entry("last.parquet", 10L, 2, 2, "b", 2, 1, 1, 0, 0);
        fixture.write(first, deleted, last);
        DataEvolutionFileStoreScan scan = fixture.scan();
        scan.withKind(ScanMode.DELTA);
        List<SimpleFileEntry> expected = scan.readSimpleEntries();
        List<SimpleFileEntry> actual = new ArrayList<>();

        scan.forEachSimpleEntry(actual::add);

        assertThat(actual).containsExactlyElementsOf(expected);
        assertThat(actual)
                .extracting(SimpleFileEntry::kind)
                .containsExactly(FileKind.ADD, FileKind.DELETE, FileKind.ADD);
    }

    @Test
    void testStreamingCommitEntriesKeepFiltersAndIndependentMetadata() throws Exception {
        Fixture fixture = new Fixture();
        fixture.write(
                entry("keep.parquet", 10L, 2, 1, "a", 1, 1, 2, 1, 2),
                entry("wrong-q.parquet", 12L, 2, 1, "a", 1, 1, 3, 1, 2),
                entry("wrong-bucket.parquet", 14L, 2, 1, "a", 1, 1, 2, 0, 2),
                entry("wrong-level.parquet", 16L, 2, 1, "a", 1, 1, 2, 1, 0),
                entry("wrong-name.parquet", 18L, 2, 1, "a", 1, 1, 2, 1, 2),
                entry("keep-later.parquet", 20L, 3, 2, "b", 2, 1, 2, 1, 2),
                entry("keep-reject.parquet", 23L, 2, 1, "a", 1, 1, 2, 1, 2),
                entry("keep-outside.parquet", 90L, 2, 1, "a", 1, 1, 2, 1, 2));
        DataEvolutionFileStoreScan scan = fixture.scan();
        scan.withPartitionFilter(Collections.singletonList(row(1, 2)));
        scan.withBucket(1);
        scan.withLevel(2);
        scan.withDataFileNameFilter(name -> name.startsWith("keep"));
        scan.withRowRanges(Collections.singletonList(new Range(10, 25)));
        scan.withManifestEntryFilter(
                value -> !value.copyWithoutStats().file().fileName().contains("reject"));
        scan.dropStats();
        List<SimpleFileEntry> expected = scan.readSimpleEntries();
        List<SimpleFileEntry> actual = new ArrayList<>();

        scan.forEachSimpleEntry(actual::add);

        assertThat(actual).containsExactlyElementsOf(expected);
        assertThat(actual)
                .extracting(SimpleFileEntry::fileName)
                .containsExactly("keep.parquet", "keep-later.parquet");
        for (int i = 0; i < expected.size(); i++) {
            assertThat(actual.get(i).identifier()).isEqualTo(expected.get(i).identifier());
        }
        assertThat(actual.get(0).firstRowId()).isEqualTo(10L);
        assertThat(actual.get(1).firstRowId()).isEqualTo(20L);
        assertThat(actual.get(0).rowCount()).isEqualTo(2L);
        assertThat(actual.get(1).rowCount()).isEqualTo(3L);
    }

    @Test
    void testStreamingCommitEntriesConsumeBeforeFilteringTheWholeManifest() throws Exception {
        Fixture fixture = new Fixture();
        fixture.write(
                entry("first.parquet", 0L, 2, 1, "a", 1, 1, 1, 0, 0),
                entry("late.parquet", 10L, 2, 2, "b", 2, 1, 1, 0, 0));
        DataEvolutionFileStoreScan scan = fixture.scan();
        AtomicInteger filtered = new AtomicInteger();
        scan.withManifestEntryFilter(
                value -> {
                    filtered.incrementAndGet();
                    if (value.fileName().equals("late.parquet")) {
                        throw new AssertionError("Consumer should stop before this entry");
                    }
                    return true;
                });
        IOException failure = new IOException("consumer stopped");

        assertThatThrownBy(
                        () ->
                                scan.forEachSimpleEntry(
                                        value -> {
                                            assertThat(value.fileName()).isEqualTo("first.parquet");
                                            throw failure;
                                        }))
                .isSameAs(failure);
        assertThat(filtered.get()).isEqualTo(1);
    }

    @Test
    void testStreamingCommitEntriesCanScanAgainAfterConsumerFailure() throws Exception {
        Fixture fixture = new Fixture();
        fixture.write(
                entry("first.parquet", 0L, 2, 1, "a", 1, 1, 1, 0, 0),
                entry("last.parquet", 10L, 2, 2, "b", 2, 1, 1, 0, 0));
        DataEvolutionFileStoreScan scan = fixture.scan();
        IOException failure = new IOException("consumer stopped");
        assertThatThrownBy(
                        () ->
                                scan.forEachSimpleEntry(
                                        value -> {
                                            throw failure;
                                        }))
                .isSameAs(failure);
        List<SimpleFileEntry> actual = new ArrayList<>();

        scan.forEachSimpleEntry(actual::add);

        assertThat(actual).containsExactlyElementsOf(scan.readSimpleEntries());
    }

    @Test
    void testConnectedRangesAcrossManifestsKeepOriginalGroupOrder() throws Exception {
        Fixture fixture = new Fixture();
        ManifestEntry nested = entry("nested.parquet", 8L, 3, 2, "a", 2, 1, 1, 0, 0);
        ManifestEntry extending = entry("extending.parquet", 10L, 3, 3, "b", 3, 1, 1, 0, 0);
        ManifestEntry adjacent = entry("adjacent.parquet", 13L, 2, 4, "a", 4, 1, 1, 0, 0);
        ManifestEntry anchor = entry("anchor.parquet", 0L, 10, 1, "a", 1, 1, 1, 0, 0);
        fixture.write(nested, extending, adjacent);
        fixture.write(anchor);
        try (ObservedIOManager io = fixture.io()) {
            assertThat(groups(fixture.scan(), io))
                    .containsExactly(
                            Arrays.asList("nested.parquet", "extending.parquet", "anchor.parquet"),
                            Collections.singletonList("adjacent.parquet"));
            assertThat(io.closed).isFalse();
        }
        assertThat(fixture.reads.get()).isEqualTo(1);
    }

    @Test
    void testDeleteUsesFullFileIdentityAndDoesNotDeleteAnotherColumnFile() throws Exception {
        Fixture fixture = new Fixture();
        ManifestEntry removed = entry("same.parquet", 0L, 2, 1, "a", 1, 1, 1, 0, 0);
        ManifestEntry otherColumn = entry("other.parquet", 0L, 2, 2, "b", 2, 1, 1, 0, 0);
        ManifestEntry otherPartition = entry("same.parquet", 100L, 2, 3, "a", 3, 1, 2, 0, 0);
        fixture.write(removed, otherColumn, otherPartition);
        fixture.write(
                ManifestEntry.create(
                        FileKind.DELETE,
                        removed.partition(),
                        removed.bucket(),
                        removed.totalBuckets(),
                        removed.file()));
        try (ObservedIOManager io = fixture.io()) {
            assertThat(groups(fixture.scan(), io))
                    .containsExactly(
                            Collections.singletonList("other.parquet"),
                            Collections.singletonList("same.parquet"));
        }
    }

    @Test
    void testAllPartitionFieldsAndExistingEntryFiltersAreApplied() throws Exception {
        Fixture fixture = new Fixture();
        fixture.write(
                entry("keep.parquet", 10L, 2, 1, "a", 1, 1, 2, 1, 2),
                entry("wrong-q.parquet", 12L, 2, 1, "a", 1, 1, 3, 1, 2),
                entry("wrong-bucket.parquet", 14L, 2, 1, "a", 1, 1, 2, 0, 2),
                entry("wrong-level.parquet", 16L, 2, 1, "a", 1, 1, 2, 1, 0),
                entry("wrong-name.parquet", 18L, 2, 1, "a", 1, 1, 2, 1, 2),
                entry("keep-outside.parquet", 90L, 2, 1, "a", 1, 1, 2, 1, 2));
        DataEvolutionFileStoreScan scan = fixture.scan();
        scan.withPartitionFilter(
                PartitionPredicate.fromMultiple(
                        fixture.partitionType, Collections.singletonList(row(1, 2))));
        scan.withBucket(1);
        scan.withLevel(2);
        scan.withDataFileNameFilter(name -> name.startsWith("keep"));
        scan.withRowRanges(Collections.singletonList(new Range(10, 20)));
        scan.withManifestEntryFilter(entry -> !entry.file().fileName().contains("outside"));
        try (ObservedIOManager io = fixture.io()) {
            assertThat(groups(scan, io)).containsExactly(Collections.singletonList("keep.parquet"));
        }
    }

    @Test
    void testCustomEntryFilterKeepsOrdinaryManifestEntryCapabilities() throws Exception {
        Fixture fixture = new Fixture();
        fixture.write(
                entry("two.parquet", 0L, 2, 1, "a", 1, 1, 1, 0, 0),
                entry("three.parquet", 10L, 3, 2, "a", 2, 1, 1, 0, 0));
        DataEvolutionFileStoreScan original = fixture.scan();
        original.withManifestEntryFilter(entry -> entry.copyWithoutStats().file().rowCount() == 2);
        List<String> expected =
                original.plan().files().stream()
                        .map(entry -> entry.file().fileName())
                        .collect(Collectors.toList());
        DataEvolutionFileStoreScan incremental = fixture.scan();
        incremental.withManifestEntryFilter(
                entry -> entry.copyWithoutStats().file().rowCount() == 2);
        try (ObservedIOManager io = fixture.io()) {
            assertThat(groups(incremental, io)).containsExactly(expected);
        }
        assertThat(expected).containsExactly("two.parquet");
    }

    @Test
    void testNullCountViewsRemainStableAcrossSpilledRows() throws Exception {
        Fixture fixture = new Fixture();
        List<ManifestEntry> entries = new ArrayList<>();
        entries.add(entry("a-null.parquet", 0L, 4, 1, "a", null, 1, 1, 0, 0));
        for (int i = 1499; i >= 0; i--) {
            entries.add(entry("filler-" + i + ".parquet", 100L + i * 2L, 2, i, "a", i, 1, 1, 0, 0));
        }
        entries.add(entry("b-not-null.parquet", 0L, 4, 2, "b", 7, 1, 1, 0, 0));
        fixture.write(entries.toArray(new ManifestEntry[0]));
        DataEvolutionFileStoreScan scan = fixture.scan();
        scan.withFilter(new PredicateBuilder(fixture.schema.logicalRowType()).isNull(3));
        List<ManifestEntry> result = new ArrayList<>();
        try (ObservedIOManager io = fixture.io()) {
            scan.forEachFileGroup(io, result::addAll);
            assertThat(io.spills.get()).isPositive();
        }
        assertThat(result)
                .extracting(entry -> entry.file().fileName())
                .containsExactly("a-null.parquet", "b-not-null.parquet");
        assertThat(result.get(0).file().valueStats().nullCounts().getLong(0)).isEqualTo(4);
        assertThat(result.get(1).file().valueStats().nullCounts().getLong(0)).isZero();
    }

    @Test
    void testStatisticsUseCompleteLatestColumnGroupBeforeDroppingStats() throws Exception {
        Fixture fixture = new Fixture();
        fixture.write(entry("old-a.parquet", 0L, 4, 1, "a", 1, 1, 1, 0, 0));
        fixture.write(
                entry("new-a.parquet", 0L, 4, 2, "a", 9, 1, 1, 0, 0),
                entry("b.parquet", 0L, 4, 3, "b", 7, 1, 1, 0, 0));
        DataEvolutionFileStoreScan matching = fixture.scan();
        matching.withFilter(new PredicateBuilder(fixture.schema.logicalRowType()).equal(3, 9));
        matching.withReadType(
                new RowType(Collections.singletonList(fixture.schema.fields().get(4))));
        matching.dropStats();
        List<ManifestEntry> result = new ArrayList<>();
        try (ObservedIOManager io = fixture.io()) {
            matching.forEachFileGroup(io, result::addAll);
        }
        assertThat(result)
                .extracting(entry -> entry.file().fileName())
                .containsExactly("old-a.parquet", "new-a.parquet", "b.parquet");
        assertThat(result)
                .allSatisfy(
                        entry ->
                                assertThat(entry.file().valueStats())
                                        .isSameAs(SimpleStats.EMPTY_STATS));
        DataEvolutionFileStoreScan staleValue = fixture.scan();
        staleValue.withFilter(new PredicateBuilder(fixture.schema.logicalRowType()).equal(3, 1));
        try (ObservedIOManager io = fixture.io()) {
            assertThat(groups(staleValue, io)).isEmpty();
        }
        assertThat(
                        fixture.factory
                                .create()
                                .read(fixture.manifests.get(0).fileName())
                                .get(0)
                                .file()
                                .valueStats()
                                .minValues()
                                .getInt(0))
                .isEqualTo(1);
    }

    @Test
    void testDedicatedSubrangeKeepsFullRangeAnchor() throws Exception {
        Fixture fixture = new Fixture();
        fixture.write(entry("b.blob", 0L, 3, 2, "b", 7, 1, 1, 0, 0));
        fixture.write(entry("anchor.parquet", 0L, 10, 1, "a", 1, 1, 1, 0, 0));
        DataEvolutionFileStoreScan scan = fixture.scan();
        scan.withReadType(new RowType(Collections.singletonList(fixture.schema.fields().get(4))));
        try (ObservedIOManager io = fixture.io()) {
            assertThat(groups(scan, io)).containsExactly(Arrays.asList("b.blob", "anchor.parquet"));
        }
    }

    @Test
    void testGlobalRangeFilteringMatchesExistingPlannerForOverlappingPartitions() throws Exception {
        Fixture fixture = new Fixture();
        // Preserve the existing global post-filter contract even for historical RowID overlaps;
        // this fixture does not assert that normal SQL creates such cross-partition overlaps.
        fixture.write(entry("older-part.parquet", 0L, 4, 1, "a", 1, 1, 1, 0, 0));
        fixture.write(entry("newer-part.parquet", 0L, 4, 2, "a", 9, 1, 2, 0, 0));
        DataEvolutionFileStoreScan original = fixture.scan();
        original.withFilter(new PredicateBuilder(fixture.schema.logicalRowType()).equal(3, 1));
        List<String> expected =
                original.plan().files().stream()
                        .map(entry -> entry.file().fileName())
                        .collect(Collectors.toList());
        DataEvolutionFileStoreScan incremental = fixture.scan();
        incremental.withFilter(new PredicateBuilder(fixture.schema.logicalRowType()).equal(3, 1));
        List<ManifestEntry> actual = new ArrayList<>();
        try (ObservedIOManager io = fixture.io()) {
            incremental.forEachFileGroup(io, actual::addAll);
        }
        assertThat(actual)
                .extracting(entry -> entry.file().fileName())
                .containsExactlyElementsOf(expected);
        assertThat(expected).isEmpty();
    }

    @Test
    void testExternalSortActuallySpillsAndReleasesItsFiles() throws Exception {
        Fixture fixture = new Fixture();
        List<ManifestEntry> entries = new ArrayList<>();
        for (int i = 1499; i >= 0; i--) {
            entries.add(entry("file-" + i + ".parquet", i * 2L, 2, i, "a", i, 1, 1, 0, 0));
        }
        fixture.write(entries.toArray(new ManifestEntry[0]));
        AtomicInteger next = new AtomicInteger();
        Set<String> names = new HashSet<>();
        try (ObservedIOManager io = fixture.io()) {
            Snapshot snapshot =
                    fixture.scan()
                            .forEachFileGroup(
                                    io,
                                    group -> {
                                        assertThat(group).hasSize(1);
                                        assertThat(group.get(0).file().firstRowId())
                                                .isEqualTo(next.getAndIncrement() * 2L);
                                        assertThat(names.add(group.get(0).file().fileName()))
                                                .isTrue();
                                    });
            assertThat(snapshot).isSameAs(fixture.snapshot);
            assertThat(io.spills.get()).isPositive();
            assertThat(io.closed).isFalse();
            assertThat(next.get()).isEqualTo(1500);
            assertThat(names).hasSize(1500);
            fixture.assertNoSpillFiles();
        }
        assertThat(fixture.reads.get()).isEqualTo(1);
    }

    @Test
    void testConsumerFailureReleasesSortButDoesNotCloseBorrowedIoManager() throws Exception {
        Fixture fixture = new Fixture();
        List<ManifestEntry> entries = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            entries.add(entry("file-" + i + ".parquet", i * 2L, 2, i, "a", i, 1, 1, 0, 0));
        }
        fixture.write(entries.toArray(new ManifestEntry[0]));
        IOException failure = new IOException("consumer stopped");
        try (ObservedIOManager io = fixture.io()) {
            assertThatThrownBy(
                            () ->
                                    fixture.scan()
                                            .forEachFileGroup(
                                                    io,
                                                    group -> {
                                                        throw failure;
                                                    }))
                    .isSameAs(failure);
            assertThat(io.spills.get()).isPositive();
            assertThat(io.closed).isFalse();
            fixture.assertNoSpillFiles();
        }
    }

    @Test
    void testUnsupportedInputsFailBeforeAnyGroupIsPublished() throws Exception {
        Fixture fixture = new Fixture();
        fixture.write(entry("valid.parquet", 0L, 2, 1, "a", 1, 1, 1, 0, 0));
        fixture.write(entry("missing-row-id.parquet", null, 2, 2, "a", 1, 1, 1, 0, 0));
        AtomicInteger callbacks = new AtomicInteger();
        try (ObservedIOManager io = fixture.io()) {
            assertThatThrownBy(
                            () ->
                                    fixture.scan()
                                            .forEachFileGroup(
                                                    io, group -> callbacks.incrementAndGet()))
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("row");
            assertThat(callbacks.get()).isZero();
            DataEvolutionFileStoreScan delta = fixture.scan();
            delta.withKind(ScanMode.DELTA);
            assertThatThrownBy(
                            () -> delta.forEachFileGroup(io, group -> callbacks.incrementAndGet()))
                    .isInstanceOf(UnsupportedOperationException.class);
            assertThat(callbacks.get()).isZero();
            fixture.assertNoSpillFiles();
        }
    }

    @Test
    void testIncrementalSnapshotPlanMatchesBatchAndKeepsMetadataInStore() throws Exception {
        Fixture fixture = new Fixture();
        fixture.write(
                entry("base.parquet", 0L, 2, 1, "a", 1, 1, 1, 0, 0),
                entry("update.parquet", 0L, 2, 2, "b", 2, 1, 1, 0, 0));
        for (int i = 1; i < 12; i++) {
            fixture.write(entry("base-" + i + ".parquet", 2L * i, 2, i, "a", i, 1, 1, 0, 0));
        }
        SnapshotReaderImpl original = fixture.reader(fixture.scan());
        original.dropStats();
        List<DataSplit> expected = original.read().dataSplits();
        assertThat(expected).hasSize(3);
        Path metadata = Files.createDirectory(directory.resolve("plan-metadata"));
        List<byte[]> transported = new ArrayList<>();
        try (ObservedIOManager io = fixture.io();
                DataFileMetaStore store = new DataFileMetaStore(metadata, 1024 * 1024)) {
            SnapshotReaderImpl incremental = fixture.reader(fixture.scan());
            incremental.dropStats();
            SnapshotReader.Plan plan = incremental.readIncrementally(store, io);
            assertThat(plan.snapshot()).isSameAs(fixture.snapshot);
            assertThat(plan.dataSplits()).containsExactlyElementsOf(expected);
            long physical = 0;
            long merged = 0;
            for (DataSplit split : plan.dataSplits()) {
                physical += split.rowCount();
                merged += split.mergedRowCount().getAsLong();
                transported.add(SplitSerializer.serialize(split));
            }
            assertThat(physical).isEqualTo(26);
            assertThat(merged).isEqualTo(24);
            store.close();
            assertThatThrownBy(() -> plan.dataSplits().get(0).dataFiles().get(0))
                    .isInstanceOf(IllegalStateException.class);
            assertThat(io.closed).isFalse();
        }
        for (int i = 0; i < expected.size(); i++) {
            assertThat(SplitSerializer.deserialize(transported.get(i))).isEqualTo(expected.get(i));
        }
        try (Stream<Path> files = Files.list(metadata)) {
            assertThat(files.count()).isZero();
        }
    }

    @ParameterizedTest
    @ValueSource(longs = {1, 3})
    void testSmallLimitStreamsCompleteGroupsAcrossOverlappingManifests(long limit)
            throws Exception {
        Fixture fixture = new Fixture();
        List<ManifestEntry> bases = new ArrayList<>();
        List<ManifestEntry> updates = new ArrayList<>();
        for (int i = 0; i < 64; i++) {
            bases.add(entry("base-" + i + ".parquet", 2L * i, 2, 1, "a", i, 1, 1, 0, 0));
            updates.add(entry("update-" + i + ".parquet", 2L * i, 2, 2, "b", i, 1, 1, 0, 0));
        }
        fixture.write(bases.toArray(new ManifestEntry[0]));
        fixture.write(updates.toArray(new ManifestEntry[0]));
        assertThat(fixture.manifests).hasSize(2);
        assertThat(fixture.manifests).extracting(ManifestFileMeta::minRowId).containsOnly(0L);
        assertThat(fixture.manifests).extracting(ManifestFileMeta::maxRowId).containsOnly(127L);
        CapturingRegistry metrics = new CapturingRegistry();
        DataEvolutionFileStoreScan scan = fixture.scan(true);
        scan.withMetrics(new ScanMetrics(metrics, "fixture"));
        Path metadata = Files.createDirectory(directory.resolve("limit-prefix-metadata"));
        try (ObservedIOManager io = fixture.io();
                DataFileMetaStore store =
                        new DataFileMetaStore(metadata, limit == 1 ? 1024 : 2048)) {
            SnapshotReader reader = fixture.reader(scan).withLimit(limit);
            SnapshotReader.Plan plan = reader.readIncrementally(store, io);
            int expectedGroups = (int) ((limit + 1) / 2);
            List<String> expectedFiles = new ArrayList<>();
            for (int i = 0; i < expectedGroups; i++) {
                expectedFiles.add("base-" + i + ".parquet");
                expectedFiles.add("update-" + i + ".parquet");
            }
            assertThat(plan.snapshot()).isSameAs(fixture.snapshot);
            assertThat(plan.dataSplits()).hasSize(1);
            DataSplit split = plan.dataSplits().get(0);
            assertThat(split.dataFiles())
                    .extracting(DataFileMeta::fileName)
                    .containsExactlyElementsOf(expectedFiles);
            assertThat(split.rawConvertible()).isFalse();
            assertThat(split.mergedRowCount().getAsLong()).isEqualTo(expectedGroups * 2L);
            assertThat(fixture.streamedGroups.get()).isEqualTo(expectedGroups);
            assertThat(store.serializedBytes()).isLessThanOrEqualTo(limit == 1 ? 1024 : 2048);
            assertThat(metrics.value(ScanMetrics.LAST_SCAN_RESULTED_TABLE_FILES))
                    .isEqualTo(expectedGroups * 2L);
            assertThat(metrics.value(ScanMetrics.LAST_SCAN_SKIPPED_TABLE_FILES))
                    .isEqualTo(128L - expectedGroups * 2);
        }
        fixture.assertNoSpillFiles();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testStreamingLimitDoesNotPruneBeforeDataFilter(boolean configureScan) throws Exception {
        Fixture fixture = new Fixture();
        fixture.write(
                entry("first-a.parquet", 0L, 2, 1, "a", 100, 1, 1, 0, 0),
                entry("last-a.parquet", 2L, 2, 1, "a", 101, 1, 1, 0, 0));
        fixture.write(
                entry("first-b.parquet", 0L, 2, 2, "b", 200, 1, 1, 0, 0),
                entry("last-b.parquet", 2L, 2, 2, "b", 201, 1, 1, 0, 0));
        Path metadata = Files.createDirectory(directory.resolve("filtered-limit-metadata"));
        try (ObservedIOManager io = fixture.io();
                DataFileMetaStore store = new DataFileMetaStore(metadata, 1024 * 1024)) {
            DataEvolutionFileStoreScan scan = fixture.scan(true);
            Predicate filter =
                    new PredicateBuilder(fixture.schema.logicalRowType()).greaterOrEqual(3, 100);
            SnapshotReader reader = fixture.reader(scan);
            if (configureScan) {
                scan.withFilter(filter);
            } else {
                reader.withFilter(filter);
            }
            reader.withLimit(1);
            SnapshotReader.Plan plan = reader.readIncrementally(store, io);
            assertThat(
                            plan.dataSplits().stream()
                                    .flatMap(split -> split.dataFiles().stream())
                                    .map(DataFileMeta::fileName)
                                    .collect(Collectors.toList()))
                    .containsExactly(
                            "first-a.parquet",
                            "first-b.parquet",
                            "last-a.parquet",
                            "last-b.parquet");
        }
        fixture.assertNoSpillFiles();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testStreamingLimitDoesNotCountRowsOutsideRanges(boolean configureScan) throws Exception {
        Fixture fixture = new Fixture();
        fixture.write(
                entry("first.parquet", 0L, 2, 1, "a", 1, 1, 1, 0, 0),
                entry("last.parquet", 2L, 2, 1, "a", 2, 1, 1, 0, 0));
        Path metadata = Files.createDirectory(directory.resolve("row-range-limit-metadata"));
        try (ObservedIOManager io = fixture.io();
                DataFileMetaStore store = new DataFileMetaStore(metadata, 1024 * 1024)) {
            DataEvolutionFileStoreScan scan = fixture.scan(true);
            List<Range> ranges = Arrays.asList(new Range(0, 0), new Range(2, 2));
            SnapshotReader reader = fixture.reader(scan);
            if (configureScan) {
                scan.withRowRanges(ranges);
            } else {
                reader.withRowRanges(ranges);
            }
            reader.withLimit(2);
            SnapshotReader.Plan plan = reader.readIncrementally(store, io);
            assertThat(
                            plan.dataSplits().stream()
                                    .flatMap(split -> split.dataFiles().stream())
                                    .map(DataFileMeta::fileName)
                                    .collect(Collectors.toList()))
                    .containsExactly("first.parquet", "last.parquet");
        }
        fixture.assertNoSpillFiles();
    }

    @Test
    void testSnapshotPackingDoesNotCutProvidersWhenPartitionsInterleave() throws Exception {
        Fixture fixture = new Fixture();
        fixture.write(
                entry("p1-a.parquet", 0L, 4, 1, "a", 1, 1, 1, 0, 0),
                entry("p2-x.parquet", 0L, 4, 1, "a", 2, 1, 2, 0, 0),
                entry("p1-b.parquet", 0L, 4, 2, "b", 3, 1, 1, 0, 0));
        Path metadata = Files.createDirectory(directory.resolve("plan-metadata"));
        try (ObservedIOManager io = fixture.io();
                DataFileMetaStore store = new DataFileMetaStore(metadata, 1024 * 1024)) {
            SnapshotReader.Plan plan = fixture.reader(fixture.scan()).readIncrementally(store, io);
            List<DataSplit> first =
                    plan.dataSplits().stream()
                            .filter(split -> split.partition().equals(row(1, 1)))
                            .collect(Collectors.toList());
            assertThat(first).hasSize(1);
            assertThat(first.get(0).dataFiles())
                    .extracting(DataFileMeta::fileName)
                    .containsExactly("p1-a.parquet", "p1-b.parquet");
            assertThat(first.get(0).rawConvertible()).isFalse();
            assertThat(plan.dataSplits()).hasSize(2);
        }
    }

    @Test
    void testPartitionPackerEvictionKeepsEveryCompleteProviderGroup() throws Exception {
        Fixture fixture = new Fixture();
        List<ManifestEntry> entries = new ArrayList<>();
        for (int i = 0; i < 1030; i++) {
            entries.add(entry("base-" + i + ".parquet", i * 2L, 2, 1, "a", i, 1, i, 0, 0));
        }
        entries.add(entry("late-a.parquet", 3000L, 2, 1, "a", 1, 1, 0, 0, 0));
        entries.add(entry("late-b.parquet", 3000L, 2, 2, "b", 2, 1, 0, 0, 0));
        fixture.write(entries.toArray(new ManifestEntry[0]));
        Path metadata = Files.createDirectory(directory.resolve("eviction-metadata"));
        try (ObservedIOManager io = fixture.io();
                DataFileMetaStore store = new DataFileMetaStore(metadata, 1024 * 1024)) {
            SnapshotReader.Plan plan = fixture.reader(fixture.scan()).readIncrementally(store, io);
            assertThat(
                            plan.dataSplits().stream()
                                    .flatMap(split -> split.dataFiles().stream())
                                    .map(DataFileMeta::fileName)
                                    .collect(Collectors.toList()))
                    .containsExactlyInAnyOrderElementsOf(
                            entries.stream()
                                    .map(entry -> entry.file().fileName())
                                    .collect(Collectors.toList()));
            List<DataSplit> updated =
                    plan.dataSplits().stream()
                            .filter(
                                    split ->
                                            split.dataFiles().stream()
                                                    .anyMatch(
                                                            file ->
                                                                    file.fileName()
                                                                            .equals(
                                                                                    "late-a.parquet")))
                            .collect(Collectors.toList());
            assertThat(updated).hasSize(1);
            assertThat(updated.get(0).dataFiles())
                    .extracting(DataFileMeta::fileName)
                    .contains("late-a.parquet", "late-b.parquet");
            assertThat(updated.get(0).rawConvertible()).isFalse();
        }
        fixture.assertNoSpillFiles();
    }

    @Test
    void testSnapshotStoreFailureDoesNotReturnAPartialPlanOrCloseBorrowedIo() throws Exception {
        Fixture fixture = new Fixture();
        List<ManifestEntry> entries = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            entries.add(entry("base-" + i + ".parquet", i * 2L, 2, i, "a", i, 1, 1, 0, 0));
        }
        fixture.write(entries.toArray(new ManifestEntry[0]));
        Path metadata = Files.createDirectory(directory.resolve("plan-metadata"));
        try (ObservedIOManager io = fixture.io();
                DataFileMetaStore store = new DataFileMetaStore(metadata, 8)) {
            assertThatThrownBy(() -> fixture.reader(fixture.scan()).readIncrementally(store, io))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("limit");
            assertThat(io.closed).isFalse();
            fixture.assertNoSpillFiles();
            try (Stream<Path> files = Files.list(metadata)) {
                assertThat(files.count()).isZero();
            }
        }
        Path unsupportedMetadata = Files.createDirectory(directory.resolve("unsupported-metadata"));
        try (ObservedIOManager io = fixture.io();
                DataFileMetaStore store = new DataFileMetaStore(unsupportedMetadata, 1024 * 1024)) {
            SnapshotReaderImpl unsupported = fixture.reader(fixture.scan());
            unsupported.withMode(ScanMode.DELTA);
            assertThatThrownBy(() -> unsupported.readIncrementally(store, io))
                    .isInstanceOf(UnsupportedOperationException.class);
            assertThat(io.closed).isFalse();
            try (Stream<Path> files = Files.list(unsupportedMetadata)) {
                assertThat(files.count()).isZero();
            }
        }
    }

    @Test
    void testIncrementalScanReportsTheSameCountsUsedByExistingGuards() throws Exception {
        Fixture fixture = new Fixture();
        fixture.write(
                entry("keep.parquet", 0L, 2, 1, "a", 1, 1, 1, 0, 0),
                entry("skip.parquet", 10L, 2, 2, "a", 2, 1, 1, 0, 0));
        CapturingRegistry originalMetrics = new CapturingRegistry();
        DataEvolutionFileStoreScan original = fixture.scan();
        original.withMetrics(new ScanMetrics(originalMetrics, "fixture"));
        original.withDataFileNameFilter(name -> name.startsWith("keep"));
        original.plan();
        CapturingRegistry incrementalMetrics = new CapturingRegistry();
        DataEvolutionFileStoreScan incremental = fixture.scan();
        incremental.withMetrics(new ScanMetrics(incrementalMetrics, "fixture"));
        incremental.withDataFileNameFilter(name -> name.startsWith("keep"));
        try (ObservedIOManager io = fixture.io()) {
            List<List<String>> accepted = new ArrayList<>();
            incremental.forEachFileGroup(
                    io,
                    group -> {
                        accepted.add(
                                group.stream()
                                        .map(entry -> entry.file().fileName())
                                        .collect(Collectors.toList()));
                        group.clear();
                    });
            assertThat(accepted).containsExactly(Collections.singletonList("keep.parquet"));
        }
        for (String metric :
                Arrays.asList(
                        ScanMetrics.LAST_SCANNED_SNAPSHOT_ID,
                        ScanMetrics.LAST_SCANNED_MANIFESTS,
                        ScanMetrics.LAST_SCAN_SKIPPED_TABLE_FILES,
                        ScanMetrics.LAST_SCAN_RESULTED_TABLE_FILES)) {
            assertThat(incrementalMetrics.value(metric))
                    .as(metric)
                    .isEqualTo(originalMetrics.value(metric));
        }
        assertThat(incrementalMetrics.value(ScanMetrics.LAST_SCANNED_SNAPSHOT_ID)).isEqualTo(17L);
        assertThat(incrementalMetrics.value(ScanMetrics.LAST_SCAN_SKIPPED_TABLE_FILES))
                .isEqualTo(1L);
        assertThat(incrementalMetrics.value(ScanMetrics.LAST_SCAN_RESULTED_TABLE_FILES))
                .isEqualTo(1L);
    }

    @Test
    void testConfiguredReadReplaysMetadataAndRejectsReuseWithoutInvalidatingThePlan()
            throws Exception {
        Fixture fixture = new Fixture();
        fixture.write(entry("base.parquet", 0L, 2, 1, "a", 1, 1, 1, 0, 0));
        Path metadata = Files.createDirectory(directory.resolve("configured-metadata"));
        try (ObservedIOManager io = fixture.io();
                DataFileMetaStore store = new DataFileMetaStore(metadata, 1024 * 1024)) {
            SnapshotReader reader =
                    fixture.reader(fixture.scan()).withIncrementalPlanning(store, io);
            SnapshotReader.Plan plan = reader.read();
            assertThat(plan.dataSplits()).hasSize(1);
            assertThatThrownBy(reader::read).isInstanceOf(IllegalStateException.class);
            assertThat(plan.dataSplits().get(0).dataFiles())
                    .extracting(DataFileMeta::fileName)
                    .containsExactly("base.parquet");
            store.close();
            assertThatThrownBy(() -> plan.dataSplits().get(0).dataFiles().get(0))
                    .isInstanceOf(IllegalStateException.class);
            assertThat(io.closed).isFalse();
        }
    }

    @Test
    void testConfiguredReadDoesNotFallBackToMaterializedChanges() throws Exception {
        Fixture fixture = new Fixture();
        fixture.write(entry("base.parquet", 0L, 2, 1, "a", 1, 1, 1, 0, 0));
        for (int operation = 0; operation < 3; operation++) {
            Path metadata = Files.createDirectory(directory.resolve("unsupported-" + operation));
            try (ObservedIOManager io = fixture.io();
                    DataFileMetaStore store = new DataFileMetaStore(metadata, 1024 * 1024)) {
                SnapshotReader reader =
                        fixture.reader(fixture.scan()).withIncrementalPlanning(store, io);
                if (operation == 0) {
                    reader.withMode(ScanMode.DELTA);
                    assertThatThrownBy(reader::read)
                            .isInstanceOf(UnsupportedOperationException.class);
                } else if (operation == 1) {
                    assertThatThrownBy(reader::readChanges)
                            .isInstanceOf(UnsupportedOperationException.class);
                } else {
                    assertThatThrownBy(() -> reader.readIncrementalDiff(fixture.snapshot))
                            .isInstanceOf(UnsupportedOperationException.class);
                }
                assertThat(io.closed).isFalse();
                try (Stream<Path> files = Files.list(metadata)) {
                    assertThat(files.count()).isZero();
                }
            }
        }
        assertThat(fixture.reads.get()).isZero();
    }

    @Test
    void testReadBuilderFactoryKeepsFiltersLimitTagAndExecutorBuilderIndependent()
            throws Exception {
        Fixture fixture = new Fixture();
        for (int i = 0; i < 8; i++) {
            fixture.write(
                    entry(
                            "base-" + i + ".parquet",
                            2L * i,
                            2,
                            i,
                            "a",
                            i + 1,
                            i == 7 ? 2 : 1,
                            1,
                            0,
                            0));
        }
        org.apache.paimon.fs.Path tablePath = fixture.paths.root();
        SnapshotReaderImpl reader = fixture.reader(fixture.scan());
        SnapshotManager snapshots = reader.snapshotManager();
        assertThat(
                        LocalFileIO.INSTANCE.tryToWriteAtomic(
                                snapshots.snapshotPath(17), fixture.snapshot.toJson()))
                .isTrue();
        Options dynamic = new Options(fixture.schema.options());
        dynamic.set(CoreOptions.SCAN_PLAN_AUTO_TAG_FOR_READ_TIME_RETAINED, Duration.ofMinutes(10));
        dynamic.set(CoreOptions.QUERY_AUTH_ENABLED, false);
        FileStoreTable table =
                FileStoreTableFactory.create(LocalFileIO.INSTANCE, tablePath).copy(dynamic.toMap());
        ReadBuilder builder =
                table.newReadBuilder()
                        .withPartitionFilter(Collections.singletonMap("p", "1"))
                        .withLimit(1)
                        .dropStats();
        Path metadata = Files.createDirectory(directory.resolve("builder-metadata"));
        String tag;
        try (ObservedIOManager io = fixture.io();
                DataFileMetaStore store = new DataFileMetaStore(metadata, 1024 * 1024)) {
            AtomicInteger factories = new AtomicInteger();
            InnerTableScan scan =
                    (InnerTableScan)
                            builder.newScan(
                                    actual -> {
                                        factories.incrementAndGet();
                                        assertThat(
                                                        actual.coreOptions()
                                                                .scanPlanAutoTagTimeRetained())
                                                .isEqualTo(Duration.ofMinutes(10));
                                        return reader.withIncrementalPlanning(store, io);
                                    });
            List<Split> splits = scan.plan().splits();
            assertThat(factories.get()).isEqualTo(1);
            assertThat(splits).hasSize(1);
            DataSplit split = (DataSplit) splits.get(0);
            assertThat(split.partition()).isEqualTo(row(1, 1));
            assertThat(split.dataFiles())
                    .extracting(DataFileMeta::fileName)
                    .doesNotContain("base-7.parquet");
            tag = scan.readProtectionTagName();
            assertThat(tag).startsWith("batch-read-17-");
            assertThat(new TagManager(LocalFileIO.INSTANCE, tablePath, null).tagExists(tag))
                    .isTrue();
            byte[] executorBuilder = InstantiationUtil.serializeObject(builder);
            store.close();
            ReadBuilder restored =
                    InstantiationUtil.deserializeObject(
                            executorBuilder, getClass().getClassLoader());
            assertThat(restored.newRead()).isNotNull();
            assertThatThrownBy(() -> split.dataFiles().get(0))
                    .isInstanceOf(IllegalStateException.class);
            assertThat(io.closed).isFalse();
        }
        LocalFileIO.INSTANCE.delete(
                new TagManager(LocalFileIO.INSTANCE, tablePath, null).tagPath(tag), false);
    }

    @Test
    void testIncrementalBatchPlanKeepsAuthWrapperAndDoesNotApplyUnsafeLimit() throws Exception {
        Fixture fixture = new Fixture();
        for (int i = 0; i < 7; i++) {
            fixture.write(entry("base-" + i + ".parquet", i * 2L, 2, i, "a", i + 1, 1, 1, 0, 0));
        }
        SnapshotReaderImpl reader = fixture.reader(fixture.scan());
        assertThat(
                        LocalFileIO.INSTANCE.tryToWriteAtomic(
                                reader.snapshotManager().snapshotPath(17),
                                fixture.snapshot.toJson()))
                .isTrue();
        Options options = new Options(fixture.schema.options());
        options.set(CoreOptions.QUERY_AUTH_ENABLED, true);
        TableQueryAuthResult auth =
                new TableQueryAuthResult(
                        Collections.singletonList(
                                JsonSerdeUtil.toFlatJson(
                                        new PredicateBuilder(fixture.schema.logicalRowType())
                                                .greaterThan(3, 0))),
                        null);
        Path metadata = Files.createDirectory(directory.resolve("auth-metadata"));
        try (ObservedIOManager io = fixture.io();
                DataFileMetaStore store = new DataFileMetaStore(metadata, 1024 * 1024)) {
            reader.withIncrementalPlanning(store, io);
            AtomicInteger authorizations = new AtomicInteger();
            AppendBatchTableScan scan =
                    new AppendBatchTableScan(
                            fixture.schema,
                            new FileSystemSchemaManager(LocalFileIO.INSTANCE, fixture.paths.root()),
                            new CoreOptions(options),
                            reader,
                            select -> {
                                authorizations.incrementAndGet();
                                return auth;
                            });
            scan.withLimit(1);
            List<Split> splits = scan.plan().splits();
            assertThat(authorizations.get()).isEqualTo(1);
            assertThat(splits).hasSize(2);
            int selected = 0;
            for (Split split : splits) {
                assertThat(split).isInstanceOf(QueryAuthSplit.class);
                QueryAuthSplit wrapped = (QueryAuthSplit) split;
                assertThat(wrapped.authResult()).isSameAs(auth);
                assertThat(wrapped.mergedRowCount()).isEmpty();
                selected += ((DataSplit) wrapped.split()).dataFiles().size();
            }
            assertThat(selected).isEqualTo(7);
        }
    }

    @Test
    void testConfiguredReadPreservesCheckedStorageFailureAndCleansOwnedFiles() throws Exception {
        Fixture fixture = new Fixture();
        fixture.write(entry("base.parquet", 0L, 2, 1, "a", 1, 1, 1, 0, 0));
        Path metadata = Files.createDirectory(directory.resolve("configured-limit"));
        try (ObservedIOManager io = fixture.io();
                DataFileMetaStore store = new DataFileMetaStore(metadata, 8)) {
            SnapshotReader reader =
                    fixture.reader(fixture.scan()).withIncrementalPlanning(store, io);
            assertThatThrownBy(reader::read)
                    .isInstanceOf(RuntimeException.class)
                    .hasCauseInstanceOf(IOException.class);
            assertThat(io.closed).isFalse();
            try (Stream<Path> files = Files.list(metadata)) {
                assertThat(files.count()).isZero();
            }
            fixture.assertNoSpillFiles();
        }
    }

    private static List<List<String>> groups(DataEvolutionFileStoreScan scan, ObservedIOManager io)
            throws Exception {
        List<List<String>> result = new ArrayList<>();
        scan.forEachFileGroup(
                io,
                group ->
                        result.add(
                                group.stream()
                                        .map(entry -> entry.file().fileName())
                                        .collect(Collectors.toList())));
        return result;
    }

    private final class Fixture {
        private final TableSchema schema;
        private final RowType partitionType;
        private final ManifestFile.Factory factory;
        private final FileStorePathFactory paths;
        private final List<ManifestFileMeta> manifests = new ArrayList<>();
        private final AtomicInteger reads = new AtomicInteger();
        private final AtomicInteger streamedGroups = new AtomicInteger();
        private final Snapshot snapshot =
                new Snapshot(
                        17,
                        0,
                        "base-list",
                        null,
                        "delta-list",
                        null,
                        null,
                        null,
                        null,
                        "test",
                        null,
                        1,
                        Snapshot.CommitKind.APPEND,
                        12345,
                        0,
                        0,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null);
        private final Path spill = directory.resolve("spill");

        private Fixture() throws Exception {
            Options options = new Options();
            options.set(CoreOptions.DATA_EVOLUTION_ENABLED, true);
            options.set(CoreOptions.ROW_TRACKING_ENABLED, true);
            options.set(CoreOptions.BUCKET, -1);
            options.set(
                    CoreOptions.DATA_EVOLUTION_METADATA_MEMORY_OPTIMIZATION_MODE,
                    DataEvolutionMetadataMemoryMode.LIGHTWEIGHT);
            options.set(CoreOptions.SORT_SPILL_BUFFER_SIZE, MemorySize.ofKibiBytes(128));
            options.set(CoreOptions.PAGE_SIZE, MemorySize.ofKibiBytes(32));
            options.set(CoreOptions.LOCAL_SORT_MAX_NUM_FILE_HANDLES, 4);
            options.set(CoreOptions.MANIFEST_SIDECAR_ENABLED, true);
            schema =
                    TableSchema.create(
                            0,
                            Schema.newBuilder()
                                    .column("p", DataTypes.INT())
                                    .column("q", DataTypes.INT())
                                    .column("id", DataTypes.INT())
                                    .column("a", DataTypes.INT())
                                    .column("b", DataTypes.INT())
                                    .partitionKeys("p", "q")
                                    .options(options.toMap())
                                    .build());
            partitionType = schema.logicalPartitionType();
            org.apache.paimon.fs.Path table =
                    new org.apache.paimon.fs.Path(directory.resolve("table").toString());
            LocalFileIO fileIO = new LocalFileIO();
            FileSystemSchemaManager schemas = new FileSystemSchemaManager(fileIO, table);
            assertThat(schemas.commit(schema)).isTrue();
            paths =
                    new FileStorePathFactory(
                            table,
                            partitionType,
                            "default",
                            "parquet",
                            "data-",
                            "changelog-",
                            false,
                            false,
                            "zstd",
                            null,
                            null,
                            CoreOptions.ExternalPathStrategy.NONE,
                            null,
                            false,
                            null);
            factory =
                    new ManifestFile.Factory(
                            fileIO,
                            schemas,
                            partitionType,
                            FileFormat.fromIdentifier("avro", new Options()),
                            "zstd",
                            paths,
                            Long.MAX_VALUE,
                            null,
                            null,
                            new CoreOptions(options));
            Files.createDirectories(spill);
        }

        private void write(ManifestEntry... entries) {
            manifests.addAll(factory.create().write(Arrays.asList(entries)));
        }

        private DataEvolutionFileStoreScan scan() {
            return scan(false);
        }

        private DataEvolutionFileStoreScan scan(boolean forbidMaterializedPlan) {
            ManifestsReader reader =
                    new ManifestsReader(partitionType, "default", null, null) {
                        @Override
                        public Result read(Snapshot requested, ScanMode mode) {
                            reads.incrementAndGet();
                            return new Result(snapshot, manifests, manifests);
                        }
                    };
            BucketSelectConverter bucketSelectConverter =
                    new BucketSelectConverter(
                            BucketMode.BUCKET_UNAWARE,
                            new CoreOptions(schema.options()),
                            schema.logicalRowType(),
                            partitionType,
                            schema.logicalBucketKeyType());
            return new DataEvolutionFileStoreScan(
                    reader, bucketSelectConverter, null, null, schema, factory, 2, false) {
                @Override
                protected List<ManifestEntry> postFilterManifestEntries(
                        List<ManifestEntry> entries) {
                    if (forbidMaterializedPlan) {
                        streamedGroups.incrementAndGet();
                    }
                    return super.postFilterManifestEntries(entries);
                }

                @Override
                public FileStoreScan.Plan plan() {
                    if (forbidMaterializedPlan) {
                        throw new AssertionError(
                                "Streaming LIMIT must not materialize scan.plan()");
                    }
                    return super.plan();
                }
            };
        }

        private ObservedIOManager io() {
            return new ObservedIOManager(spill.toString());
        }

        private SnapshotReaderImpl reader(DataEvolutionFileStoreScan scan) {
            return new SnapshotReaderImpl(
                    scan,
                    schema,
                    new CoreOptions(schema.options()),
                    new SnapshotManager(
                            LocalFileIO.INSTANCE,
                            new org.apache.paimon.fs.Path(directory.resolve("table").toString()),
                            null,
                            null,
                            null),
                    null,
                    new DataEvolutionSplitGenerator(4096, 1024, false),
                    (target, predicate) ->
                            ((DataEvolutionFileStoreScan) target).withFilter(predicate),
                    paths,
                    "incremental-fixture",
                    null,
                    null);
        }

        private void assertNoSpillFiles() throws IOException {
            try (Stream<Path> files = Files.walk(spill)) {
                assertThat(files.filter(Files::isRegularFile).count()).isZero();
            }
        }
    }

    private static final class ObservedIOManager extends IOManagerImpl {
        private final AtomicInteger spills = new AtomicInteger();
        private boolean closed;

        private ObservedIOManager(String directory) {
            super(directory);
        }

        @Override
        public BufferFileWriter createBufferFileWriter(FileIOChannel.ID channel)
                throws IOException {
            spills.incrementAndGet();
            return super.createBufferFileWriter(channel);
        }

        @Override
        public void close() throws Exception {
            closed = true;
            super.close();
        }
    }

    private static final class CapturingRegistry extends TestMetricRegistry {
        private MetricGroup group;

        @Override
        public MetricGroup createMetricGroup(String name, Map<String, String> variables) {
            group = super.createMetricGroup(name, variables);
            return group;
        }

        private Object value(String name) {
            return ((Gauge<?>) group.getMetrics().get(name)).getValue();
        }
    }

    private static ManifestEntry entry(
            String name,
            Long first,
            long count,
            long sequence,
            String column,
            Integer value,
            int p,
            int q,
            int bucket,
            int level) {
        BinaryArray nulls = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(nulls, 1, Long.BYTES);
        writer.writeLong(0, value == null ? count : 0);
        writer.complete();
        return ManifestEntry.create(
                FileKind.ADD,
                row(p, q),
                bucket,
                2,
                DataFileMeta.create(
                        name,
                        100,
                        count,
                        DataFileMeta.EMPTY_MIN_KEY,
                        DataFileMeta.EMPTY_MAX_KEY,
                        SimpleStats.EMPTY_STATS,
                        new SimpleStats(valueRow(value), valueRow(value), nulls),
                        sequence,
                        sequence,
                        0,
                        level,
                        Collections.emptyList(),
                        Timestamp.fromEpochMillis(12345),
                        0L,
                        null,
                        FileSource.APPEND,
                        Collections.singletonList(column),
                        null,
                        first,
                        Collections.singletonList(column),
                        null));
    }

    private static BinaryRow row(int... values) {
        BinaryRow row = new BinaryRow(values.length);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        for (int i = 0; i < values.length; i++) {
            writer.writeInt(i, values[i]);
        }
        writer.complete();
        return row;
    }

    private static BinaryRow valueRow(Integer value) {
        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        if (value == null) {
            writer.setNullAt(0);
        } else {
            writer.writeInt(0, value);
        }
        writer.complete();
        return row;
    }
}
