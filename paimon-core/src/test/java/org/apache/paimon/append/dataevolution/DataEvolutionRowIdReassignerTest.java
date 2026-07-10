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

package org.apache.paimon.append.dataevolution;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.globalindex.btree.BTreeIndexOptions;
import org.apache.paimon.globalindex.sorted.SortedGlobalIndexBuilder;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.IndexManifestFile;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.operation.FileStoreCommitImpl;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DataEvolutionRowIdReassigner}. */
public class DataEvolutionRowIdReassignerTest extends TableTestBase {

    private static final int LARGE_ENTRY_COUNT = 10_000;
    private static final int LARGE_MANIFEST_FILE_COUNT = 20;
    private static final int ONE_MILLION_ENTRY_COUNT = 1_000_000;
    private static final int ONE_MILLION_MANIFEST_FILE_COUNT = 200;
    private static final long LARGE_ENTRY_ROW_COUNT = 2L;
    private static final long RANDOM_PARTITION_SEED = 20260519L;
    private static final String LARGE_FILE_PREFIX = "large-partition-";
    private static final String LARGE_FILE_SUFFIX = ".parquet";
    private static final int LARGE_PARTITION_ID_WIDTH = 7;
    private static final String[] DATA_INTEGRITY_PARTITIONS = {"a", "b", "c", "d"};
    private static final int DATA_INTEGRITY_FILES_PER_PARTITION = 6;
    private static final long SCRAMBLED_ROW_ID_GAP = 100_000_000L;
    private static final long SCRAMBLED_ROW_ID_SEED = 20260710L;

    @Override
    protected Schema schemaDefault() {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("pt", DataTypes.STRING());
        schemaBuilder.column("id", DataTypes.INT());
        schemaBuilder.column("payload", DataTypes.STRING());
        schemaBuilder.partitionKeys("pt");
        schemaBuilder.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.GLOBAL_INDEX_ENABLED.key(), "true");
        schemaBuilder.option(BTreeIndexOptions.BTREE_INDEX_RECORDS_PER_RANGE.key(), "1");
        return schemaBuilder.build();
    }

    private Schema unpartitionedSchema() {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("id", DataTypes.INT());
        schemaBuilder.column("payload", DataTypes.STRING());
        schemaBuilder.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        return schemaBuilder.build();
    }

    @Test
    public void testReassignRowIdsByPartition() throws Exception {
        FileStoreTable table = createTableWithInterleavedPartitions();

        assertThat(table.snapshotManager().latestSnapshot().nextRowId()).isEqualTo(5L);

        DataEvolutionRowIdReassigner.Result result =
                new DataEvolutionRowIdReassigner(table).reassign("test-reassign-row-id");

        assertThat(result.reassigned).isTrue();
        assertThat(result.previousSnapshotId).isEqualTo(5L);
        assertThat(result.newSnapshotId).isEqualTo(6L);
        assertThat(result.firstAssignedRowId).isEqualTo(5L);
        assertThat(result.nextRowId).isEqualTo(10L);
        assertThat(result.fileCount).isEqualTo(5L);
        assertThat(result.rowCount).isEqualTo(5L);
        assertThat(result.indexFileCount).isEqualTo(0L);

        Map<String, List<Long>> rowIdsByPartition = rowIdsByPartition(table);
        assertThat(rowIdsByPartition).hasSize(2);
        assertThat(rowIdsByPartition).containsEntry("pt=a/", Arrays.asList(5L, 6L, 7L));
        assertThat(rowIdsByPartition).containsEntry("pt=b/", Arrays.asList(8L, 9L));
        assertThat(table.snapshotManager().latestSnapshot().nextRowId()).isEqualTo(10L);
    }

    @Test
    public void testReassignNullPartitionAcrossManifestGroups() throws Exception {
        createTableDefault();
        FileStoreTable table =
                getTableDefault()
                        .copy(
                                Collections.singletonMap(
                                        CoreOptions.ROW_TRACKING_PARTITION_GROUP_ON_COMMIT.key(),
                                        "false"));
        writeRowsInPartitions(table, null, 0, "a", 1);
        writeOneRow(table, "z", 2);
        writeOneRow(table, null, 3);

        BinaryRow nullPartition =
                new InternalRowSerializer(table.schema().logicalPartitionType())
                        .toBinaryRow(GenericRow.of((Object) null));
        String nullPartitionPath = table.store().pathFactory().getPartitionString(nullPartition);
        Map<String, List<Long>> beforeRowIds = rowIdsByPartition(table);
        assertThat(beforeRowIds)
                .containsEntry(nullPartitionPath, Arrays.asList(1L, 3L))
                .containsEntry("pt=a/", Collections.singletonList(0L))
                .containsEntry("pt=z/", Collections.singletonList(2L));

        DataEvolutionRowIdReassigner.Result result =
                new DataEvolutionRowIdReassigner(table)
                        .reassign("test-reassign-null-partition-row-id");

        assertThat(result.firstAssignedRowId).isEqualTo(4L);
        assertThat(result.nextRowId).isEqualTo(6L);
        assertThat(result.fileCount).isEqualTo(2L);
        assertThat(result.rowCount).isEqualTo(2L);
        assertThat(rowIdsByPartition(table))
                .containsEntry(nullPartitionPath, Arrays.asList(4L, 5L))
                .containsEntry("pt=a/", Collections.singletonList(0L))
                .containsEntry("pt=z/", Collections.singletonList(2L));
    }

    @Test
    public void testReassignRetriesWithLatestNextRowIdAfterConcurrentAppend() throws Exception {
        FileStoreTable table = createTableWithInterleavedPartitions();
        Snapshot before = table.snapshotManager().latestSnapshot();
        Map<String, SimpleStats> valueStatsBefore = valueStatsByFile(table);
        assertThat(before.nextRowId()).isEqualTo(5L);
        assertThat(valueStatsBefore.values())
                .allSatisfy(stats -> assertThat(stats).isNotEqualTo(SimpleStats.EMPTY_STATS));

        AtomicBoolean appended = new AtomicBoolean();
        DataEvolutionRowIdReassigner.Result result =
                new DataEvolutionRowIdReassigner(
                                table,
                                null,
                                () -> {
                                    if (appended.compareAndSet(false, true)) {
                                        try {
                                            writeOneRow(table, "c", 100);
                                        } catch (Exception e) {
                                            throw new RuntimeException(e);
                                        }
                                    }
                                })
                        .reassign("test-reassign-conflict-retry");

        assertThat(appended).isTrue();
        assertThat(result.reassigned).isTrue();
        assertThat(result.previousSnapshotId).isEqualTo(before.id() + 1);
        assertThat(result.newSnapshotId).isEqualTo(before.id() + 2);
        assertThat(result.firstAssignedRowId).isEqualTo(6L);
        assertThat(result.nextRowId).isEqualTo(11L);
        assertThat(result.fileCount).isEqualTo(5L);
        assertThat(result.rowCount).isEqualTo(5L);
        assertThat(rowIdsByPartition(table))
                .containsEntry("pt=a/", Arrays.asList(6L, 7L, 8L))
                .containsEntry("pt=b/", Arrays.asList(9L, 10L))
                .containsEntry("pt=c/", Collections.singletonList(5L));
        assertThat(valueStatsByFile(table)).containsAllEntriesOf(valueStatsBefore);
        assertThat(table.snapshotManager().latestSnapshot().nextRowId()).isEqualTo(11L);
    }

    @Test
    public void testReassignLeavesDisjointConcurrentAppendOutOfPlan() throws Exception {
        FileStoreTable table = createTableWithInterleavedPartitions();
        Snapshot before = table.snapshotManager().latestSnapshot();

        AtomicBoolean appended = new AtomicBoolean();
        DataEvolutionRowIdReassigner.Result result =
                new DataEvolutionRowIdReassigner(
                                table,
                                null,
                                () -> {
                                    if (appended.compareAndSet(false, true)) {
                                        try {
                                            writeOneRow(table, "a", 100);
                                        } catch (Exception e) {
                                            throw new RuntimeException(e);
                                        }
                                    }
                                })
                        .reassign("test-reassign-append-planned-partition");

        assertThat(result.previousSnapshotId).isEqualTo(before.id() + 1);
        assertThat(result.newSnapshotId).isEqualTo(before.id() + 2);
        assertThat(result.firstAssignedRowId).isEqualTo(6L);
        assertThat(result.nextRowId).isEqualTo(11L);
        assertThat(result.fileCount).isEqualTo(5L);
        assertThat(result.rowCount).isEqualTo(5L);
        assertThat(rowIdsByPartition(table))
                .containsEntry("pt=a/", Arrays.asList(5L, 6L, 7L, 8L))
                .containsEntry("pt=b/", Arrays.asList(9L, 10L));
    }

    @Test
    public void testReassignAbortsForPartiallyOverlappingConcurrentAppend() throws Exception {
        FileStoreTable table = createTableWithInterleavedPartitions();
        Snapshot before = table.snapshotManager().latestSnapshot();

        AtomicBoolean appended = new AtomicBoolean();
        assertThatThrownBy(
                        () ->
                                new DataEvolutionRowIdReassigner(
                                                table,
                                                null,
                                                () -> {
                                                    if (appended.compareAndSet(false, true)) {
                                                        try {
                                                            writePartialPayload(
                                                                    table,
                                                                    "a",
                                                                    4L,
                                                                    "overlap-4",
                                                                    "overlap-5");
                                                        } catch (Exception e) {
                                                            throw new RuntimeException(e);
                                                        }
                                                    }
                                                })
                                        .reassign("test-reassign-partially-overlapping-append"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("partially overlaps planned range");

        assertThat(appended).isTrue();
        assertThat(table.snapshotManager().latestSnapshot().id()).isEqualTo(before.id() + 1);
    }

    @Test
    public void testReassignAbortsWhenConcurrentAppendMergesPlannedManifests() throws Exception {
        FileStoreTable originalTable = createTableWithInterleavedPartitions();
        List<String> plannedManifestFiles = dataManifestFileNames(originalTable);
        assertThat(plannedManifestFiles).hasSizeGreaterThan(1);

        FileStoreTable table = withManifestMergeOnNextAppend(originalTable);
        Snapshot before = table.snapshotManager().latestSnapshot();

        AtomicBoolean merged = new AtomicBoolean();
        assertThatThrownBy(
                        () ->
                                new DataEvolutionRowIdReassigner(
                                                table,
                                                null,
                                                () -> {
                                                    if (merged.compareAndSet(false, true)) {
                                                        try {
                                                            writeOneRow(table, "c", 100);
                                                            Snapshot appendSnapshot =
                                                                    table.snapshotManager()
                                                                            .latestSnapshot();
                                                            assertThat(appendSnapshot.commitKind())
                                                                    .isEqualTo(
                                                                            Snapshot.CommitKind
                                                                                    .APPEND);
                                                            assertThat(dataManifestFileNames(table))
                                                                    .doesNotContainAnyElementsOf(
                                                                            plannedManifestFiles);
                                                        } catch (Exception e) {
                                                            throw new RuntimeException(e);
                                                        }
                                                    }
                                                })
                                        .reassign("test-reassign-append-manifest-merge"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("planned manifest")
                .hasMessageContaining("no longer exists");

        assertThat(merged).isTrue();
        assertThat(table.snapshotManager().latestSnapshot().id()).isEqualTo(before.id() + 1);
        assertThat(rowIdsByPartition(table))
                .containsEntry("pt=a/", Arrays.asList(0L, 2L, 4L))
                .containsEntry("pt=b/", Arrays.asList(1L, 3L))
                .containsEntry("pt=c/", Collections.singletonList(5L));
        assertThat(readTableRows(table))
                .containsExactly("0|a|v0", "100|c|v100", "1|b|v1", "2|a|v2", "3|b|v3", "4|a|v4");
    }

    @Test
    public void testReassignKeepsHistoricalAddDeleteEntriesConsistent() throws Exception {
        FileStoreTable table = createTableWithInterleavedPartitions();
        FileEntry.Identifier compactedFile = compactOneFile(table, "pt=a/");
        long oldFirstRowId = assertAddDeleteEntriesConsistent(table, compactedFile);

        new DataEvolutionRowIdReassigner(table)
                .reassign("test-reassign-historical-add-delete-without-retry");

        assertThat(assertAddDeleteEntriesConsistent(table, compactedFile))
                .isNotEqualTo(oldFirstRowId);
        assertThat(readTableRows(table))
                .containsExactly("0|a|v0", "1|b|v1", "2|a|v2", "3|b|v3", "4|a|v4");
    }

    @Test
    public void testReassignKeepsHistoricalAddDeleteEntriesConsistentAfterRetry() throws Exception {
        FileStoreTable table = createTableWithInterleavedPartitions();
        FileEntry.Identifier compactedFile = compactOneFile(table, "pt=a/");
        long oldFirstRowId = assertAddDeleteEntriesConsistent(table, compactedFile);

        AtomicBoolean appended = new AtomicBoolean();
        new DataEvolutionRowIdReassigner(
                        table,
                        null,
                        () -> {
                            if (appended.compareAndSet(false, true)) {
                                try {
                                    writeOneRow(table, "c", 100);
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        })
                .reassign("test-reassign-historical-add-delete");

        assertThat(appended).isTrue();
        assertThat(assertAddDeleteEntriesConsistent(table, compactedFile))
                .isNotEqualTo(oldFirstRowId);
        assertThat(readTableRows(table))
                .containsExactly("0|a|v0", "100|c|v100", "1|b|v1", "2|a|v2", "3|b|v3", "4|a|v4");
    }

    @Test
    public void testReassignRetriesAfterConcurrentPartialColumnAppend() throws Exception {
        createTableDefault();
        FileStoreTable originalTable = getTableDefault();
        writeRows(originalTable, "a", 0, 1);
        writeOneRow(originalTable, "b", 2);
        writeRows(originalTable, "a", 3, 4);
        FileStoreTable table = originalTable;
        Snapshot before = table.snapshotManager().latestSnapshot();

        AtomicBoolean appended = new AtomicBoolean();
        DataEvolutionRowIdReassigner.Result result =
                new DataEvolutionRowIdReassigner(
                                table,
                                null,
                                () -> {
                                    if (appended.compareAndSet(false, true)) {
                                        try {
                                            writePartialPayload(
                                                    table, "a", 0L, "updated-0", "updated-1");
                                            List<ManifestEntry> sharedRangeEntries =
                                                    currentEntriesAtRange(table, "pt=a/", 0L, 1L);
                                            assertThat(sharedRangeEntries).hasSize(2);
                                            assertThat(
                                                            sharedRangeEntries
                                                                    .get(0)
                                                                    .file()
                                                                    .maxSequenceNumber())
                                                    .isNotEqualTo(
                                                            sharedRangeEntries
                                                                    .get(1)
                                                                    .file()
                                                                    .maxSequenceNumber());
                                            assertThat(
                                                            table.snapshotManager()
                                                                    .latestSnapshot()
                                                                    .nextRowId())
                                                    .isEqualTo(5L);
                                        } catch (Exception e) {
                                            throw new RuntimeException(e);
                                        }
                                    }
                                })
                        .reassign("test-reassign-partial-column-append");

        assertThat(appended).isTrue();
        assertThat(result.previousSnapshotId).isEqualTo(before.id() + 1);
        assertThat(result.newSnapshotId).isEqualTo(before.id() + 2);
        assertThat(result.firstAssignedRowId).isEqualTo(5L);
        assertThat(result.nextRowId).isEqualTo(9L);
        assertThat(result.fileCount).isEqualTo(3L);
        assertThat(result.rowCount).isEqualTo(4L);
        assertThat(rowIdsByPartition(table))
                .containsEntry("pt=a/", Arrays.asList(5L, 5L, 7L))
                .containsEntry("pt=b/", Collections.singletonList(2L));
        assertThat(readTableRows(table))
                .containsExactly("0|a|updated-0", "1|a|updated-1", "2|b|v2", "3|a|v3", "4|a|v4");
    }

    @Test
    public void testReassignAdvancesAcrossRepeatedAppendConflicts() throws Exception {
        FileStoreTable table = createTableWithInterleavedPartitions();
        Snapshot before = table.snapshotManager().latestSnapshot();

        AtomicInteger beforeCommits = new AtomicInteger();
        DataEvolutionRowIdReassigner.Result result =
                new DataEvolutionRowIdReassigner(
                                table,
                                null,
                                () -> {
                                    try {
                                        int attempt = beforeCommits.getAndIncrement();
                                        if (attempt == 0) {
                                            writeOneRow(table, "c", 100);
                                            writeOneRow(table, "d", 101);
                                        } else if (attempt == 1) {
                                            writeOneRow(table, "a", 102);
                                        }
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }
                                })
                        .reassign("test-reassign-multiple-append-conflicts");

        assertThat(beforeCommits).hasValue(3);
        assertThat(result.previousSnapshotId).isEqualTo(before.id() + 3);
        assertThat(result.newSnapshotId).isEqualTo(before.id() + 4);
        assertThat(result.firstAssignedRowId).isEqualTo(8L);
        assertThat(result.nextRowId).isEqualTo(13L);
        assertThat(result.fileCount).isEqualTo(5L);
        assertThat(result.rowCount).isEqualTo(5L);
        assertThat(rowIdsByPartition(table))
                .containsEntry("pt=a/", Arrays.asList(7L, 8L, 9L, 10L))
                .containsEntry("pt=b/", Arrays.asList(11L, 12L))
                .containsEntry("pt=c/", Collections.singletonList(5L))
                .containsEntry("pt=d/", Collections.singletonList(6L));
    }

    @Test
    public void testReassignPartitionFilterAfterConcurrentAppendOutsideFilter() throws Exception {
        FileStoreTable table = createTableWithInterleavedPartitions();
        Snapshot before = table.snapshotManager().latestSnapshot();

        AtomicBoolean appended = new AtomicBoolean();
        DataEvolutionRowIdReassigner.Result result =
                new DataEvolutionRowIdReassigner(
                                table,
                                partitionPredicate(table, "a"),
                                () -> {
                                    if (appended.compareAndSet(false, true)) {
                                        try {
                                            writeOneRow(table, "c", 100);
                                        } catch (Exception e) {
                                            throw new RuntimeException(e);
                                        }
                                    }
                                })
                        .reassign("test-reassign-filter-after-append-conflict");

        assertThat(result.previousSnapshotId).isEqualTo(before.id() + 1);
        assertThat(result.newSnapshotId).isEqualTo(before.id() + 2);
        assertThat(result.firstAssignedRowId).isEqualTo(6L);
        assertThat(result.nextRowId).isEqualTo(9L);
        assertThat(result.fileCount).isEqualTo(3L);
        assertThat(result.rowCount).isEqualTo(3L);
        assertThat(rowIdsByPartition(table))
                .containsEntry("pt=a/", Arrays.asList(6L, 7L, 8L))
                .containsEntry("pt=b/", Arrays.asList(1L, 3L))
                .containsEntry("pt=c/", Collections.singletonList(5L));
    }

    @Test
    public void testReassignDoesNotExpandPlanForNewlyNonContiguousPartition() throws Exception {
        FileStoreTable table = createTableWithPartiallyOverlappedPartitions();
        Snapshot before = table.snapshotManager().latestSnapshot();

        AtomicBoolean appended = new AtomicBoolean();
        DataEvolutionRowIdReassigner.Result result =
                new DataEvolutionRowIdReassigner(
                                table,
                                null,
                                () -> {
                                    if (appended.compareAndSet(false, true)) {
                                        try {
                                            writeOneRow(table, "c", 100);
                                        } catch (Exception e) {
                                            throw new RuntimeException(e);
                                        }
                                    }
                                })
                        .reassign("test-reassign-append-new-partition");

        assertThat(result.previousSnapshotId).isEqualTo(before.id() + 1);
        assertThat(result.newSnapshotId).isEqualTo(before.id() + 2);
        assertThat(result.firstAssignedRowId).isEqualTo(6L);
        assertThat(result.nextRowId).isEqualTo(8L);
        assertThat(result.fileCount).isEqualTo(2L);
        assertThat(result.rowCount).isEqualTo(2L);
        assertThat(expandedRowIdsByPartition(table))
                .containsEntry("pt=a/", Arrays.asList(6L, 7L))
                .containsEntry("pt=b/", Collections.singletonList(3L))
                .containsEntry("pt=c/", Arrays.asList(0L, 1L, 5L));
    }

    @Test
    public void testReassignAbortsAfterConcurrentOverwrite() throws Exception {
        FileStoreTable table = createTableWithInterleavedPartitions();
        Snapshot before = table.snapshotManager().latestSnapshot();

        AtomicBoolean overwritten = new AtomicBoolean();
        assertThatThrownBy(
                        () ->
                                new DataEvolutionRowIdReassigner(
                                                table,
                                                null,
                                                () -> {
                                                    if (overwritten.compareAndSet(false, true)) {
                                                        try {
                                                            overwriteOneRow(table, "c", 100);
                                                        } catch (Exception e) {
                                                            throw new RuntimeException(e);
                                                        }
                                                    }
                                                })
                                        .reassign("test-reassign-overwrite-conflict"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("OVERWRITE snapshot");

        assertThat(table.snapshotManager().latestSnapshot().id()).isEqualTo(before.id() + 1);
        assertThat(table.snapshotManager().latestSnapshot().commitKind())
                .isEqualTo(Snapshot.CommitKind.OVERWRITE);
    }

    @Test
    public void testReassignAbortsAfterConcurrentManifestCompaction() throws Exception {
        FileStoreTable table = createTableWithInterleavedPartitions();
        Snapshot before = table.snapshotManager().latestSnapshot();

        AtomicBoolean compacted = new AtomicBoolean();
        assertThatThrownBy(
                        () ->
                                new DataEvolutionRowIdReassigner(
                                                table,
                                                null,
                                                () -> {
                                                    if (compacted.compareAndSet(false, true)) {
                                                        try {
                                                            compactManifests(table);
                                                        } catch (Exception e) {
                                                            throw new RuntimeException(e);
                                                        }
                                                    }
                                                })
                                        .reassign("test-reassign-compact-conflict"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("COMPACT snapshot");

        assertThat(table.snapshotManager().latestSnapshot().id()).isEqualTo(before.id() + 1);
        assertThat(table.snapshotManager().latestSnapshot().commitKind())
                .isEqualTo(Snapshot.CommitKind.COMPACT);
    }

    @Test
    public void testReassignContinuesAfterConcurrentAnalyze() throws Exception {
        FileStoreTable table = createTableWithInterleavedPartitions();
        Snapshot before = table.snapshotManager().latestSnapshot();

        AtomicBoolean analyzed = new AtomicBoolean();
        DataEvolutionRowIdReassigner.Result result =
                new DataEvolutionRowIdReassigner(
                                table,
                                null,
                                () -> {
                                    if (analyzed.compareAndSet(false, true)) {
                                        try {
                                            updateStatistics(table);
                                        } catch (Exception e) {
                                            throw new RuntimeException(e);
                                        }
                                    }
                                })
                        .reassign("test-reassign-analyze-conflict");

        assertThat(result.previousSnapshotId).isEqualTo(before.id() + 1);
        assertThat(result.newSnapshotId).isEqualTo(before.id() + 2);
        assertThat(result.firstAssignedRowId).isEqualTo(5L);
        assertThat(result.nextRowId).isEqualTo(10L);
        assertThat(table.snapshotManager().latestSnapshot().statistics()).isNotNull();
        assertThat(table.snapshotManager().latestSnapshot().commitKind())
                .isEqualTo(Snapshot.CommitKind.OVERWRITE);
    }

    @Test
    public void testReassignMultiRowFilesByPartition() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        writeRows(table, "a", 0, 1);
        writeRows(table, "b", 2, 3);
        writeRows(table, "a", 4, 5);

        assertThat(table.snapshotManager().latestSnapshot().nextRowId()).isEqualTo(6L);

        DataEvolutionRowIdReassigner.Result result =
                new DataEvolutionRowIdReassigner(table).reassign("test-reassign-multi-row");

        assertThat(result.firstAssignedRowId).isEqualTo(6L);
        assertThat(result.nextRowId).isEqualTo(10L);
        assertThat(result.fileCount).isEqualTo(2L);
        assertThat(result.rowCount).isEqualTo(4L);
        Map<String, List<Long>> rowIdsByPartition = expandedRowIdsByPartition(table);
        assertThat(rowIdsByPartition).hasSize(2);
        assertThat(rowIdsByPartition).containsEntry("pt=a/", Arrays.asList(6L, 7L, 8L, 9L));
        assertThat(rowIdsByPartition).containsEntry("pt=b/", Arrays.asList(2L, 3L));
    }

    @Test
    public void testReassignOnlyOverlappingPartitions() throws Exception {
        FileStoreTable table = createTableWithPartiallyOverlappedPartitions();
        createBTreeIndex(table);

        assertThat(table.snapshotManager().latestSnapshot().nextRowId()).isEqualTo(5L);

        DataEvolutionRowIdReassigner.Result result =
                new DataEvolutionRowIdReassigner(table).reassign("test-reassign-partial-row-id");

        assertThat(result.firstAssignedRowId).isEqualTo(5L);
        assertThat(result.nextRowId).isEqualTo(7L);
        assertThat(result.fileCount).isEqualTo(2L);
        assertThat(result.rowCount).isEqualTo(2L);
        assertThat(result.indexFileCount).isEqualTo(2L);

        Map<String, List<Long>> rowIdsByPartition = expandedRowIdsByPartition(table);
        assertThat(rowIdsByPartition).containsEntry("pt=a/", Arrays.asList(5L, 6L));
        assertThat(rowIdsByPartition).containsEntry("pt=b/", Collections.singletonList(3L));
        assertThat(rowIdsByPartition).containsEntry("pt=c/", Arrays.asList(0L, 1L));
        assertThat(globalIndexRanges(table))
                .containsExactly(
                        new Range(0, 1),
                        new Range(0, 1),
                        new Range(3, 3),
                        new Range(5, 5),
                        new Range(6, 6));

        Predicate oldPartitionPredicate =
                new PredicateBuilder(table.rowType()).equal(table.rowType().getFieldIndex("id"), 1);
        assertThat(readPayloads(table, oldPartitionPredicate)).containsExactly("v1");
        Predicate reassignedPartitionPredicate =
                new PredicateBuilder(table.rowType()).equal(table.rowType().getFieldIndex("id"), 4);
        assertThat(readPayloads(table, reassignedPartitionPredicate)).containsExactly("v4");
    }

    @Test
    public void testReassignOnlyOuterPartitionWhenInnerRangeCanStay() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        writeOneRow(table, "a", 0);
        writeOneRow(table, "b", 1);
        writeOneRow(table, "c", 2);
        writeOneRow(table, "c", 3);
        writeOneRow(table, "d", 4);
        writeOneRow(table, "d", 5);
        writeOneRow(table, "b", 6);

        DataEvolutionRowIdReassigner.Result result =
                new DataEvolutionRowIdReassigner(table).reassign("test-reassign-only-b-row-id");

        assertThat(result.firstAssignedRowId).isEqualTo(7L);
        assertThat(result.nextRowId).isEqualTo(9L);
        assertThat(result.fileCount).isEqualTo(2L);
        assertThat(result.rowCount).isEqualTo(2L);

        assertThat(rowIdsByPartition(table))
                .containsEntry("pt=a/", Collections.singletonList(0L))
                .containsEntry("pt=b/", Arrays.asList(7L, 8L))
                .containsEntry("pt=c/", Arrays.asList(2L, 3L))
                .containsEntry("pt=d/", Arrays.asList(4L, 5L));
    }

    @Test
    public void testPartitionFilterOnlyReassignsSelectedPartitions() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        writeOneRow(table, "a", 0);
        writeOneRow(table, "b", 1);
        writeOneRow(table, "c", 2);
        writeOneRow(table, "c", 3);
        writeOneRow(table, "d", 4);
        writeOneRow(table, "d", 5);
        writeOneRow(table, "b", 6);

        DataEvolutionRowIdReassigner.Result cResult =
                new DataEvolutionRowIdReassigner(table, partitionPredicate(table, "c"))
                        .reassign("test-filter-c-row-id");

        assertThat(cResult.reassigned).isFalse();
        assertThat(table.snapshotManager().latestSnapshot().nextRowId()).isEqualTo(7L);
        assertThat(rowIdsByPartition(table))
                .containsEntry("pt=a/", Collections.singletonList(0L))
                .containsEntry("pt=b/", Arrays.asList(1L, 6L))
                .containsEntry("pt=c/", Arrays.asList(2L, 3L))
                .containsEntry("pt=d/", Arrays.asList(4L, 5L));

        DataEvolutionRowIdReassigner.Result bResult =
                new DataEvolutionRowIdReassigner(table, partitionPredicate(table, "b"))
                        .reassign("test-filter-b-row-id");

        assertThat(bResult.reassigned).isTrue();
        assertThat(bResult.firstAssignedRowId).isEqualTo(7L);
        assertThat(bResult.nextRowId).isEqualTo(9L);
        assertThat(bResult.fileCount).isEqualTo(2L);
        assertThat(bResult.rowCount).isEqualTo(2L);
        assertThat(rowIdsByPartition(table))
                .containsEntry("pt=a/", Collections.singletonList(0L))
                .containsEntry("pt=b/", Arrays.asList(7L, 8L))
                .containsEntry("pt=c/", Arrays.asList(2L, 3L))
                .containsEntry("pt=d/", Arrays.asList(4L, 5L));
    }

    @Test
    public void testReassignReusesUnaffectedManifestFiles() throws Exception {
        FileStoreTable table = createTableWithPartiallyOverlappedPartitions();
        Map<String, Set<String>> partitionsByManifest = currentPartitionsByManifest(table);
        List<String> unaffectedManifests = new ArrayList<>();
        List<String> affectedManifests = new ArrayList<>();
        for (Map.Entry<String, Set<String>> entry : partitionsByManifest.entrySet()) {
            if (!entry.getValue().contains("pt=a/")) {
                unaffectedManifests.add(entry.getKey());
            }
            if (entry.getValue().contains("pt=a/")) {
                affectedManifests.add(entry.getKey());
            }
        }
        assertThat(unaffectedManifests).isNotEmpty();
        assertThat(affectedManifests).isNotEmpty();

        new DataEvolutionRowIdReassigner(table).reassign("test-reassign-reuse-manifest");

        List<String> afterManifestFiles = dataManifestFileNames(table);
        assertThat(afterManifestFiles).containsAll(unaffectedManifests);
        assertThat(afterManifestFiles).doesNotContainAnyElementsOf(affectedManifests);
    }

    @Test
    public void testSkipWhenPartitionRowIdsAreContiguous() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        writeRows(table, "a", 0, 1);
        writeRows(table, "a", 2);
        writeRows(table, "b", 3, 4);

        assertThat(table.snapshotManager().latestSnapshot().id()).isEqualTo(3L);
        assertThat(table.snapshotManager().latestSnapshot().nextRowId()).isEqualTo(5L);

        DataEvolutionRowIdReassigner.Result result =
                new DataEvolutionRowIdReassigner(table).reassign("test-skip-row-id");

        assertThat(result.reassigned).isFalse();
        assertThat(result.skipReason).isEqualTo("no partition requires row-id reassignment");
        assertThat(result.previousSnapshotId).isEqualTo(3L);
        assertThat(result.newSnapshotId).isEqualTo(3L);
        assertThat(result.firstAssignedRowId).isEqualTo(5L);
        assertThat(result.nextRowId).isEqualTo(5L);
        assertThat(result.fileCount).isEqualTo(0L);
        assertThat(result.rowCount).isEqualTo(0L);
        assertThat(result.indexFileCount).isEqualTo(0L);
        assertThat(table.snapshotManager().latestSnapshot().id()).isEqualTo(3L);
        assertThat(expandedRowIdsByPartition(table))
                .containsEntry("pt=a/", Arrays.asList(0L, 1L, 2L))
                .containsEntry("pt=b/", Arrays.asList(3L, 4L));
    }

    @Test
    public void testSkipUnpartitionedTable() throws Exception {
        Identifier identifier = Identifier.create(database, "unpartitioned_table");
        catalog.createTable(identifier, unpartitionedSchema(), false);
        FileStoreTable table = getTable(identifier);
        writeUnpartitionedRows(table, 0, 1, 2);

        assertThat(table.snapshotManager().latestSnapshot().id()).isEqualTo(1L);
        assertThat(table.snapshotManager().latestSnapshot().nextRowId()).isEqualTo(3L);

        DataEvolutionRowIdReassigner.Result result =
                new DataEvolutionRowIdReassigner(table).reassign("test-skip-unpartitioned-row-id");

        assertThat(result.reassigned).isFalse();
        assertThat(result.skipReason).isEqualTo("table is not partitioned");
        assertThat(result.previousSnapshotId).isEqualTo(1L);
        assertThat(result.newSnapshotId).isEqualTo(1L);
        assertThat(result.firstAssignedRowId).isEqualTo(3L);
        assertThat(result.nextRowId).isEqualTo(3L);
        assertThat(result.fileCount).isEqualTo(0L);
        assertThat(result.rowCount).isEqualTo(0L);
        assertThat(result.indexFileCount).isEqualTo(0L);
        assertThat(table.snapshotManager().latestSnapshot().id()).isEqualTo(1L);
    }

    @Test
    public void testReassignGlobalIndexRowRanges() throws Exception {
        FileStoreTable table = createTableWithInterleavedPartitions();
        createBTreeIndex(table);

        assertThat(table.snapshotManager().latestSnapshot().nextRowId()).isEqualTo(5L);

        DataEvolutionRowIdReassigner.Result result =
                new DataEvolutionRowIdReassigner(table).reassign("test-reassign-row-id-index");

        assertThat(result.firstAssignedRowId).isEqualTo(5L);
        assertThat(result.nextRowId).isEqualTo(10L);
        assertThat(result.indexFileCount).isEqualTo(5L);

        List<Range> indexRanges = globalIndexRanges(table);
        assertThat(indexRanges)
                .containsExactly(
                        new Range(5, 5),
                        new Range(6, 6),
                        new Range(7, 7),
                        new Range(8, 8),
                        new Range(9, 9));

        Predicate predicate =
                new PredicateBuilder(table.rowType()).equal(table.rowType().getFieldIndex("id"), 4);
        assertThat(readPayloads(table, predicate)).containsExactly("v4");

        DataEvolutionRowIdReassigner.Result secondResult =
                new DataEvolutionRowIdReassigner(table).reassign("test-reassign-row-id-index-2");

        assertThat(secondResult.reassigned).isFalse();
        assertThat(secondResult.previousSnapshotId).isEqualTo(7L);
        assertThat(secondResult.newSnapshotId).isEqualTo(7L);
        assertThat(secondResult.firstAssignedRowId).isEqualTo(10L);
        assertThat(secondResult.nextRowId).isEqualTo(10L);
        assertThat(secondResult.fileCount).isEqualTo(0L);
        assertThat(secondResult.rowCount).isEqualTo(0L);
        assertThat(secondResult.indexFileCount).isEqualTo(0L);
        assertThat(globalIndexRanges(table))
                .containsExactly(
                        new Range(5, 5),
                        new Range(6, 6),
                        new Range(7, 7),
                        new Range(8, 8),
                        new Range(9, 9));
        assertThat(readPayloads(table, predicate)).containsExactly("v4");
    }

    @Test
    public void testReassignKeepsConcurrentDisjointGlobalIndexRange() throws Exception {
        FileStoreTable table = createTableWithInterleavedPartitions();
        createBTreeIndex(table);
        Snapshot before = table.snapshotManager().latestSnapshot();

        AtomicBoolean indexed = new AtomicBoolean();
        DataEvolutionRowIdReassigner.Result result =
                new DataEvolutionRowIdReassigner(
                                table,
                                null,
                                () -> {
                                    if (indexed.compareAndSet(false, true)) {
                                        try {
                                            writeOneRow(table, "a", 100);
                                            appendGlobalIndexRange(table, "pt=a/", new Range(5, 5));
                                        } catch (Exception e) {
                                            throw new RuntimeException(e);
                                        }
                                    }
                                })
                        .reassign("test-reassign-disjoint-global-index");

        assertThat(indexed).isTrue();
        assertThat(result.previousSnapshotId).isEqualTo(before.id() + 1);
        assertThat(result.newSnapshotId).isEqualTo(before.id() + 2);
        assertThat(result.firstAssignedRowId).isEqualTo(6L);
        assertThat(result.nextRowId).isEqualTo(11L);
        assertThat(result.indexFileCount).isEqualTo(5L);
        assertThat(globalIndexRanges(table))
                .containsExactly(
                        new Range(5, 5),
                        new Range(6, 6),
                        new Range(7, 7),
                        new Range(8, 8),
                        new Range(9, 9),
                        new Range(10, 10));
        assertThat(readTableRows(table))
                .containsExactly("0|a|v0", "100|a|v100", "1|b|v1", "2|a|v2", "3|b|v3", "4|a|v4");
    }

    @Test
    public void testDropUnsafeGlobalIndexEntryWhenRangeCannotBeRewrittenByMetadataOnly()
            throws Exception {
        FileStoreTable table = createTableWithInterleavedPartitions();
        createBTreeIndex(table);
        replaceGlobalIndexRangesWithPartitionSpanningRanges(table);
        Snapshot before = table.snapshotManager().latestSnapshot();

        assertThat(globalIndexRanges(table))
                .containsExactly(
                        new Range(0, 4),
                        new Range(0, 4),
                        new Range(0, 4),
                        new Range(1, 3),
                        new Range(1, 3));

        DataEvolutionRowIdReassigner.Result result =
                new DataEvolutionRowIdReassigner(table)
                        .reassign("test-drop-unsafe-sparse-index-range-entry");

        assertThat(result.reassigned).isTrue();
        assertThat(result.skipReason).isNull();
        assertThat(result.previousSnapshotId).isEqualTo(before.id());
        assertThat(result.newSnapshotId).isEqualTo(before.id() + 1);
        assertThat(result.firstAssignedRowId).isEqualTo(5L);
        assertThat(result.nextRowId).isEqualTo(10L);
        assertThat(result.fileCount).isEqualTo(5L);
        assertThat(result.rowCount).isEqualTo(5L);
        assertThat(result.indexFileCount).isEqualTo(0L);

        Map<String, List<Long>> rowIdsByPartition = rowIdsByPartition(table);
        assertThat(rowIdsByPartition).hasSize(2);
        assertThat(rowIdsByPartition).containsEntry("pt=a/", Arrays.asList(5L, 6L, 7L));
        assertThat(rowIdsByPartition).containsEntry("pt=b/", Arrays.asList(8L, 9L));
        assertThat(globalIndexRanges(table)).isEmpty();

        Predicate predicate =
                new PredicateBuilder(table.rowType()).equal(table.rowType().getFieldIndex("id"), 4);
        assertThat(readPayloads(table, predicate)).containsExactly("v4");
    }

    @Test
    public void testFullReassignPreservesDataInTableWithLargeRowIdGaps() throws Exception {
        FileStoreTable table = createDataIntegrityTable();
        long scrambledNextRowId = scrambleCurrentRowIds(table);
        createBTreeIndex(table);

        Snapshot before = table.snapshotManager().latestSnapshot();
        List<String> expectedRows = readTableRows(table);
        Map<String, List<Object>> expectedFileMetadata = fileMetadataWithoutRowId(table);
        Map<String, SimpleStats> expectedValueStats = valueStatsByFile(table);
        Map<String, Long> rowCountsByPartition = rowCountsByPartition(table);
        Map<String, List<Long>> scrambledRowIds = rowIdsByPartition(table);
        int expectedFileCount = expectedFileMetadata.size();
        int expectedIndexFileCount = globalIndexRanges(table).size();

        assertThat(before.nextRowId()).isEqualTo(scrambledNextRowId);
        assertThat(before.totalRecordCount()).isEqualTo(expectedRows.size());
        assertThat(expectedFileCount)
                .isEqualTo(DATA_INTEGRITY_PARTITIONS.length * DATA_INTEGRITY_FILES_PER_PARTITION);
        assertThat(expectedIndexFileCount).isGreaterThan(0);
        assertPartitionsHaveLargeRowIdGaps(scrambledRowIds);
        assertTableDataQueryable(table, expectedRows);

        DataEvolutionRowIdReassigner.Result result =
                new DataEvolutionRowIdReassigner(table)
                        .reassign("test-full-reassign-data-integrity");

        assertThat(result.reassigned).isTrue();
        assertThat(result.previousSnapshotId).isEqualTo(before.id());
        assertThat(result.newSnapshotId).isEqualTo(before.id() + 1);
        assertThat(result.firstAssignedRowId).isEqualTo(scrambledNextRowId);
        assertThat(result.fileCount).isEqualTo(expectedFileCount);
        assertThat(result.rowCount).isEqualTo(expectedRows.size());
        assertThat(result.indexFileCount).isEqualTo(expectedIndexFileCount);

        long expectedNextRowId =
                assertContiguousRowIds(
                        expandedRowIdsByPartition(table),
                        rowCountsByPartition,
                        Arrays.asList(DATA_INTEGRITY_PARTITIONS),
                        scrambledNextRowId);
        assertThat(result.nextRowId).isEqualTo(expectedNextRowId);
        assertThat(table.snapshotManager().latestSnapshot().nextRowId())
                .isEqualTo(expectedNextRowId);
        assertThat(table.snapshotManager().latestSnapshot().totalRecordCount())
                .isEqualTo(before.totalRecordCount());
        assertThat(fileMetadataWithoutRowId(table)).isEqualTo(expectedFileMetadata);
        assertThat(valueStatsByFile(table)).isEqualTo(expectedValueStats);
        List<Range> reassignedIndexRanges = globalIndexRanges(table);
        assertThat(reassignedIndexRanges).hasSize(expectedIndexFileCount);
        for (Range range : reassignedIndexRanges) {
            assertThat(range.from).isGreaterThanOrEqualTo(scrambledNextRowId);
            assertThat(range.to).isLessThan(expectedNextRowId);
        }
        assertTableDataQueryable(table, expectedRows);
    }

    @Test
    public void testPartitionReassignPreservesDataAndOtherPartitionRowIds() throws Exception {
        FileStoreTable table = createDataIntegrityTable();
        long scrambledNextRowId = scrambleCurrentRowIds(table);
        createBTreeIndex(table);

        Snapshot before = table.snapshotManager().latestSnapshot();
        List<String> expectedRows = readTableRows(table);
        Map<String, List<Object>> expectedFileMetadata = fileMetadataWithoutRowId(table);
        Map<String, SimpleStats> expectedValueStats = valueStatsByFile(table);
        Map<String, Long> rowCountsByPartition = rowCountsByPartition(table);
        Map<String, List<Long>> beforeFirstRowIds = rowIdsByPartition(table);
        Map<String, List<Long>> beforeExpandedRowIds = expandedRowIdsByPartition(table);
        Map<String, List<Range>> beforeIndexRanges = globalIndexRangesByPartition(table);
        String reassignedPartition = "c";
        String reassignedPartitionPath = "pt=" + reassignedPartition + "/";

        assertThat(before.nextRowId()).isEqualTo(scrambledNextRowId);
        assertThat(before.totalRecordCount()).isEqualTo(expectedRows.size());
        assertPartitionsHaveLargeRowIdGaps(beforeFirstRowIds);
        assertTableDataQueryable(table, expectedRows);

        DataEvolutionRowIdReassigner.Result result =
                new DataEvolutionRowIdReassigner(
                                table, partitionPredicate(table, reassignedPartition))
                        .reassign("test-partition-reassign-data-integrity");

        long reassignedRowCount = rowCountsByPartition.get(reassignedPartitionPath);
        long expectedNextRowId = scrambledNextRowId + reassignedRowCount;
        assertThat(result.reassigned).isTrue();
        assertThat(result.previousSnapshotId).isEqualTo(before.id());
        assertThat(result.newSnapshotId).isEqualTo(before.id() + 1);
        assertThat(result.firstAssignedRowId).isEqualTo(scrambledNextRowId);
        assertThat(result.nextRowId).isEqualTo(expectedNextRowId);
        assertThat(result.fileCount).isEqualTo(DATA_INTEGRITY_FILES_PER_PARTITION);
        assertThat(result.rowCount).isEqualTo(reassignedRowCount);
        assertThat(result.indexFileCount)
                .isEqualTo(beforeIndexRanges.get(reassignedPartitionPath).size());

        Map<String, List<Long>> afterFirstRowIds = rowIdsByPartition(table);
        Map<String, List<Long>> afterExpandedRowIds = expandedRowIdsByPartition(table);
        Map<String, List<Range>> afterIndexRanges = globalIndexRangesByPartition(table);
        assertContiguousRowIds(
                afterExpandedRowIds,
                rowCountsByPartition,
                Collections.singletonList(reassignedPartition),
                scrambledNextRowId);
        assertThat(afterFirstRowIds.get(reassignedPartitionPath))
                .isNotEqualTo(beforeFirstRowIds.get(reassignedPartitionPath));
        for (String partition : DATA_INTEGRITY_PARTITIONS) {
            String partitionPath = "pt=" + partition + "/";
            if (!partition.equals(reassignedPartition)) {
                assertThat(afterFirstRowIds.get(partitionPath))
                        .isEqualTo(beforeFirstRowIds.get(partitionPath));
                assertThat(afterExpandedRowIds.get(partitionPath))
                        .isEqualTo(beforeExpandedRowIds.get(partitionPath));
                assertThat(afterIndexRanges.get(partitionPath))
                        .isEqualTo(beforeIndexRanges.get(partitionPath));
            }
        }
        assertThat(afterIndexRanges.get(reassignedPartitionPath))
                .hasSameSizeAs(beforeIndexRanges.get(reassignedPartitionPath));
        for (Range range : afterIndexRanges.get(reassignedPartitionPath)) {
            assertThat(range.from).isGreaterThanOrEqualTo(scrambledNextRowId);
            assertThat(range.to).isLessThan(expectedNextRowId);
        }

        assertThat(table.snapshotManager().latestSnapshot().nextRowId())
                .isEqualTo(expectedNextRowId);
        assertThat(table.snapshotManager().latestSnapshot().totalRecordCount())
                .isEqualTo(before.totalRecordCount());
        assertThat(fileMetadataWithoutRowId(table)).isEqualTo(expectedFileMetadata);
        assertThat(valueStatsByFile(table)).isEqualTo(expectedValueStats);
        assertTableDataQueryable(table, expectedRows);
    }

    @Test
    public void testReassignManyOutOfOrderPartitionEntries() throws Exception {
        verifyReassignOutOfOrderPartitionEntries(LARGE_ENTRY_COUNT, LARGE_MANIFEST_FILE_COUNT);
    }

    @Disabled("Writes one million manifest entries; run manually for large metadata stress tests.")
    @Test
    public void testReassignOneMillionOutOfOrderPartitionEntries() throws Exception {
        verifyReassignOutOfOrderPartitionEntries(
                ONE_MILLION_ENTRY_COUNT, ONE_MILLION_MANIFEST_FILE_COUNT);
    }

    private FileStoreTable createTableWithInterleavedPartitions() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        writeOneRow(table, "a", 0);
        writeOneRow(table, "b", 1);
        writeOneRow(table, "a", 2);
        writeOneRow(table, "b", 3);
        writeOneRow(table, "a", 4);
        return table;
    }

    private FileStoreTable createTableWithPartiallyOverlappedPartitions() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        writeRows(table, "c", 0, 1);
        writeOneRow(table, "a", 2);
        writeOneRow(table, "b", 3);
        writeOneRow(table, "a", 4);
        return table;
    }

    private FileStoreTable createDataIntegrityTable() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        int nextId = 0;
        for (int file = 0; file < DATA_INTEGRITY_FILES_PER_PARTITION; file++) {
            for (int partition = 0; partition < DATA_INTEGRITY_PARTITIONS.length; partition++) {
                int rowCount = 3 + (file + partition) % 5;
                int[] ids = new int[rowCount];
                for (int row = 0; row < rowCount; row++) {
                    ids[row] = nextId++;
                }
                writeRows(table, DATA_INTEGRITY_PARTITIONS[partition], ids);
            }
        }
        return table;
    }

    private long scrambleCurrentRowIds(FileStoreTable table) throws Exception {
        Snapshot latest = table.snapshotManager().latestSnapshot();
        assertThat(latest.indexManifest()).isNull();
        List<ManifestEntry> currentEntries = currentEntries(table);
        List<Long> rowIdSlots = new ArrayList<>(currentEntries.size());
        for (int i = 0; i < currentEntries.size(); i++) {
            rowIdSlots.add((i + 1L) * SCRAMBLED_ROW_ID_GAP);
        }
        Collections.shuffle(rowIdSlots, new Random(SCRAMBLED_ROW_ID_SEED));

        Map<FileEntry.Identifier, Long> assignments = new HashMap<>();
        for (int i = 0; i < currentEntries.size(); i++) {
            assignments.put(currentEntries.get(i).identifier(), rowIdSlots.get(i));
        }

        ManifestFile manifestFile = table.store().manifestFileFactory().create();
        ManifestList manifestList = table.store().manifestListFactory().create();
        List<ManifestFileMeta> rewrittenManifestMetas = new ArrayList<>();
        int rewrittenEntryCount = 0;
        for (ManifestFileMeta manifestMeta : manifestList.readDataManifests(latest)) {
            List<ManifestEntry> entries =
                    manifestFile.read(manifestMeta.fileName(), manifestMeta.fileSize());
            boolean rewritten = false;
            for (int i = 0; i < entries.size(); i++) {
                ManifestEntry entry = entries.get(i);
                Long firstRowId =
                        entry.kind() == FileKind.ADD ? assignments.get(entry.identifier()) : null;
                if (firstRowId != null) {
                    entries.set(i, entry.assignFirstRowId(firstRowId));
                    rewritten = true;
                    rewrittenEntryCount++;
                }
            }
            if (rewritten) {
                rewrittenManifestMetas.addAll(manifestFile.write(entries));
            } else {
                rewrittenManifestMetas.add(manifestMeta);
            }
        }
        assertThat(rewrittenEntryCount).isEqualTo(currentEntries.size());

        Pair<String, Long> baseManifestList = manifestList.write(rewrittenManifestMetas);
        Pair<String, Long> deltaManifestList = manifestList.write(Collections.emptyList());
        long scrambledNextRowId = (currentEntries.size() + 10L) * SCRAMBLED_ROW_ID_GAP;
        try (FileStoreCommitImpl commit =
                (FileStoreCommitImpl) table.store().newCommit("test-scramble-row-ids", table)) {
            assertThat(
                            commit.replaceManifestList(
                                    latest,
                                    latest.totalRecordCount(),
                                    baseManifestList,
                                    deltaManifestList,
                                    null,
                                    scrambledNextRowId))
                    .isTrue();
        }

        Map<FileEntry.Identifier, Long> committedAssignments = new HashMap<>();
        for (ManifestEntry entry : currentEntries(table)) {
            committedAssignments.put(entry.identifier(), entry.file().nonNullFirstRowId());
        }
        assertThat(committedAssignments).isEqualTo(assignments);
        return scrambledNextRowId;
    }

    private void verifyReassignOutOfOrderPartitionEntries(int entryCount, int manifestFileCount)
            throws Exception {
        assertThat(entryCount % 2).isEqualTo(0);
        createTableDefault();
        FileStoreTable table = getTableDefault();
        writeOneRow(table, "seed", -1);

        long oldNextRowId = writeOutOfOrderManifestSnapshot(table, entryCount, manifestFileCount);
        Snapshot before = table.snapshotManager().latestSnapshot();
        List<String> beforeManifestFiles = dataManifestFileNames(table);
        assertThat(beforeManifestFiles).hasSize(manifestFileCount);
        assertThat(before.nextRowId()).isEqualTo(oldNextRowId);
        assertThat(before.totalRecordCount()).isEqualTo(logicalRowCount(entryCount));
        ExpectedLargeManifestAssignment expectedAssignment =
                expectedLargeManifestAssignment(table, before, entryCount);

        DataEvolutionRowIdReassigner.Result result =
                new DataEvolutionRowIdReassigner(table)
                        .reassign("test-reassign-large-out-of-order-row-id");

        assertThat(result.reassigned).isTrue();
        assertThat(result.previousSnapshotId).isEqualTo(before.id());
        assertThat(result.newSnapshotId).isEqualTo(before.id() + 1);
        assertThat(result.firstAssignedRowId).isEqualTo(oldNextRowId);
        assertThat(result.nextRowId)
                .isEqualTo(oldNextRowId + expectedAssignment.reassignedRowCount);
        assertThat(result.fileCount).isEqualTo(expectedAssignment.reassignedEntryCount);
        assertThat(result.rowCount).isEqualTo(expectedAssignment.reassignedRowCount);
        assertThat(result.indexFileCount).isEqualTo(0L);

        assertReassignedOutOfOrderPartitionEntries(
                table, entryCount, oldNextRowId, expectedAssignment, beforeManifestFiles);
    }

    private long writeOutOfOrderManifestSnapshot(
            FileStoreTable table, int entryCount, int manifestFileCount) throws Exception {
        Snapshot latest = table.snapshotManager().latestSnapshot();
        ManifestFile manifestFile = table.store().manifestFileFactory().create();
        ManifestList manifestList = table.store().manifestListFactory().create();
        InternalRowSerializer partitionSerializer =
                new InternalRowSerializer(table.schema().logicalPartitionType());

        int partitionCount = largePartitionCount(entryCount);
        assertThat(entryCount).isGreaterThanOrEqualTo(manifestFileCount);
        List<ManifestFileMeta> manifestMetas = new ArrayList<>();
        Random random = new Random(RANDOM_PARTITION_SEED);
        int entryStart = 0;
        for (int manifestIndex = 0; manifestIndex < manifestFileCount; manifestIndex++) {
            int currentEntryCount =
                    entryCount / manifestFileCount
                            + (manifestIndex < entryCount % manifestFileCount ? 1 : 0);
            List<ManifestEntry> entries = new ArrayList<>(currentEntryCount);
            for (int entryOffset = 0; entryOffset < currentEntryCount; entryOffset++) {
                int entryIndex = entryStart + entryOffset;
                int partitionId = random.nextInt(partitionCount);
                entries.add(
                        largeManifestEntry(
                                partitionSerializer,
                                partitionId,
                                entryIndex,
                                logicalRowCount(entryIndex)));
            }
            manifestMetas.addAll(manifestFile.write(entries));
            entryStart += currentEntryCount;
        }
        assertThat(entryStart).isEqualTo(entryCount);
        assertThat(manifestMetas).hasSize(manifestFileCount);

        Pair<String, Long> baseManifestList = manifestList.write(manifestMetas);
        Pair<String, Long> deltaManifestList = manifestList.write(Collections.emptyList());
        long oldNextRowId = logicalRowCount(entryCount) + 1L;
        try (FileStoreCommitImpl commit =
                (FileStoreCommitImpl)
                        table.store().newCommit("test-large-out-of-order-source", table)) {
            assertThat(
                            commit.replaceManifestList(
                                    latest,
                                    logicalRowCount(entryCount),
                                    baseManifestList,
                                    deltaManifestList,
                                    latest.indexManifest(),
                                    oldNextRowId))
                    .isTrue();
        }
        return oldNextRowId;
    }

    private ManifestEntry largeManifestEntry(
            InternalRowSerializer partitionSerializer,
            int partitionId,
            int entryOrdinal,
            long firstRowId) {
        BinaryRow partition =
                partitionSerializer
                        .toBinaryRow(
                                GenericRow.of(BinaryString.fromString(largePartition(partitionId))))
                        .copy();
        long sequenceNumber = sequenceNumber(partitionId);
        DataFileMeta file =
                DataFileMeta.forAppend(
                        largeFileName(partitionId, entryOrdinal),
                        1L,
                        LARGE_ENTRY_ROW_COUNT,
                        SimpleStats.EMPTY_STATS,
                        sequenceNumber,
                        sequenceNumber,
                        0L,
                        Collections.emptyList(),
                        null,
                        FileSource.APPEND,
                        null,
                        null,
                        firstRowId,
                        null);
        return ManifestEntry.create(FileKind.ADD, partition, 0, 1, file);
    }

    private void assertReassignedOutOfOrderPartitionEntries(
            FileStoreTable table,
            int entryCount,
            long firstAssignedRowId,
            ExpectedLargeManifestAssignment expectedAssignment,
            List<String> beforeManifestFiles) {
        List<String> afterManifestFiles = dataManifestFileNames(table);
        assertThat(afterManifestFiles).hasSize(beforeManifestFiles.size());
        assertThat(afterManifestFiles)
                .doesNotContainAnyElementsOf(expectedAssignment.rewrittenManifestFiles);
        List<String> reusedManifestFiles = new ArrayList<>(beforeManifestFiles);
        reusedManifestFiles.removeAll(expectedAssignment.rewrittenManifestFiles);
        assertThat(afterManifestFiles).containsAll(reusedManifestFiles);

        List<ManifestEntry> entries =
                table.store()
                        .newScan()
                        .withSnapshot(table.snapshotManager().latestSnapshot())
                        .plan()
                        .files();
        assertThat(entries).hasSize(entryCount);

        int partitionCount = largePartitionCount(entryCount);
        int[] entriesByPartition = new int[partitionCount];
        long[] minRowIdByPartition = new long[partitionCount];
        long[] maxRowIdByPartition = new long[partitionCount];
        Arrays.fill(minRowIdByPartition, Long.MAX_VALUE);
        Arrays.fill(maxRowIdByPartition, Long.MIN_VALUE);
        List<Range> ranges = new ArrayList<>(entryCount);
        long reassignedEntryCount = 0;
        long reassignedRowCount = 0;
        for (ManifestEntry entry : entries) {
            Range range = entry.file().nonNullRowIdRange();
            ranges.add(range);
            assertThat(range.count()).isEqualTo(LARGE_ENTRY_ROW_COUNT);
            if (range.from >= firstAssignedRowId) {
                reassignedEntryCount++;
                reassignedRowCount += range.count();
            }

            int partitionId = partitionIdFromLargeFileName(entry.file().fileName());
            entriesByPartition[partitionId]++;
            minRowIdByPartition[partitionId] =
                    Math.min(minRowIdByPartition[partitionId], range.from);
            maxRowIdByPartition[partitionId] = Math.max(maxRowIdByPartition[partitionId], range.to);
            long sequenceNumber = sequenceNumber(partitionId);
            assertThat(entry.file().minSequenceNumber()).isEqualTo(sequenceNumber);
            assertThat(entry.file().maxSequenceNumber()).isEqualTo(sequenceNumber);
        }

        Collections.sort(
                ranges,
                (left, right) -> {
                    int result = Long.compare(left.from, right.from);
                    return result == 0 ? Long.compare(left.to, right.to) : result;
                });
        long previousEnd = Long.MIN_VALUE;
        for (Range range : ranges) {
            assertThat(range.from).isGreaterThan(previousEnd);
            previousEnd = range.to;
        }

        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            assertThat(entriesByPartition[partitionId])
                    .isEqualTo(expectedAssignment.entriesByPartition[partitionId]);
            if (entriesByPartition[partitionId] == 0) {
                continue;
            }

            if (expectedAssignment.partitionsToReassign[partitionId]) {
                assertThat(minRowIdByPartition[partitionId])
                        .isGreaterThanOrEqualTo(firstAssignedRowId);
                assertThat(maxRowIdByPartition[partitionId])
                        .isEqualTo(
                                minRowIdByPartition[partitionId]
                                        + logicalRowCount(entriesByPartition[partitionId])
                                        - 1);
            } else {
                assertThat(minRowIdByPartition[partitionId])
                        .isEqualTo(expectedAssignment.minRowIdByPartition[partitionId]);
                assertThat(maxRowIdByPartition[partitionId])
                        .isEqualTo(expectedAssignment.maxRowIdByPartition[partitionId]);
            }
        }
        assertThat(reassignedEntryCount).isEqualTo(expectedAssignment.reassignedEntryCount);
        assertThat(reassignedRowCount).isEqualTo(expectedAssignment.reassignedRowCount);
        assertThat(table.snapshotManager().latestSnapshot().nextRowId())
                .isEqualTo(firstAssignedRowId + reassignedRowCount);
    }

    private long logicalRowCount(long entryCount) {
        return entryCount * LARGE_ENTRY_ROW_COUNT;
    }

    private int largePartitionCount(int entryCount) {
        return entryCount / 2;
    }

    private ExpectedLargeManifestAssignment expectedLargeManifestAssignment(
            FileStoreTable table, Snapshot snapshot, int entryCount) {
        int partitionCount = largePartitionCount(entryCount);
        int[] entriesByPartition = new int[partitionCount];
        long[] minRowIdByPartition = new long[partitionCount];
        long[] maxRowIdByPartition = new long[partitionCount];
        long logicalRowCount = logicalRowCount(entryCount);
        assertThat(logicalRowCount).isLessThanOrEqualTo((long) Integer.MAX_VALUE);
        boolean[] rowIdExists = new boolean[(int) logicalRowCount];
        Arrays.fill(minRowIdByPartition, Long.MAX_VALUE);
        Arrays.fill(maxRowIdByPartition, Long.MIN_VALUE);

        ManifestFile manifestFile = table.store().manifestFileFactory().create();
        ManifestList manifestList = table.store().manifestListFactory().create();
        List<ManifestFileMeta> manifestMetas = manifestList.readDataManifests(snapshot);
        long existingRowCount = 0;
        for (ManifestFileMeta manifestMeta : manifestMetas) {
            for (ManifestEntry entry :
                    manifestFile.read(manifestMeta.fileName(), manifestMeta.fileSize())) {
                if (entry.kind() != FileKind.ADD) {
                    continue;
                }
                Range range = entry.file().nonNullRowIdRange();
                assertThat(range.count()).isEqualTo(LARGE_ENTRY_ROW_COUNT);
                assertThat(range.from).isBetween(0L, logicalRowCount - 1);
                assertThat(range.to).isBetween(0L, logicalRowCount - 1);
                for (long rowId = range.from; rowId <= range.to; rowId++) {
                    assertThat(rowIdExists[(int) rowId]).isFalse();
                    rowIdExists[(int) rowId] = true;
                    existingRowCount++;
                }
                int partitionId = partitionIdFromLargeFileName(entry.file().fileName());
                entriesByPartition[partitionId]++;
                minRowIdByPartition[partitionId] =
                        Math.min(minRowIdByPartition[partitionId], range.from);
                maxRowIdByPartition[partitionId] =
                        Math.max(maxRowIdByPartition[partitionId], range.to);
            }
        }
        assertThat(existingRowCount).isEqualTo(logicalRowCount);

        boolean[] partitionsToReassign = new boolean[partitionCount];
        long reassignedEntryCount = 0;
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            int partitionEntryCount = entriesByPartition[partitionId];
            if (partitionEntryCount == 0) {
                continue;
            }
            if (maxRowIdByPartition[partitionId] - minRowIdByPartition[partitionId] + 1
                    != logicalRowCount(partitionEntryCount)) {
                partitionsToReassign[partitionId] = true;
                reassignedEntryCount += partitionEntryCount;
            }
        }

        Set<String> rewrittenManifestFiles = new HashSet<>();
        for (ManifestFileMeta manifestMeta : manifestMetas) {
            for (ManifestEntry entry :
                    manifestFile.read(manifestMeta.fileName(), manifestMeta.fileSize())) {
                if (entry.kind() != FileKind.ADD) {
                    continue;
                }
                int partitionId = partitionIdFromLargeFileName(entry.file().fileName());
                if (partitionsToReassign[partitionId]) {
                    rewrittenManifestFiles.add(manifestMeta.fileName());
                    break;
                }
            }
        }
        assertThat(reassignedEntryCount).isGreaterThan(0L);
        return new ExpectedLargeManifestAssignment(
                entriesByPartition,
                minRowIdByPartition,
                maxRowIdByPartition,
                partitionsToReassign,
                reassignedEntryCount,
                logicalRowCount(reassignedEntryCount),
                rewrittenManifestFiles);
    }

    private String largePartition(int partitionId) {
        String partition = Integer.toString(partitionId);
        StringBuilder builder = new StringBuilder("p");
        for (int i = partition.length(); i < LARGE_PARTITION_ID_WIDTH; i++) {
            builder.append('0');
        }
        return builder.append(partition).toString();
    }

    private String largeFileName(int partitionId, int entryOrdinal) {
        return LARGE_FILE_PREFIX + partitionId + "-" + entryOrdinal + LARGE_FILE_SUFFIX;
    }

    private int partitionIdFromLargeFileName(String fileName) {
        String withoutPrefixAndSuffix =
                fileName.substring(
                        LARGE_FILE_PREFIX.length(), fileName.length() - LARGE_FILE_SUFFIX.length());
        return Integer.parseInt(
                withoutPrefixAndSuffix.substring(0, withoutPrefixAndSuffix.indexOf('-')));
    }

    private long sequenceNumber(int partitionId) {
        return 10_000_000L + partitionId;
    }

    private void writeOneRow(FileStoreTable table, String partition, int id) throws Exception {
        writeRows(table, partition, id);
    }

    private void writeRows(FileStoreTable table, String partition, int... ids) throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            for (int id : ids) {
                write.write(row(partition, id));
            }
            commit.commit(write.prepareCommit());
        }
    }

    private void writePartialPayload(
            FileStoreTable table, String partition, long firstRowId, String... payloads)
            throws Exception {
        RowType writeType = table.rowType().project(Arrays.asList("pt", "payload"));
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite().withWriteType(writeType);
                BatchTableCommit commit = builder.newCommit()) {
            for (String payload : payloads) {
                write.write(
                        GenericRow.of(
                                BinaryString.fromString(partition),
                                BinaryString.fromString(payload)));
            }
            List<CommitMessage> messages = write.prepareCommit();
            assignFirstRowId(messages, firstRowId);
            commit.commit(messages);
        }
    }

    private FileStoreTable withManifestMergeOnNextAppend(FileStoreTable table) {
        Map<String, String> mergeOptions = new HashMap<>();
        mergeOptions.put(CoreOptions.MANIFEST_TARGET_FILE_SIZE.key(), "1GB");
        mergeOptions.put(CoreOptions.MANIFEST_MERGE_MIN_COUNT.key(), "2");
        mergeOptions.put(CoreOptions.MANIFEST_FULL_COMPACTION_FILE_SIZE.key(), "1GB");
        mergeOptions.put(CoreOptions.MANIFEST_SORT_ENABLED.key(), "false");
        return table.copy(mergeOptions);
    }

    private void assignFirstRowId(List<CommitMessage> messages, long firstRowId) {
        for (CommitMessage message : messages) {
            CommitMessageImpl commitMessage = (CommitMessageImpl) message;
            List<DataFileMeta> files =
                    new ArrayList<>(commitMessage.newFilesIncrement().newFiles());
            commitMessage.newFilesIncrement().newFiles().clear();
            for (DataFileMeta file : files) {
                commitMessage.newFilesIncrement().newFiles().add(file.assignFirstRowId(firstRowId));
            }
        }
    }

    private void writeRowsInPartitions(
            FileStoreTable table,
            String firstPartition,
            int firstId,
            String secondPartition,
            int secondId)
            throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            write.write(row(firstPartition, firstId));
            write.write(row(secondPartition, secondId));
            commit.commit(write.prepareCommit());
        }
    }

    private GenericRow row(String partition, int id) {
        return GenericRow.of(
                partition == null ? null : BinaryString.fromString(partition),
                id,
                BinaryString.fromString("v" + id));
    }

    private void overwriteOneRow(FileStoreTable table, String partition, int id) throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder().withOverwrite();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            write.write(
                    GenericRow.of(
                            BinaryString.fromString(partition),
                            id,
                            BinaryString.fromString("v" + id)));
            commit.commit(write.prepareCommit());
        }
    }

    private void compactManifests(FileStoreTable table) throws Exception {
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.compactManifests();
        }
    }

    private FileEntry.Identifier compactOneFile(FileStoreTable table, String partitionPath)
            throws Exception {
        ManifestEntry compactBefore =
                currentEntries(table).stream()
                        .filter(
                                entry ->
                                        table.store()
                                                .pathFactory()
                                                .getPartitionString(entry.partition())
                                                .equals(partitionPath))
                        .findFirst()
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Cannot find file in partition " + partitionPath));
        DataEvolutionCompactTask task =
                new DataEvolutionCompactTask(
                        compactBefore.partition(),
                        Collections.singletonList(compactBefore.file()),
                        false);
        CommitMessage message = task.doCompact(table, "test-compact-before-reassign");
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(Collections.singletonList(message));
        }
        return compactBefore.identifier();
    }

    private long assertAddDeleteEntriesConsistent(
            FileStoreTable table, FileEntry.Identifier identifier) {
        ManifestFile manifestFile = table.store().manifestFileFactory().create();
        ManifestList manifestList = table.store().manifestListFactory().create();
        Snapshot latest = table.snapshotManager().latestSnapshot();
        List<ManifestEntry> matchingEntries = new ArrayList<>();
        Set<String> matchingManifestFiles = new HashSet<>();
        for (ManifestFileMeta manifestMeta : manifestList.readDataManifests(latest)) {
            for (ManifestEntry entry :
                    manifestFile.read(manifestMeta.fileName(), manifestMeta.fileSize())) {
                if (entry.identifier().equals(identifier)) {
                    matchingEntries.add(entry);
                    matchingManifestFiles.add(manifestMeta.fileName());
                }
            }
        }

        assertThat(matchingEntries).hasSize(2);
        assertThat(matchingManifestFiles).hasSize(2);
        ManifestEntry add =
                matchingEntries.stream()
                        .filter(entry -> entry.kind() == FileKind.ADD)
                        .findFirst()
                        .orElseThrow(
                                () -> new AssertionError("Missing ADD entry for " + identifier));
        ManifestEntry delete =
                matchingEntries.stream()
                        .filter(entry -> entry.kind() == FileKind.DELETE)
                        .findFirst()
                        .orElseThrow(
                                () -> new AssertionError("Missing DELETE entry for " + identifier));
        assertThat(delete.partition()).isEqualTo(add.partition());
        assertThat(delete.bucket()).isEqualTo(add.bucket());
        assertThat(delete.totalBuckets()).isEqualTo(add.totalBuckets());
        assertThat(delete.file()).isEqualTo(add.file());
        return add.file().nonNullFirstRowId();
    }

    private void updateStatistics(FileStoreTable table) throws Exception {
        Snapshot latest = table.snapshotManager().latestSnapshot();
        Statistics statistics =
                new Statistics(
                        latest.id(),
                        latest.schemaId(),
                        latest.totalRecordCount(),
                        latest.totalRecordCount() * 100L);
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.updateStatistics(statistics);
        }
    }

    private void writeUnpartitionedRows(FileStoreTable table, int... ids) throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            for (int id : ids) {
                write.write(GenericRow.of(id, BinaryString.fromString("v" + id)));
            }
            commit.commit(write.prepareCommit());
        }
    }

    private PartitionPredicate partitionPredicate(FileStoreTable table, String partition) {
        return PartitionPredicate.fromMaps(
                table.schema().logicalPartitionType(),
                Collections.singletonList(Collections.singletonMap("pt", partition)),
                table.coreOptions().partitionDefaultName());
    }

    private List<ManifestEntry> currentEntries(FileStoreTable table) {
        return table.store()
                .newScan()
                .withSnapshot(table.snapshotManager().latestSnapshot())
                .plan()
                .files();
    }

    private List<ManifestEntry> currentEntriesAtRange(
            FileStoreTable table, String partitionPath, long firstRowId, long lastRowId) {
        List<ManifestEntry> result = new ArrayList<>();
        for (ManifestEntry entry : currentEntries(table)) {
            String partition = table.store().pathFactory().getPartitionString(entry.partition());
            Range range = entry.file().nonNullRowIdRange();
            if (partitionPath.equals(partition)
                    && range.from == firstRowId
                    && range.to == lastRowId) {
                result.add(entry);
            }
        }
        return result;
    }

    private Map<String, Long> rowCountsByPartition(FileStoreTable table) {
        Map<String, Long> result = new LinkedHashMap<>();
        for (ManifestEntry entry : currentEntries(table)) {
            String partition = table.store().pathFactory().getPartitionString(entry.partition());
            result.merge(partition, entry.file().rowCount(), Long::sum);
        }
        return result;
    }

    private Map<String, List<Object>> fileMetadataWithoutRowId(FileStoreTable table) {
        Map<String, List<Object>> result = new LinkedHashMap<>();
        for (ManifestEntry entry : currentEntries(table)) {
            DataFileMeta file = entry.file();
            String partition = table.store().pathFactory().getPartitionString(entry.partition());
            String key = partition + entry.bucket() + "/" + file.fileName();
            List<Object> metadata =
                    Arrays.asList(
                            entry.kind(),
                            entry.totalBuckets(),
                            file.fileSize(),
                            file.rowCount(),
                            file.minSequenceNumber(),
                            file.maxSequenceNumber(),
                            file.schemaId(),
                            file.level(),
                            file.extraFiles(),
                            file.deleteRowCount(),
                            file.fileSource(),
                            file.valueStatsCols(),
                            file.externalPath(),
                            file.writeCols());
            assertThat(result.put(key, metadata)).isNull();
        }
        return result;
    }

    private void assertPartitionsHaveLargeRowIdGaps(
            Map<String, List<Long>> firstRowIdsByPartition) {
        assertThat(firstRowIdsByPartition).hasSize(DATA_INTEGRITY_PARTITIONS.length);
        for (String partition : DATA_INTEGRITY_PARTITIONS) {
            List<Long> firstRowIds = firstRowIdsByPartition.get("pt=" + partition + "/");
            assertThat(firstRowIds).hasSize(DATA_INTEGRITY_FILES_PER_PARTITION);
            for (int i = 1; i < firstRowIds.size(); i++) {
                assertThat(firstRowIds.get(i) - firstRowIds.get(i - 1))
                        .isGreaterThanOrEqualTo(SCRAMBLED_ROW_ID_GAP);
            }
        }
    }

    private long assertContiguousRowIds(
            Map<String, List<Long>> rowIdsByPartition,
            Map<String, Long> rowCountsByPartition,
            List<String> partitions,
            long firstRowId) {
        long nextRowId = firstRowId;
        for (String partition : partitions) {
            String partitionPath = "pt=" + partition + "/";
            long rowCount = rowCountsByPartition.get(partitionPath);
            List<Long> rowIds = rowIdsByPartition.get(partitionPath);
            assertThat(rowIds).hasSize(Math.toIntExact(rowCount));
            for (int i = 0; i < rowIds.size(); i++) {
                assertThat(rowIds.get(i)).isEqualTo(nextRowId + i);
            }
            nextRowId += rowCount;
        }
        return nextRowId;
    }

    private Map<String, List<Long>> rowIdsByPartition(FileStoreTable table) {
        Map<String, List<Long>> result = new LinkedHashMap<>();
        for (ManifestEntry entry : currentEntries(table)) {
            String partition = table.store().pathFactory().getPartitionString(entry.partition());
            result.computeIfAbsent(partition, k -> new ArrayList<>())
                    .add(entry.file().nonNullFirstRowId());
        }
        for (List<Long> rowIds : result.values()) {
            Collections.sort(rowIds);
        }
        return result;
    }

    private Map<String, SimpleStats> valueStatsByFile(FileStoreTable table) {
        Map<String, SimpleStats> result = new LinkedHashMap<>();
        for (ManifestEntry entry : currentEntries(table)) {
            SimpleStats previous = result.put(entry.file().fileName(), entry.file().valueStats());
            assertThat(previous).isNull();
        }
        return result;
    }

    private Map<String, List<Long>> expandedRowIdsByPartition(FileStoreTable table) {
        Map<String, List<Long>> result = new LinkedHashMap<>();
        for (ManifestEntry entry : currentEntries(table)) {
            String partition = table.store().pathFactory().getPartitionString(entry.partition());
            List<Long> rowIds = result.computeIfAbsent(partition, k -> new ArrayList<>());
            Range range = entry.file().nonNullRowIdRange();
            for (long rowId = range.from; rowId <= range.to; rowId++) {
                rowIds.add(rowId);
            }
        }
        for (List<Long> rowIds : result.values()) {
            Collections.sort(rowIds);
        }
        return result;
    }

    private Map<String, Set<String>> currentPartitionsByManifest(FileStoreTable table) {
        Snapshot latest = table.snapshotManager().latestSnapshot();
        ManifestFile manifestFile = table.store().manifestFileFactory().create();
        ManifestList manifestList = table.store().manifestListFactory().create();
        List<ManifestFileMeta> manifestMetas = manifestList.readDataManifests(latest);

        Set<FileEntry.Identifier> deletedIdentifiers = new HashSet<>();
        for (ManifestFileMeta manifestMeta : manifestMetas) {
            if (manifestMeta.numDeletedFiles() <= 0) {
                continue;
            }
            for (ManifestEntry entry :
                    manifestFile.read(manifestMeta.fileName(), manifestMeta.fileSize())) {
                if (entry.kind() == FileKind.DELETE) {
                    deletedIdentifiers.add(entry.identifier());
                }
            }
        }

        Map<String, Set<String>> result = new LinkedHashMap<>();
        for (ManifestFileMeta manifestMeta : manifestMetas) {
            if (manifestMeta.numAddedFiles() <= 0) {
                continue;
            }
            for (ManifestEntry entry :
                    manifestFile.read(manifestMeta.fileName(), manifestMeta.fileSize())) {
                if (entry.kind() != FileKind.ADD
                        || deletedIdentifiers.contains(entry.identifier())) {
                    continue;
                }
                String partition =
                        table.store().pathFactory().getPartitionString(entry.partition());
                result.computeIfAbsent(manifestMeta.fileName(), k -> new LinkedHashSet<>())
                        .add(partition);
            }
        }
        return result;
    }

    private List<String> dataManifestFileNames(FileStoreTable table) {
        ManifestList manifestList = table.store().manifestListFactory().create();
        Snapshot latest = table.snapshotManager().latestSnapshot();
        List<String> result = new ArrayList<>();
        for (ManifestFileMeta manifestMeta : manifestList.readDataManifests(latest)) {
            result.add(manifestMeta.fileName());
        }
        return result;
    }

    private void createBTreeIndex(FileStoreTable table) throws Exception {
        SortedGlobalIndexBuilder builder =
                new SortedGlobalIndexBuilder(table, "btree").withIndexField("id");
        List<DataSplit> dataSplits =
                builder.scan()
                        .map(Pair::getRight)
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Expected scan result when building index."));
        List<CommitMessage> commitMessages = new ArrayList<>();
        for (DataSplit dataSplit : SortedGlobalIndexBuilder.splitByContiguousRowRange(dataSplits)) {
            commitMessages.addAll(builder.build(dataSplit, ioManager));
        }
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(commitMessages);
        }
    }

    private void replaceGlobalIndexRangesWithPartitionSpanningRanges(FileStoreTable table)
            throws Exception {
        Snapshot latest = table.snapshotManager().latestSnapshot();
        IndexManifestFile indexManifestFile = table.store().indexManifestFileFactory().create();
        List<IndexManifestEntry> rewritten = new ArrayList<>();
        for (IndexManifestEntry entry : indexManifestFile.read(latest.indexManifest())) {
            GlobalIndexMeta globalIndex = entry.indexFile().globalIndexMeta();
            assertThat(globalIndex).isNotNull();

            Range staleRowRange;
            String partition = table.store().pathFactory().getPartitionString(entry.partition());
            if (partition.equals("pt=a/")) {
                staleRowRange = new Range(0, 4);
            } else if (partition.equals("pt=b/")) {
                staleRowRange = new Range(1, 3);
            } else {
                throw new IllegalStateException("Unexpected partition " + partition);
            }

            GlobalIndexMeta staleGlobalIndex =
                    new GlobalIndexMeta(
                            staleRowRange.from,
                            staleRowRange.to,
                            globalIndex.indexFieldId(),
                            globalIndex.extraFieldIds(),
                            globalIndex.indexMeta());
            IndexFileMeta indexFile = entry.indexFile();
            rewritten.add(
                    new IndexManifestEntry(
                            entry.kind(),
                            entry.partition(),
                            entry.bucket(),
                            new IndexFileMeta(
                                    indexFile.indexType(),
                                    indexFile.fileName(),
                                    indexFile.fileSize(),
                                    indexFile.rowCount(),
                                    indexFile.dvRanges(),
                                    indexFile.externalPath(),
                                    staleGlobalIndex)));
        }

        String staleIndexManifest = indexManifestFile.writeWithoutRolling(rewritten);
        replaceLatestSnapshotIndexManifest(table, latest, staleIndexManifest);
    }

    private void appendGlobalIndexRange(FileStoreTable table, String partition, Range rowRange)
            throws Exception {
        Snapshot latest = table.snapshotManager().latestSnapshot();
        IndexManifestFile indexManifestFile = table.store().indexManifestFileFactory().create();
        List<IndexManifestEntry> entries = indexManifestFile.read(latest.indexManifest());
        IndexManifestEntry template = null;
        for (IndexManifestEntry entry : entries) {
            if (table.store()
                    .pathFactory()
                    .getPartitionString(entry.partition())
                    .equals(partition)) {
                template = entry;
                break;
            }
        }
        assertThat(template).isNotNull();
        IndexFileMeta indexFile = template.indexFile();
        GlobalIndexMeta globalIndex = indexFile.globalIndexMeta();
        assertThat(globalIndex).isNotNull();
        entries.add(
                new IndexManifestEntry(
                        template.kind(),
                        template.partition(),
                        template.bucket(),
                        new IndexFileMeta(
                                indexFile.indexType(),
                                indexFile.fileName(),
                                indexFile.fileSize(),
                                indexFile.rowCount(),
                                indexFile.dvRanges(),
                                indexFile.externalPath(),
                                new GlobalIndexMeta(
                                        rowRange.from,
                                        rowRange.to,
                                        globalIndex.indexFieldId(),
                                        globalIndex.extraFieldIds(),
                                        globalIndex.indexMeta()))));
        replaceLatestSnapshotIndexManifest(
                table, latest, indexManifestFile.writeWithoutRolling(entries));
    }

    private void replaceLatestSnapshotIndexManifest(
            FileStoreTable table, Snapshot latest, String indexManifest) throws Exception {
        Snapshot staleSnapshot =
                new Snapshot(
                        latest.version(),
                        latest.id(),
                        latest.schemaId(),
                        latest.baseManifestList(),
                        latest.baseManifestListSize(),
                        latest.deltaManifestList(),
                        latest.deltaManifestListSize(),
                        latest.changelogManifestList(),
                        latest.changelogManifestListSize(),
                        indexManifest,
                        latest.commitUser(),
                        latest.commitIdentifier(),
                        latest.commitKind(),
                        latest.timeMillis(),
                        latest.totalRecordCount(),
                        latest.deltaRecordCount(),
                        latest.changelogRecordCount(),
                        latest.watermark(),
                        latest.statistics(),
                        latest.properties(),
                        latest.nextRowId(),
                        latest.operation());
        SnapshotManager snapshotManager = table.snapshotManager();
        snapshotManager
                .fileIO()
                .overwriteFileUtf8(
                        snapshotManager.snapshotPath(latest.id()), staleSnapshot.toJson());
        snapshotManager.invalidateCache();
    }

    private List<Range> globalIndexRanges(FileStoreTable table) {
        List<Range> ranges = new ArrayList<>();
        List<IndexManifestEntry> entries =
                table.store()
                        .indexManifestFileFactory()
                        .create()
                        .read(table.snapshotManager().latestSnapshot().indexManifest());
        for (IndexManifestEntry entry : entries) {
            GlobalIndexMeta globalIndex = entry.indexFile().globalIndexMeta();
            if (globalIndex != null) {
                ranges.add(globalIndex.rowRange());
            }
        }
        Collections.sort(
                ranges,
                (left, right) -> {
                    int result = Long.compare(left.from, right.from);
                    return result == 0 ? Long.compare(left.to, right.to) : result;
                });
        return ranges;
    }

    private Map<String, List<Range>> globalIndexRangesByPartition(FileStoreTable table) {
        Map<String, List<Range>> result = new LinkedHashMap<>();
        List<IndexManifestEntry> entries =
                table.store()
                        .indexManifestFileFactory()
                        .create()
                        .read(table.snapshotManager().latestSnapshot().indexManifest());
        for (IndexManifestEntry entry : entries) {
            GlobalIndexMeta globalIndex = entry.indexFile().globalIndexMeta();
            if (globalIndex == null) {
                continue;
            }
            String partition = table.store().pathFactory().getPartitionString(entry.partition());
            result.computeIfAbsent(partition, ignored -> new ArrayList<>())
                    .add(globalIndex.rowRange());
        }
        for (List<Range> ranges : result.values()) {
            Collections.sort(
                    ranges,
                    (left, right) -> {
                        int compare = Long.compare(left.from, right.from);
                        return compare == 0 ? Long.compare(left.to, right.to) : compare;
                    });
        }
        return result;
    }

    private void assertTableDataQueryable(FileStoreTable table, List<String> expectedRows)
            throws Exception {
        assertThat(readTableRows(table)).containsExactlyElementsOf(expectedRows);

        PredicateBuilder predicateBuilder = new PredicateBuilder(table.rowType());
        int partitionField = table.rowType().getFieldIndex("pt");
        for (String partition : DATA_INTEGRITY_PARTITIONS) {
            Predicate predicate =
                    predicateBuilder.equal(partitionField, BinaryString.fromString(partition));
            assertThat(readTableRows(table, predicate))
                    .containsExactlyElementsOf(rowsForPartition(expectedRows, partition));
        }

        int idField = table.rowType().getFieldIndex("id");
        int[] pointIds = {0, expectedRows.size() / 2, expectedRows.size() - 1};
        for (int id : pointIds) {
            Predicate predicate = predicateBuilder.equal(idField, id);
            assertThat(readTableRows(table, predicate)).containsAll(rowsForId(expectedRows, id));
        }
    }

    private List<String> rowsForPartition(List<String> rows, String partition) {
        List<String> result = new ArrayList<>();
        for (String row : rows) {
            if (row.split("\\|", 3)[1].equals(partition)) {
                result.add(row);
            }
        }
        return result;
    }

    private List<String> rowsForId(List<String> rows, int id) {
        List<String> result = new ArrayList<>();
        String prefix = id + "|";
        for (String row : rows) {
            if (row.startsWith(prefix)) {
                result.add(row);
            }
        }
        return result;
    }

    private List<String> readTableRows(FileStoreTable table) throws Exception {
        return readTableRows(table.newReadBuilder());
    }

    private List<String> readTableRows(FileStoreTable table, Predicate predicate) throws Exception {
        return readTableRows(table.newReadBuilder().withFilter(predicate));
    }

    private List<String> readTableRows(ReadBuilder readBuilder) throws Exception {
        List<String> result = new ArrayList<>();
        readBuilder
                .newRead()
                .createReader(readBuilder.newScan().plan())
                .forEachRemaining(
                        row -> {
                            InternalRow projected = row;
                            String partition = projected.getString(0).toString();
                            int id = projected.getInt(1);
                            String payload = projected.getString(2).toString();
                            result.add(id + "|" + partition + "|" + payload);
                        });
        Collections.sort(result);
        return result;
    }

    private List<String> readPayloads(FileStoreTable table, Predicate predicate) throws Exception {
        ReadBuilder readBuilder = table.newReadBuilder().withFilter(predicate);
        List<String> result = new ArrayList<>();
        readBuilder
                .newRead()
                .createReader(readBuilder.newScan().plan())
                .forEachRemaining(
                        row -> {
                            InternalRow projected = row;
                            result.add(projected.getString(2).toString());
                        });
        return result;
    }

    private static class ExpectedLargeManifestAssignment {

        private final int[] entriesByPartition;
        private final long[] minRowIdByPartition;
        private final long[] maxRowIdByPartition;
        private final boolean[] partitionsToReassign;
        private final long reassignedEntryCount;
        private final long reassignedRowCount;
        private final Set<String> rewrittenManifestFiles;

        private ExpectedLargeManifestAssignment(
                int[] entriesByPartition,
                long[] minRowIdByPartition,
                long[] maxRowIdByPartition,
                boolean[] partitionsToReassign,
                long reassignedEntryCount,
                long reassignedRowCount,
                Set<String> rewrittenManifestFiles) {
            this.entriesByPartition = entriesByPartition;
            this.minRowIdByPartition = minRowIdByPartition;
            this.maxRowIdByPartition = maxRowIdByPartition;
            this.partitionsToReassign = partitionsToReassign;
            this.reassignedEntryCount = reassignedEntryCount;
            this.reassignedRowCount = reassignedRowCount;
            this.rewrittenManifestFiles = rewrittenManifestFiles;
        }
    }
}
