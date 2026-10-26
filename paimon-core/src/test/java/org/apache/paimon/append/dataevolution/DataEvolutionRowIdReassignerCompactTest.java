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
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.globalindex.GlobalIndexBuilderUtils;
import org.apache.paimon.globalindex.ScanResult;
import org.apache.paimon.globalindex.sorted.SortedGlobalIndexScanner;
import org.apache.paimon.globalindex.sorted.SortedGlobalIndexTestUtils;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.options.ExpireConfig;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.schema.Schema;
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
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.paimon.append.dataevolution.SerializationAssignment.planFile;
import static org.apache.paimon.append.dataevolution.SerializationAssignment.readPlan;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for row-id reassignment across concurrent compaction snapshots. */
public class DataEvolutionRowIdReassignerCompactTest extends TableTestBase {

    @Override
    protected Schema schemaDefault() {
        return Schema.newBuilder()
                .column("pt", DataTypes.STRING())
                .column("id", DataTypes.INT())
                .column("payload", DataTypes.STRING())
                .partitionKeys("pt")
                .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                .option(CoreOptions.GLOBAL_INDEX_ENABLED.key(), "true")
                .build();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testReassignRetriesAfterConcurrentManifestCompaction(boolean append)
            throws Exception {
        FileStoreTable table = createTableWithInterleavedPartitions();
        Snapshot before = table.snapshotManager().latestSnapshot();
        List<String> plannedManifestFiles = dataManifestFileNames(table);

        AtomicBoolean compacted = new AtomicBoolean();
        DataEvolutionRowIdReassigner.Result result =
                new DataEvolutionRowIdReassigner(
                                table,
                                null,
                                () -> {
                                    if (compacted.compareAndSet(false, true)) {
                                        try {
                                            if (append) {
                                                writeOneRow(table, "c", 100);
                                            }
                                            compactManifests(table);
                                            assertThat(
                                                            table.snapshotManager()
                                                                    .latestSnapshot()
                                                                    .commitKind())
                                                    .isEqualTo(Snapshot.CommitKind.COMPACT);
                                            assertThat(dataManifestFileNames(table))
                                                    .doesNotContainAnyElementsOf(
                                                            plannedManifestFiles);
                                        } catch (Exception e) {
                                            throw new RuntimeException(e);
                                        }
                                    }
                                })
                        .reassign("test-reassign-manifest-compact-conflict");

        long firstRowId = append ? 6L : 5L;
        assertThat(compacted).isTrue();
        assertThat(result.previousSnapshotId).isEqualTo(before.id() + (append ? 2 : 1));
        assertThat(result.newSnapshotId).isEqualTo(result.previousSnapshotId + 1);
        assertThat(result.firstAssignedRowId).isEqualTo(firstRowId);
        assertThat(result.nextRowId).isEqualTo(firstRowId + 5);
        assertThat(result.rowCount).isEqualTo(5L);
        assertThat(rowIdsByPartition(table))
                .containsEntry("pt=a/", Arrays.asList(firstRowId, firstRowId + 1, firstRowId + 2))
                .containsEntry("pt=b/", Arrays.asList(firstRowId + 3, firstRowId + 4));
        if (append) {
            assertThat(rowIdsByPartition(table))
                    .containsEntry("pt=c/", Collections.singletonList(5L));
        }
        assertThat(readTableRows(table)).hasSize(append ? 6 : 5);
        assertPersistedPlan(table);
    }

    @ParameterizedTest
    @ValueSource(strings = {"pt=a/", "pt=b/"})
    public void testReassignRetriesAfterConcurrentDataCompaction(String compactedPartition)
            throws Exception {
        FileStoreTable table = createTableWithInterleavedPartitions();
        Snapshot before = table.snapshotManager().latestSnapshot();
        Map<FileEntry.Identifier, Long> oldRowIds =
                currentEntries(table).stream()
                        .collect(
                                Collectors.toMap(
                                        ManifestEntry::identifier,
                                        entry -> entry.file().nonNullFirstRowId()));
        List<FileEntry.Identifier> compactedFiles = new ArrayList<>();

        AtomicBoolean compacted = new AtomicBoolean();
        DataEvolutionRowIdReassigner.Result result =
                new DataEvolutionRowIdReassigner(
                                table,
                                partitionPredicate(table, "a"),
                                () -> {
                                    if (compacted.compareAndSet(false, true)) {
                                        try {
                                            compactedFiles.add(
                                                    compactOneFile(table, compactedPartition));
                                            assertThat(
                                                            table.snapshotManager()
                                                                    .latestSnapshot()
                                                                    .commitKind())
                                                    .isEqualTo(Snapshot.CommitKind.COMPACT);
                                        } catch (Exception e) {
                                            throw new RuntimeException(e);
                                        }
                                    }
                                })
                        .reassign("test-reassign-data-compact-conflict");

        assertThat(result.previousSnapshotId).isEqualTo(before.id() + 1);
        assertThat(result.newSnapshotId).isEqualTo(before.id() + 2);
        assertThat(result.firstAssignedRowId).isEqualTo(5L);
        assertThat(result.nextRowId).isEqualTo(8L);
        assertThat(rowIdsByPartition(table))
                .containsEntry("pt=a/", Arrays.asList(5L, 6L, 7L))
                .containsEntry("pt=b/", Arrays.asList(1L, 3L));
        long oldFirstRowId = oldRowIds.get(compactedFiles.get(0));
        assertThat(assertAddDeleteEntriesConsistent(table, compactedFiles.get(0)))
                .isEqualTo(
                        compactedPartition.equals("pt=a/")
                                ? 5L + oldFirstRowId / 2
                                : oldFirstRowId);
        assertThat(readTableRows(table))
                .containsExactly("0|a|v0", "1|b|v1", "2|a|v2", "3|b|v3", "4|a|v4");
        assertPersistedPlan(table);
    }

    @Test
    public void testReassignRetriesAfterConcurrentDataCompactionMergesFiles() throws Exception {
        FileStoreTable table = createTableWithInterleavedPartitions();
        writeOneRow(table, "a", 5);
        Snapshot before = table.snapshotManager().latestSnapshot();

        AtomicBoolean compacted = new AtomicBoolean();
        DataEvolutionRowIdReassigner.Result result =
                new DataEvolutionRowIdReassigner(
                                table,
                                null,
                                () -> {
                                    if (compacted.compareAndSet(false, true)) {
                                        try {
                                            compactNormalFiles(table, "pt=a/", 4L);
                                        } catch (Exception e) {
                                            throw new RuntimeException(e);
                                        }
                                    }
                                })
                        .reassign("test-reassign-data-compact-merge");

        assertThat(result.previousSnapshotId).isEqualTo(before.id() + 1);
        assertThat(result.newSnapshotId).isEqualTo(before.id() + 2);
        assertThat(result.firstAssignedRowId).isEqualTo(6L);
        assertThat(result.nextRowId).isEqualTo(12L);
        assertThat(result.rowCount).isEqualTo(6L);
        assertThat(rowIdsByPartition(table))
                .containsEntry("pt=a/", Arrays.asList(6L, 7L, 8L))
                .containsEntry("pt=b/", Arrays.asList(10L, 11L));
        assertThat(expandedRowIdsByPartition(table))
                .containsEntry("pt=a/", Arrays.asList(6L, 7L, 8L, 9L));
        assertThat(readTableRows(table))
                .containsExactly("0|a|v0", "1|b|v1", "2|a|v2", "3|b|v3", "4|a|v4", "5|a|v5");
        assertPersistedPlan(table);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testReassignReplansAfterPartiallyOverlappingConcurrentCompaction(boolean filter)
            throws Exception {
        FileStoreTable table = createTableWithInterleavedPartitions();
        createBTreeIndex(table);
        Snapshot before = table.snapshotManager().latestSnapshot();

        AtomicBoolean compacted = new AtomicBoolean();
        DataEvolutionRowIdReassigner.Result result =
                new DataEvolutionRowIdReassigner(
                                table,
                                filter ? partitionPredicate(table, "a") : null,
                                () -> {
                                    if (compacted.compareAndSet(false, true)) {
                                        try {
                                            writeOneRow(table, "a", 5);
                                            compactNormalFiles(table, "pt=a/", 4L);
                                        } catch (Exception e) {
                                            throw new RuntimeException(e);
                                        }
                                    }
                                })
                        .reassign("test-reassign-partially-overlapping-compact");

        assertThat(compacted).isTrue();
        assertThat(result.reassigned).isTrue();
        assertThat(result.previousSnapshotId).isEqualTo(before.id() + 2);
        assertThat(result.newSnapshotId).isEqualTo(before.id() + 3);
        assertThat(result.firstAssignedRowId).isEqualTo(6L);
        assertThat(result.nextRowId).isEqualTo(filter ? 10L : 12L);
        assertThat(result.rowCount).isEqualTo(filter ? 4L : 6L);
        assertThat(expandedRowIdsByPartition(table))
                .containsEntry("pt=a/", Arrays.asList(6L, 7L, 8L, 9L))
                .containsEntry("pt=b/", filter ? Arrays.asList(1L, 3L) : Arrays.asList(10L, 11L));
        assertThat(readTableRows(table))
                .containsExactly("0|a|v0", "1|b|v1", "2|a|v2", "3|b|v3", "4|a|v4", "5|a|v5");
        assertThat(result.indexFileCount).isEqualTo(filter ? 3L : 5L);
        List<Range> indexRanges = new ArrayList<>();
        for (IndexManifestEntry entry : table.store().newIndexFileHandler().scanEntries()) {
            indexRanges.add(entry.indexFile().globalIndexMeta().rowRange());
        }
        assertThat(indexRanges)
                .containsExactlyInAnyOrderElementsOf(
                        filter
                                ? Arrays.asList(
                                        new Range(1, 1),
                                        new Range(3, 3),
                                        new Range(6, 6),
                                        new Range(7, 7),
                                        new Range(8, 8))
                                : Arrays.asList(
                                        new Range(6, 6),
                                        new Range(7, 7),
                                        new Range(8, 8),
                                        new Range(10, 10),
                                        new Range(11, 11)));
        assertPersistedPlan(table);
    }

    @Test
    public void testReplanningDoesNotIncludeNewlyFragmentedPartitions() throws Exception {
        FileStoreTable table = createTableWithInterleavedPartitions();
        Snapshot before = table.snapshotManager().latestSnapshot();
        AtomicBoolean compacted = new AtomicBoolean();
        DataEvolutionRowIdReassigner.Result result =
                new DataEvolutionRowIdReassigner(
                                table,
                                null,
                                () -> {
                                    if (compacted.compareAndSet(false, true)) {
                                        try {
                                            writeOneRow(table, "a", 5);
                                            compactNormalFiles(table, "pt=a/", 4L);
                                            writeOneRow(table, "c", 6);
                                            writeOneRow(table, "d", 7);
                                            writeOneRow(table, "c", 8);
                                        } catch (Exception e) {
                                            throw new RuntimeException(e);
                                        }
                                    }
                                })
                        .reassign("test-replan-original-partitions");

        assertThat(result.previousSnapshotId).isEqualTo(before.id() + 5);
        assertThat(result.newSnapshotId).isEqualTo(before.id() + 6);
        assertThat(result.firstAssignedRowId).isEqualTo(9L);
        assertThat(result.nextRowId).isEqualTo(15L);
        assertThat(expandedRowIdsByPartition(table))
                .containsEntry("pt=a/", Arrays.asList(9L, 10L, 11L, 12L))
                .containsEntry("pt=b/", Arrays.asList(13L, 14L))
                .containsEntry("pt=c/", Arrays.asList(6L, 8L))
                .containsEntry("pt=d/", Collections.singletonList(7L));
        assertThat(readTableRows(table)).hasSize(9);
        assertPersistedPlan(table);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testReplanningKeepsNullPartition(boolean filter) throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        writeOneRow(table, null, 0);
        writeOneRow(table, "b", 1);
        writeOneRow(table, null, 2);
        writeOneRow(table, "b", 3);
        writeOneRow(table, null, 4);
        String nullPartitionPath =
                table.store()
                        .pathFactory()
                        .getPartitionString(
                                currentEntries(table).stream()
                                        .filter(entry -> entry.partition().isNullAt(0))
                                        .findFirst()
                                        .get()
                                        .partition());
        AtomicBoolean compacted = new AtomicBoolean();
        DataEvolutionRowIdReassigner.Result result =
                new DataEvolutionRowIdReassigner(
                                table,
                                filter ? partitionPredicate(table, null) : null,
                                () -> {
                                    if (compacted.compareAndSet(false, true)) {
                                        try {
                                            writeOneRow(table, null, 5);
                                            compactNormalFiles(table, nullPartitionPath, 4L);
                                        } catch (Exception e) {
                                            throw new RuntimeException(e);
                                        }
                                    }
                                })
                        .reassign("test-replan-null-partition");

        assertThat(result.reassigned).isTrue();
        assertThat(result.rowCount).isEqualTo(filter ? 4L : 6L);
        assertThat(expandedRowIdsByPartition(table))
                .containsEntry(nullPartitionPath, Arrays.asList(6L, 7L, 8L, 9L))
                .containsEntry("pt=b/", filter ? Arrays.asList(1L, 3L) : Arrays.asList(10L, 11L));
        assertThat(readTableRows(table))
                .containsExactly(
                        "0|null|v0", "1|b|v1", "2|null|v2", "3|b|v3", "4|null|v4", "5|null|v5");
        assertPersistedPlan(table);
    }

    @Test
    public void testReassignReplansAcrossRepeatedCompactionConflicts() throws Exception {
        FileStoreTable table = createTableWithInterleavedPartitions();
        Snapshot before = table.snapshotManager().latestSnapshot();
        AtomicInteger attempts = new AtomicInteger();
        DataEvolutionRowIdReassigner.Result result =
                new DataEvolutionRowIdReassigner(
                                table,
                                null,
                                () -> {
                                    int attempt = attempts.getAndIncrement();
                                    if (attempt < 2) {
                                        try {
                                            writeOneRow(table, "a", 5 + attempt);
                                            compactNormalFiles(table, "pt=a/", 4L);
                                            compactManifests(table);
                                        } catch (Exception e) {
                                            throw new RuntimeException(e);
                                        }
                                    }
                                })
                        .reassign("test-replan-repeated-compactions");

        assertThat(attempts).hasValue(3);
        assertThat(result.previousSnapshotId).isEqualTo(before.id() + 6);
        assertThat(result.newSnapshotId).isEqualTo(before.id() + 7);
        assertThat(result.firstAssignedRowId).isEqualTo(7L);
        assertThat(result.nextRowId).isEqualTo(14L);
        assertThat(expandedRowIdsByPartition(table))
                .containsEntry("pt=a/", Arrays.asList(7L, 8L, 9L, 10L, 11L))
                .containsEntry("pt=b/", Arrays.asList(12L, 13L));
        assertThat(readTableRows(table))
                .containsExactly(
                        "0|a|v0", "1|b|v1", "2|a|v2", "3|b|v3", "4|a|v4", "5|a|v5", "6|a|v6");
        assertPersistedPlan(table);
    }

    @Test
    public void testReplanningSkipsWhenNoPartitionStillRequiresReassignment() throws Exception {
        createTableDefault();
        FileStoreTable table =
                getTableDefault()
                        .copy(
                                Collections.singletonMap(
                                        CoreOptions
                                                .DATA_EVOLUTION_REASSIGN_SKIP_CONTIGUOUS_ROW_COUNT
                                                .key(),
                                        "1"));
        writeOneRow(table, "a", 0);
        writeOneRow(table, "b", 1);
        writeOneRow(table, "a", 2);
        Snapshot before = table.snapshotManager().latestSnapshot();
        AtomicBoolean compacted = new AtomicBoolean();
        DataEvolutionRowIdReassigner.Result result =
                new DataEvolutionRowIdReassigner(
                                table,
                                partitionPredicate(table, "a"),
                                () -> {
                                    if (compacted.compareAndSet(false, true)) {
                                        try {
                                            writeOneRow(table, "a", 3);
                                            compactNormalFiles(table, "pt=a/", 2L);
                                        } catch (Exception e) {
                                            throw new RuntimeException(e);
                                        }
                                    }
                                })
                        .reassign("test-replan-nothing-to-reassign");

        assertThat(result.reassigned).isFalse();
        assertThat(result.previousSnapshotId).isEqualTo(before.id() + 2);
        assertThat(result.newSnapshotId).isEqualTo(result.previousSnapshotId);
        assertThat(result.firstAssignedRowId).isEqualTo(4L);
        assertThat(result.nextRowId).isEqualTo(4L);
        assertThat(result.skipReason).contains("after concurrent compaction");
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(result.newSnapshotId);
        assertThat(expandedRowIdsByPartition(table))
                .containsEntry("pt=a/", Arrays.asList(0L, 2L, 3L))
                .containsEntry("pt=b/", Collections.singletonList(1L));
        assertThat(readTableRows(table)).containsExactly("0|a|v0", "1|b|v1", "2|a|v2", "3|a|v3");
    }

    @Test
    public void testReplanningStillRejectsConcurrentOverwrite() throws Exception {
        FileStoreTable table = createTableWithInterleavedPartitions();
        AtomicBoolean compacted = new AtomicBoolean();
        assertThatThrownBy(
                        () ->
                                new DataEvolutionRowIdReassigner(
                                                table,
                                                null,
                                                () -> {
                                                    if (compacted.compareAndSet(false, true)) {
                                                        try {
                                                            writeOneRow(table, "a", 5);
                                                            compactNormalFiles(table, "pt=a/", 4L);
                                                            BatchWriteBuilder builder =
                                                                    table.newBatchWriteBuilder()
                                                                            .withOverwrite();
                                                            try (BatchTableWrite write =
                                                                            builder.newWrite();
                                                                    BatchTableCommit commit =
                                                                            builder.newCommit()) {
                                                                write.write(
                                                                        GenericRow.of(
                                                                                BinaryString
                                                                                        .fromString(
                                                                                                "a"),
                                                                                6,
                                                                                BinaryString
                                                                                        .fromString(
                                                                                                "v6")));
                                                                commit.commit(
                                                                        write.prepareCommit());
                                                            }
                                                        } catch (Exception e) {
                                                            throw new RuntimeException(e);
                                                        }
                                                    }
                                                })
                                        .reassign("test-replan-rejects-overwrite"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("OVERWRITE snapshot");
        assertThat(table.snapshotManager().latestSnapshot().commitKind())
                .isEqualTo(Snapshot.CommitKind.OVERWRITE);
        assertThat(readTableRows(table)).containsExactly("1|b|v1", "3|b|v3", "6|a|v6");
    }

    @Test
    public void testReassignRetriesAfterConcurrentCompactionSplitsFiles() throws Exception {
        FileStoreTable table = createTableWithInterleavedPartitions();
        writeOneRow(table, "a", 5);
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.DATA_EVOLUTION_COMPACTION_SPLIT_LARGE_FILES.key(), "true");
        options.put(CoreOptions.TARGET_FILE_SIZE.key(), "1b");
        FileStoreTable compactTable = table.copy(options);
        AtomicBoolean compacted = new AtomicBoolean();
        DataEvolutionRowIdReassigner.Result result =
                new DataEvolutionRowIdReassigner(
                                table,
                                null,
                                () -> {
                                    if (compacted.compareAndSet(false, true)) {
                                        try {
                                            DataEvolutionNormalCompactTask task =
                                                    compactNormalFiles(compactTable, "pt=a/", 4L);
                                            assertThat(task.compactAfter()).hasSize(2);
                                        } catch (Exception e) {
                                            throw new RuntimeException(e);
                                        }
                                    }
                                })
                        .reassign("test-reassign-split-compact-conflict");

        assertThat(result.firstAssignedRowId).isEqualTo(6L);
        assertThat(result.nextRowId).isEqualTo(12L);
        assertThat(rowIdsByPartition(table))
                .containsEntry("pt=a/", Arrays.asList(6L, 7L, 8L, 9L))
                .containsEntry("pt=b/", Arrays.asList(10L, 11L));
        assertThat(readTableRows(table))
                .containsExactly("0|a|v0", "1|b|v1", "2|a|v2", "3|b|v3", "4|a|v4", "5|a|v5");
        assertPersistedPlan(table);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    public void testReassignFailsWhenPlannedManifestExpiresDuringRewrite(int parallelism)
            throws Exception {
        FileStoreTable table =
                createTableWithInterleavedPartitions()
                        .copy(
                                Collections.singletonMap(
                                        CoreOptions.SCAN_MANIFEST_PARALLELISM.key(),
                                        String.valueOf(parallelism)));
        table.store().setManifestCache(null);
        Snapshot before = table.snapshotManager().latestSnapshot();
        AtomicBoolean compacted = new AtomicBoolean();
        assertThatThrownBy(
                        () ->
                                new DataEvolutionRowIdReassigner(
                                                table,
                                                null,
                                                () -> {},
                                                manifest -> {
                                                    if (compacted.compareAndSet(false, true)) {
                                                        try {
                                                            compactManifests(table);
                                                            expireSnapshots(table, 1);
                                                            Path oldManifest =
                                                                    table.store()
                                                                            .pathFactory()
                                                                            .toManifestFilePath(
                                                                                    manifest
                                                                                            .fileName());
                                                            assertThat(
                                                                            table.fileIO()
                                                                                    .exists(
                                                                                            oldManifest))
                                                                    .isFalse();
                                                        } catch (Exception e) {
                                                            throw new RuntimeException(e);
                                                        }
                                                    }
                                                })
                                        .reassign("test-reassign-expired-manifest-during-rewrite"))
                .hasRootCauseInstanceOf(FileNotFoundException.class);

        assertThat(compacted).isTrue();
        assertThat(table.snapshotManager().latestSnapshot().id()).isEqualTo(before.id() + 1);
        assertThat(rowIdsByPartition(table))
                .containsEntry("pt=a/", Arrays.asList(0L, 2L, 4L))
                .containsEntry("pt=b/", Arrays.asList(1L, 3L));
        assertThat(readTableRows(table))
                .containsExactly("0|a|v0", "1|b|v1", "2|a|v2", "3|b|v3", "4|a|v4");
    }

    @Test
    public void testReassignDoesNotRetryMissingManifestWithoutNewSnapshot() throws Exception {
        FileStoreTable table =
                createTableWithInterleavedPartitions()
                        .copy(
                                Collections.singletonMap(
                                        CoreOptions.SCAN_MANIFEST_PARALLELISM.key(), "1"));
        Snapshot before = table.snapshotManager().latestSnapshot();
        AtomicInteger attempts = new AtomicInteger();
        assertThatThrownBy(
                        () ->
                                new DataEvolutionRowIdReassigner(
                                                table,
                                                null,
                                                () -> {},
                                                manifest -> {
                                                    attempts.incrementAndGet();
                                                    table.fileIO()
                                                            .deleteQuietly(
                                                                    table.store()
                                                                            .pathFactory()
                                                                            .toManifestFilePath(
                                                                                    manifest
                                                                                            .fileName()));
                                                })
                                        .reassign("test-missing-manifest-without-progress"))
                .hasRootCauseInstanceOf(FileNotFoundException.class);
        assertThat(attempts).hasValue(1);
        assertThat(table.snapshotManager().latestSnapshot().id()).isEqualTo(before.id());
    }

    @Test
    public void testReassignDoesNotRetryOtherFailuresAfterConcurrentCompaction() throws Exception {
        FileStoreTable table =
                createTableWithInterleavedPartitions()
                        .copy(
                                Collections.singletonMap(
                                        CoreOptions.SCAN_MANIFEST_PARALLELISM.key(), "1"));
        Snapshot before = table.snapshotManager().latestSnapshot();
        IllegalStateException failure = new IllegalStateException("Manifest read failure");
        assertThatThrownBy(
                        () ->
                                new DataEvolutionRowIdReassigner(
                                                table,
                                                null,
                                                () -> {},
                                                manifest -> {
                                                    try {
                                                        compactManifests(table);
                                                    } catch (Exception e) {
                                                        throw new RuntimeException(e);
                                                    }
                                                    throw failure;
                                                })
                                        .reassign("test-other-manifest-failure-after-compaction"))
                .isSameAs(failure);
        assertThat(table.snapshotManager().latestSnapshot().id()).isEqualTo(before.id() + 1);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testReassignAfterPlanningSnapshotExpires(boolean compact) throws Exception {
        FileStoreTable table = createTableWithInterleavedPartitions();
        table.store().setManifestCache(null);
        Snapshot before = table.snapshotManager().latestSnapshot();
        AtomicBoolean compacted = new AtomicBoolean();
        DataEvolutionRowIdReassigner.Result result =
                new DataEvolutionRowIdReassigner(
                                table,
                                null,
                                () -> {
                                    if (compacted.compareAndSet(false, true)) {
                                        try {
                                            if (compact) {
                                                compactManifests(table);
                                            } else {
                                                writeOneRow(table, "c", 100);
                                            }
                                            expireSnapshots(table, 1);
                                            assertThat(
                                                            table.snapshotManager()
                                                                    .snapshotExists(before.id()))
                                                    .isFalse();
                                            assertThat(
                                                            table.fileIO()
                                                                    .exists(
                                                                            table.store()
                                                                                    .pathFactory()
                                                                                    .toManifestListPath(
                                                                                            before
                                                                                                    .baseManifestList())))
                                                    .isFalse();
                                        } catch (Exception e) {
                                            throw new RuntimeException(e);
                                        }
                                    }
                                })
                        .reassign("test-reassign-expired-planning-snapshot");
        assertThat(result.reassigned).isTrue();
        assertThat(result.previousSnapshotId).isEqualTo(before.id() + 1);
        assertThat(result.firstAssignedRowId).isEqualTo(compact ? 5L : 6L);
        assertThat(result.nextRowId).isEqualTo(compact ? 10L : 11L);
        assertThat(readTableRows(table))
                .hasSize(compact ? 5 : 6)
                .contains("0|a|v0", "1|b|v1", "2|a|v2", "3|b|v3", "4|a|v4");
        assertPersistedPlan(table);
    }

    private void expireSnapshots(FileStoreTable table, int retained) {
        table.newExpireSnapshots()
                .config(
                        ExpireConfig.builder()
                                .snapshotRetainMin(retained)
                                .snapshotRetainMax(retained)
                                .build())
                .expire();
    }

    private void createBTreeIndex(FileStoreTable table) throws Exception {
        ScanResult<DataSplit> scan =
                new SortedGlobalIndexScanner(table, "btree")
                        .withIndexFields(Collections.singletonList("id"))
                        .scan()
                        .get();
        List<CommitMessage> messages = new ArrayList<>();
        for (DataSplit split : GlobalIndexBuilderUtils.splitByContiguousRowRange(scan.entries())) {
            messages.addAll(
                    SortedGlobalIndexTestUtils.buildIndex(
                            table, "btree", "id", split, scan.scanSnapshotId()));
        }
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(messages);
        }
    }

    private void writeOneRow(FileStoreTable table, String partition, int id) throws Exception {
        writeOneRow(table, partition, id, false);
    }

    private void writeOneRow(FileStoreTable table, String partition, int id, boolean overwrite)
            throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        if (overwrite) {
            builder.withOverwrite();
        }
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

    private List<String> readTableRows(FileStoreTable table) throws Exception {
        ReadBuilder builder = table.newReadBuilder();
        List<String> result = new ArrayList<>();
        builder.newRead()
                .createReader(builder.newScan().plan())
                .forEachRemaining(
                        row ->
                                result.add(
                                        row.getInt(1)
                                                + "|"
                                                + (row.isNullAt(0) ? null : row.getString(0))
                                                + "|"
                                                + row.getString(2)));
        Collections.sort(result);
        return result;
    }

    private long assertAddDeleteEntriesConsistent(
            FileStoreTable table, FileEntry.Identifier identifier) {
        ManifestFile manifestFile = table.store().manifestFileFactory().create();
        Snapshot latest = table.snapshotManager().latestSnapshot();
        ManifestList manifestList = table.store().manifestListFactory().create();
        List<ManifestEntry> matchingEntries = new ArrayList<>();
        for (ManifestFileMeta meta : manifestList.readDataManifests(latest)) {
            for (ManifestEntry entry : manifestFile.read(meta.fileName(), meta.fileSize())) {
                if (entry.identifier().equals(identifier)) {
                    matchingEntries.add(entry);
                }
            }
        }
        assertThat(matchingEntries).hasSize(2);
        assertThat(matchingEntries)
                .extracting(ManifestEntry::kind)
                .containsExactlyInAnyOrder(FileKind.ADD, FileKind.DELETE);
        assertThat(matchingEntries.get(0).file()).isEqualTo(matchingEntries.get(1).file());
        return matchingEntries.get(0).file().nonNullFirstRowId();
    }

    private void assertPersistedPlan(FileStoreTable table) throws Exception {
        Snapshot latest = table.snapshotManager().latestSnapshot();
        assertThat(planFile(latest)).isNotNull();
        SerializationAssignment assignment =
                readPlan(table.fileIO(), table.store().pathFactory(), planFile(latest));
        assertThat(assignment.snapshotId()).isEqualTo(latest.id());
        assertThat(assignment.nextRowId()).isEqualTo(latest.nextRowId());
        Map<String, ManifestEntry> previous = new HashMap<>();
        for (ManifestEntry entry :
                table.store().newScan().withSnapshot(latest.id() - 1).plan().files()) {
            previous.put(entry.file().fileName(), entry);
        }
        for (ManifestEntry entry : currentEntries(table)) {
            Range oldRange = previous.get(entry.file().fileName()).file().nonNullRowIdRange();
            assertThat(assignment.mapRowRange(entry.partition(), oldRange))
                    .isEqualTo(entry.file().nonNullRowIdRange());
        }
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

    private void compactManifests(FileStoreTable table) throws Exception {
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.compactManifests();
        }
    }

    private DataEvolutionNormalCompactTask compactNormalFiles(
            FileStoreTable table, String partitionPath, long firstRowId) throws Exception {
        List<ManifestEntry> entries =
                currentEntries(table).stream()
                        .filter(
                                entry ->
                                        table.store()
                                                .pathFactory()
                                                .getPartitionString(entry.partition())
                                                .equals(partitionPath))
                        .filter(entry -> entry.file().nonNullFirstRowId() >= firstRowId)
                        .collect(Collectors.toList());
        List<DataFileMeta> files =
                entries.stream()
                        .map(ManifestEntry::file)
                        .sorted(Comparator.comparingLong(DataFileMeta::nonNullFirstRowId))
                        .collect(Collectors.toList());
        DataEvolutionNormalCompactTask task =
                new DataEvolutionNormalCompactTask(entries.get(0).partition(), files);
        CommitMessage message = task.doCompact(table, "test-reassign-normal-compact");
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(Collections.singletonList(message));
        }
        assertThat(task.compactBefore()).hasSize(2);
        assertThat(table.snapshotManager().latestSnapshot().commitKind())
                .isEqualTo(Snapshot.CommitKind.COMPACT);
        return task;
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
        DataFilePathFactory pathFactory =
                table.store()
                        .pathFactory()
                        .createDataFilePathFactory(
                                compactBefore.partition(), compactBefore.bucket());
        Path compactAfterPath = pathFactory.newPath();
        table.fileIO().copyFile(pathFactory.toPath(compactBefore.file()), compactAfterPath, false);
        DataFileMeta compactAfter = compactBefore.file().rename(compactAfterPath.getName());
        CommitMessage message =
                new CommitMessageImpl(
                        compactBefore.partition(),
                        compactBefore.bucket(),
                        compactBefore.totalBuckets(),
                        DataIncrement.emptyIncrement(),
                        new CompactIncrement(
                                Collections.singletonList(compactBefore.file()),
                                Collections.singletonList(compactAfter),
                                Collections.emptyList()));
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(Collections.singletonList(message));
        }
        return compactBefore.identifier();
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

    private List<String> dataManifestFileNames(FileStoreTable table) {
        ManifestList manifestList = table.store().manifestListFactory().create();
        Snapshot latest = table.snapshotManager().latestSnapshot();
        List<String> result = new ArrayList<>();
        for (ManifestFileMeta manifestMeta : manifestList.readDataManifests(latest)) {
            result.add(manifestMeta.fileName());
        }
        return result;
    }
}
