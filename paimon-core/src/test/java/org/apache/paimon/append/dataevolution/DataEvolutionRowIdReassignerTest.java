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
import org.apache.paimon.globalindex.btree.BTreeGlobalIndexBuilder;
import org.apache.paimon.globalindex.btree.BTreeIndexOptions;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.manifest.IndexManifestEntry;
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
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

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
        assertThat(result.skipReason).isEqualTo("partition row IDs are already contiguous");
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
                write.write(
                        GenericRow.of(
                                BinaryString.fromString(partition),
                                id,
                                BinaryString.fromString("v" + id)));
            }
            commit.commit(write.prepareCommit());
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

    private Map<String, List<Long>> rowIdsByPartition(FileStoreTable table) {
        List<ManifestEntry> entries =
                table.store()
                        .newScan()
                        .withSnapshot(table.snapshotManager().latestSnapshot())
                        .plan()
                        .files();
        Map<String, List<Long>> result = new LinkedHashMap<>();
        for (ManifestEntry entry : entries) {
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
        List<ManifestEntry> entries =
                table.store()
                        .newScan()
                        .withSnapshot(table.snapshotManager().latestSnapshot())
                        .plan()
                        .files();
        Map<String, List<Long>> result = new LinkedHashMap<>();
        for (ManifestEntry entry : entries) {
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
        BTreeGlobalIndexBuilder builder = new BTreeGlobalIndexBuilder(table).withIndexField("id");
        List<DataSplit> dataSplits =
                builder.scan()
                        .map(Pair::getRight)
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Expected scan result when building index."));
        List<CommitMessage> commitMessages = new ArrayList<>();
        for (DataSplit dataSplit : BTreeGlobalIndexBuilder.splitByContiguousRowRange(dataSplits)) {
            commitMessages.addAll(builder.build(dataSplit, ioManager));
        }
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(commitMessages);
        }
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
