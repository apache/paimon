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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.append.dataevolution.DataEvolutionCompactCoordinator;
import org.apache.paimon.append.dataevolution.DataEvolutionCompactTask;
import org.apache.paimon.append.dataevolution.DataEvolutionCompactionCommitPreparation;
import org.apache.paimon.append.dataevolution.DataEvolutionRowIdReassigner;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.BlobData;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.deletionvectors.BitmapDeletionVector;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.deletionvectors.append.BaseAppendDeleteFileMaintainer;
import org.apache.paimon.format.blob.BlobFileFormat;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.index.DeletionVectorMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.EndOfScanException;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RangeHelper;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.apache.paimon.table.BucketMode.UNAWARE_BUCKET;
import static org.apache.paimon.types.VectorType.isVectorStoreFile;
import static org.apache.paimon.utils.DataEvolutionUtils.retrieveAnchorFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Tests filename-anchored deletion vectors for data evolution tables. */
public class DataEvolutionDeletionVectorTest extends DataEvolutionTestBase {

    private static final Range FULL_RANGE = new Range(0, 14);
    private static final Range FIRST_RANGE = new Range(0, 4);
    private static final int VECTOR_DIM = 2;
    private static final List<DvSpec> DEFAULT_DV_SPECS =
            Arrays.asList(
                    new DvSpec(new Range(0, 4), 1, 4),
                    new DvSpec(new Range(5, 9), 6),
                    new DvSpec(new Range(10, 14), 10, 12));

    @Test
    public void testReadAfterDeletionVectors() throws Exception {
        // basic read
        createTableDefault();
        FileStoreTable table = getTableDefault();
        writeBaseRows(table);
        assertBaseFileLayout(table);
        commitDeletionVectors(table, DEFAULT_DV_SPECS);

        assertReadMatrix(getTableDefault(), "base");
    }

    @Test
    public void testReadAfterUpdatingDeletionVectors() throws Exception {
        // update DVs then read
        createTableDefault();
        FileStoreTable table = getTableDefault();
        writeBaseRows(table);
        assertBaseFileLayout(table);
        commitDeletionVectors(
                table,
                Arrays.asList(new DvSpec(new Range(0, 4), 1), new DvSpec(new Range(5, 9), 6)));
        table = getTableDefault();
        commitDeletionVectors(table, DEFAULT_DV_SPECS);

        assertReadMatrix(getTableDefault(), "base");
    }

    @Test
    public void testRowIdReassignKeepsAndMergesDeletionVectors() throws Exception {
        FileStoreTable table = createPartitionedReassignTable("reassign_dv_table", false);
        writePartitionRows(table, "a", 0, 1, 2);
        writePartitionRows(table, "b", 3, 4);
        writePartitionRows(table, "a", 5, 6, 7);

        BinaryRow partitionA = partition(table, "a");
        BinaryRow partitionB = partition(table, "b");
        commitDeletionVectors(
                table,
                partitionA,
                Arrays.asList(new DvSpec(new Range(0, 2), 1), new DvSpec(new Range(5, 7), 6)));
        commitDeletionVectors(
                table, partitionB, Collections.singletonList(new DvSpec(new Range(3, 4), 3)));

        long historicalSnapshotId = table.latestSnapshot().get().id();
        List<String> anchorFilesBefore = liveDeletionVectorDataFileNames(table);
        List<String> deletionVectorFilesBefore = liveDeletionVectorIndexFileNames(table);
        assertThat(readPartitionedRowsWithRowIds(table))
                .containsExactly("a|0|0", "a|2|2", "b|4|4", "a|5|5", "a|7|7");

        DataEvolutionRowIdReassigner.Result result =
                new DataEvolutionRowIdReassigner(table).reassign("test-reassign-dv");

        assertThat(result.firstAssignedRowId).isEqualTo(8L);
        assertThat(result.nextRowId).isEqualTo(14L);
        assertThat(readPartitionedRowsWithRowIds(table))
                .containsExactly("b|4|4", "a|0|8", "a|2|10", "a|5|11", "a|7|13");
        assertThat(liveDeletionVectorDataFileNames(table)).isEqualTo(anchorFilesBefore);
        assertThat(liveDeletionVectorIndexFileNames(table)).isEqualTo(deletionVectorFilesBefore);

        commitDeletionVectors(
                table,
                partitionA,
                Arrays.asList(new DvSpec(new Range(8, 10), 8), new DvSpec(new Range(11, 13), 13)));

        assertThat(readPartitionedRowsWithRowIds(table))
                .containsExactly("b|4|4", "a|2|10", "a|5|11");
        assertThat(liveDeletionVectorDataFileNames(table)).isEqualTo(anchorFilesBefore);

        FileStoreTable historicalTable =
                table.copy(
                        Collections.singletonMap(
                                CoreOptions.SCAN_SNAPSHOT_ID.key(),
                                String.valueOf(historicalSnapshotId)));
        assertThat(readPartitionedRowsWithRowIds(historicalTable))
                .containsExactly("a|0|0", "a|2|2", "b|4|4", "a|5|5", "a|7|7");
    }

    @Test
    public void testRowIdReassignKeepsPartialWriteAndBlobFileOffsets() throws Exception {
        FileStoreTable table = createPartitionedReassignTable("reassign_dedicated_dv_table", true);
        writeDedicatedPartitionRows(table, "a", 0, 1, 2, 3, 4);
        writeDedicatedPartitionRows(table, "b", 5, 6);
        writeDedicatedPartitionRows(table, "a", 7, 8, 9, 10, 11);
        writePartialStrings(table, "a", 0L, 0, 1, 2, 3, 4);

        BinaryRow partitionA = partition(table, "a");
        commitDeletionVectors(
                table,
                partitionA,
                Arrays.asList(new DvSpec(new Range(0, 4), 1), new DvSpec(new Range(7, 11), 8)));

        Map<String, Range> relativeRangesBefore = relativeFileRanges(table, partitionA);
        assertThat(relativeRangesBefore.entrySet())
                .anyMatch(
                        entry ->
                                BlobFileFormat.isBlobFile(entry.getKey())
                                        && entry.getValue().count() < 5
                                        && entry.getValue().from > 0);
        assertThat(normalFilesByRange(table).get(new Range(0, 4)))
                .hasSize(2)
                .anyMatch(
                        file ->
                                file.writeCols().contains("f1")
                                        && !file.writeCols().contains("f0"));

        new DataEvolutionRowIdReassigner(table).reassign("test-reassign-dedicated-dv");

        assertThat(relativeFileRanges(table, partitionA)).isEqualTo(relativeRangesBefore);
        assertThat(readPartitionedRowsWithRowIds(table))
                .containsExactly(
                        "b|5|base-5|5|5",
                        "b|6|base-6|6|6",
                        "a|0|updated-0|0|12",
                        "a|2|updated-2|2|14",
                        "a|3|updated-3|3|15",
                        "a|4|updated-4|4|16",
                        "a|7|base-7|7|17",
                        "a|9|base-9|9|19",
                        "a|10|base-10|10|20",
                        "a|11|base-11|11|21");
    }

    @Test
    public void testRowIdReassignAbortsAfterConcurrentDeletionVectorCommit() throws Exception {
        FileStoreTable table =
                createPartitionedReassignTable("reassign_concurrent_dv_table", false);
        writePartitionRows(table, "a", 0, 1, 2);
        writePartitionRows(table, "b", 3, 4);
        writePartitionRows(table, "a", 5, 6, 7);
        Snapshot before = table.latestSnapshot().get();
        BinaryRow partitionA = partition(table, "a");
        AtomicBoolean committed = new AtomicBoolean();

        DataEvolutionRowIdReassigner reassigner =
                reassignerWithBeforeCommit(
                        table,
                        () -> {
                            if (committed.compareAndSet(false, true)) {
                                try {
                                    commitDeletionVectors(
                                            table,
                                            partitionA,
                                            Collections.singletonList(
                                                    new DvSpec(new Range(0, 2), 1)));
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        });

        assertThatThrownBy(() -> reassigner.reassign("test-reassign-concurrent-dv"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("OVERWRITE snapshot");

        assertThat(committed).isTrue();
        assertThat(table.latestSnapshot().get().id()).isEqualTo(before.id() + 1);
        assertThat(table.latestSnapshot().get().commitKind())
                .isEqualTo(Snapshot.CommitKind.OVERWRITE);
        assertThat(readPartitionedRowsWithRowIds(table))
                .containsExactly("a|0|0", "a|2|2", "b|3|3", "b|4|4", "a|5|5", "a|6|6", "a|7|7");
    }

    @Test
    public void testStaleCompactionIsRejectedAfterRowIdReassign() throws Exception {
        FileStoreTable table = createPartitionedReassignTable("stale_compaction_dv_table", false);
        writePartitionRows(table, "a", 0, 1, 2);
        writePartitionRows(table, "b", 3, 4);
        writePartitionRows(table, "a", 5, 6, 7);
        writePartialStrings(table, "a", 0L, 0, 1, 2);

        BinaryRow partitionA = partition(table, "a");
        commitDeletionVectors(
                table, partitionA, Collections.singletonList(new DvSpec(new Range(0, 2), 1)));

        Snapshot compactSnapshot = table.latestSnapshot().get();
        Map<String, String> dynamicOptions = new HashMap<>();
        dynamicOptions.put(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "2");
        FileStoreTable compactTable = table.copy(dynamicOptions);
        DataEvolutionCompactCoordinator coordinator =
                new DataEvolutionCompactCoordinator(compactTable, false, false, compactSnapshot);
        List<CommitMessage> staleCompactMessages = new ArrayList<>();
        try {
            while (true) {
                for (DataEvolutionCompactTask task : coordinator.plan()) {
                    staleCompactMessages.add(task.doCompact(compactTable, "test-stale-compact"));
                }
            }
        } catch (EndOfScanException ignored) {
        }
        assertThat(staleCompactMessages).isNotEmpty();
        staleCompactMessages.addAll(
                new DataEvolutionCompactionCommitPreparation(compactTable, compactSnapshot)
                        .prepare(staleCompactMessages));

        DataEvolutionRowIdReassigner.Result result =
                new DataEvolutionRowIdReassigner(table).reassign("test-before-stale-compact");
        assertThat(result.firstAssignedRowId).isEqualTo(8L);
        List<String> reassignedRows =
                Arrays.asList("b|3|3", "b|4|4", "a|0|8", "a|2|10", "a|5|11", "a|6|12", "a|7|13");
        assertThat(readPartitionedRowsWithRowIds(table)).containsExactlyElementsOf(reassignedRows);
        long reassignSnapshotId = table.latestSnapshot().get().id();

        Throwable failure = catchThrowable(() -> commit(table, staleCompactMessages));

        assertThat(readPartitionedRowsWithRowIds(table))
                .as(
                        "stale compaction must not change reassigned row IDs; commit failure: %s",
                        failure)
                .containsExactlyElementsOf(reassignedRows);
        assertThat(table.latestSnapshot().get().id()).isEqualTo(reassignSnapshotId);
        assertThat(failure)
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Row ID existence conflict");
    }

    @Test
    public void testReadAfterAddingColumnAndDeletionVectors() throws Exception {
        // DVs with adding new columns.
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.STRING());
        schemaBuilder.column("f3", DataTypes.BLOB());
        schemaBuilder.option(CoreOptions.TARGET_FILE_SIZE.key(), "128 MB");
        schemaBuilder.option(CoreOptions.BLOB_TARGET_FILE_SIZE.key(), "1 b");
        schemaBuilder.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        catalog.createTable(identifier(), schemaBuilder.build(), true);

        FileStoreTable table = getTableDefault();
        for (int batch = 0; batch < 3; batch++) {
            BatchWriteBuilder builder = table.newBatchWriteBuilder();
            try (BatchTableWrite write = builder.newWrite();
                    BatchTableCommit commit = builder.newCommit()) {
                for (int rowId = batch * 5; rowId < batch * 5 + 5; rowId++) {
                    write.write(
                            GenericRow.of(
                                    rowId,
                                    BinaryString.fromString("name-" + rowId),
                                    new BlobData(new byte[] {(byte) rowId})));
                }
                commit.commit(write.prepareCommit());
            }
        }
        assertBaseFileLayout(table);
        commitDeletionVectors(table, DEFAULT_DV_SPECS);

        catalog.alterTable(
                identifier(),
                SchemaChange.addColumn(
                        "f2", DataTypes.STRING(), null, SchemaChange.Move.before("f2", "f3")),
                false);
        table = getTableDefault();
        updateStructuredColumn(table);

        assertReadMatrix(getTableDefault(), "updated");
    }

    @Test
    public void testDataEvolutionDeletionFilesDoNotLeakAcrossSplits() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        writeBaseRows(table);
        assertBaseFileLayout(table);
        updateStructuredColumn(table);
        commitDeletionVectors(table, Collections.singletonList(new DvSpec(new Range(0, 4), 1)));

        table = getTableDefault();
        Map<String, String> dynamicOptions = new HashMap<>();
        dynamicOptions.put(CoreOptions.SOURCE_SPLIT_TARGET_SIZE.key(), "1 B");
        dynamicOptions.put(CoreOptions.SOURCE_SPLIT_OPEN_FILE_COST.key(), "1 B");
        table = table.copy(dynamicOptions);

        ReadBuilder readBuilder = table.newReadBuilder();
        TableScan.Plan plan = readBuilder.newScan().plan();
        List<DataSplit> splits =
                plan.splits().stream()
                        .map(DataEvolutionDeletionVectorTest::toDataSplit)
                        .sorted(Comparator.comparingLong(split -> splitRowRange(split).from))
                        .collect(Collectors.toList());
        assertThat(splits).hasSize(3);
        assertDeletionFileRanges(splits.get(0), new Range(0, 4));
        assertDeletionFileRanges(splits.get(1));
        assertDeletionFileRanges(splits.get(2));
        assertThat(splits.get(1).mergedRowCount()).hasValue(5L);
        assertThat(splits.get(2).mergedRowCount()).hasValue(5L);

        assertThat(readRows(readBuilder, plan))
                .containsExactly(
                        "0|name-0|updated-0|0",
                        "2|name-2|updated-2|2",
                        "3|name-3|updated-3|3",
                        "4|name-4|updated-4|4",
                        "5|name-5|updated-5|5",
                        "6|name-6|updated-6|6",
                        "7|name-7|updated-7|7",
                        "8|name-8|updated-8|8",
                        "9|name-9|updated-9|9",
                        "10|name-10|updated-10|10",
                        "11|name-11|updated-11|11",
                        "12|name-12|updated-12|12",
                        "13|name-13|updated-13|13",
                        "14|name-14|updated-14|14");
    }

    @Test
    public void testLimitPushDownWithHeavilyDeletedFirstRange() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        writeBaseRows(table);
        assertBaseFileLayout(table);
        commitDeletionVectors(
                table, Collections.singletonList(new DvSpec(new Range(0, 4), 1, 2, 3, 4)));

        table = getTableDefault();
        Map<String, String> dynamicOptions = new HashMap<>();
        dynamicOptions.put(CoreOptions.SOURCE_SPLIT_TARGET_SIZE.key(), "1 B");
        dynamicOptions.put(CoreOptions.SOURCE_SPLIT_OPEN_FILE_COST.key(), "1 B");
        table = table.copy(dynamicOptions);

        ReadBuilder readBuilder = table.newReadBuilder().withLimit(2);
        TableScan.Plan plan = readBuilder.newScan().plan();
        List<DataSplit> splits =
                plan.splits().stream()
                        .map(DataEvolutionDeletionVectorTest::toDataSplit)
                        .sorted(Comparator.comparingLong(split -> splitRowRange(split).from))
                        .collect(Collectors.toList());

        // Limit pushdown works at split level. If the first split's DV cardinality is ignored,
        // limit=2 would incorrectly keep only the first split even though it has one visible row.
        assertThat(splits).hasSize(2);
        assertThat(splits.get(0).mergedRowCount()).hasValue(1L);
        assertThat(splits.get(1).mergedRowCount()).hasValue(5L);
        assertThat(readRows(table.newReadBuilder(), plan))
                .containsExactly(
                        "0|name-0|base-0|0",
                        "5|name-5|base-5|5",
                        "6|name-6|base-6|6",
                        "7|name-7|base-7|7",
                        "8|name-8|base-8|8",
                        "9|name-9|base-9|9");
    }

    @Test
    public void testCompactRewritesDeletionVectors() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        writeBaseRows(table);
        updateStructuredColumn(table);
        commitDeletionVectors(table, DEFAULT_DV_SPECS);
        List<String> oldAnchorFiles = new ArrayList<>(anchorFilesByRange(table).values());

        Map<String, String> dynamicOptions = new HashMap<>();
        dynamicOptions.put(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "2");
        compactDataEvolutionTable(getTableDefault().copy(dynamicOptions), false);

        table = getTableDefault();
        assertRegularFileRowRanges(
                table.store().newScan().plan().files().stream()
                        .map(ManifestEntry::file)
                        .collect(Collectors.toList()),
                Collections.singletonList(FULL_RANGE));
        assertRowsAndProjections(table, "updated");

        DataSplit fullRangeSplit = planDataSplit(table, FULL_RANGE);
        assertDeletionFileRanges(fullRangeSplit, FULL_RANGE);
        assertThat(fullRangeSplit.mergedRowCount()).hasValue(10L);
        String newAnchorFile = anchorFilesByRange(table).get(FULL_RANGE);
        List<String> liveDeletionVectorDataFileNames = liveDeletionVectorDataFileNames(table);
        assertThat(liveDeletionVectorDataFileNames).containsExactly(newAnchorFile);
        assertThat(liveDeletionVectorDataFileNames).doesNotContainAnyElementsOf(oldAnchorFiles);
    }

    @Test
    public void testCompactRenamesDeletionVectorForSameRowRange() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        writeBaseRows(table);
        updateStructuredColumn(table);
        commitDeletionVectors(table, Collections.singletonList(new DvSpec(FIRST_RANGE, 1, 4)));
        String oldAnchorFile = anchorFilesByRange(table).get(FIRST_RANGE);

        Map<String, String> dynamicOptions = new HashMap<>();
        dynamicOptions.put(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "2");
        dynamicOptions.put(CoreOptions.TARGET_FILE_SIZE.key(), "1 B");
        compactDataEvolutionTable(getTableDefault().copy(dynamicOptions), false);

        table = getTableDefault();
        assertRegularFileRowRanges(
                table.store().newScan().plan().files().stream()
                        .map(ManifestEntry::file)
                        .collect(Collectors.toList()),
                Arrays.asList(new Range(0, 4), new Range(5, 9), new Range(10, 14)));
        assertThat(readRows(table.newReadBuilder()))
                .containsExactlyElementsOf(expectedRowsExcluding("updated", FULL_RANGE, 1, 4));

        DataSplit firstRangeSplit = planDataSplit(table, FIRST_RANGE);
        assertDeletionFileRanges(firstRangeSplit, FIRST_RANGE);
        assertThat(firstRangeSplit.mergedRowCount()).hasValue(3L);
        String newAnchorFile = anchorFilesByRange(table).get(FIRST_RANGE);
        assertThat(newAnchorFile).isNotEqualTo(oldAnchorFile);
        assertThat(liveDeletionVectorDataFileNames(table)).containsExactly(newAnchorFile);
    }

    @Test
    public void testCompactRewritesOnlyExistingDeletionVectors() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        writeBaseRows(table);
        updateStructuredColumn(table);
        commitDeletionVectors(table, Collections.singletonList(new DvSpec(new Range(5, 9), 6)));

        Map<String, String> dynamicOptions = new HashMap<>();
        dynamicOptions.put(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "2");
        compactDataEvolutionTable(getTableDefault().copy(dynamicOptions), false);

        table = getTableDefault();
        assertRegularFileRowRanges(
                table.store().newScan().plan().files().stream()
                        .map(ManifestEntry::file)
                        .collect(Collectors.toList()),
                Collections.singletonList(FULL_RANGE));
        assertThat(readRows(table.newReadBuilder()))
                .containsExactlyElementsOf(expectedRowsExcluding("updated", FULL_RANGE, 6));

        DataSplit fullRangeSplit = planDataSplit(table, FULL_RANGE);
        assertDeletionFileRanges(fullRangeSplit, FULL_RANGE);
        assertThat(fullRangeSplit.mergedRowCount()).hasValue(14L);
        assertThat(liveDeletionVectorDataFileNames(table))
                .containsExactly(anchorFilesByRange(table).get(FULL_RANGE));
    }

    @Test
    public void testCompactWithoutDeletionVectors() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        writeBaseRows(table);
        updateStructuredColumn(table);

        Map<String, String> dynamicOptions = new HashMap<>();
        dynamicOptions.put(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "2");
        compactDataEvolutionTable(getTableDefault().copy(dynamicOptions), false);

        table = getTableDefault();
        assertRegularFileRowRanges(
                table.store().newScan().plan().files().stream()
                        .map(ManifestEntry::file)
                        .collect(Collectors.toList()),
                Collections.singletonList(FULL_RANGE));
        assertThat(readRows(table.newReadBuilder()))
                .containsExactlyElementsOf(expectedRowsExcluding("updated", FULL_RANGE));

        DataSplit fullRangeSplit = planDataSplit(table, FULL_RANGE);
        assertDeletionFileRanges(fullRangeSplit);
        assertThat(fullRangeSplit.mergedRowCount()).hasValue(15L);
        assertThat(liveDeletionVectorDataFileNames(table)).isEmpty();
    }

    @Test
    public void testCompactMaterializesDeletionVectors() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        writeBaseRows(table);
        updateStructuredColumn(table);
        commitDeletionVectors(table, DEFAULT_DV_SPECS);
        List<String> oldAnchorFiles = new ArrayList<>(anchorFilesByRange(table).values());

        Map<String, String> dynamicOptions = new HashMap<>();
        dynamicOptions.put(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "2");
        dynamicOptions.put(CoreOptions.DATA_EVOLUTION_COMPACTION_REWRITE_ROW_IDS.key(), "true");
        compactDataEvolutionTable(getTableDefault().copy(dynamicOptions), false);

        table = getTableDefault();
        List<String> expectedRows = expectedRows("updated", FULL_RANGE);
        assertThat(readRows(table.newReadBuilder())).containsExactlyElementsOf(expectedRows);
        assertThat(readProjectedStrings(table.newReadBuilder().withProjection(new int[] {2})))
                .containsExactlyElementsOf(expectedProjectedStrings("updated", FULL_RANGE));
        assertThat(readProjectedBlobValues(table.newReadBuilder().withProjection(new int[] {3})))
                .containsExactlyElementsOf(expectedBlobValues(FULL_RANGE));

        List<Range> materializedRanges = normalFileRowRanges(table);
        assertThat(materializedRanges).containsExactly(new Range(15, 24));
        // blobs should be compacted to a single range too.
        assertBlobFileRowRanges(table, Collections.singletonList(new Range(15, 24)));
        DataSplit materializedSplit = planDataSplit(table, materializedRanges.get(0));
        assertDeletionFileRanges(materializedSplit);
        assertThat(materializedSplit.mergedRowCount()).hasValue(10L);
        assertThat(
                        readRows(
                                table.newReadBuilder()
                                        .withRowRanges(
                                                Collections.singletonList(
                                                        materializedRanges.get(0)))))
                .containsExactlyElementsOf(expectedRows);
        assertThat(liveDeletionVectorDataFileNames(table)).isEmpty();
        assertThat(liveDeletionVectorDataFileNames(table))
                .doesNotContainAnyElementsOf(oldAnchorFiles);
    }

    @Test
    public void testMaterializeCompactionUsesRemainingSizeForLargeDeletedRange() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        writeRowsWithLargeFirstRange(table);
        commitDeletionVectors(
                table, Collections.singletonList(new DvSpec(FIRST_RANGE, 1, 2, 3, 4)));

        Map<Range, List<DataFileMeta>> normalFilesByRange = normalFilesByRange(table);
        List<DataFileMeta> firstRangeFiles = normalFilesByRange.get(FIRST_RANGE);
        List<DataFileMeta> secondRangeFiles = normalFilesByRange.get(new Range(5, 9));
        long firstRangeWeight = fileWeight(firstRangeFiles);
        long estimatedFirstRangeWeight = estimatedFileWeight(firstRangeFiles, 1D / 5);
        long targetFileSize =
                estimatedFirstRangeWeight + Math.max(1L, fileWeight(secondRangeFiles) / 2);
        assertThat(targetFileSize).isLessThan(firstRangeWeight);

        Map<String, String> dynamicOptions = new HashMap<>();
        dynamicOptions.put(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "2");
        dynamicOptions.put(CoreOptions.TARGET_FILE_SIZE.key(), targetFileSize + " B");
        dynamicOptions.put(CoreOptions.SOURCE_SPLIT_OPEN_FILE_COST.key(), "1 B");
        dynamicOptions.put(CoreOptions.DATA_EVOLUTION_COMPACTION_REWRITE_ROW_IDS.key(), "true");
        compactDataEvolutionTable(getTableDefault().copy(dynamicOptions), false);

        table = getTableDefault();
        // The compacted rows are assigned new row-tracking ids, while f0 keeps original values.
        assertThat(normalFileRowRanges(table))
                .containsExactly(new Range(10, 14), new Range(15, 20));
        assertBlobFileRowRanges(
                table,
                Arrays.asList(
                        new Range(10, 10),
                        new Range(11, 11),
                        new Range(12, 12),
                        new Range(13, 13),
                        new Range(14, 14),
                        new Range(15, 20)));
        assertThat(readF0Values(table.newReadBuilder()))
                .containsExactly(0, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14);
        assertThat(liveDeletionVectorDataFileNames(table)).isEmpty();
    }

    @Test
    public void testMaterializeCompactionMergesSmallFilesWithInterleavedDeletionVectors()
            throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        writeBaseRows(table);
        updateStructuredColumn(table);
        commitDeletionVectors(
                table,
                Arrays.asList(new DvSpec(FIRST_RANGE, 1), new DvSpec(new Range(10, 14), 12)));

        Map<String, String> dynamicOptions = new HashMap<>();
        dynamicOptions.put(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "2");
        dynamicOptions.put(CoreOptions.DATA_EVOLUTION_COMPACTION_REWRITE_ROW_IDS.key(), "true");
        compactDataEvolutionTable(getTableDefault().copy(dynamicOptions), false);

        table = getTableDefault();
        Range materializedRange = new Range(15, 27);
        assertThat(normalFileRowRanges(table)).containsExactly(materializedRange);
        assertBlobFileRowRanges(table, Collections.singletonList(materializedRange));
        assertThat(readRows(table.newReadBuilder()))
                .containsExactlyElementsOf(expectedRowsExcluding("updated", FULL_RANGE, 1, 12));
        DataSplit split = planDataSplit(table, materializedRange);
        assertDeletionFileRanges(split);
        assertThat(split.mergedRowCount()).hasValue(13L);
        assertThat(liveDeletionVectorDataFileNames(table)).isEmpty();
    }

    @Test
    public void testMaterializeCompactionHandlesFullyDeletedRange() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        writeBaseRows(table);
        updateStructuredColumn(table);
        commitDeletionVectors(
                table, Collections.singletonList(new DvSpec(FIRST_RANGE, 0, 1, 2, 3, 4)));

        Map<String, String> dynamicOptions = new HashMap<>();
        dynamicOptions.put(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "2");
        dynamicOptions.put(CoreOptions.DATA_EVOLUTION_COMPACTION_REWRITE_ROW_IDS.key(), "true");
        compactDataEvolutionTable(getTableDefault().copy(dynamicOptions), false);

        table = getTableDefault();
        Range materializedRange = new Range(15, 24);
        assertThat(normalFileRowRanges(table)).containsExactly(materializedRange);
        assertBlobFileRowRanges(table, Collections.singletonList(materializedRange));
        assertThat(readRows(table.newReadBuilder()))
                .containsExactlyElementsOf(
                        expectedRowsExcluding("updated", FULL_RANGE, 0, 1, 2, 3, 4));
        DataSplit split = planDataSplit(table, materializedRange);
        assertDeletionFileRanges(split);
        assertThat(split.mergedRowCount()).hasValue(10L);
        assertThat(liveDeletionVectorDataFileNames(table)).isEmpty();
    }

    @Test
    public void testMaterializeCompactionFailsFastForDedicatedVectorFiles() throws Exception {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.VECTOR(VECTOR_DIM, DataTypes.FLOAT()));
        schemaBuilder.option(CoreOptions.TARGET_FILE_SIZE.key(), "128 MB");
        schemaBuilder.option(CoreOptions.VECTOR_TARGET_FILE_SIZE.key(), "128 MB");
        schemaBuilder.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.VECTOR_FIELD.key(), "f1");
        schemaBuilder.option(CoreOptions.VECTOR_FILE_FORMAT.key(), "json");
        schemaBuilder.option(CoreOptions.FILE_COMPRESSION.key(), "none");
        catalog.createTable(identifier("vector_dv_table"), schemaBuilder.build(), true);

        FileStoreTable table = getTable(identifier("vector_dv_table"));
        for (int batch = 0; batch < 2; batch++) {
            BatchWriteBuilder builder = table.newBatchWriteBuilder();
            try (BatchTableWrite write = builder.newWrite();
                    BatchTableCommit commit = builder.newCommit()) {
                for (int rowId = batch * 5; rowId < batch * 5 + 5; rowId++) {
                    write.write(
                            GenericRow.of(
                                    rowId,
                                    BinaryVector.fromPrimitiveArray(
                                            new float[] {rowId, rowId + 0.5F})));
                }
                commit.commit(write.prepareCommit());
            }
        }
        assertThat(
                        table.store().newScan().plan().files().stream()
                                .map(ManifestEntry::file)
                                .anyMatch(file -> isVectorStoreFile(file.fileName())))
                .isTrue();
        commitDeletionVectors(table, Collections.singletonList(new DvSpec(FIRST_RANGE, 1)));

        Map<String, String> dynamicOptions = new HashMap<>();
        dynamicOptions.put(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "2");
        dynamicOptions.put(CoreOptions.DATA_EVOLUTION_COMPACTION_REWRITE_ROW_IDS.key(), "true");
        assertThatThrownBy(() -> compactDataEvolutionTable(table.copy(dynamicOptions), false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Materializing deletion vectors for vector-store files is not supported.");
    }

    @Test
    public void testBlobCompactKeepsDeletionVectors() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        writeBaseRows(table);
        commitDeletionVectors(table, DEFAULT_DV_SPECS);

        Map<String, String> dynamicOptions = new HashMap<>();
        dynamicOptions.put(CoreOptions.BLOB_TARGET_FILE_SIZE.key(), "128 MB");
        compactDataEvolutionTable(getTableDefault().copy(dynamicOptions), true);

        table = getTableDefault();
        assertRowsAndProjections(table, "base");
        assertFirstBlobFileRowRanges(table, Arrays.asList(new Range(0, 4), new Range(5, 9)), 3);
        assertDeletionFileRanges(
                planDataSplit(table, FULL_RANGE),
                new Range(0, 4),
                new Range(5, 9),
                new Range(10, 14));
    }

    private FileStoreTable createPartitionedReassignTable(String tableName, boolean dedicated)
            throws Exception {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("pt", DataTypes.STRING());
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.STRING());
        if (dedicated) {
            schemaBuilder.column("f2", DataTypes.BLOB());
            schemaBuilder.option(CoreOptions.BLOB_TARGET_FILE_SIZE.key(), "1 b");
            schemaBuilder.option(CoreOptions.FILE_COMPRESSION.key(), "none");
        }
        schemaBuilder.partitionKeys("pt");
        schemaBuilder.option(CoreOptions.TARGET_FILE_SIZE.key(), "128 MB");
        schemaBuilder.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        catalog.createTable(identifier(tableName), schemaBuilder.build(), false);
        return getTable(identifier(tableName));
    }

    private void writePartitionRows(FileStoreTable table, String partition, int... values)
            throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            for (int value : values) {
                write.write(
                        GenericRow.of(
                                BinaryString.fromString(partition),
                                value,
                                BinaryString.fromString("base-" + value)));
            }
            commit.commit(write.prepareCommit());
        }
    }

    private void writeDedicatedPartitionRows(FileStoreTable table, String partition, int... values)
            throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            for (int value : values) {
                write.write(
                        GenericRow.of(
                                BinaryString.fromString(partition),
                                value,
                                BinaryString.fromString("base-" + value),
                                new BlobData(new byte[] {(byte) value})));
            }
            commit.commit(write.prepareCommit());
        }
    }

    private void writePartialStrings(
            FileStoreTable table, String partition, long firstRowId, int... values)
            throws Exception {
        RowType writeType = table.rowType().project(Arrays.asList("pt", "f1"));
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite().withWriteType(writeType);
                BatchTableCommit commit = builder.newCommit()) {
            for (int value : values) {
                write.write(
                        GenericRow.of(
                                BinaryString.fromString(partition),
                                BinaryString.fromString("updated-" + value)));
            }
            List<CommitMessage> messages = write.prepareCommit();
            setFirstRowId(messages, firstRowId);
            commit.commit(messages);
        }
    }

    private static BinaryRow partition(FileStoreTable table, String value) {
        return new InternalRowSerializer(table.schema().logicalPartitionType())
                .toBinaryRow(GenericRow.of(BinaryString.fromString(value)));
    }

    private static List<String> readPartitionedRowsWithRowIds(FileStoreTable table)
            throws IOException {
        RowType readType = SpecialFields.rowTypeWithRowId(table.rowType());
        int rowIdIndex = table.rowType().getFieldCount();
        boolean withBlob = rowIdIndex == 4;
        ReadBuilder readBuilder = table.newReadBuilder().withReadType(readType);
        List<String> rows = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan())) {
            reader.forEachRemaining(
                    row -> {
                        String value = row.getString(0) + "|" + row.getInt(1);
                        if (withBlob) {
                            value +=
                                    "|"
                                            + row.getString(2)
                                            + "|"
                                            + (row.getBlob(3).toData()[0] & 0xFF);
                        }
                        rows.add(value + "|" + row.getLong(rowIdIndex));
                    });
        }
        rows.sort(
                Comparator.comparingLong(
                        row -> Long.parseLong(row.substring(row.lastIndexOf('|') + 1))));
        return rows;
    }

    private static Map<String, Range> relativeFileRanges(
            FileStoreTable table, BinaryRow partition) {
        List<DataFileMeta> dataFiles = currentDataFiles(table, partition);
        RangeHelper<DataFileMeta> rangeHelper = new RangeHelper<>(DataFileMeta::nonNullRowIdRange);
        Map<String, Range> result = new HashMap<>();
        for (List<DataFileMeta> group : rangeHelper.mergeOverlappingRanges(dataFiles)) {
            Range anchorRange = retrieveAnchorFile(group, file -> file).nonNullRowIdRange();
            for (DataFileMeta file : group) {
                Range range = file.nonNullRowIdRange();
                result.put(
                        file.fileName(),
                        new Range(range.from - anchorRange.from, range.to - anchorRange.from));
            }
        }
        return result;
    }

    private static List<DataFileMeta> currentDataFiles(FileStoreTable table, BinaryRow partition) {
        return table.store().newScan().plan().files().stream()
                .filter(entry -> entry.partition().equals(partition))
                .map(ManifestEntry::file)
                .collect(Collectors.toList());
    }

    private static DataEvolutionRowIdReassigner reassignerWithBeforeCommit(
            FileStoreTable table, Runnable beforeCommit) throws Exception {
        Constructor<DataEvolutionRowIdReassigner> constructor =
                DataEvolutionRowIdReassigner.class.getDeclaredConstructor(
                        FileStoreTable.class, PartitionPredicate.class, Runnable.class);
        constructor.setAccessible(true);
        return constructor.newInstance(table, null, beforeCommit);
    }

    @Override
    protected Schema schemaDefault() {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.STRING());
        schemaBuilder.column("f2", DataTypes.STRING());
        schemaBuilder.column("f3", DataTypes.BLOB());
        schemaBuilder.option(CoreOptions.TARGET_FILE_SIZE.key(), "128 MB");
        schemaBuilder.option(CoreOptions.BLOB_TARGET_FILE_SIZE.key(), "1 b");
        schemaBuilder.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        return schemaBuilder.build();
    }

    private void writeBaseRows(FileStoreTable table) throws Exception {
        for (int batch = 0; batch < 3; batch++) {
            BatchWriteBuilder builder = table.newBatchWriteBuilder();
            try (BatchTableWrite write = builder.newWrite();
                    BatchTableCommit commit = builder.newCommit()) {
                for (int rowId = batch * 5; rowId < batch * 5 + 5; rowId++) {
                    write.write(
                            GenericRow.of(
                                    rowId,
                                    BinaryString.fromString("name-" + rowId),
                                    BinaryString.fromString("base-" + rowId),
                                    new BlobData(new byte[] {(byte) rowId})));
                }
                commit.commit(write.prepareCommit());
            }
        }
    }

    private void writeRowsWithLargeFirstRange(FileStoreTable table) throws Exception {
        for (int batch = 0; batch < 3; batch++) {
            BatchWriteBuilder builder = table.newBatchWriteBuilder();
            try (BatchTableWrite write = builder.newWrite();
                    BatchTableCommit commit = builder.newCommit()) {
                for (int rowId = batch * 5; rowId < batch * 5 + 5; rowId++) {
                    write.write(
                            GenericRow.of(
                                    rowId,
                                    BinaryString.fromString(
                                            batch == 0 ? largeString(rowId) : "name-" + rowId),
                                    BinaryString.fromString("base-" + rowId),
                                    new BlobData(new byte[] {(byte) rowId})));
                }
                commit.commit(write.prepareCommit());
            }
        }
    }

    private static String largeString(int rowId) {
        StringBuilder builder = new StringBuilder(32 * 1024);
        long value = rowId + 17L;
        for (int i = 0; i < 32 * 1024; i++) {
            value = value * 1103515245 + 12345;
            builder.append((char) ('a' + ((value >>> 16) % 26)));
        }
        return builder.toString();
    }

    private void updateStructuredColumn(FileStoreTable table) throws Exception {
        RowType writeType = table.rowType().project(Collections.singletonList("f2"));
        for (int batch = 0; batch < 3; batch++) {
            BatchWriteBuilder builder = table.newBatchWriteBuilder();
            try (BatchTableWrite write = builder.newWrite().withWriteType(writeType);
                    BatchTableCommit commit = builder.newCommit()) {
                long firstRowId = batch * 5L;
                for (int rowId = batch * 5; rowId < batch * 5 + 5; rowId++) {
                    write.write(GenericRow.of(BinaryString.fromString("updated-" + rowId)));
                }
                List<CommitMessage> commitables = write.prepareCommit();
                setFirstRowId(commitables, firstRowId);
                commit.commit(commitables);
            }
        }
    }

    private void compactDataEvolutionTable(FileStoreTable table, boolean compactBlob)
            throws Exception {
        Snapshot snapshot = table.latestSnapshot().get();
        DataEvolutionCompactCoordinator coordinator =
                new DataEvolutionCompactCoordinator(table, compactBlob, false, snapshot);
        List<CommitMessage> commitMessages = new ArrayList<>();
        try {
            while (true) {
                for (DataEvolutionCompactTask task : coordinator.plan()) {
                    commitMessages.add(task.doCompact(table, "test-compact"));
                }
            }
        } catch (EndOfScanException ignored) {
        }
        assertThat(commitMessages).isNotEmpty();

        commitMessages.addAll(
                new DataEvolutionCompactionCommitPreparation(table, snapshot)
                        .prepare(commitMessages));
        commit(table, commitMessages);
    }

    private void commitDeletionVectors(FileStoreTable table, List<DvSpec> deletionVectorSpecs)
            throws Exception {
        commitDeletionVectors(table, BinaryRow.EMPTY_ROW, deletionVectorSpecs);
    }

    private void commitDeletionVectors(
            FileStoreTable table, BinaryRow partition, List<DvSpec> deletionVectorSpecs)
            throws Exception {
        BaseAppendDeleteFileMaintainer maintainer =
                BaseAppendDeleteFileMaintainer.forUnawareAppend(
                        table.store().newIndexFileHandler(),
                        table.latestSnapshot().get(),
                        partition);
        Map<Range, String> anchorFiles = anchorFilesByRange(table, partition);

        for (DvSpec spec : deletionVectorSpecs) {
            DeletionVector deletionVector = new BitmapDeletionVector();
            for (long rowId : spec.deletedRowIds) {
                deletionVector.delete(rowId - spec.range.from);
            }
            maintainer.notifyNewDeletionVector(anchorFiles.get(spec.range), deletionVector);
        }

        List<IndexFileMeta> newIndexFiles = new ArrayList<>();
        List<IndexFileMeta> deletedIndexFiles = new ArrayList<>();
        for (IndexManifestEntry entry : maintainer.persist()) {
            if (entry.kind() == FileKind.ADD) {
                newIndexFiles.add(entry.indexFile());
            } else if (entry.kind() == FileKind.DELETE) {
                deletedIndexFiles.add(entry.indexFile());
            }
        }

        commit(
                table,
                Collections.singletonList(
                        new CommitMessageImpl(
                                partition,
                                UNAWARE_BUCKET,
                                null,
                                new DataIncrement(
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        newIndexFiles,
                                        deletedIndexFiles),
                                CompactIncrement.emptyIncrement())));
    }

    private static void commit(FileStoreTable table, List<CommitMessage> commitMessages)
            throws Exception {
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(commitMessages);
        }
    }

    private Map<Range, String> anchorFilesByRange(FileStoreTable table) {
        return anchorFilesByRange(table, BinaryRow.EMPTY_ROW);
    }

    private Map<Range, String> anchorFilesByRange(FileStoreTable table, BinaryRow partition) {
        List<DataFileMeta> dataFiles = currentDataFiles(table, partition);
        RangeHelper<DataFileMeta> rangeHelper = new RangeHelper<>(DataFileMeta::nonNullRowIdRange);
        Map<Range, String> result = new HashMap<>();
        for (List<DataFileMeta> group : rangeHelper.mergeOverlappingRanges(dataFiles)) {
            DataFileMeta anchor = retrieveAnchorFile(group, file -> file);
            result.put(anchor.nonNullRowIdRange(), anchor.fileName());
        }
        return result;
    }

    private static void assertReadMatrix(FileStoreTable table, String structuredValuePrefix)
            throws Exception {
        assertRowsAndProjections(table, structuredValuePrefix);

        DataSplit fullRangeSplit = planDataSplit(table, FULL_RANGE);
        assertDeletionFileRanges(
                fullRangeSplit, new Range(0, 4), new Range(5, 9), new Range(10, 14));
        assertThat(fullRangeSplit.mergedRowCount()).hasValue(10L);
        assertThat(planDataSplit(table, FIRST_RANGE).mergedRowCount()).hasValue(3L);
    }

    private static void assertRowsAndProjections(FileStoreTable table, String structuredValuePrefix)
            throws Exception {
        List<String> expectedRows = expectedRows(structuredValuePrefix, FULL_RANGE);
        List<String> expectedFirstRangeRows = expectedRows(structuredValuePrefix, FIRST_RANGE);
        List<String> expectedProjectedStrings =
                expectedProjectedStrings(structuredValuePrefix, FULL_RANGE);
        List<Integer> expectedBlobValues = expectedBlobValues(FULL_RANGE);

        assertThat(readRows(table.newReadBuilder())).containsExactlyElementsOf(expectedRows);
        assertThat(
                        readRows(
                                table.newReadBuilder()
                                        .withRowRanges(Collections.singletonList(FULL_RANGE))))
                .containsExactlyElementsOf(expectedRows);
        assertThat(
                        readRows(
                                table.newReadBuilder()
                                        .withRowRanges(Collections.singletonList(FIRST_RANGE))))
                .containsExactlyElementsOf(expectedFirstRangeRows);
        assertThat(
                        readRows(
                                table.newReadBuilder()
                                        .withRowRanges(Collections.singletonList(new Range(4, 4)))))
                .isEmpty();
        assertThat(
                        readRows(
                                table.newReadBuilder()
                                        .withRowRanges(Collections.singletonList(new Range(7, 7)))))
                .containsExactly(expectedRow(structuredValuePrefix, 7));

        assertThat(readProjectedStrings(table.newReadBuilder().withProjection(new int[] {2})))
                .containsExactlyElementsOf(expectedProjectedStrings);
        assertThat(
                        readProjectedStrings(
                                table.newReadBuilder()
                                        .withProjection(new int[] {2})
                                        .withRowRanges(Collections.singletonList(FULL_RANGE))))
                .containsExactlyElementsOf(expectedProjectedStrings);
        assertThat(readProjectedBlobValues(table.newReadBuilder().withProjection(new int[] {3})))
                .containsExactlyElementsOf(expectedBlobValues);
        assertThat(
                        readProjectedBlobValues(
                                table.newReadBuilder()
                                        .withProjection(new int[] {3})
                                        .withRowRanges(Collections.singletonList(FULL_RANGE))))
                .containsExactlyElementsOf(expectedBlobValues);
    }

    private static void assertDeletionFileRanges(DataSplit split, Range... expectedRanges) {
        if (!split.deletionFiles().isPresent()) {
            assertThat(expectedRanges).isEmpty();
            return;
        }

        List<DeletionFile> deletionFiles = split.deletionFiles().get();
        assertThat(deletionFiles).hasSize(split.dataFiles().size());

        Map<Range, DeletionFile> actual = new HashMap<>();
        RangeHelper<DataFileMeta> rangeHelper = new RangeHelper<>(DataFileMeta::nonNullRowIdRange);
        for (List<DataFileMeta> group : rangeHelper.mergeOverlappingRanges(split.dataFiles())) {
            DataFileMeta anchor = retrieveAnchorFile(group, file -> file);
            DeletionFile deletionFile = deletionFiles.get(split.dataFiles().indexOf(anchor));
            if (deletionFile != null) {
                actual.put(anchor.nonNullRowIdRange(), deletionFile);
            }
        }

        assertThat(deletionFiles.stream().filter(file -> file != null).count())
                .isEqualTo((long) expectedRanges.length);
        assertThat(actual.keySet()).containsExactlyInAnyOrder(expectedRanges);
    }

    private static List<String> expectedRows(String structuredValuePrefix, Range range) {
        List<String> rows = new ArrayList<>();
        for (int rowId = (int) range.from; rowId <= range.to; rowId++) {
            if (!isDeletedByDefaultDv(rowId)) {
                rows.add(expectedRow(structuredValuePrefix, rowId));
            }
        }
        return rows;
    }

    private static List<String> expectedRowsExcluding(
            String structuredValuePrefix, Range range, long... deletedRowIds) {
        List<String> rows = new ArrayList<>();
        for (int rowId = (int) range.from; rowId <= range.to; rowId++) {
            if (!isDeleted(rowId, deletedRowIds)) {
                rows.add(expectedRow(structuredValuePrefix, rowId));
            }
        }
        return rows;
    }

    private static List<String> expectedProjectedStrings(
            String structuredValuePrefix, Range range) {
        List<String> rows = new ArrayList<>();
        for (int rowId = (int) range.from; rowId <= range.to; rowId++) {
            if (!isDeletedByDefaultDv(rowId)) {
                rows.add(structuredValuePrefix + "-" + rowId);
            }
        }
        return rows;
    }

    private static List<Integer> expectedBlobValues(Range range) {
        List<Integer> rows = new ArrayList<>();
        for (int rowId = (int) range.from; rowId <= range.to; rowId++) {
            if (!isDeletedByDefaultDv(rowId)) {
                rows.add(rowId);
            }
        }
        return rows;
    }

    private static boolean isDeletedByDefaultDv(int rowId) {
        for (DvSpec spec : DEFAULT_DV_SPECS) {
            for (long deletedRowId : spec.deletedRowIds) {
                if (deletedRowId == rowId) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean isDeleted(int rowId, long... deletedRowIds) {
        for (long deletedRowId : deletedRowIds) {
            if (deletedRowId == rowId) {
                return true;
            }
        }
        return false;
    }

    private static String expectedRow(String structuredValuePrefix, int rowId) {
        return rowId + "|name-" + rowId + "|" + structuredValuePrefix + "-" + rowId + "|" + rowId;
    }

    private static void assertBaseFileLayout(FileStoreTable table) {
        assertRegularFileRowRanges(
                table.store().newScan().plan().files().stream()
                        .map(ManifestEntry::file)
                        .collect(Collectors.toList()),
                Arrays.asList(new Range(0, 4), new Range(5, 9), new Range(10, 14)));
        assertFirstBlobFileRowRanges(
                table, Arrays.asList(new Range(0, 0), new Range(1, 1), new Range(2, 2)), 15);
    }

    private static DataSplit planDataSplit(FileStoreTable table, Range range) {
        ReadBuilder readBuilder =
                table.newReadBuilder().withRowRanges(Collections.singletonList(range));
        TableScan.Plan plan = readBuilder.newScan().plan();
        assertThat(plan.splits()).hasSize(1);
        return toDataSplit(plan.splits().get(0));
    }

    private static List<String> liveDeletionVectorDataFileNames(FileStoreTable table) {
        List<String> result = new ArrayList<>();
        for (IndexManifestEntry entry :
                table.store()
                        .newIndexFileHandler()
                        .scan(table.latestSnapshot().get(), DELETION_VECTORS_INDEX)) {
            Map<String, DeletionVectorMeta> dvRanges = entry.indexFile().dvRanges();
            if (dvRanges != null) {
                for (DeletionVectorMeta meta : dvRanges.values()) {
                    result.add(meta.dataFileName());
                }
            }
        }
        Collections.sort(result);
        return result;
    }

    private static List<String> liveDeletionVectorIndexFileNames(FileStoreTable table) {
        List<String> result = new ArrayList<>();
        for (IndexManifestEntry entry :
                table.store()
                        .newIndexFileHandler()
                        .scan(table.latestSnapshot().get(), DELETION_VECTORS_INDEX)) {
            result.add(entry.indexFile().fileName());
        }
        Collections.sort(result);
        return result;
    }

    private static List<String> readRows(ReadBuilder readBuilder, TableScan.Plan plan)
            throws IOException {
        List<String> rows = new ArrayList<>();
        try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(plan)) {
            reader.forEachRemaining(row -> rows.add(formatRow(row)));
        }
        rows.sort(Comparator.comparingInt(DataEvolutionDeletionVectorTest::rowId));
        return rows;
    }

    private static List<String> readRows(ReadBuilder readBuilder) throws IOException {
        return readRows(readBuilder, readBuilder.newScan().plan());
    }

    private static List<Integer> readF0Values(ReadBuilder readBuilder) throws IOException {
        List<Integer> values = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan())) {
            reader.forEachRemaining(row -> values.add(row.getInt(0)));
        }
        Collections.sort(values);
        return values;
    }

    private static List<String> readProjectedStrings(ReadBuilder readBuilder) throws IOException {
        List<String> rows = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan())) {
            reader.forEachRemaining(row -> rows.add(row.getString(0).toString()));
        }
        rows.sort(Comparator.comparingInt(DataEvolutionDeletionVectorTest::projectedRowId));
        return rows;
    }

    private static List<Integer> readProjectedBlobValues(ReadBuilder readBuilder)
            throws IOException {
        List<Integer> rows = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan())) {
            reader.forEachRemaining(row -> rows.add(row.getBlob(0).toData()[0] & 0xFF));
        }
        Collections.sort(rows);
        return rows;
    }

    private static int rowId(String row) {
        return Integer.parseInt(row.substring(0, row.indexOf('|')));
    }

    private static int projectedRowId(String row) {
        return Integer.parseInt(row.substring(row.lastIndexOf('-') + 1));
    }

    private static String formatRow(InternalRow row) {
        return row.getInt(0)
                + "|"
                + row.getString(1)
                + "|"
                + row.getString(2)
                + "|"
                + (row.getBlob(3).toData()[0] & 0xFF);
    }

    private static DataSplit toDataSplit(Split split) {
        if (split instanceof IndexedSplit) {
            return ((IndexedSplit) split).dataSplit();
        }
        return (DataSplit) split;
    }

    private static void assertRegularFileRowRanges(
            List<DataFileMeta> dataFiles, List<Range> expected) {
        assertThat(normalFileRowRanges(dataFiles)).isEqualTo(expected);
    }

    private static List<Range> normalFileRowRanges(FileStoreTable table) {
        return normalFileRowRanges(
                table.store().newScan().plan().files().stream()
                        .map(ManifestEntry::file)
                        .collect(Collectors.toList()));
    }

    private static Map<Range, List<DataFileMeta>> normalFilesByRange(FileStoreTable table) {
        List<DataFileMeta> normalFiles =
                table.store().newScan().plan().files().stream()
                        .map(ManifestEntry::file)
                        .filter(DataEvolutionDeletionVectorTest::isNormalFile)
                        .collect(Collectors.toList());
        RangeHelper<DataFileMeta> rangeHelper = new RangeHelper<>(DataFileMeta::nonNullRowIdRange);
        Map<Range, List<DataFileMeta>> result = new HashMap<>();
        for (List<DataFileMeta> group : rangeHelper.mergeOverlappingRanges(normalFiles)) {
            DataFileMeta anchor = retrieveAnchorFile(group, file -> file);
            result.put(anchor.nonNullRowIdRange(), group);
        }
        return result;
    }

    private static List<Range> normalFileRowRanges(List<DataFileMeta> dataFiles) {
        return dataFiles.stream()
                .filter(DataEvolutionDeletionVectorTest::isNormalFile)
                .map(DataFileMeta::nonNullRowIdRange)
                .sorted(Comparator.comparingLong(range -> range.from))
                .collect(Collectors.toList());
    }

    private static long fileWeight(List<DataFileMeta> files) {
        return estimatedFileWeight(files, 1D);
    }

    private static long estimatedFileWeight(List<DataFileMeta> files, double remainingRatio) {
        long weight = 0L;
        for (DataFileMeta file : files) {
            weight += Math.max((long) Math.ceil(file.fileSize() * remainingRatio), 1L);
        }
        return weight;
    }

    private static void assertFirstBlobFileRowRanges(
            FileStoreTable table, List<Range> expectedFirstRanges, int expectedCount) {
        List<Range> actual = blobFileRowRanges(table);
        assertThat(actual).hasSize(expectedCount);
        assertThat(actual.subList(0, expectedFirstRanges.size())).isEqualTo(expectedFirstRanges);
    }

    private static void assertBlobFileRowRanges(FileStoreTable table, List<Range> expected) {
        assertThat(blobFileRowRanges(table)).isEqualTo(expected);
    }

    private static List<Range> blobFileRowRanges(FileStoreTable table) {
        List<Range> actual =
                table.store().newScan().plan().files().stream()
                        .map(ManifestEntry::file)
                        .filter(file -> BlobFileFormat.isBlobFile(file.fileName()))
                        .map(DataFileMeta::nonNullRowIdRange)
                        .sorted(Comparator.comparingLong(range -> range.from))
                        .collect(Collectors.toList());
        return actual;
    }

    private static Range splitRowRange(DataSplit split) {
        return split.dataFiles().stream()
                .filter(DataEvolutionDeletionVectorTest::isNormalFile)
                .map(DataFileMeta::nonNullRowIdRange)
                .min(Comparator.comparingLong(range -> range.from))
                .get();
    }

    private static boolean isNormalFile(DataFileMeta file) {
        return !BlobFileFormat.isBlobFile(file.fileName()) && !isVectorStoreFile(file.fileName());
    }

    private static class DvSpec {

        private final Range range;
        private final long[] deletedRowIds;

        private DvSpec(Range range, long... deletedRowIds) {
            this.range = range;
            this.deletedRowIds = deletedRowIds;
        }
    }
}
