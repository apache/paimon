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
import org.apache.paimon.append.cluster.IncrementalClusterManager;
import org.apache.paimon.append.cluster.IncrementalClusterRewriter;
import org.apache.paimon.append.cluster.Sorter;
import org.apache.paimon.catalog.RenamingSnapshotCommit;
import org.apache.paimon.catalog.SnapshotCommit;
import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.deletionvectors.BitmapDeletionVector;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.deletionvectors.append.BaseAppendDeleteFileMaintainer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.globalindex.DataEvolutionGlobalIndexScanner;
import org.apache.paimon.globalindex.GlobalIndexBuilderUtils;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.sorted.SortedGlobalIndexScanner;
import org.apache.paimon.globalindex.sorted.SortedGlobalIndexTestUtils;
import org.apache.paimon.index.DataEvolutionIndexSourceMeta;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.operation.commit.RowIdExistenceConflictException;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.MutableObjectIterator;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.paimon.errors.ErrorMessages.DATA_EVOLUTION_ROW_ID_CONFLICT_MESSAGE;
import static org.apache.paimon.table.BucketMode.UNAWARE_BUCKET;
import static org.apache.paimon.utils.DataEvolutionUtils.retrieveAnchorFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Behavioral tests for sorting data-evolution rows into fresh row-ID ranges. */
class DataEvolutionClusterTest extends TableTestBase {

    @Test
    void testClusterSortsLatestValuesAndAppliesDeletionVectors() throws Exception {
        FileStoreTable table = createTable(true);
        append(table, "a", 30, 10, 20);
        append(table, "a", 60, 40, 50);
        updatePayload(table, "a", 0L, 30, 10, 20);
        delete(table, "a", new Range(0, 2), 2L);

        Snapshot snapshot = table.latestSnapshot().get();
        List<CommitMessage> messages = prepareCluster(table, snapshot, null, true);
        assertThat(messages).isNotEmpty();
        commitCluster(table, snapshot, messages);

        assertThat(readRows(table))
                .containsExactly(
                        "a|10|updated-10|6",
                        "a|30|updated-30|7",
                        "a|40|base-40|8",
                        "a|50|base-50|9",
                        "a|60|base-60|10");
        assertThat(table.latestSnapshot().get().nextRowId()).isEqualTo(11L);
        assertThat(
                        table.store().newIndexFileHandler().scanEntries().stream()
                                .filter(entry -> entry.indexFile().globalIndexMeta() == null))
                .isEmpty();
    }

    @Test
    void testFullClusterSkipsAlreadyClusteredPartition() throws Exception {
        FileStoreTable table = createTable(false);
        append(table, "a", 30, 10, 20);
        Snapshot snapshot = table.latestSnapshot().get();
        commitCluster(table, snapshot, prepareCluster(table, snapshot, null, true));
        List<String> clusteredRows = readRows(table);
        List<String> clusteredFiles = fileNames(table, "a");

        Snapshot clusteredSnapshot = table.latestSnapshot().get();
        assertThat(prepareCluster(table, clusteredSnapshot, null, true)).isEmpty();
        assertThat(readRows(table)).isEqualTo(clusteredRows);
        assertThat(fileNames(table, "a")).isEqualTo(clusteredFiles);
    }

    @Test
    void testClusterRemovesFullyDeletedGroup() throws Exception {
        FileStoreTable table = createTable(true);
        append(table, "a", 30, 10, 20);
        delete(table, "a", new Range(0, 2), 0L, 1L, 2L);

        Snapshot snapshot = table.latestSnapshot().get();
        List<CommitMessage> messages = prepareCluster(table, snapshot, null, true);
        assertThat(messages).isNotEmpty();
        commitCluster(table, snapshot, messages);

        assertThat(readRows(table)).isEmpty();
        assertThat(fileNames(table, "a")).isEmpty();
        assertThat(table.store().newIndexFileHandler().scanEntries()).isEmpty();
        assertThat(table.latestSnapshot().get().nextRowId()).isEqualTo(3L);
    }

    @Test
    void testClusterHonorsOutputFileRowLimit() throws Exception {
        FileStoreTable table = createTable(false);
        append(table, "a", 70, 10, 50, 30, 60, 20, 40);
        table = table.copy(Collections.singletonMap(CoreOptions.TARGET_FILE_ROW_NUM.key(), "2"));

        Snapshot snapshot = table.latestSnapshot().get();
        commitCluster(table, snapshot, prepareCluster(table, snapshot, null, true));

        List<DataFileMeta> files =
                table.store().newScan().withSnapshot(table.latestSnapshot().get()).plan().files()
                        .stream()
                        .map(entry -> entry.file())
                        .collect(Collectors.toList());
        assertThat(files).hasSize(4);
        assertThat(files).allSatisfy(file -> assertThat(file.rowCount()).isBetween(1L, 2L));
        assertThat(
                        files.stream()
                                .map(DataFileMeta::nonNullRowIdRange)
                                .sorted(Comparator.comparingLong(range -> range.from)))
                .containsExactly(
                        new Range(7, 8), new Range(9, 10), new Range(11, 12), new Range(13, 13));
        assertThat(readRows(table))
                .containsExactly(
                        "a|10|base-10|7",
                        "a|20|base-20|8",
                        "a|30|base-30|9",
                        "a|40|base-40|10",
                        "a|50|base-50|11",
                        "a|60|base-60|12",
                        "a|70|base-70|13");
    }

    @Test
    void testPartialSortKeyUpdateDirtiesClusteredGroup() throws Exception {
        FileStoreTable table = createTable(false);
        append(table, "a", 30, 10, 20);
        Snapshot snapshot = table.latestSnapshot().get();
        commitCluster(table, snapshot, prepareCluster(table, snapshot, null, true));

        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        RowType writeType = table.rowType().project(Arrays.asList("pt", "id"));
        try (BatchTableWrite write = builder.newWrite().withWriteType(writeType);
                BatchTableCommit commit = builder.newCommit()) {
            for (int id : new int[] {50, 10, 30}) {
                write.write(GenericRow.of(BinaryString.fromString("a"), id));
            }
            List<CommitMessage> updates = write.prepareCommit();
            assignFirstRowId(updates, 3L);
            commit.commit(updates);
        }

        Snapshot updated = table.latestSnapshot().get();
        List<CommitMessage> messages = prepareCluster(table, updated, null, true);
        assertThat(messages).isNotEmpty();
        commitCluster(table, updated, messages);

        assertThat(readRows(table))
                .containsExactly("a|10|base-20|6", "a|30|base-30|7", "a|50|base-10|8");
    }

    @Test
    void testNonClusteringUpdatesDoNotDirtyClusteredGroups() throws Exception {
        FileStoreTable table = createTable(false);
        append(table, "a", 60, 10, 50, 30, 20, 40);
        table = table.copy(Collections.singletonMap(CoreOptions.TARGET_FILE_ROW_NUM.key(), "3"));
        Snapshot initial = table.latestSnapshot().get();
        commitCluster(table, initial, prepareMappedCluster(table, initial, true));
        commit(table, prepareGlobalIndex(table, table.latestSnapshot().get(), "id"));
        List<String> indexes = globalIndexNames(table);
        assertThat(indexes).isNotEmpty();

        updatePayload(table, "a", 6L, 10, 20, 30);
        updatePayload(table, "a", 9L, 40, 50, 60);
        Snapshot updated = table.latestSnapshot().get();
        List<String> files = fileNames(table, "a");
        assertThat(files).hasSize(4);
        List<String> latestRows =
                Arrays.asList(
                        "a|10|updated-10|6",
                        "a|20|updated-20|7",
                        "a|30|updated-30|8",
                        "a|40|updated-40|9",
                        "a|50|updated-50|10",
                        "a|60|updated-60|11");
        for (boolean fullCompaction : new boolean[] {false, true}) {
            List<CommitMessage> messages = prepareMappedCluster(table, updated, fullCompaction);
            assertThat(messages).isEmpty();
            commitCluster(table, updated, messages);
            assertThat(table.latestSnapshot().get().id()).isEqualTo(updated.id());
            assertThat(table.latestSnapshot().get().nextRowId()).isEqualTo(12L);
            assertThat(fileNames(table, "a")).isEqualTo(files);
            assertThat(globalIndexNames(table)).isEqualTo(indexes);
            assertThat(readRows(table)).containsExactlyElementsOf(latestRows);
        }
        assertIndexRows(
                table,
                new PredicateBuilder(table.rowType())
                        .equal(table.rowType().getFieldIndex("id"), 20),
                new Range(7, 7));

        append(table, "a", 90, 70, 80);
        Snapshot appended = table.latestSnapshot().get();
        IncrementalClusterManager manager = new IncrementalClusterManager(table, null, appended);
        Map<BinaryRow, CompactUnit> units = manager.createCompactUnits(true);
        assertThat(units).containsOnlyKeys(partition(table, "a"));
        assertThat(units.get(partition(table, "a")).files())
                .hasSize(5)
                .extracting(DataFileMeta::fileName)
                .containsAll(files);
        commitCluster(table, appended, rewriteClusterUnits(table, manager, units, true));

        assertThat(readRows(table))
                .containsExactly(
                        "a|10|updated-10|15",
                        "a|20|updated-20|16",
                        "a|30|updated-30|17",
                        "a|40|updated-40|18",
                        "a|50|updated-50|19",
                        "a|60|updated-60|20",
                        "a|70|base-70|21",
                        "a|80|base-80|22",
                        "a|90|base-90|23");
        assertThat(table.latestSnapshot().get().nextRowId()).isEqualTo(24L);
        assertIndexRows(
                table,
                new PredicateBuilder(table.rowType())
                        .equal(table.rowType().getFieldIndex("id"), 20),
                new Range(16, 16));
    }

    @Test
    void testIncrementalClusterWaitsForCompactionThreshold() throws Exception {
        FileStoreTable table = createTable(false);
        append(table, "a", 30, 10, 20);
        assertThat(prepareCluster(table, table.latestSnapshot().get(), null, false)).isEmpty();

        append(table, "a", 60, 40, 50);
        Snapshot snapshot = table.latestSnapshot().get();
        List<CommitMessage> messages = prepareCluster(table, snapshot, null, false);
        assertThat(messages).isNotEmpty();
        commitCluster(table, snapshot, messages);

        assertThat(readRows(table))
                .containsExactly(
                        "a|10|base-10|6",
                        "a|20|base-20|7",
                        "a|30|base-30|8",
                        "a|40|base-40|9",
                        "a|50|base-50|10",
                        "a|60|base-60|11");
    }

    @Test
    void testMinorClusterSelectsCompleteDirtyGroupAndRetainsUntouchedGroup() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.TARGET_FILE_ROW_NUM.key(), "3");
        options.put(CoreOptions.FILE_COMPRESSION.key(), "none");
        options.put(CoreOptions.COMPACTION_MAX_SIZE_AMPLIFICATION_PERCENT.key(), "10000");
        options.put(CoreOptions.COMPACTION_SIZE_RATIO.key(), "0");
        FileStoreTable table = createTable(false).copy(options);
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            for (int id : new int[] {60, 10, 50, 30, 20, 40}) {
                Random random = new Random(id);
                int length = id <= 30 ? 8192 : 131072;
                StringBuilder payload = new StringBuilder(length);
                for (int i = 0; i < length; i++) {
                    payload.append((char) ('a' + random.nextInt(26)));
                }
                write.write(
                        GenericRow.of(
                                BinaryString.fromString("a"),
                                id,
                                BinaryString.fromString(payload.toString())));
            }
            commit.commit(write.prepareCommit());
        }
        Snapshot initial = table.latestSnapshot().get();
        commitCluster(table, initial, prepareCluster(table, initial, null, true));

        List<DataFileMeta> clusteredFiles =
                table.store().newScan().withSnapshot(table.latestSnapshot().get()).plan().files()
                        .stream()
                        .map(entry -> entry.file())
                        .sorted(Comparator.comparingLong(DataFileMeta::nonNullFirstRowId))
                        .collect(Collectors.toList());
        assertThat(clusteredFiles).hasSize(2);
        DataFileMeta dirtyBase = clusteredFiles.get(0);
        DataFileMeta untouchedBase = clusteredFiles.get(1);
        assertThat(dirtyBase.nonNullRowIdRange()).isEqualTo(new Range(6, 8));
        assertThat(untouchedBase.nonNullRowIdRange()).isEqualTo(new Range(9, 11));
        assertThat(untouchedBase.fileSize()).isGreaterThan(dirtyBase.fileSize() * 8);
        List<String> expected = new ArrayList<>(readRows(table).subList(3, 6));

        updatePayload(table, "a", 6L, 10, 20, 30);
        updateSortKeys(table, "a", 6L, 10, 20, 30);
        append(table, "a", 90, 70, 80);
        Snapshot snapshot = table.latestSnapshot().get();
        IncrementalClusterManager manager = new IncrementalClusterManager(table, null, snapshot);
        Map<BinaryRow, CompactUnit> units = manager.createCompactUnits(false);
        assertThat(units).hasSize(1);
        CompactUnit unit = units.get(partition(table, "a"));
        assertThat(unit.files())
                .extracting(DataFileMeta::fileName)
                .contains(dirtyBase.fileName())
                .doesNotContain(untouchedBase.fileName());
        assertThat(unit.files())
                .filteredOn(file -> file.nonNullRowIdRange().equals(new Range(6, 8)))
                .hasSize(3);
        commitCluster(table, snapshot, rewriteClusterUnits(table, manager, units));

        expected.addAll(
                Arrays.asList(
                        "a|10|updated-10|15",
                        "a|20|updated-20|16",
                        "a|30|updated-30|17",
                        "a|70|base-70|18",
                        "a|80|base-80|19",
                        "a|90|base-90|20"));
        assertThat(readRows(table)).containsExactlyElementsOf(expected);
        assertThat(fileNames(table, "a"))
                .contains(untouchedBase.fileName())
                .doesNotContain(dirtyBase.fileName());
    }

    @Test
    void testConcurrentPartialUpdateRejectsStaleCluster() throws Exception {
        FileStoreTable table = createTable(true);
        append(table, "a", 30, 10, 20);
        Snapshot snapshot = table.latestSnapshot().get();
        List<CommitMessage> messages = prepareCluster(table, snapshot, null, true);

        updatePayload(table, "a", 0L, 30, 10, 20);
        long updateSnapshot = table.latestSnapshot().get().id();
        assertThatThrownBy(() -> commitCluster(table, snapshot, messages))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining(DATA_EVOLUTION_ROW_ID_CONFLICT_MESSAGE);
        assertThat(table.latestSnapshot().get().id()).isEqualTo(updateSnapshot);
        assertThat(readRows(table))
                .containsExactly("a|30|updated-30|0", "a|10|updated-10|1", "a|20|updated-20|2");
    }

    @Test
    void testConcurrentDeleteRejectsStaleCluster() throws Exception {
        FileStoreTable table = createTable(true);
        append(table, "a", 30, 10, 20);
        Snapshot snapshot = table.latestSnapshot().get();
        List<CommitMessage> messages = prepareCluster(table, snapshot, null, true);

        delete(table, "a", new Range(0, 2), 1L);
        long deleteSnapshot = table.latestSnapshot().get().id();
        // The delete changes the anchor's DV metadata, so normal file conflict detection rejects
        // the stale removal before the additional row-ID conflict check is reached.
        assertThatThrownBy(() -> commitCluster(table, snapshot, messages))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("File deletion conflicts detected!");
        assertThat(table.latestSnapshot().get().id()).isEqualTo(deleteSnapshot);
        assertThat(readRows(table)).containsExactly("a|30|base-30|0", "a|20|base-20|2");
    }

    @Test
    void testConcurrentIndependentAppendIsPreserved() throws Exception {
        FileStoreTable table = createTable(true);
        append(table, "a", 30, 10, 20);
        Snapshot snapshot = table.latestSnapshot().get();
        List<CommitMessage> messages = prepareCluster(table, snapshot, null, true);

        append(table, "a", 60, 40, 50);
        commitCluster(table, snapshot, messages);

        assertThat(readRows(table))
                .containsExactly(
                        "a|60|base-60|3",
                        "a|40|base-40|4",
                        "a|50|base-50|5",
                        "a|10|base-10|6",
                        "a|20|base-20|7",
                        "a|30|base-30|8");
    }

    @Test
    void testPartitionClusterDropsOnlyAffectedGlobalIndexes() throws Exception {
        FileStoreTable table = createTable(true);
        append(table, "a", 30, 10, 20);
        append(table, "b", 60, 40, 50);
        addGlobalIndex(table, "a", "index-a", new Range(0, 2), "id");
        addGlobalIndex(table, "b", "index-b", new Range(3, 5), "id");
        List<String> untouchedFiles = fileNames(table, "b");

        Snapshot snapshot = table.latestSnapshot().get();
        List<CommitMessage> messages =
                prepareCluster(table, snapshot, partitionPredicate(table, "a"), true);
        addGlobalIndex(table, "a", "concurrent-index-a", new Range(0, 2), "payload");
        assertThat(globalIndexNames(table))
                .containsExactly("concurrent-index-a", "index-a", "index-b");
        commitCluster(table, snapshot, messages);

        assertThat(globalIndexNames(table)).containsExactly("index-b");
        assertThat(fileNames(table, "b")).isEqualTo(untouchedFiles);
        assertThat(readRows(table))
                .containsExactly(
                        "b|60|base-60|3",
                        "b|40|base-40|4",
                        "b|50|base-50|5",
                        "a|10|base-10|6",
                        "a|20|base-20|7",
                        "a|30|base-30|8");
    }

    @Test
    void testWithoutDeletionVectorsDropsIndexCommittedAfterPreparation() throws Exception {
        FileStoreTable table = createTable(false);
        append(table, "a", 30, 10, 20);
        addGlobalIndex(table, "a", "index-before-plan", new Range(0, 2), "id");
        Snapshot snapshot = table.latestSnapshot().get();
        List<CommitMessage> messages = prepareCluster(table, snapshot, null, true);
        messages =
                IncrementalClusterRewriter.create(table, snapshot).prepareCommitMessages(messages);

        addGlobalIndex(table, "a", "index-after-preparation", new Range(0, 2), "payload");
        assertThat(globalIndexNames(table))
                .containsExactly("index-after-preparation", "index-before-plan");
        commitChecked(table, messages);

        assertThat(globalIndexNames(table)).isEmpty();
        assertThat(readRows(table))
                .containsExactly("a|10|base-10|3", "a|20|base-20|4", "a|30|base-30|5");
    }

    @ParameterizedTest
    @CsvSource({"btree,false", "btree,true", "bitmap,false", "bitmap,true"})
    void testClusterRemapsScalarIndexPostings(String indexType, boolean deletionVectors)
            throws Exception {
        FileStoreTable table = createTable(deletionVectors);
        appendIndexRows(
                table,
                new int[] {30, 10, 20, 40, 50},
                new String[] {"x", "x", null, "deleted", "y"});
        commit(
                table,
                prepareGlobalIndex(
                        table, table.latestSnapshot().get(), "payload", indexType, null));
        if (deletionVectors) {
            delete(table, "a", new Range(0, 4), 3L);
        }
        table = table.copy(Collections.singletonMap(CoreOptions.TARGET_FILE_ROW_NUM.key(), "2"));
        Snapshot snapshot = table.latestSnapshot().get();
        commitCluster(table, snapshot, prepareMappedCluster(table, snapshot, true));

        PredicateBuilder predicates = new PredicateBuilder(table.rowType());
        int payload = table.rowType().getFieldIndex("payload");
        assertIndexRows(
                table,
                predicates.equal(payload, BinaryString.fromString("x")),
                new Range(5, 5),
                new Range(7, 7));
        assertIndexRows(table, predicates.isNull(payload), new Range(6, 6));
        assertIndexRows(
                table,
                predicates.equal(payload, BinaryString.fromString("y")),
                new Range(deletionVectors ? 8 : 9, deletionVectors ? 8 : 9));
        assertIndexRows(
                table,
                predicates.equal(payload, BinaryString.fromString("deleted")),
                deletionVectors ? new Range[0] : new Range[] {new Range(8, 8)});
        assertThat(readRows(table))
                .containsExactlyElementsOf(
                        deletionVectors
                                ? Arrays.asList("a|10|x|5", "a|20|null|6", "a|30|x|7", "a|50|y|8")
                                : Arrays.asList(
                                        "a|10|x|5",
                                        "a|20|null|6",
                                        "a|30|x|7",
                                        "a|40|deleted|8",
                                        "a|50|y|9"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"btree", "bitmap"})
    void testClusterPreservesExactPartialIndexCoverage(String indexType) throws Exception {
        FileStoreTable table = createTable(false);
        append(table, "a", 90, 10, 50);
        append(table, "a", 80, 20, 40);
        commit(
                table,
                prepareGlobalIndex(
                        table, table.latestSnapshot().get(), "id", indexType, new Range(0, 2)));
        table = table.copy(Collections.singletonMap(CoreOptions.TARGET_FILE_ROW_NUM.key(), "2"));
        Snapshot snapshot = table.latestSnapshot().get();
        commitCluster(table, snapshot, prepareMappedCluster(table, snapshot, true));

        PredicateBuilder predicates = new PredicateBuilder(table.rowType());
        int id = table.rowType().getFieldIndex("id");
        assertIndexRows(table, predicates.equal(id, 10), new Range(6, 6));
        assertIndexRows(table, predicates.equal(id, 50), new Range(9, 9));
        assertIndexRows(table, predicates.equal(id, 90), new Range(11, 11));
        assertIndexRows(table, predicates.equal(id, 20));
        List<Range> coverage =
                table.store().newIndexFileHandler().scanEntries().stream()
                        .filter(entry -> entry.indexFile().globalIndexMeta() != null)
                        .map(entry -> entry.indexFile().globalIndexMeta().rowRange())
                        .collect(Collectors.toList());
        assertThat(Range.sortAndMergeOverlap(coverage, true))
                .containsExactly(new Range(6, 6), new Range(9, 9), new Range(11, 11));
        FileStoreTable detail =
                table.copy(
                        Collections.singletonMap(
                                CoreOptions.SCALAR_INDEX_SEARCH_MODE.key(), "detail"));
        assertThat(readRows(detail, predicates.equal(id, 20))).containsExactly("a|20|base-20|7");
        assertThat(readRows(detail, predicates.equal(id, 80))).containsExactly("a|80|base-80|10");
    }

    @Test
    void testClusterRejectsTooManyIndexCoverageFragments() throws Exception {
        FileStoreTable table = createTable(false);
        int[] indexed = new int[65];
        int[] unindexed = new int[65];
        for (int i = 0; i < indexed.length; i++) {
            indexed[i] = i * 2 + 1;
            unindexed[i] = i * 2 + 2;
        }
        append(table, "a", indexed);
        append(table, "a", unindexed);
        commit(
                table,
                prepareGlobalIndex(
                        table, table.latestSnapshot().get(), "id", "btree", new Range(0, 64)));
        Snapshot snapshot = table.latestSnapshot().get();
        List<String> beforeFiles = fileNames(table, "a");
        List<String> beforeIndexes = globalIndexNames(table);
        List<String> beforeRows = readRows(table);
        List<CommitMessage> messages = prepareMappedCluster(table, snapshot, true);
        assertThat(messages).hasSize(1);
        assertThat(((CommitMessageImpl) messages.get(0)).compactIncrement().compactAfter())
                .hasSize(1);

        // One indexed row alternates with one unindexed row: 65 exact coverage fragments.
        assertThatThrownBy(() -> commitCluster(table, snapshot, messages))
                .isInstanceOf(RuntimeException.class)
                .hasStackTraceContaining("coverage fragments")
                .hasStackTraceContaining("64");
        assertThat(table.latestSnapshot().get().id()).isEqualTo(snapshot.id());
        assertThat(table.latestSnapshot().get().nextRowId()).isEqualTo(130L);
        assertThat(fileNames(table, "a")).isEqualTo(beforeFiles);
        assertThat(globalIndexNames(table)).isEqualTo(beforeIndexes);
        assertThat(readRows(table)).isEqualTo(beforeRows);
        assertIndexRows(
                table,
                new PredicateBuilder(table.rowType()).equal(table.rowType().getFieldIndex("id"), 1),
                new Range(0, 0));
    }

    @ParameterizedTest
    @ValueSource(strings = {"btree", "bitmap"})
    void testClusterCombinesFullyIndexedSourceRangesBeforeCountingFragments(String indexType)
            throws Exception {
        FileStoreTable table = createTable(false);
        int[] odd = new int[65];
        int[] even = new int[65];
        for (int i = 0; i < odd.length; i++) {
            odd[i] = i * 2 + 1;
            even[i] = i * 2 + 2;
        }
        append(table, "a", odd);
        append(table, "a", even);
        Snapshot indexed = table.latestSnapshot().get();
        List<CommitMessage> indexes =
                new ArrayList<>(
                        prepareGlobalIndex(table, indexed, "id", indexType, new Range(0, 64)));
        indexes.addAll(prepareGlobalIndex(table, indexed, "id", indexType, new Range(65, 129)));
        commit(table, indexes);
        assertThat(globalIndexNames(table)).hasSize(2);
        Snapshot snapshot = table.latestSnapshot().get();
        List<CommitMessage> messages = prepareMappedCluster(table, snapshot, true);
        assertThat(messages).hasSize(1);
        assertThat(((CommitMessageImpl) messages.get(0)).compactIncrement().compactAfter())
                .hasSize(1);

        // Each old range maps to 65 alternating positions, but their union has no coverage gap.
        commitCluster(table, snapshot, messages);

        assertThat(table.latestSnapshot().get().nextRowId()).isEqualTo(260L);
        assertThat(
                        table.store().newIndexFileHandler().scanEntries().stream()
                                .filter(entry -> entry.indexFile().globalIndexMeta() != null)
                                .map(entry -> entry.indexFile().globalIndexMeta().rowRange()))
                .isNotEmpty()
                .allSatisfy(range -> assertThat(range).isEqualTo(new Range(130, 259)));
        PredicateBuilder predicates = new PredicateBuilder(table.rowType());
        int id = table.rowType().getFieldIndex("id");
        List<String> expectedRows = new ArrayList<>();
        for (int value = 1; value <= 130; value++) {
            long rowId = 129L + value;
            assertIndexRows(table, predicates.equal(id, value), new Range(rowId, rowId));
            expectedRows.add("a|" + value + "|base-" + value + "|" + rowId);
        }
        assertIndexRows(table, predicates.equal(id, 0));
        assertIndexRows(table, predicates.isNull(id));
        assertThat(readRows(table)).containsExactlyElementsOf(expectedRows);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testMinorClusterRemapsIndexesAcrossDisjointRanges(boolean deletionVectors)
            throws Exception {
        FileStoreTable table = createMinorClusterTable(deletionVectors);
        List<String> untouchedRows = rowsInRange(table, new Range(12, 14));
        List<String> untouchedFiles = filesInRange(table, new Range(12, 14));
        commit(table, prepareGlobalIndex(table, table.latestSnapshot().get(), "id"));
        List<String> oldIndexFiles = globalIndexNames(table);
        Snapshot snapshot = table.latestSnapshot().get();
        List<CommitMessage> messages = prepareMinorCluster(table, snapshot);
        messages =
                IncrementalClusterRewriter.create(table, snapshot).prepareCommitMessages(messages);
        List<CommitMessage> preparedGapIndex =
                prepareGlobalIndex(table, snapshot, "payload", "bitmap", new Range(12, 14));
        commit(table, prepareGlobalIndex(table, snapshot, "payload"));
        commitChecked(table, messages);

        assertThat(globalIndexNames(table)).isNotEmpty().doesNotContainAnyElementsOf(oldIndexFiles);
        PredicateBuilder predicates = new PredicateBuilder(table.rowType());
        int id = table.rowType().getFieldIndex("id");
        int payload = table.rowType().getFieldIndex("payload");
        assertIndexRows(table, predicates.equal(id, 40), new Range(12, 12));
        assertIndexRows(table, predicates.equal(id, 50), new Range(13, 13));
        assertIndexRows(table, predicates.equal(id, 60), new Range(14, 14));
        long seventy = deletionVectors ? 23 : 24;
        assertIndexRows(table, predicates.equal(id, 70), new Range(seventy, seventy));
        assertIndexRows(
                table,
                predicates.equal(payload, BinaryString.fromString("updated-70")),
                new Range(seventy, seventy));
        assertMinorClusterResult(table, deletionVectors, untouchedRows, untouchedFiles, 21L, 4);

        // A pre-clustering index can still commit for an unchanged row-ID group.
        commit(table, preparedGapIndex);
        assertIndexRows(table, predicates.equal(id, 40), new Range(12, 12));
        assertMinorClusterResult(table, deletionVectors, untouchedRows, untouchedFiles, 21L, 4);
    }

    @Test
    void testMinorClusterRemapsIndexWhenAllSelectedRowsAreDeleted() throws Exception {
        FileStoreTable table = createMinorClusterTable(true);
        List<String> untouchedRows = rowsInRange(table, new Range(12, 14));
        List<String> untouchedFiles = filesInRange(table, new Range(12, 14));
        commit(table, prepareGlobalIndex(table, table.latestSnapshot().get(), "id"));
        delete(table, "a", new Range(9, 11), 9L, 10L, 11L);
        delete(table, "a", new Range(15, 17), 15L, 16L, 17L);
        delete(table, "a", new Range(18, 20), 18L, 19L, 20L);
        Snapshot snapshot = table.latestSnapshot().get();
        List<CommitMessage> messages = prepareMinorCluster(table, snapshot);
        assertThat(messages)
                .allSatisfy(
                        message ->
                                assertThat(
                                                ((CommitMessageImpl) message)
                                                        .compactIncrement()
                                                        .compactAfter())
                                        .isEmpty());
        commitCluster(table, snapshot, messages);

        PredicateBuilder predicates = new PredicateBuilder(table.rowType());
        int id = table.rowType().getFieldIndex("id");
        assertIndexRows(table, predicates.equal(id, 40), new Range(12, 12));
        assertIndexRows(table, predicates.equal(id, 20));
        assertIndexRows(table, predicates.equal(id, 100));
        assertThat(
                        table.store().newIndexFileHandler().scanEntries().stream()
                                .filter(entry -> entry.indexFile().globalIndexMeta() != null)
                                .map(entry -> entry.indexFile().globalIndexMeta().rowRange()))
                .containsExactly(new Range(12, 14));
        assertThat(readRows(table)).containsExactlyElementsOf(untouchedRows);
        assertThat(fileNames(table, "a")).containsExactlyElementsOf(untouchedFiles);
        assertThat(table.latestSnapshot().get().nextRowId()).isEqualTo(21L);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testMinorClusterRetryRemapsConcurrentIndexAfterAppend(boolean deletionVectors)
            throws Exception {
        Map<String, String> retryOptions = new HashMap<>();
        retryOptions.put(CoreOptions.COMMIT_MIN_RETRY_WAIT.key(), "0ms");
        retryOptions.put(CoreOptions.COMMIT_MAX_RETRY_WAIT.key(), "0ms");
        FileStoreTable original = createMinorClusterTable(deletionVectors).copy(retryOptions);
        List<String> untouchedRows = rowsInRange(original, new Range(12, 14));
        List<String> untouchedFiles = filesInRange(original, new Range(12, 14));
        commit(original, prepareGlobalIndex(original, original.latestSnapshot().get(), "id"));
        Snapshot scanSnapshot = original.latestSnapshot().get();
        List<CommitMessage> messages = prepareMinorCluster(original, scanSnapshot);
        AtomicInteger attempts = new AtomicInteger();
        CatalogEnvironment environment =
                new CatalogEnvironment(null, null, null, null, null, null, false, false) {
                    @Override
                    public SnapshotCommit snapshotCommit(
                            SnapshotManager manager, CoreOptions options, String commitUser) {
                        SnapshotCommit delegate = new RenamingSnapshotCommit(manager, Lock.empty());
                        return new SnapshotCommit(Lock.empty()) {
                            @Override
                            protected boolean commitImpl(
                                    String baseUuid,
                                    Snapshot snapshot,
                                    String branch,
                                    List<PartitionStatistics> statistics)
                                    throws Exception {
                                assertThat(snapshot.commitKind())
                                        .isEqualTo(Snapshot.CommitKind.COMPACT);
                                int attempt = attempts.incrementAndGet();
                                assertThat(
                                                original.store()
                                                        .indexManifestFileFactory()
                                                        .create()
                                                        .read(snapshot.indexManifest()))
                                        .isNotEmpty();
                                if (attempt == 1) {
                                    assertThat(original.latestSnapshot().get().id())
                                            .isEqualTo(scanSnapshot.id());
                                    DataEvolutionClusterTest.this.commit(
                                            original,
                                            prepareGlobalIndex(original, scanSnapshot, "payload"));
                                    append(original, "a", 200, 180, 190);
                                }
                                boolean committed =
                                        delegate.commit(baseUuid, snapshot, branch, statistics);
                                assertThat(committed).isEqualTo(attempt == 2);
                                return committed;
                            }

                            @Override
                            protected void closeImpl() throws Exception {
                                delegate.close();
                            }
                        };
                    }
                };
        FileStoreTable committingTable =
                FileStoreTableFactory.create(
                        original.fileIO(), original.location(), original.schema(), environment);
        commitCluster(committingTable, scanSnapshot, messages);

        assertThat(attempts).hasValue(2);
        assertThat(original.latestSnapshot().get().id()).isEqualTo(scanSnapshot.id() + 3);
        long seventy = deletionVectors ? 26 : 27;
        PredicateBuilder predicates = new PredicateBuilder(original.rowType());
        assertIndexRows(
                original,
                predicates.equal(original.rowType().getFieldIndex("id"), 70),
                new Range(seventy, seventy));
        assertIndexRows(
                original,
                predicates.equal(
                        original.rowType().getFieldIndex("payload"),
                        BinaryString.fromString("updated-70")),
                new Range(seventy, seventy));
        untouchedRows.addAll(
                Arrays.asList("a|200|base-200|21", "a|180|base-180|22", "a|190|base-190|23"));
        assertMinorClusterResult(original, deletionVectors, untouchedRows, untouchedFiles, 24L, 5);
        FileStoreTable detail =
                original.copy(
                        Collections.singletonMap(
                                CoreOptions.SCALAR_INDEX_SEARCH_MODE.key(), "detail"));
        assertThat(readRows(detail, predicates.equal(original.rowType().getFieldIndex("id"), 180)))
                .containsExactly("a|180|base-180|22");
    }

    @Test
    void testClusterDoesNotMakeStaleIndexKeysFresh() throws Exception {
        FileStoreTable table =
                createTable(false)
                        .copy(
                                Collections.singletonMap(
                                        CoreOptions.GLOBAL_INDEX_COLUMN_UPDATE_ACTION.key(),
                                        "ignore"));
        append(table, "a", 30, 10, 20);
        Snapshot indexed = table.latestSnapshot().get();
        List<CommitMessage> indexes =
                new ArrayList<>(prepareGlobalIndex(table, indexed, "payload"));
        indexes.addAll(prepareGlobalIndex(table, indexed, "id"));
        commit(table, indexes);
        updatePayload(table, "a", 0L, 30, 10, 20);
        Snapshot snapshot = table.latestSnapshot().get();
        commitCluster(table, snapshot, prepareMappedCluster(table, snapshot, true));

        PredicateBuilder predicates = new PredicateBuilder(table.rowType());
        assertIndexRows(
                table, predicates.equal(table.rowType().getFieldIndex("id"), 20), new Range(4, 4));
        assertThat(
                        table.store().newIndexFileHandler().scanEntries().stream()
                                .filter(entry -> entry.indexFile().globalIndexMeta() != null)
                                .flatMap(
                                        entry ->
                                                entry.indexFile().globalIndexMeta()
                                                        .getIndexedFieldIds().stream()))
                .doesNotContain(table.rowType().getField("payload").id());
        FileStoreTable detail =
                table.copy(
                        Collections.singletonMap(
                                CoreOptions.SCALAR_INDEX_SEARCH_MODE.key(), "detail"));
        assertThat(
                        readRows(
                                detail,
                                predicates.equal(
                                        table.rowType().getFieldIndex("payload"),
                                        BinaryString.fromString("updated-20"))))
                .containsExactly("a|20|updated-20|4");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testClusterKeepsIndexPayloadsAfterUncertainPublication(boolean publicationThrows)
            throws Exception {
        FileStoreTable original = createTable(false);
        append(original, "a", 30, 10, 20);
        commit(original, prepareGlobalIndex(original, original.latestSnapshot().get(), "id"));
        Snapshot scanSnapshot = original.latestSnapshot().get();
        List<CommitMessage> messages = prepareMappedCluster(original, scanSnapshot, true);
        AtomicInteger publications = new AtomicInteger();
        CatalogEnvironment environment =
                new CatalogEnvironment(null, null, null, null, null, null, false, false) {
                    @Override
                    public SnapshotCommit snapshotCommit(
                            SnapshotManager manager, CoreOptions options, String commitUser) {
                        SnapshotCommit delegate = new RenamingSnapshotCommit(manager, Lock.empty());
                        return new SnapshotCommit(Lock.empty()) {
                            @Override
                            protected boolean commitImpl(
                                    String baseUuid,
                                    Snapshot snapshot,
                                    String branch,
                                    List<PartitionStatistics> statistics)
                                    throws Exception {
                                publications.incrementAndGet();
                                assertThat(delegate.commit(baseUuid, snapshot, branch, statistics))
                                        .isTrue();
                                if (publicationThrows) {
                                    throw new IOException("Publication response lost");
                                }
                                return false;
                            }

                            @Override
                            protected void closeImpl() throws Exception {
                                delegate.close();
                            }
                        };
                    }
                };
        FileStoreTable committingTable =
                FileStoreTableFactory.create(
                        original.fileIO(), original.location(), original.schema(), environment);
        commitCluster(committingTable, scanSnapshot, messages);

        assertThat(publications).hasValue(1);
        assertThat(original.latestSnapshot().get().id()).isEqualTo(scanSnapshot.id() + 1);
        assertThat(original.latestSnapshot().get().nextRowId()).isEqualTo(6L);
        List<IndexFileMeta> indexes =
                original.store().newIndexFileHandler().scanEntries().stream()
                        .map(IndexManifestEntry::indexFile)
                        .filter(file -> file.globalIndexMeta() != null)
                        .collect(Collectors.toList());
        assertThat(indexes).isNotEmpty();
        for (IndexFileMeta index : indexes) {
            assertThat(
                            original.fileIO()
                                    .exists(
                                            original.store()
                                                    .pathFactory()
                                                    .globalIndexFileFactory()
                                                    .toPath(index)))
                    .isTrue();
        }
        PredicateBuilder predicates = new PredicateBuilder(original.rowType());
        int id = original.rowType().getFieldIndex("id");
        assertIndexRows(original, predicates.equal(id, 10), new Range(3, 3));
        assertIndexRows(original, predicates.equal(id, 20), new Range(4, 4));
        assertIndexRows(original, predicates.equal(id, 30), new Range(5, 5));
        assertThat(readRows(original))
                .containsExactly("a|10|base-10|3", "a|20|base-20|4", "a|30|base-30|5");
    }

    @Test
    void testClusterRemapsCompositeBTreeIndex() throws Exception {
        FileStoreTable table = createTable(false);
        appendIndexRows(table, new int[] {30, 10, 20, 40}, new String[] {"x", "x", null, "y"});
        Snapshot indexed = table.latestSnapshot().get();
        List<String> fields = Arrays.asList("id", "payload");
        List<CommitMessage> indexes = new ArrayList<>();
        for (DataSplit split :
                GlobalIndexBuilderUtils.splitByContiguousRowRange(
                        new SortedGlobalIndexScanner(table, "btree")
                                .withIndexFields(fields)
                                .withSnapshot(indexed)
                                .scan()
                                .get()
                                .entries())) {
            indexes.addAll(
                    SortedGlobalIndexTestUtils.buildIndex(
                            table, "btree", fields, split, indexed.id()));
        }
        assertThat(indexes).isNotEmpty();
        commit(table, indexes);
        table = table.copy(Collections.singletonMap(CoreOptions.TARGET_FILE_ROW_NUM.key(), "2"));
        Snapshot snapshot = table.latestSnapshot().get();
        commitCluster(table, snapshot, prepareMappedCluster(table, snapshot, true));

        PredicateBuilder predicates = new PredicateBuilder(table.rowType());
        int id = table.rowType().getFieldIndex("id");
        int payload = table.rowType().getFieldIndex("payload");
        assertIndexRows(
                table,
                PredicateBuilder.and(
                        predicates.equal(id, 10),
                        predicates.equal(payload, BinaryString.fromString("x"))),
                new Range(4, 4));
        assertIndexRows(
                table,
                PredicateBuilder.and(predicates.equal(id, 20), predicates.isNull(payload)),
                new Range(5, 5));
        assertIndexRows(
                table,
                PredicateBuilder.and(
                        predicates.equal(id, 30),
                        predicates.equal(payload, BinaryString.fromString("x"))),
                new Range(6, 6));
        assertIndexRows(
                table,
                PredicateBuilder.and(
                        predicates.equal(id, 20),
                        predicates.equal(payload, BinaryString.fromString("y"))));
    }

    @Test
    void testClusterPreservesIndexOnlyDeleteForAnotherPartition() throws Exception {
        FileStoreTable table = createTable(false);
        append(table, "a", 30, 10, 20);
        append(table, "b", 60, 40, 50);
        commit(table, prepareGlobalIndex(table, table.latestSnapshot().get(), "id"));
        List<String> untouchedFiles = fileNames(table, "b");
        List<IndexFileMeta> deletedIndexes =
                table.store().newIndexFileHandler().scanEntries().stream()
                        .filter(entry -> entry.partition().equals(partition(table, "b")))
                        .map(IndexManifestEntry::indexFile)
                        .filter(file -> file.globalIndexMeta() != null)
                        .collect(Collectors.toList());
        assertThat(deletedIndexes).hasSize(1);
        Snapshot snapshot = table.latestSnapshot().get();
        IncrementalClusterManager manager =
                new IncrementalClusterManager(table, partitionPredicate(table, "a"), snapshot);
        Map<BinaryRow, CompactUnit> units = manager.createCompactUnits(true);
        assertThat(units).containsOnlyKeys(partition(table, "a"));
        List<CommitMessage> messages =
                manager.prepareCommitMessages(rewriteClusterUnits(table, manager, units, true));
        messages.add(
                new CommitMessageImpl(
                                partition(table, "b"),
                                UNAWARE_BUCKET,
                                null,
                                DataIncrement.emptyIncrement(),
                                new CompactIncrement(
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        deletedIndexes))
                        .withCheckFromSnapshot(snapshot.id()));
        commitChecked(table, messages);

        assertThat(table.store().newIndexFileHandler().scanEntries())
                .allSatisfy(
                        entry -> assertThat(entry.partition()).isEqualTo(partition(table, "a")));
        PredicateBuilder predicates = new PredicateBuilder(table.rowType());
        int id = table.rowType().getFieldIndex("id");
        assertIndexRows(table, predicates.equal(id, 10), new Range(6, 6));
        assertIndexRows(table, predicates.equal(id, 20), new Range(7, 7));
        assertIndexRows(table, predicates.equal(id, 30), new Range(8, 8));
        assertIndexRows(table, predicates.equal(id, 40));
        assertThat(fileNames(table, "b")).isEqualTo(untouchedFiles);
        assertThat(readRows(table))
                .containsExactly(
                        "b|60|base-60|3",
                        "b|40|base-40|4",
                        "b|50|base-50|5",
                        "a|10|base-10|6",
                        "a|20|base-20|7",
                        "a|30|base-30|8");
    }

    @Test
    void testAdjacentIndexCoverageKeepsSeparateFreshnessBaselines() throws Exception {
        FileStoreTable table = createTable(false);
        append(table, "a", 30, 10, 20);
        append(table, "a", 60, 40, 50);
        Snapshot indexed = table.latestSnapshot().get();
        commit(table, prepareGlobalIndex(table, indexed, "id"));
        Snapshot snapshot = table.latestSnapshot().get();
        List<DataFileMeta> selected =
                table.store().newScan().withSnapshot(snapshot).plan().files().stream()
                        .map(entry -> entry.file())
                        .filter(file -> file.nonNullRowIdRange().equals(new Range(0, 2)))
                        .collect(Collectors.toList());
        assertThat(selected).hasSize(1);
        List<String> untouchedFiles = filesInRange(table, new Range(3, 5));
        IncrementalClusterManager manager = new IncrementalClusterManager(table, null, snapshot);
        Map<BinaryRow, CompactUnit> units =
                Collections.singletonMap(
                        partition(table, "a"), CompactUnit.fromFiles(1, selected, true));
        commitCluster(table, snapshot, rewriteClusterUnits(table, manager, units, true));

        Snapshot clustered = table.latestSnapshot().get();
        Map<Range, Long> baselines =
                table.store().newIndexFileHandler().scanEntries().stream()
                        .map(IndexManifestEntry::indexFile)
                        .filter(file -> file.globalIndexMeta() != null)
                        .collect(
                                Collectors.toMap(
                                        file -> file.globalIndexMeta().rowRange(),
                                        file ->
                                                DataEvolutionIndexSourceMeta.fromIndexFile(file)
                                                        .scanSnapshotId()));
        assertThat(baselines)
                .hasSize(2)
                .containsEntry(new Range(3, 5), indexed.id())
                .containsEntry(new Range(6, 8), clustered.id());
        assertThat(filesInRange(table, new Range(3, 5))).isEqualTo(untouchedFiles);
        PredicateBuilder predicates = new PredicateBuilder(table.rowType());
        int id = table.rowType().getFieldIndex("id");
        assertIndexRows(table, predicates.equal(id, 10), new Range(6, 6));
        assertIndexRows(table, predicates.equal(id, 60), new Range(3, 3));

        append(table, "a", 90, 70, 80);
        Snapshot next = table.latestSnapshot().get();
        commitCluster(table, next, prepareMappedCluster(table, next, true));

        // Neither the retained coverage nor the first rewrite became stale on the second pass.
        assertIndexRows(table, predicates.equal(id, 10), new Range(12, 12));
        assertIndexRows(table, predicates.equal(id, 20), new Range(13, 13));
        assertIndexRows(table, predicates.equal(id, 40), new Range(15, 15));
        assertIndexRows(table, predicates.equal(id, 60), new Range(17, 17));
        assertIndexRows(table, predicates.equal(id, 70));
        assertThat(readRows(table))
                .containsExactly(
                        "a|10|base-10|12",
                        "a|20|base-20|13",
                        "a|30|base-30|14",
                        "a|40|base-40|15",
                        "a|50|base-50|16",
                        "a|60|base-60|17",
                        "a|70|base-70|18",
                        "a|80|base-80|19",
                        "a|90|base-90|20");
    }

    @Test
    void testMetadataOnlyCompactDoesNotReplayOldClusteringMapping() throws Exception {
        FileStoreTable table = createTable(false);
        append(table, "a", 30, 10, 20);
        commit(table, prepareGlobalIndex(table, table.latestSnapshot().get(), "id"));
        Snapshot snapshot = table.latestSnapshot().get();
        List<DataFileMeta> initialFiles =
                table.store().newScan().withSnapshot(snapshot).plan().files().stream()
                        .map(entry -> entry.file())
                        .collect(Collectors.toList());
        assertThat(initialFiles).hasSize(1);
        IncrementalClusterManager manager = new IncrementalClusterManager(table, null, snapshot);
        Map<BinaryRow, CompactUnit> units =
                Collections.singletonMap(
                        partition(table, "a"), CompactUnit.fromFiles(1, initialFiles, true));
        commitCluster(table, snapshot, rewriteClusterUnits(table, manager, units, true));

        Snapshot clustered = table.latestSnapshot().get();
        DataFileMeta clusteredFile =
                table.store().newScan().withSnapshot(clustered).plan().files().get(0).file();
        assertThat(clusteredFile.nonNullRowIdRange()).isEqualTo(new Range(3, 5));
        assertThat(clusteredFile.extraFiles()).anyMatch(ClusteringRowIdMapping::isMappingFile);
        List<IndexFileMeta> deletedIndexes =
                table.store().newIndexFileHandler().scanEntries().stream()
                        .map(IndexManifestEntry::indexFile)
                        .filter(file -> file.globalIndexMeta() != null)
                        .collect(Collectors.toList());
        assertThat(deletedIndexes).isNotEmpty();
        commit(
                table,
                new CommitMessageImpl(
                        partition(table, "a"),
                        UNAWARE_BUCKET,
                        null,
                        DataIncrement.emptyIncrement(),
                        new CompactIncrement(
                                Collections.singletonList(clusteredFile),
                                Collections.singletonList(clusteredFile.upgrade(2)),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                deletedIndexes)));

        assertThat(table.latestSnapshot().get().id()).isEqualTo(clustered.id() + 1);
        assertThat(table.latestSnapshot().get().nextRowId()).isEqualTo(6L);
        assertThat(fileNames(table, "a")).containsExactly(clusteredFile.fileName());
        DataFileMeta upgraded =
                table.store()
                        .newScan()
                        .withSnapshot(table.latestSnapshot().get())
                        .plan()
                        .files()
                        .get(0)
                        .file();
        assertThat(upgraded.level()).isEqualTo(2);
        assertThat(upgraded.nonNullRowIdRange()).isEqualTo(new Range(3, 5));
        assertThat(upgraded.extraFiles()).isEqualTo(clusteredFile.extraFiles());
        assertThat(globalIndexNames(table)).isEmpty();
        assertThat(readRows(table))
                .containsExactly("a|10|base-10|3", "a|20|base-20|4", "a|30|base-30|5");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testClusterRetryDropsIndexCommittedBeforeSnapshotPublication(boolean deletionVectors)
            throws Exception {
        Map<String, String> retryOptions = new HashMap<>();
        retryOptions.put(CoreOptions.COMMIT_MIN_RETRY_WAIT.key(), "0ms");
        retryOptions.put(CoreOptions.COMMIT_MAX_RETRY_WAIT.key(), "0ms");
        FileStoreTable original = createTable(deletionVectors).copy(retryOptions);
        append(original, "a", 30, 10, 20);
        if (deletionVectors) {
            delete(original, "a", new Range(0, 2), 1L);
        }
        commit(original, prepareGlobalIndex(original, original.latestSnapshot().get(), "id"));
        Snapshot scanSnapshot = original.latestSnapshot().get();
        List<CommitMessage> messages = prepareCluster(original, scanSnapshot, null, true);
        AtomicInteger attempts = new AtomicInteger();
        CatalogEnvironment environment =
                new CatalogEnvironment(null, null, null, null, null, null, false, false) {
                    @Override
                    public SnapshotCommit snapshotCommit(
                            SnapshotManager manager, CoreOptions options, String commitUser) {
                        SnapshotCommit delegate = new RenamingSnapshotCommit(manager, Lock.empty());
                        return new SnapshotCommit(Lock.empty()) {
                            @Override
                            protected boolean commitImpl(
                                    String baseUuid,
                                    Snapshot snapshot,
                                    String branch,
                                    List<PartitionStatistics> statistics)
                                    throws Exception {
                                assertThat(snapshot.commitKind())
                                        .isEqualTo(Snapshot.CommitKind.COMPACT);
                                int attempt = attempts.incrementAndGet();
                                // Index cleanup has already been prepared for this candidate.
                                assertThat(
                                                original.store()
                                                        .indexManifestFileFactory()
                                                        .create()
                                                        .read(snapshot.indexManifest()))
                                        .isEmpty();
                                if (attempt == 1) {
                                    assertThat(original.latestSnapshot().get().id())
                                            .isEqualTo(scanSnapshot.id());
                                    DataEvolutionClusterTest.this.commit(
                                            original,
                                            prepareGlobalIndex(original, scanSnapshot, "payload"));
                                    assertThat(globalIndexNames(original)).hasSize(2);
                                }
                                boolean committed =
                                        delegate.commit(baseUuid, snapshot, branch, statistics);
                                assertThat(committed).isEqualTo(attempt == 2);
                                return committed;
                            }

                            @Override
                            protected void closeImpl() throws Exception {
                                delegate.close();
                            }
                        };
                    }
                };
        FileStoreTable committingTable =
                FileStoreTableFactory.create(
                        original.fileIO(), original.location(), original.schema(), environment);
        commitCluster(committingTable, scanSnapshot, messages);

        assertThat(attempts).hasValue(2);
        assertThat(original.latestSnapshot().get().id()).isEqualTo(scanSnapshot.id() + 2);
        assertThat(original.latestSnapshot().get().nextRowId())
                .isEqualTo(deletionVectors ? 5L : 6L);
        assertThat(globalIndexNames(original)).isEmpty();
        assertThat(readRows(original))
                .containsExactlyElementsOf(
                        deletionVectors
                                ? Arrays.asList("a|20|base-20|3", "a|30|base-30|4")
                                : Arrays.asList(
                                        "a|10|base-10|3", "a|20|base-20|4", "a|30|base-30|5"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testClusterRejectsPreviouslyPreparedPartialUpdate(boolean deletionVectors)
            throws Exception {
        FileStoreTable table = createTable(deletionVectors);
        append(table, "a", 30, 10, 20);
        Snapshot scanSnapshot = table.latestSnapshot().get();
        List<CommitMessage> updates =
                preparePayloadUpdate(table, "a", 0L, 30, 10, 20).stream()
                        .map(
                                message ->
                                        ((CommitMessageImpl) message)
                                                .withCheckFromSnapshot(scanSnapshot.id()))
                        .collect(Collectors.toList());
        commitCluster(table, scanSnapshot, prepareCluster(table, scanSnapshot, null, true));
        commit(table, prepareGlobalIndex(table, table.latestSnapshot().get(), "id"));

        assertStaleCommitPreservesCluster(
                table, updates, RowIdExistenceConflictException.class, "Row ID existence conflict");
    }

    @Test
    void testClusterRejectsPreviouslyPreparedDelete() throws Exception {
        FileStoreTable table = createTable(true);
        append(table, "a", 30, 10, 20);
        Snapshot scanSnapshot = table.latestSnapshot().get();
        CommitMessage deletion =
                ((CommitMessageImpl) prepareDelete(table, "a", new Range(0, 2), 1L))
                        .withCheckFromSnapshot(scanSnapshot.id());
        commitCluster(table, scanSnapshot, prepareCluster(table, scanSnapshot, null, true));
        commit(table, prepareGlobalIndex(table, table.latestSnapshot().get(), "id"));

        assertStaleCommitPreservesCluster(
                table,
                Collections.singletonList(deletion),
                RuntimeException.class,
                "File deletion conflicts detected!");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testClusterRejectsPreviouslyBuiltGlobalIndex(boolean deletionVectors) throws Exception {
        FileStoreTable table = createTable(deletionVectors);
        append(table, "a", 30, 10, 20);
        Snapshot scanSnapshot = table.latestSnapshot().get();
        List<CommitMessage> indexes = prepareGlobalIndex(table, scanSnapshot, "id");
        commitCluster(table, scanSnapshot, prepareCluster(table, scanSnapshot, null, true));
        commit(table, prepareGlobalIndex(table, table.latestSnapshot().get(), "id"));

        assertStaleCommitPreservesCluster(
                table, indexes, RuntimeException.class, "Global index row ID existence conflict");
    }

    private void assertStaleCommitPreservesCluster(
            FileStoreTable table,
            List<CommitMessage> staleMessages,
            Class<? extends RuntimeException> exceptionClass,
            String conflictMessage)
            throws Exception {
        Snapshot clustered = table.latestSnapshot().get();
        List<String> files = fileNames(table, "a");
        List<String> indexes = globalIndexNames(table);
        assertThat(indexes).isNotEmpty();
        assertThatThrownBy(() -> commit(table, staleMessages))
                .isInstanceOf(exceptionClass)
                .hasMessageContaining(conflictMessage);
        assertThat(table.latestSnapshot().get().id()).isEqualTo(clustered.id());
        assertThat(table.latestSnapshot().get().nextRowId()).isEqualTo(6L);
        assertThat(fileNames(table, "a")).isEqualTo(files);
        assertThat(globalIndexNames(table)).isEqualTo(indexes);
        assertThat(readRows(table))
                .containsExactly("a|10|base-10|3", "a|20|base-20|4", "a|30|base-30|5");
    }

    private FileStoreTable createTable(boolean deletionVectors) throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("pt", DataTypes.STRING())
                        .column("id", DataTypes.INT())
                        .column("payload", DataTypes.STRING())
                        .partitionKeys("pt")
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .option(
                                CoreOptions.DELETION_VECTORS_ENABLED.key(),
                                String.valueOf(deletionVectors))
                        .option(CoreOptions.CLUSTERING_COLUMNS.key(), "id")
                        .option(CoreOptions.CLUSTERING_STRATEGY.key(), "order")
                        .option(CoreOptions.CLUSTERING_INCREMENTAL.key(), "true")
                        .option(CoreOptions.NUM_SORTED_RUNS_COMPACTION_TRIGGER.key(), "2")
                        .build();
        catalog.createTable(identifier(), schema, false);
        return getTableDefault();
    }

    private FileStoreTable createMinorClusterTable(boolean deletionVectors) throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.TARGET_FILE_ROW_NUM.key(), "3");
        options.put(CoreOptions.FILE_COMPRESSION.key(), "none");
        options.put(CoreOptions.COMPACTION_MAX_SIZE_AMPLIFICATION_PERCENT.key(), "10000");
        options.put(CoreOptions.COMPACTION_SIZE_RATIO.key(), "0");
        FileStoreTable table = createTable(deletionVectors).copy(options);
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            for (int id : new int[] {90, 10, 60, 30, 80, 40, 20, 70, 50}) {
                Random random = new Random(id);
                int length = id >= 40 && id <= 60 ? 262144 : 8192;
                StringBuilder payload = new StringBuilder(length);
                for (int i = 0; i < length; i++) {
                    payload.append((char) ('a' + random.nextInt(26)));
                }
                write.write(
                        GenericRow.of(
                                BinaryString.fromString("a"),
                                id,
                                BinaryString.fromString(payload.toString())));
            }
            commit.commit(write.prepareCommit());
        }
        Snapshot initial = table.latestSnapshot().get();
        commitCluster(table, initial, prepareCluster(table, initial, null, true));
        List<DataFileMeta> clusteredFiles =
                table.store().newScan().withSnapshot(table.latestSnapshot().get()).plan().files()
                        .stream()
                        .map(entry -> entry.file())
                        .sorted(Comparator.comparingLong(DataFileMeta::nonNullFirstRowId))
                        .collect(Collectors.toList());
        assertThat(clusteredFiles)
                .extracting(DataFileMeta::nonNullRowIdRange)
                .containsExactly(new Range(9, 11), new Range(12, 14), new Range(15, 17));
        assertThat(clusteredFiles.get(1).fileSize())
                .isGreaterThan(
                        (clusteredFiles.get(0).fileSize() + clusteredFiles.get(2).fileSize()) * 8);

        updatePayload(table, "a", 9L, 10, 20, 30);
        updatePayload(table, "a", 15L, 70, 80, 90);
        updateSortKeys(table, "a", 9L, 10, 20, 30);
        updateSortKeys(table, "a", 15L, 70, 80, 90);
        append(table, "a", 120, 100, 110);
        if (deletionVectors) {
            delete(table, "a", new Range(9, 11), 9L);
        }
        return table;
    }

    private List<CommitMessage> prepareMinorCluster(FileStoreTable table, Snapshot snapshot)
            throws Exception {
        IncrementalClusterManager manager = new IncrementalClusterManager(table, null, snapshot);
        Map<BinaryRow, CompactUnit> units = manager.createCompactUnits(false);
        assertThat(units).hasSize(1);
        List<DataFileMeta> selected = units.get(partition(table, "a")).files();
        assertThat(selected)
                .extracting(DataFileMeta::nonNullRowIdRange)
                .containsExactlyInAnyOrder(
                        new Range(9, 11),
                        new Range(9, 11),
                        new Range(9, 11),
                        new Range(15, 17),
                        new Range(15, 17),
                        new Range(15, 17),
                        new Range(18, 20));
        assertThat(selected)
                .extracting(DataFileMeta::fileName)
                .doesNotContainAnyElementsOf(filesInRange(table, new Range(12, 14)));
        return rewriteClusterUnits(table, manager, units, true);
    }

    private void assertMinorClusterResult(
            FileStoreTable table,
            boolean deletionVectors,
            List<String> untouchedRows,
            List<String> untouchedFiles,
            long firstAssignedRowId,
            int expectedFileCount)
            throws Exception {
        List<String> expected = new ArrayList<>(untouchedRows);
        long rowId = firstAssignedRowId;
        for (int id : new int[] {10, 20, 30, 70, 80, 90, 100, 110, 120}) {
            if (deletionVectors && id == 10) {
                continue;
            }
            expected.add("a|" + id + "|" + (id < 100 ? "updated-" : "base-") + id + "|" + rowId++);
        }
        assertThat(readRows(table)).containsExactlyElementsOf(expected);
        assertThat(table.latestSnapshot().get().nextRowId()).isEqualTo(rowId);
        assertThat(filesInRange(table, new Range(12, 14)))
                .containsExactlyElementsOf(untouchedFiles);
        assertThat(fileNames(table, "a")).hasSize(expectedFileCount).containsAll(untouchedFiles);
    }

    private List<String> rowsInRange(FileStoreTable table, Range range) throws Exception {
        return readRows(table).stream()
                .filter(
                        row -> {
                            long rowId = Long.parseLong(row.substring(row.lastIndexOf('|') + 1));
                            return rowId >= range.from && rowId <= range.to;
                        })
                .collect(Collectors.toList());
    }

    private List<String> filesInRange(FileStoreTable table, Range range) {
        return table.store().newScan().withSnapshot(table.latestSnapshot().get()).plan().files()
                .stream()
                .filter(entry -> entry.partition().equals(partition(table, "a")))
                .map(entry -> entry.file())
                .filter(file -> file.nonNullRowIdRange().equals(range))
                .map(DataFileMeta::fileName)
                .sorted()
                .collect(Collectors.toList());
    }

    private void append(FileStoreTable table, String partition, int... values) throws Exception {
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

    private void updatePayload(
            FileStoreTable table, String partition, long firstRowId, int... values)
            throws Exception {
        commit(table, preparePayloadUpdate(table, partition, firstRowId, values));
    }

    private void updateSortKeys(
            FileStoreTable table, String partition, long firstRowId, int... values)
            throws Exception {
        RowType writeType = table.rowType().project(Arrays.asList("pt", "id"));
        try (BatchTableWrite write =
                table.newBatchWriteBuilder().newWrite().withWriteType(writeType)) {
            for (int value : values) {
                write.write(GenericRow.of(BinaryString.fromString(partition), value));
            }
            List<CommitMessage> messages = write.prepareCommit();
            assignFirstRowId(messages, firstRowId);
            commit(table, messages);
        }
    }

    private List<CommitMessage> preparePayloadUpdate(
            FileStoreTable table, String partition, long firstRowId, int... values)
            throws Exception {
        RowType writeType = table.rowType().project(Arrays.asList("pt", "payload"));
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite().withWriteType(writeType)) {
            for (int value : values) {
                write.write(
                        GenericRow.of(
                                BinaryString.fromString(partition),
                                BinaryString.fromString("updated-" + value)));
            }
            List<CommitMessage> messages = write.prepareCommit();
            assignFirstRowId(messages, firstRowId);
            return messages;
        }
    }

    private void assignFirstRowId(List<CommitMessage> messages, long firstRowId) {
        for (CommitMessage message : messages) {
            List<DataFileMeta> files = ((CommitMessageImpl) message).newFilesIncrement().newFiles();
            List<DataFileMeta> assigned =
                    files.stream()
                            .map(file -> file.assignFirstRowId(firstRowId))
                            .collect(Collectors.toList());
            files.clear();
            files.addAll(assigned);
        }
    }

    private List<CommitMessage> prepareCluster(
            FileStoreTable table,
            Snapshot snapshot,
            PartitionPredicate partitionPredicate,
            boolean fullCompaction)
            throws Exception {
        IncrementalClusterManager manager =
                new IncrementalClusterManager(table, partitionPredicate, snapshot);
        return rewriteClusterUnits(table, manager, manager.createCompactUnits(fullCompaction));
    }

    private List<CommitMessage> prepareMappedCluster(
            FileStoreTable table, Snapshot snapshot, boolean fullCompaction) throws Exception {
        IncrementalClusterManager manager = new IncrementalClusterManager(table, null, snapshot);
        return rewriteClusterUnits(
                table, manager, manager.createCompactUnits(fullCompaction), true);
    }

    // Run the engine-independent read/sort/write stages in-process. The source selection and
    // split planning are the same shared manager used by the distributed engine implementations.
    private List<CommitMessage> rewriteClusterUnits(
            FileStoreTable table,
            IncrementalClusterManager manager,
            Map<BinaryRow, CompactUnit> units)
            throws Exception {
        return rewriteClusterUnits(table, manager, units, false);
    }

    private List<CommitMessage> rewriteClusterUnits(
            FileStoreTable table,
            IncrementalClusterManager manager,
            Map<BinaryRow, CompactUnit> units,
            boolean preserveIndexes)
            throws Exception {
        Map<BinaryRow, Pair<List<DataSplit>, CommitMessage>> planned =
                manager.toSplitsAndRewriteDvFiles(units);
        List<CommitMessage> messages = new ArrayList<>();
        FileStoreTable writeTable =
                table.copy(Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), "true"));
        for (Map.Entry<BinaryRow, CompactUnit> entry : units.entrySet()) {
            Pair<List<DataSplit>, CommitMessage> partitionPlan = planned.get(entry.getKey());
            assertThat(partitionPlan.getRight()).isNull();
            List<DataFileMeta> after = new ArrayList<>();
            List<Long> oldRowIds = new ArrayList<>();
            RowType readType =
                    preserveIndexes
                            ? SpecialFields.rowTypeWithRowId(table.rowType())
                            : table.rowType();
            ProjectedRow dataRow = ProjectedRow.from(table.rowType(), readType);
            try (IOManager ioManager = IOManager.create(tempPath.toString());
                    RecordReaderIterator<InternalRow> reader =
                            new RecordReaderIterator<>(
                                    table.newReadBuilder()
                                            .withReadType(readType)
                                            .newRead()
                                            .createReader(
                                                    new ArrayList<>(partitionPlan.getLeft())));
                    BatchTableWrite write =
                            writeTable
                                    .newBatchWriteBuilder()
                                    .newWrite()
                                    .withWriteType(table.rowType())) {
                if (reader.hasNext()) {
                    Sorter sorter =
                            Sorter.getSorter(reader, ioManager, readType, table.coreOptions());
                    try {
                        MutableObjectIterator<BinaryRow> sorted = sorter.sort();
                        BinaryRow row = new BinaryRow(sorter.arity());
                        while ((row = sorted.next(row)) != null) {
                            InternalRow sortedRow = sorter.removeSortKey(row);
                            if (preserveIndexes) {
                                oldRowIds.add(sortedRow.getLong(readType.getFieldCount() - 1));
                            }
                            write.write(dataRow.replaceRow(sortedRow));
                        }
                    } finally {
                        sorter.close();
                    }
                }
                for (CommitMessage output : write.prepareCommit()) {
                    after.addAll(((CommitMessageImpl) output).newFilesIncrement().newFiles());
                }
            }
            DataFilePathFactory pathFactory =
                    table.store().pathFactory().createDataFilePathFactory(entry.getKey(), 0);
            int rowOffset = 0;
            for (int i = 0; preserveIndexes && i < after.size(); i++) {
                DataFileMeta file = after.get(i);
                String mappingName = ClusteringRowIdMapping.fileName(file.fileName());
                try (ClusteringRowIdMapping.Writer mapping =
                        ClusteringRowIdMapping.createWriter(
                                table.fileIO(), pathFactory.toAlignedPath(mappingName, file))) {
                    for (long row = 0; row < file.rowCount(); row++) {
                        mapping.write(oldRowIds.get(rowOffset++));
                    }
                }
                List<String> extraFiles = new ArrayList<>(file.extraFiles());
                extraFiles.add(mappingName);
                after.set(i, file.copy(extraFiles));
            }
            assertThat(rowOffset).isEqualTo(oldRowIds.size());
            messages.add(
                    new CommitMessageImpl(
                            entry.getKey(),
                            0,
                            null,
                            DataIncrement.emptyIncrement(),
                            new CompactIncrement(
                                    entry.getValue().files(),
                                    IncrementalClusterManager.upgrade(
                                            after, entry.getValue().outputLevel()),
                                    Collections.emptyList())));
        }
        return messages;
    }

    private void commitCluster(
            FileStoreTable table, Snapshot snapshot, List<CommitMessage> messages)
            throws Exception {
        commitChecked(
                table,
                IncrementalClusterRewriter.create(table, snapshot).prepareCommitMessages(messages));
    }

    private void commitChecked(FileStoreTable table, List<CommitMessage> messages)
            throws Exception {
        try (TableCommitImpl commit = table.newCommit("test-cluster")) {
            commit.materializeDvRowIdCheck().commit(messages);
        }
    }

    private void delete(FileStoreTable table, String value, Range range, long... rowIds)
            throws Exception {
        commit(table, prepareDelete(table, value, range, rowIds));
    }

    private CommitMessage prepareDelete(
            FileStoreTable table, String value, Range range, long... rowIds) throws Exception {
        BinaryRow partition = partition(table, value);
        List<DataFileMeta> group =
                table.store().newScan().withSnapshot(table.latestSnapshot().get()).plan().files()
                        .stream()
                        .filter(entry -> entry.partition().equals(partition))
                        .map(entry -> entry.file())
                        .filter(file -> file.nonNullRowIdRange().equals(range))
                        .collect(Collectors.toList());
        DataFileMeta anchor = retrieveAnchorFile(group, file -> file);
        BaseAppendDeleteFileMaintainer maintainer =
                BaseAppendDeleteFileMaintainer.forUnawareAppend(
                        table.store().newIndexFileHandler(),
                        table.latestSnapshot().get(),
                        partition);
        DeletionVector vector = new BitmapDeletionVector();
        for (long rowId : rowIds) {
            vector.delete(rowId - range.from);
        }
        maintainer.notifyNewDeletionVector(anchor.fileName(), vector);
        List<IndexFileMeta> additions = new ArrayList<>();
        List<IndexFileMeta> removals = new ArrayList<>();
        for (IndexManifestEntry entry : maintainer.persist()) {
            (entry.kind() == FileKind.ADD ? additions : removals).add(entry.indexFile());
        }
        return new CommitMessageImpl(
                partition,
                UNAWARE_BUCKET,
                null,
                new DataIncrement(
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        additions,
                        removals),
                CompactIncrement.emptyIncrement());
    }

    private void addGlobalIndex(
            FileStoreTable table, String value, String fileName, Range range, String column)
            throws Exception {
        commit(table, globalIndexMessage(table, value, fileName, range, column));
    }

    private CommitMessage globalIndexMessage(
            FileStoreTable table, String value, String fileName, Range range, String column) {
        IndexFileMeta index =
                new IndexFileMeta(
                        "test-global-index",
                        fileName,
                        1,
                        range.count(),
                        new GlobalIndexMeta(
                                range.from,
                                range.to,
                                table.rowType().getField(column).id(),
                                null,
                                null),
                        null);
        return new CommitMessageImpl(
                partition(table, value),
                UNAWARE_BUCKET,
                null,
                DataIncrement.indexIncrement(Collections.singletonList(index)),
                CompactIncrement.emptyIncrement());
    }

    private void commit(FileStoreTable table, CommitMessage message) throws Exception {
        commit(table, Collections.singletonList(message));
    }

    private void commit(FileStoreTable table, List<CommitMessage> messages) throws Exception {
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(messages);
        }
    }

    private List<CommitMessage> prepareGlobalIndex(
            FileStoreTable table, Snapshot snapshot, String column) throws Exception {
        return prepareGlobalIndex(table, snapshot, column, "btree", null);
    }

    private List<CommitMessage> prepareGlobalIndex(
            FileStoreTable table,
            Snapshot snapshot,
            String column,
            String indexType,
            Range fileRange)
            throws Exception {
        List<CommitMessage> messages = new ArrayList<>();
        for (DataSplit split :
                GlobalIndexBuilderUtils.splitByContiguousRowRange(
                        new SortedGlobalIndexScanner(table, indexType)
                                .withIndexFields(Collections.singletonList(column))
                                .withSnapshot(snapshot)
                                .scan()
                                .get()
                                .entries())) {
            if (fileRange != null) {
                Optional<DataSplit> filtered =
                        split.filterDataFile(file -> file.nonNullRowIdRange().equals(fileRange));
                if (!filtered.isPresent()) {
                    continue;
                }
                split = filtered.get();
            }
            messages.addAll(
                    SortedGlobalIndexTestUtils.buildIndex(
                            table, indexType, column, split, snapshot.id()));
        }
        assertThat(messages).isNotEmpty();
        return messages;
    }

    private List<String> readRows(FileStoreTable table) throws Exception {
        return readRows(table, null);
    }

    private List<String> readRows(FileStoreTable table, Predicate predicate) throws Exception {
        ReadBuilder builder =
                table.newReadBuilder()
                        .withReadType(SpecialFields.rowTypeWithRowId(table.rowType()));
        if (predicate != null) {
            builder.withFilter(predicate);
        }
        List<String> rows = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                builder.newRead().createReader(builder.newScan().plan())) {
            reader.forEachRemaining(
                    row -> {
                        if (predicate == null || predicate.test(row)) {
                            rows.add(
                                    row.getString(0)
                                            + "|"
                                            + row.getInt(1)
                                            + "|"
                                            + (row.isNullAt(2) ? "null" : row.getString(2))
                                            + "|"
                                            + row.getLong(3));
                        }
                    });
        }
        rows.sort(
                Comparator.comparingLong(
                        row -> Long.parseLong(row.substring(row.lastIndexOf('|') + 1))));
        return rows;
    }

    private void appendIndexRows(FileStoreTable table, int[] ids, String[] payloads)
            throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            for (int i = 0; i < ids.length; i++) {
                write.write(
                        GenericRow.of(
                                BinaryString.fromString("a"),
                                ids[i],
                                payloads[i] == null ? null : BinaryString.fromString(payloads[i])));
            }
            commit.commit(write.prepareCommit());
        }
    }

    private void assertIndexRows(FileStoreTable table, Predicate predicate, Range... expected)
            throws Exception {
        Optional<DataEvolutionGlobalIndexScanner> scanner =
                DataEvolutionGlobalIndexScanner.create(
                        table, PartitionPredicate.ALWAYS_TRUE, predicate);
        if (!scanner.isPresent() && expected.length == 0) {
            return;
        }
        assertThat(scanner).isPresent();
        try (DataEvolutionGlobalIndexScanner index = scanner.get()) {
            Optional<GlobalIndexResult> result = index.scan(predicate);
            assertThat(result).isPresent();
            assertThat(result.get().results().toRangeList()).containsExactly(expected);
        }
    }

    private List<String> fileNames(FileStoreTable table, String value) {
        BinaryRow partition = partition(table, value);
        return table.store().newScan().withSnapshot(table.latestSnapshot().get()).plan().files()
                .stream()
                .filter(entry -> entry.partition().equals(partition))
                .map(entry -> entry.file().fileName())
                .sorted()
                .collect(Collectors.toList());
    }

    private List<String> globalIndexNames(FileStoreTable table) {
        return table.store().newIndexFileHandler().scanEntries().stream()
                .filter(entry -> entry.indexFile().globalIndexMeta() != null)
                .map(entry -> entry.indexFile().fileName())
                .sorted()
                .collect(Collectors.toList());
    }

    private BinaryRow partition(FileStoreTable table, String value) {
        return new InternalRowSerializer(table.schema().logicalPartitionType())
                .toBinaryRow(GenericRow.of(BinaryString.fromString(value)));
    }

    private PartitionPredicate partitionPredicate(FileStoreTable table, String value) {
        return PartitionPredicate.fromMaps(
                table.schema().logicalPartitionType(),
                Collections.singletonList(Collections.singletonMap("pt", value)),
                table.coreOptions().partitionDefaultName());
    }
}
