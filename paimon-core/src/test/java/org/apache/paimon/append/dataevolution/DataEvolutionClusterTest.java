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
import org.apache.paimon.globalindex.GlobalIndexBuilderUtils;
import org.apache.paimon.globalindex.sorted.SortedGlobalIndexScanner;
import org.apache.paimon.globalindex.sorted.SortedGlobalIndexTestUtils;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.operation.commit.RowIdExistenceConflictException;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.partition.PartitionStatistics;
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
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
                .hasSize(2);
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

    // Run the engine-independent read/sort/write stages in-process. The source selection and
    // split planning are the same shared manager used by the distributed engine implementations.
    private List<CommitMessage> rewriteClusterUnits(
            FileStoreTable table,
            IncrementalClusterManager manager,
            Map<BinaryRow, CompactUnit> units)
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
            try (IOManager ioManager = IOManager.create(tempPath.toString());
                    RecordReaderIterator<InternalRow> reader =
                            new RecordReaderIterator<>(
                                    table.newReadBuilder()
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
                            Sorter.getSorter(
                                    reader, ioManager, table.rowType(), table.coreOptions());
                    try {
                        MutableObjectIterator<BinaryRow> sorted = sorter.sort();
                        BinaryRow row = new BinaryRow(sorter.arity());
                        while ((row = sorted.next(row)) != null) {
                            write.write(sorter.removeSortKey(row));
                        }
                    } finally {
                        sorter.close();
                    }
                }
                for (CommitMessage output : write.prepareCommit()) {
                    after.addAll(((CommitMessageImpl) output).newFilesIncrement().newFiles());
                }
            }
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
        commit(
                table,
                new CommitMessageImpl(
                        partition(table, value),
                        UNAWARE_BUCKET,
                        null,
                        DataIncrement.indexIncrement(Collections.singletonList(index)),
                        CompactIncrement.emptyIncrement()));
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
        List<CommitMessage> messages = new ArrayList<>();
        for (DataSplit split :
                GlobalIndexBuilderUtils.splitByContiguousRowRange(
                        new SortedGlobalIndexScanner(table, "btree")
                                .withIndexFields(Collections.singletonList(column))
                                .withSnapshot(snapshot)
                                .scan()
                                .get()
                                .entries())) {
            messages.addAll(
                    SortedGlobalIndexTestUtils.buildIndex(
                            table, "btree", column, split, snapshot.id()));
        }
        assertThat(messages).isNotEmpty();
        return messages;
    }

    private List<String> readRows(FileStoreTable table) throws Exception {
        ReadBuilder builder =
                table.newReadBuilder()
                        .withReadType(SpecialFields.rowTypeWithRowId(table.rowType()));
        List<String> rows = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                builder.newRead().createReader(builder.newScan().plan())) {
            reader.forEachRemaining(
                    row ->
                            rows.add(
                                    row.getString(0)
                                            + "|"
                                            + row.getInt(1)
                                            + "|"
                                            + row.getString(2)
                                            + "|"
                                            + row.getLong(3)));
        }
        rows.sort(
                Comparator.comparingLong(
                        row -> Long.parseLong(row.substring(row.lastIndexOf('|') + 1))));
        return rows;
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
