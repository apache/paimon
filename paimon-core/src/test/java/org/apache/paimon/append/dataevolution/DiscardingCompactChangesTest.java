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
import org.apache.paimon.catalog.RenamingSnapshotCommit;
import org.apache.paimon.catalog.SnapshotCommit;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.FileStoreCommit;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real data-evolution compaction commits with concurrent column updates. */
public class DiscardingCompactChangesTest extends TableTestBase {

    @Override
    protected Schema schemaDefault() {
        return Schema.newBuilder()
                .column("pt", DataTypes.STRING())
                .column("id", DataTypes.INT())
                .column("quality", DataTypes.INT())
                .partitionKeys("pt")
                .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                .build();
    }

    @Test
    void testDiscardOnlyConflictingTask() throws Exception {
        FileStoreTable table = prepare();
        CommitMessageImpl taskA = compact(table, "a");
        CommitMessageImpl taskB = compact(table, "b");
        updateQuality(table, "a", 0, 99);

        List<CommitMessage> messages = Arrays.asList(taskA, taskB);
        assertThatThrownBy(() -> commit(table, messages, false)).hasMessageContaining("data files");
        commit(table, messages, true);

        List<String> liveFiles = liveFiles(table);
        assertThat(liveFiles)
                .containsAll(fileNames(taskA.compactIncrement().compactBefore()))
                .containsAll(fileNames(taskB.compactIncrement().compactAfter()))
                .doesNotContainAnyElementsOf(fileNames(taskA.compactIncrement().compactAfter()))
                .doesNotContainAnyElementsOf(fileNames(taskB.compactIncrement().compactBefore()));
        assertRows(table, 99, 0, 0, 0);
    }

    @Test
    void testAllTasksConflictCreatesIdempotentEmptyCompact() throws Exception {
        FileStoreTable table = prepare();
        CommitMessageImpl taskA = compact(table, "a");
        updateQuality(table, "a", 0, 99);
        long before = table.snapshotManager().latestSnapshotId();

        FileStoreTable enabled =
                table.copy(
                        Collections.singletonMap(
                                CoreOptions.DATA_EVOLUTION_COMPACTION_DISCARD_CONFLICTING_TASKS
                                        .key(),
                                "true"));
        ManifestCommittable committable = new ManifestCommittable(42);
        committable.addFileCommittable(taskA);
        try (FileStoreCommit commit = enabled.store().newCommit("compact-task-filter", enabled)) {
            commit.commit(committable, false);
            assertThat(commit.filterCommitted(Collections.singletonList(committable))).isEmpty();
        }
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(before + 1);
        assertThat(table.snapshotManager().latestSnapshot().commitKind())
                .isEqualTo(Snapshot.CommitKind.COMPACT);
        assertThat(liveFiles(table))
                .containsAll(fileNames(taskA.compactIncrement().compactBefore()))
                .doesNotContainAnyElementsOf(fileNames(taskA.compactIncrement().compactAfter()));
        assertRows(table, 99, 0, 0, 0);
    }

    @Test
    void testSharedInputTasksFailClosed() throws Exception {
        FileStoreTable table = prepare();
        CommitMessageImpl first = compact(table, "a");
        CommitMessageImpl second = compact(table, "a");
        CommitMessageImpl independent = compact(table, "b");
        updateQuality(table, "a", 0, 99);

        long before = table.snapshotManager().latestSnapshotId();
        assertThatThrownBy(() -> commit(table, Arrays.asList(first, second, independent), true))
                .hasStackTraceContaining("not previously added");
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(before);
        assertRows(table, 99, 0, 0, 0);
    }

    @Test
    void testRetryChecksNewSnapshotAndDiscardsNewConflict() throws Exception {
        FileStoreTable table = prepare();
        CommitMessageImpl taskA = compact(table, "a");
        CommitMessageImpl taskB = compact(table, "b");
        updateQuality(table, "a", 0, 99);
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
                                if (snapshot.commitKind() == Snapshot.CommitKind.COMPACT
                                        && attempts.getAndIncrement() == 0) {
                                    updateQuality(table, "b", 2, 77);
                                }
                                return delegate.commit(baseUuid, snapshot, branch, statistics);
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
                        table.fileIO(), table.location(), table.schema(), environment);

        commit(committingTable, Arrays.asList(taskA, taskB), true);
        assertThat(attempts.get()).isEqualTo(2);
        assertThat(liveFiles(table))
                .containsAll(fileNames(taskA.compactIncrement().compactBefore()))
                .containsAll(fileNames(taskB.compactIncrement().compactBefore()))
                .doesNotContainAnyElementsOf(fileNames(taskA.compactIncrement().compactAfter()))
                .doesNotContainAnyElementsOf(fileNames(taskB.compactIncrement().compactAfter()));
        assertRows(table, 99, 0, 77, 0);
    }

    @Test
    void testRowIdBaselineConflictStillFails() throws Exception {
        FileStoreTable table = prepare();
        long baseline = table.snapshotManager().latestSnapshotId();
        CommitMessageImpl task = compact(table, "a");
        updateQuality(table, "a", 0, 99);
        long before = table.snapshotManager().latestSnapshotId();

        assertThatThrownBy(
                        () ->
                                commit(
                                        table,
                                        Collections.singletonList(
                                                task.withCheckFromSnapshot(baseline)),
                                        true))
                .hasStackTraceContaining("conflict");
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(before);
        assertRows(table, 99, 0, 0, 0);
    }

    @Test
    void testOrdinaryWriteUnaffected() throws Exception {
        FileStoreTable table = prepare();
        FileStoreTable enabled =
                table.copy(
                        Collections.singletonMap(
                                CoreOptions.DATA_EVOLUTION_COMPACTION_DISCARD_CONFLICTING_TASKS
                                        .key(),
                                "true"));
        write(enabled, GenericRow.of(BinaryString.fromString("c"), 4, 7));
        assertThat(read(table)).hasSize(5);
    }

    @Test
    void testCompactOutputRebasedAfterRowIdReassignment() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        write(table, GenericRow.of(BinaryString.fromString("a"), 0, 0));
        write(table, GenericRow.of(BinaryString.fromString("a"), 1, 0));
        write(table, GenericRow.of(BinaryString.fromString("b"), 2, 0));
        write(table, GenericRow.of(BinaryString.fromString("b"), 3, 0));
        write(table, GenericRow.of(BinaryString.fromString("a"), 4, 0));
        CommitMessageImpl taskB = compact(table, "b");
        long planned = table.snapshotManager().latestSnapshotId();
        assertThat(new DataEvolutionRowIdReassigner(table).reassign().reassigned).isTrue();

        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.COMMIT_LAST_SAFE_SNAPSHOT.key(), Long.toString(planned));
        options.put(CoreOptions.DATA_EVOLUTION_COMPACTION_DISCARD_CONFLICTING_TASKS.key(), "true");
        try (BatchTableCommit commit = table.copy(options).newBatchWriteBuilder().newCommit()) {
            commit.commit(Collections.singletonList(taskB));
        }
        assertThat(liveFiles(table))
                .containsAll(fileNames(taskB.compactIncrement().compactAfter()))
                .doesNotContainAnyElementsOf(fileNames(taskB.compactIncrement().compactBefore()));
        assertThat(read(table)).hasSize(5);
    }

    private FileStoreTable prepare() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        for (String partition : Arrays.asList("a", "b")) {
            for (int id = 0; id < 2; id++) {
                write(table, GenericRow.of(BinaryString.fromString(partition), id, 0));
            }
        }
        return table;
    }

    private CommitMessageImpl compact(FileStoreTable table, String partition) throws Exception {
        List<ManifestEntry> inputs =
                table.store().newScan().plan().files().stream()
                        .filter(
                                entry ->
                                        entry.partition().getString(0).toString().equals(partition))
                        .sorted(Comparator.comparingLong(ManifestEntry::firstRowId))
                        .collect(Collectors.toList());
        assertThat(inputs).hasSize(2);
        return (CommitMessageImpl)
                new DataEvolutionNormalCompactTask(
                                inputs.get(0).partition(),
                                inputs.stream()
                                        .map(ManifestEntry::file)
                                        .collect(Collectors.toList()))
                        .doCompact(table, "compact-worker");
    }

    private void updateQuality(FileStoreTable table, String partition, long firstRowId, int quality)
            throws Exception {
        try (BatchTableWrite write =
                        table.newBatchWriteBuilder()
                                .newWrite()
                                .withWriteType(table.rowType().project("pt", "quality"));
                BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            write.write(GenericRow.of(BinaryString.fromString(partition), quality));
            List<CommitMessage> messages = write.prepareCommit();
            for (CommitMessage message : messages) {
                List<DataFileMeta> files =
                        ((CommitMessageImpl) message).newFilesIncrement().newFiles();
                List<DataFileMeta> assigned =
                        files.stream()
                                .map(file -> file.assignFirstRowId(firstRowId))
                                .collect(Collectors.toList());
                files.clear();
                files.addAll(assigned);
            }
            commit.commit(messages);
        }
    }

    private void commit(FileStoreTable table, List<CommitMessage> messages, boolean discard)
            throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(
                CoreOptions.DATA_EVOLUTION_COMPACTION_DISCARD_CONFLICTING_TASKS.key(),
                Boolean.toString(discard));
        try (BatchTableCommit commit = table.copy(options).newBatchWriteBuilder().newCommit()) {
            commit.commit(messages);
        }
    }

    private List<String> liveFiles(FileStoreTable table) {
        return table.store().newScan().plan().files().stream()
                .map(ManifestEntry::fileName)
                .collect(Collectors.toList());
    }

    private List<String> fileNames(List<DataFileMeta> files) {
        return files.stream().map(DataFileMeta::fileName).collect(Collectors.toList());
    }

    private void assertRows(FileStoreTable table, int... expectedQuality) throws Exception {
        List<InternalRow> rows = new ArrayList<>(read(table));
        rows.sort(
                Comparator.comparing((InternalRow row) -> row.getString(0).toString())
                        .thenComparingInt(row -> row.getInt(1)));
        assertThat(rows).hasSize(expectedQuality.length);
        for (int i = 0; i < rows.size(); i++) {
            assertThat(rows.get(i).getInt(2)).isEqualTo(expectedQuality[i]);
        }
    }
}
