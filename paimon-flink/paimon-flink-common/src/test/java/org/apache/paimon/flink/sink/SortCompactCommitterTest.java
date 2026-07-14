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

package org.apache.paimon.flink.sink;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.TestAppendFileStore;
import org.apache.paimon.TestKeyValueGenerator;
import org.apache.paimon.append.SortCompactCommitMessageRewriter;
import org.apache.paimon.append.SortCompactPlanMetadata;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.sink.listener.CommitListener;
import org.apache.paimon.flink.sink.listener.CommitListenerFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOFinder;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.TraceableFileIO;

import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.paimon.io.DataFileTestUtils.newFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

/** Test for {@link SortCompactCommitter}. */
public class SortCompactCommitterTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testMergePartialCommittablesBeforeRewrite() throws Exception {
        TestAppendFileStore store = createAppendStore(new HashMap<>());
        CommitMessageImpl initial =
                store.writeDataFiles(
                        BinaryRow.EMPTY_ROW, 0, Arrays.asList("data-0.orc", "data-1.orc"));
        store.commit(initial);

        FileStoreTable table =
                FileStoreTableFactory.create(
                        store.fileIO(), store.options().path(), store.schema());
        SnapshotReader.Plan plan = table.newSnapshotReader().read();
        Long baseSnapshotId = plan.snapshotId();
        List<DataSplit> dataSplits = plan.dataSplits();
        long snapshotsBeforeCommit = table.snapshotManager().snapshotCount();

        DataFileMeta sorted0 = newFile("sorted-0.orc", 0, 0, 100, 100);
        DataFileMeta sorted1 = newFile("sorted-1.orc", 0, 101, 200, 200);
        CommitMessageImpl written0 =
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        0,
                        table.coreOptions().bucket(),
                        new DataIncrement(
                                Collections.singletonList(sorted0),
                                Collections.emptyList(),
                                Collections.emptyList()),
                        CompactIncrement.emptyIncrement());
        CommitMessageImpl written1 =
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        0,
                        table.coreOptions().bucket(),
                        new DataIncrement(
                                Collections.singletonList(sorted1),
                                Collections.emptyList(),
                                Collections.emptyList()),
                        CompactIncrement.emptyIncrement());

        ManifestCommittable first = new ManifestCommittable(1L, 10L);
        first.addFileCommittable(written0);
        ManifestCommittable second = new ManifestCommittable(2L, 20L);
        second.addFileCommittable(written1);

        String commitUser = UUID.randomUUID().toString();
        try (TableCommitImpl commit = table.newCommit(commitUser)) {
            SortCompactCommitter committer =
                    new SortCompactCommitter(
                            table,
                            commit,
                            Committer.createContext(commitUser, null, true, false, null, 1, 1),
                            new SortCompactCommitMessageRewriter(
                                    table,
                                    baseSnapshotId == null ? 0L : baseSnapshotId,
                                    dataSplits));
            committer.commit(Arrays.asList(first, second));
        }

        assertThat(table.snapshotManager().snapshotCount()).isEqualTo(snapshotsBeforeCommit + 1);
        Snapshot latest = table.snapshotManager().latestSnapshot();
        assertThat(latest.commitKind()).isEqualTo(Snapshot.CommitKind.COMPACT);

        List<DataFileMeta> remainingFiles = new ArrayList<>();
        for (ManifestEntry entry : table.store().newScan().plan().files()) {
            remainingFiles.add(entry.file());
        }
        assertThat(remainingFiles)
                .containsExactlyInAnyOrder(
                        sorted0.assignFileSource(FileSource.COMPACT),
                        sorted1.assignFileSource(FileSource.COMPACT));
    }

    @Test
    public void testCountsCompactAfterInMetrics() throws Exception {
        TestAppendFileStore store = createAppendStore(new HashMap<>());
        CommitMessageImpl initial =
                store.writeDataFiles(
                        BinaryRow.EMPTY_ROW, 0, Arrays.asList("data-0.orc", "data-1.orc"));
        store.commit(initial);

        FileStoreTable table =
                FileStoreTableFactory.create(
                        store.fileIO(), store.options().path(), store.schema());
        SnapshotReader.Plan plan = table.newSnapshotReader().read();
        Long baseSnapshotId = plan.snapshotId();
        List<DataSplit> dataSplits = plan.dataSplits();

        DataFileMeta sorted0 = newFile("sorted-0.orc", 0, 0, 100, 100);
        DataFileMeta sorted1 = newFile("sorted-1.orc", 0, 101, 200, 200);
        CommitMessageImpl written =
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        0,
                        table.coreOptions().bucket(),
                        new DataIncrement(
                                Arrays.asList(sorted0, sorted1),
                                Collections.emptyList(),
                                Collections.emptyList()),
                        CompactIncrement.emptyIncrement());

        ManifestCommittable manifestCommittable = new ManifestCommittable(1L, 10L);
        manifestCommittable.addFileCommittable(written);

        String commitUser = UUID.randomUUID().toString();
        OperatorMetricGroup metricGroup = UnregisteredMetricsGroup.createOperatorMetricGroup();
        try (TableCommitImpl commit = table.newCommit(commitUser)) {
            SortCompactCommitter committer =
                    new SortCompactCommitter(
                            table,
                            commit,
                            Committer.createContext(
                                    commitUser, metricGroup, true, false, null, 1, 1),
                            new SortCompactCommitMessageRewriter(
                                    table,
                                    baseSnapshotId == null ? 0L : baseSnapshotId,
                                    dataSplits));
            committer.commit(Collections.singletonList(manifestCommittable));
            assertThat(committer.getCommitterMetrics().getNumBytesOutCounter().getCount())
                    .isGreaterThan(0);
            assertThat(committer.getCommitterMetrics().getNumRecordsOutCounter().getCount())
                    .isGreaterThan(0);
        }
    }

    @Test
    public void testCommitUsesCapturedPlanMetadataWhenBaseSnapshotExpired() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        TestAppendFileStore store = createAppendStore(options);
        CommitMessageImpl initial =
                store.writeDataFiles(
                        BinaryRow.EMPTY_ROW, 0, Arrays.asList("data-0.orc", "data-1.orc"));
        store.commit(initial);

        Map<String, List<Integer>> dvs = new HashMap<>();
        dvs.put("data-0.orc", Arrays.asList(1, 3, 5));
        CommitMessageImpl dvMessage = store.writeDVIndexFiles(BinaryRow.EMPTY_ROW, 0, dvs);
        store.commit(dvMessage);

        FileStoreTable table =
                FileStoreTableFactory.create(
                        store.fileIO(), store.options().path(), store.schema());
        SnapshotReader.Plan plan = table.newSnapshotReader().read();
        long baseSnapshotId = plan.snapshotId();
        List<DataSplit> dataSplits = plan.dataSplits();
        SortCompactPlanMetadata planMetadata =
                SortCompactPlanMetadata.capture(table, baseSnapshotId, dataSplits);

        CommitMessageImpl keepLatestSnapshotAlive =
                store.writeDataFiles(
                        BinaryRow.EMPTY_ROW, 0, Collections.singletonList("data-2.orc"));
        store.commit(keepLatestSnapshotAlive);

        DataFileMeta sorted = newFile("sorted-0.orc", 0, 0, 100, 100);
        CommitMessageImpl written =
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        0,
                        table.coreOptions().bucket(),
                        new DataIncrement(
                                Collections.singletonList(sorted),
                                Collections.emptyList(),
                                Collections.emptyList()),
                        CompactIncrement.emptyIncrement());
        ManifestCommittable manifestCommittable = new ManifestCommittable(1L, 10L);
        manifestCommittable.addFileCommittable(written);

        table.snapshotManager().deleteSnapshot(baseSnapshotId);

        List<CommitMessage> rewritten =
                new SortCompactCommitMessageRewriter(
                                table, baseSnapshotId, dataSplits, planMetadata)
                        .rewrite(Collections.singletonList(written));
        assertThat(((CommitMessageImpl) rewritten.get(0)).compactIncrement().deletedIndexFiles())
                .isNotEmpty();

        String commitUser = UUID.randomUUID().toString();
        try (TableCommitImpl commit = table.newCommit(commitUser)) {
            SortCompactCommitter committer =
                    new SortCompactCommitter(
                            table,
                            commit,
                            Committer.createContext(commitUser, null, true, false, null, 1, 1),
                            new SortCompactCommitMessageRewriter(
                                    table, baseSnapshotId, dataSplits, planMetadata));
            committer.commit(Collections.singletonList(manifestCommittable));
        }

        Snapshot latest = table.snapshotManager().latestSnapshot();
        assertThat(latest.commitKind()).isEqualTo(Snapshot.CommitKind.COMPACT);
        assertThat(fileNames(table)).contains("sorted-0.orc");
    }

    private static Set<String> fileNames(FileStoreTable table) {
        Set<String> names = new HashSet<>();
        for (ManifestEntry entry : table.store().newScan().plan().files()) {
            names.add(entry.file().fileName());
        }
        return names;
    }

    @Test
    public void testCommitDeleteOnlyWhenWriterProducesNoCommittable() throws Exception {
        TestAppendFileStore store = createAppendStore(new HashMap<>());
        CommitMessageImpl initial =
                store.writeDataFiles(
                        BinaryRow.EMPTY_ROW, 0, Arrays.asList("data-0.orc", "data-1.orc"));
        store.commit(initial);

        FileStoreTable table =
                FileStoreTableFactory.create(
                        store.fileIO(), store.options().path(), store.schema());
        SnapshotReader.Plan plan = table.newSnapshotReader().read();
        long snapshotsBeforeCommit = table.snapshotManager().snapshotCount();

        String commitUser = UUID.randomUUID().toString();
        try (TableCommitImpl commit = table.newCommit(commitUser)) {
            SortCompactCommitter committer =
                    new SortCompactCommitter(
                            table,
                            commit,
                            Committer.createContext(commitUser, null, true, false, null, 1, 1),
                            new SortCompactCommitMessageRewriter(
                                    table,
                                    plan.snapshotId() == null ? 0L : plan.snapshotId(),
                                    plan.dataSplits()));
            committer.commit(Collections.emptyList());
        }

        assertThat(table.snapshotManager().snapshotCount()).isEqualTo(snapshotsBeforeCommit + 1);
        assertThat(table.snapshotManager().latestSnapshot().commitKind())
                .isEqualTo(Snapshot.CommitKind.COMPACT);
        assertThat(table.store().newScan().plan().files()).isEmpty();
    }

    @Test
    public void testFilterAndCommitRecoveryDoesNotDeletePlannedInput() throws Exception {
        TestAppendFileStore store = createAppendStore(new HashMap<>());
        CommitMessageImpl initial =
                store.writeDataFiles(
                        BinaryRow.EMPTY_ROW, 0, Arrays.asList("data-0.orc", "data-1.orc"));
        store.commit(initial);

        FileStoreTable table =
                FileStoreTableFactory.create(
                        store.fileIO(), store.options().path(), store.schema());
        SnapshotReader.Plan plan = table.newSnapshotReader().read();
        long snapshotsBeforeRecover = table.snapshotManager().snapshotCount();

        String commitUser = UUID.randomUUID().toString();
        try (TableCommitImpl commit = table.newCommit(commitUser)) {
            SortCompactCommitter committer =
                    new SortCompactCommitter(
                            table,
                            commit,
                            Committer.createContext(commitUser, null, true, false, null, 1, 1),
                            new SortCompactCommitMessageRewriter(
                                    table,
                                    plan.snapshotId() == null ? 0L : plan.snapshotId(),
                                    plan.dataSplits()));
            int committed = committer.filterAndCommit(Collections.emptyList(), true, true);
            assertThat(committed).isZero();
        }

        assertThat(table.snapshotManager().snapshotCount()).isEqualTo(snapshotsBeforeRecover);
        assertThat(table.store().newScan().plan().files()).hasSize(2);
    }

    @Test
    public void testFilterAndCommitDeleteOnlyWhenWriterProducesNoCommittable() throws Exception {
        TestAppendFileStore store = createAppendStore(new HashMap<>());
        CommitMessageImpl initial =
                store.writeDataFiles(
                        BinaryRow.EMPTY_ROW, 0, Arrays.asList("data-0.orc", "data-1.orc"));
        store.commit(initial);

        FileStoreTable table =
                FileStoreTableFactory.create(
                        store.fileIO(), store.options().path(), store.schema());
        SnapshotReader.Plan plan = table.newSnapshotReader().read();
        long snapshotsBeforeCommit = table.snapshotManager().snapshotCount();

        String commitUser = UUID.randomUUID().toString();
        try (TableCommitImpl commit = table.newCommit(commitUser)) {
            SortCompactCommitter committer =
                    new SortCompactCommitter(
                            table,
                            commit,
                            Committer.createContext(commitUser, null, true, false, null, 1, 1),
                            new SortCompactCommitMessageRewriter(
                                    table,
                                    plan.snapshotId() == null ? 0L : plan.snapshotId(),
                                    plan.dataSplits()));
            int committed = committer.filterAndCommit(Collections.emptyList(), false, true);
            assertThat(committed).isEqualTo(1);
            assertThat(committer.filterAndCommit(Collections.emptyList(), false, true)).isZero();
        }

        assertThat(table.snapshotManager().snapshotCount()).isEqualTo(snapshotsBeforeCommit + 1);
        assertThat(table.snapshotManager().latestSnapshot().commitKind())
                .isEqualTo(Snapshot.CommitKind.COMPACT);
        assertThat(table.store().newScan().plan().files()).isEmpty();
    }

    @Test
    public void testFilterAndCommitRecoveryDoesNotCreateDvIndexFiles() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        TestAppendFileStore store = createAppendStore(options);
        CommitMessageImpl initial =
                store.writeDataFiles(
                        BinaryRow.EMPTY_ROW, 0, Arrays.asList("data-0.orc", "data-1.orc"));
        store.commit(initial);

        Map<String, List<Integer>> dvs = new HashMap<>();
        dvs.put("data-0.orc", Arrays.asList(1, 3, 5));
        CommitMessageImpl dvMessage = store.writeDVIndexFiles(BinaryRow.EMPTY_ROW, 0, dvs);
        store.commit(dvMessage);

        FileStoreTable table =
                FileStoreTableFactory.create(
                        store.fileIO(), store.options().path(), store.schema());
        SnapshotReader.Plan plan = table.newSnapshotReader().read();
        Long baseSnapshotId = plan.snapshotId();
        List<DataSplit> dataSplits = plan.dataSplits();
        SortCompactPlanMetadata planMetadata =
                SortCompactPlanMetadata.capture(table, baseSnapshotId, dataSplits);

        CommitMessageImpl written =
                store.writeDataFiles(
                        BinaryRow.EMPTY_ROW, 0, Collections.singletonList("sorted-0.orc"));
        ManifestCommittable manifestCommittable = new ManifestCommittable(1L, 10L);
        manifestCommittable.addFileCommittable(written);

        String commitUser = UUID.randomUUID().toString();
        try (TableCommitImpl commit = table.newCommit(commitUser)) {
            SortCompactCommitter committer =
                    new SortCompactCommitter(
                            table,
                            commit,
                            Committer.createContext(commitUser, null, true, false, null, 1, 1),
                            new SortCompactCommitMessageRewriter(
                                    table, baseSnapshotId, dataSplits, planMetadata));
            committer.filterAndCommit(Collections.singletonList(manifestCommittable), true, false);

            Set<String> indexFilesBeforeRecovery = listIndexFileNames(table);
            int committed =
                    committer.filterAndCommit(
                            Collections.singletonList(manifestCommittable), true, false);
            Set<String> indexFilesAfterRecovery = listIndexFileNames(table);

            assertThat(committed).isZero();
            assertThat(indexFilesAfterRecovery).isEqualTo(indexFilesBeforeRecovery);
        }
    }

    @Test
    public void testFilterAndCommitRecoveryNotifiesListeners() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(FlinkConnectorOptions.COMMIT_CUSTOM_LISTENERS.key(), "counting-listener");
        TestAppendFileStore store = createAppendStore(options);
        CommitMessageImpl initial =
                store.writeDataFiles(
                        BinaryRow.EMPTY_ROW, 0, Arrays.asList("data-0.orc", "data-1.orc"));
        store.commit(initial);

        FileStoreTable table =
                FileStoreTableFactory.create(
                        store.fileIO(), store.options().path(), store.schema());
        SnapshotReader.Plan plan = table.newSnapshotReader().read();
        Long baseSnapshotId = plan.snapshotId();
        List<DataSplit> dataSplits = plan.dataSplits();

        CommitMessageImpl written =
                store.writeDataFiles(
                        BinaryRow.EMPTY_ROW, 0, Collections.singletonList("sorted-0.orc"));
        ManifestCommittable manifestCommittable = new ManifestCommittable(1L, 10L);
        manifestCommittable.addFileCommittable(written);

        CountingCommitListener.NOTIFY_COUNT.set(0);
        String commitUser = UUID.randomUUID().toString();
        try (TableCommitImpl commit = table.newCommit(commitUser)) {
            SortCompactCommitter committer =
                    new SortCompactCommitter(
                            table,
                            commit,
                            Committer.createContext(commitUser, null, true, false, null, 1, 1),
                            new SortCompactCommitMessageRewriter(
                                    table,
                                    baseSnapshotId == null ? 0L : baseSnapshotId,
                                    dataSplits));
            assertThat(
                            committer.filterAndCommit(
                                    Collections.singletonList(manifestCommittable), false, true))
                    .isEqualTo(1);
            assertThat(CountingCommitListener.NOTIFY_COUNT.get()).isEqualTo(1);

            assertThat(
                            committer.filterAndCommit(
                                    Collections.singletonList(manifestCommittable), false, true))
                    .isZero();
            assertThat(CountingCommitListener.NOTIFY_COUNT.get()).isEqualTo(2);
        }
    }

    @Test
    public void testCommitFailureAbortsWrittenMessages() throws Exception {
        TestAppendFileStore store = createAppendStore(new HashMap<>());
        CommitMessageImpl initial =
                store.writeDataFiles(
                        BinaryRow.EMPTY_ROW, 0, Arrays.asList("data-0.orc", "data-1.orc"));
        store.commit(initial);

        FileStoreTable table =
                FileStoreTableFactory.create(
                        store.fileIO(), store.options().path(), store.schema());
        SnapshotReader.Plan plan = table.newSnapshotReader().read();
        Long baseSnapshotId = plan.snapshotId();
        List<DataSplit> dataSplits = plan.dataSplits();

        DataFileMeta sorted = newFile("sorted-0.orc", 0, 0, 100, 100);
        CommitMessageImpl written =
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        0,
                        table.coreOptions().bucket(),
                        new DataIncrement(
                                Collections.singletonList(sorted),
                                Collections.emptyList(),
                                Collections.emptyList()),
                        CompactIncrement.emptyIncrement());
        ManifestCommittable manifestCommittable = new ManifestCommittable(1L, 10L);
        manifestCommittable.addFileCommittable(written);

        AtomicInteger abortCount = new AtomicInteger(0);
        SortCompactCommitMessageRewriter rewriter =
                new SortCompactCommitMessageRewriter(
                        table, baseSnapshotId == null ? 0L : baseSnapshotId, dataSplits) {
                    @Override
                    public void abortWrittenMessages(List<CommitMessage> writtenMessages) {
                        abortCount.incrementAndGet();
                    }
                };

        String commitUser = UUID.randomUUID().toString();
        try (TableCommitImpl commit = table.newCommit(commitUser)) {
            TableCommitImpl spiedCommit = spy(commit);
            doThrow(new RuntimeException("commit failed"))
                    .when(spiedCommit)
                    .commitMultiple(anyList(), eq(false));
            SortCompactCommitter committer =
                    new SortCompactCommitter(
                            table,
                            spiedCommit,
                            Committer.createContext(commitUser, null, true, false, null, 1, 1),
                            rewriter);
            assertThatThrownBy(
                            () -> committer.commit(Collections.singletonList(manifestCommittable)))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("commit failed");
        }
        assertThat(abortCount).hasValue(1);
    }

    @Test
    public void testCommitFailureAfterSnapshotDoesNotAbortWrittenMessages() throws Exception {
        TestAppendFileStore store = createAppendStore(new HashMap<>());
        CommitMessageImpl initial =
                store.writeDataFiles(
                        BinaryRow.EMPTY_ROW, 0, Collections.singletonList("data-0.orc"));
        store.commit(initial);

        FileStoreTable table =
                FileStoreTableFactory.create(
                        store.fileIO(), store.options().path(), store.schema());
        SnapshotReader.Plan plan = table.newSnapshotReader().read();
        Long baseSnapshotId = plan.snapshotId();
        List<DataSplit> dataSplits = plan.dataSplits();

        DataFileMeta sorted = newFile("sorted-0.orc", 0, 0, 100, 100);
        CommitMessageImpl written =
                store.writeDataFiles(
                        BinaryRow.EMPTY_ROW, 0, Collections.singletonList("sorted-0.orc"));
        ManifestCommittable manifestCommittable = new ManifestCommittable(1L, 10L);
        manifestCommittable.addFileCommittable(written);

        AtomicBoolean aborted = new AtomicBoolean(false);
        SortCompactCommitMessageRewriter rewriter =
                new SortCompactCommitMessageRewriter(
                        table, baseSnapshotId == null ? 0L : baseSnapshotId, dataSplits) {
                    @Override
                    public void abortWrittenMessages(List<CommitMessage> writtenMessages) {
                        aborted.set(true);
                        super.abortWrittenMessages(writtenMessages);
                    }
                };

        String commitUser = UUID.randomUUID().toString();
        try (TableCommitImpl commit = table.newCommit(commitUser)) {
            TableCommitImpl spiedCommit = spy(commit);
            doAnswer(
                            invocation -> {
                                invocation.callRealMethod();
                                throw new RuntimeException("failed after snapshot");
                            })
                    .when(spiedCommit)
                    .commitMultiple(anyList(), eq(false));
            SortCompactCommitter committer =
                    new SortCompactCommitter(
                            table,
                            spiedCommit,
                            Committer.createContext(commitUser, null, true, false, null, 1, 1),
                            rewriter);
            assertThatThrownBy(
                            () -> committer.commit(Collections.singletonList(manifestCommittable)))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("failed after snapshot");
        }

        assertThat(aborted).isFalse();
        assertThat(table.snapshotManager().latestSnapshot().commitKind())
                .isEqualTo(Snapshot.CommitKind.COMPACT);
    }

    /** A test {@link CommitListener} that counts notifications. */
    public static class CountingCommitListener implements CommitListener {

        static final AtomicInteger NOTIFY_COUNT = new AtomicInteger(0);

        @Override
        public void notifyCommittable(List<ManifestCommittable> committables) {
            NOTIFY_COUNT.incrementAndGet();
        }

        @Override
        public void snapshotState() {}

        @Override
        public void close() {}

        /** Factory for {@link CountingCommitListener}. */
        public static class Factory implements CommitListenerFactory {

            @Override
            public String identifier() {
                return "counting-listener";
            }

            @Override
            public Optional<CommitListener> create(
                    Committer.Context context, FileStoreTable table) {
                return Optional.of(new CountingCommitListener());
            }
        }
    }

    private static Set<String> listIndexFileNames(FileStoreTable table) throws Exception {
        Set<String> indexFiles = new HashSet<>();
        collectIndexFileNames(table.fileIO(), table.location(), indexFiles);
        return indexFiles;
    }

    private static void collectIndexFileNames(FileIO fileIO, Path path, Set<String> indexFiles)
            throws Exception {
        if (!fileIO.exists(path)) {
            return;
        }
        for (FileStatus status : fileIO.listStatus(path)) {
            Path child = status.getPath();
            if (status.isDir()) {
                if ("index".equals(child.getName())) {
                    for (FileStatus indexStatus : fileIO.listStatus(child)) {
                        if (!indexStatus.isDir()) {
                            indexFiles.add(indexStatus.getPath().getName());
                        }
                    }
                }
                collectIndexFileNames(fileIO, child, indexFiles);
            }
        }
    }

    private TestAppendFileStore createAppendStore(Map<String, String> dynamicOptions)
            throws Exception {
        String root = TraceableFileIO.SCHEME + "://" + tempDir.toString();
        Path path = new Path(tempDir.toUri());
        FileIO fileIO = FileIOFinder.find(new Path(root));
        SchemaManager schemaManage = new SchemaManager(new LocalFileIO(), path);

        Map<String, String> options = new HashMap<>(dynamicOptions);
        options.put(CoreOptions.PATH.key(), root);
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        schemaManage,
                        new Schema(
                                TestKeyValueGenerator.DEFAULT_ROW_TYPE.getFields(),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                options,
                                null));
        return new TestAppendFileStore(
                fileIO,
                schemaManage,
                new CoreOptions(options),
                tableSchema,
                RowType.of(),
                RowType.of(),
                TestKeyValueGenerator.DEFAULT_ROW_TYPE,
                (new Path(root)).getName());
    }
}
