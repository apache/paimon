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

package org.apache.paimon.append;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.BaseAppendFileStoreWrite;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.source.EndOfScanException;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.util.Collections.singletonMap;
import static org.apache.paimon.SnapshotTest.newSnapshotManager;
import static org.apache.paimon.testutils.assertj.PaimonAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for append table compaction. */
public class AppendOnlyTableCompactionTest {

    private static final Random random = new Random();

    @TempDir private Path tempDir;
    private FileStoreTable appendOnlyFileStoreTable;
    private SnapshotManager snapshotManager;
    private AppendCompactCoordinator coordinator;
    private BaseAppendFileStoreWrite write;
    private org.apache.paimon.fs.Path path;
    private TableSchema tableSchema;
    private final String commitUser = UUID.randomUUID().toString();

    @BeforeEach
    public void createNegativeAppendOnlyTable() throws Exception {
        FileIO fileIO = new LocalFileIO();
        path = new org.apache.paimon.fs.Path(tempDir.toString());
        tableSchema = new FileSystemSchemaManager(fileIO, path).createTable(schema());
        snapshotManager = newSnapshotManager(fileIO, path);
        recreate();
    }

    private void recreate() {
        appendOnlyFileStoreTable =
                FileStoreTableFactory.create(
                        LocalFileIO.create(),
                        new org.apache.paimon.fs.Path(tempDir.toString()),
                        tableSchema);
        coordinator = new AppendCompactCoordinator(appendOnlyFileStoreTable, true);
        write = (BaseAppendFileStoreWrite) appendOnlyFileStoreTable.store().newWrite(commitUser);
    }

    @Test
    public void noCompaction() throws Exception {
        List<CommitMessage> messages = writeCommit(10);

        messages.forEach(
                message ->
                        assertThat(((CommitMessageImpl) message).compactIncrement().isEmpty())
                                .isTrue());
    }

    @Test
    public void compactionTaskTest() throws Exception {
        // commit 11 files
        List<CommitMessage> messages = writeCommit(11);
        commit(messages);

        // first compact, six files left after commit compact and update restored files
        // test run method invoke scan and compactPlan
        List<AppendCompactTask> tasks = coordinator.run();
        assertThat(tasks.size()).isEqualTo(1);
        AppendCompactTask task = tasks.get(0);
        assertThat(task.compactBefore().size()).isEqualTo(11);
        List<CommitMessage> result = doCompact(tasks);
        assertThat(result.size()).isEqualTo(1);
        commit(result);
        coordinator.scan();
        assertThat(coordinator.listRestoredFiles().size()).isEqualTo(1);
        messages = writeCommit(11);
        commit(messages);

        // second compact, only one file left after updateRestored
        coordinator.scan();
        assertThat(coordinator.listRestoredFiles().size()).isEqualTo(12);
        tasks = coordinator.compactPlan();
        assertThat(tasks.size()).isEqualTo(1);
        // before update, zero file left
        assertThat(coordinator.listRestoredFiles().size()).isEqualTo(0);
        commit(doCompact(tasks));
        coordinator.scan();
        // one file is loaded from delta
        List<DataFileMeta> last = new ArrayList<>(coordinator.listRestoredFiles());
        assertThat(last.size()).isEqualTo(1);
        assertThat(last.get(0).rowCount()).isEqualTo(22);
    }

    @Test
    public void testScanSkipBigFiles() throws Exception {
        List<CommitMessage> messages = writeCommit(11);
        commit(messages);
        tableSchema = tableSchema.copy(singletonMap("target-file-size", "1 b"));
        recreate();
        assertThat(coordinator.filesIterator().next()).isNull();
    }

    @Test
    public void testCompactionLot() throws Exception {
        // test continuous compaction
        assertThat(snapshotManager.latestSnapshotId()).isNull();

        long count = 0;
        for (int i = 90; i < 100; i++) {
            count += i;
            commit(writeCommit(i));
            List<AppendCompactTask> tasks = coordinator.run();
            assertThat(tasks).hasSizeGreaterThan(0);
            commit(doCompact(tasks));
            // scan the file generated itself
            assertThat(coordinator.scan()).isTrue();
            assertThat(
                            coordinator.listRestoredFiles().stream()
                                    .map(DataFileMeta::rowCount)
                                    .reduce(Long::sum)
                                    .get())
                    .isEqualTo(count);
        }

        assertThat(appendOnlyFileStoreTable.store().newScan().plan().files().size())
                .isEqualTo(coordinator.listRestoredFiles().size());

        List<AppendCompactTask> tasks = coordinator.run();
        while (tasks.size() != 0) {
            commit(doCompact(tasks));
            tasks = coordinator.run();
        }

        int remainedSize = appendOnlyFileStoreTable.store().newScan().plan().files().size();
        assertThat(remainedSize).isEqualTo(coordinator.listRestoredFiles().size()).isEqualTo(1);
    }

    private static Schema schema() {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.STRING());
        schemaBuilder.column("f2", DataTypes.STRING());
        schemaBuilder.column("f3", DataTypes.STRING());
        schemaBuilder.option("compaction.min.file-num", "3");
        schemaBuilder.option("bucket", "-1");
        return schemaBuilder.build();
    }

    private void commit(List<CommitMessage> messages) throws Exception {
        TableCommitImpl commit = appendOnlyFileStoreTable.newCommit(commitUser);
        commit.commit(messages);
        commit.close();
    }

    private List<CommitMessage> writeCommit(int number) throws Exception {
        List<CommitMessage> messages = new ArrayList<>();
        StreamTableWrite writer = appendOnlyFileStoreTable.newStreamWriteBuilder().newWrite();
        for (int i = 0; i < number; i++) {
            writer.write(randomRow());
            messages.addAll(writer.prepareCommit(true, i));
        }
        return messages;
    }

    private List<CommitMessage> doCompact(List<AppendCompactTask> tasks) throws Exception {
        List<CommitMessage> result = new ArrayList<>();
        for (AppendCompactTask task : tasks) {
            result.add(task.doCompact(appendOnlyFileStoreTable, write));
        }
        return result;
    }

    @Test
    public void testBatchScanFileNumLimitWithRemainingFiles() throws Exception {
        commit(writeCommit(10));

        Map<String, String> options = new HashMap<>();
        options.put("compaction.file-num-limit", "5");
        AppendCompactCoordinator batchCoordinator = createBatchCoordinator(options);

        assertThat(batchCoordinator.scan()).isTrue();
        assertThat(batchCoordinator.batchRemainFiles()).isTrue();
        assertThat(batchCoordinator.listRestoredFiles().size()).isEqualTo(5);
    }

    @ParameterizedTest
    @CsvSource({"10,5", "12,5", "11,5", "5,5", "4,5"})
    public void testBatchChunksKeepEveryEligibleFile(int numberOfFiles, int fileLimit)
            throws Exception {
        commit(writeCommit(numberOfFiles));
        List<String> expectedFiles =
                appendOnlyFileStoreTable.store().newScan().plan().files().stream()
                        .map(entry -> entry.file().fileName())
                        .collect(Collectors.toList());
        assertThat(expectedFiles).hasSize(numberOfFiles);
        List<String> expectedRows = readRows();
        assertThat(expectedRows).hasSize(numberOfFiles);
        Map<String, String> options = new HashMap<>();
        options.put("compaction.file-num-limit", String.valueOf(fileLimit));
        AppendCompactCoordinator batchCoordinator = createBatchCoordinator(options);
        List<AppendCompactTask> tasks = new ArrayList<>();
        boolean finished = false;
        for (int round = 0; round <= numberOfFiles; round++) {
            try {
                tasks.addAll(batchCoordinator.run());
            } catch (EndOfScanException e) {
                finished = true;
                break;
            }
            if (!batchCoordinator.batchRemainFiles()) {
                finished = true;
                break;
            }
        }
        assertThat(finished).isTrue();
        List<String> coveredFiles = new ArrayList<>();
        tasks.forEach(
                task -> {
                    assertThat(task.compactBefore()).hasSizeLessThanOrEqualTo(fileLimit);
                    task.compactBefore().forEach(file -> coveredFiles.add(file.fileName()));
                });
        batchCoordinator.listRestoredFiles().forEach(file -> coveredFiles.add(file.fileName()));
        assertThat(coveredFiles).containsExactlyInAnyOrderElementsOf(expectedFiles);

        commit(doCompact(tasks));
        assertThat(appendOnlyFileStoreTable.store().newScan().plan().files())
                .hasSize((numberOfFiles + fileLimit - 1) / fileLimit);
        assertThat(readRows()).containsExactlyInAnyOrderElementsOf(expectedRows);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testLaterBatchRejectsConcurrentCompaction(boolean replaceBeforeRewrite)
            throws Exception {
        commit(writeCommit(6));
        List<String> expectedRows = readRows();
        FileStoreTable compactTable =
                appendOnlyFileStoreTable.copy(singletonMap("compaction.file-num-limit", "2"));
        AppendCompactCoordinator batchCoordinator =
                new AppendCompactCoordinator(compactTable, snapshotManager.latestSnapshot(), null);

        List<AppendCompactTask> firstBatch = batchCoordinator.run();
        assertThat(firstBatch).hasSize(1);
        try (TableCommitImpl firstCommit =
                appendOnlyFileStoreTable.newCommit(UUID.randomUUID().toString())) {
            firstCommit.commit(doCompact(firstBatch));
        }
        assertThat(batchCoordinator.batchRemainFiles()).isTrue();
        String firstBatchOutput = firstBatch.get(0).compactAfter().get(0).fileName();

        List<AppendCompactTask> laterBatch = batchCoordinator.run();
        assertThat(laterBatch).hasSize(1);
        assertThat(laterBatch.get(0).compactBefore()).hasSize(2);
        AppendCompactTask concurrentTask =
                new AppendCompactTask(
                        laterBatch.get(0).partition(), laterBatch.get(0).compactBefore());
        List<CommitMessage> staleMessages =
                replaceBeforeRewrite ? Collections.emptyList() : doCompact(laterBatch);
        try (TableCommitImpl concurrentCommit =
                appendOnlyFileStoreTable.newCommit(UUID.randomUUID().toString())) {
            concurrentCommit.commit(doCompact(Collections.singletonList(concurrentTask)));
        }
        long concurrentSnapshotId = snapshotManager.latestSnapshotId();
        List<String> committedFiles =
                appendOnlyFileStoreTable.store().newScan().plan().files().stream()
                        .map(entry -> entry.file().fileName())
                        .collect(Collectors.toList());
        assertThat(committedFiles).hasSize(4).contains(firstBatchOutput);

        // The same conflict must be detected whether the rewrite starts before or after the
        // concurrent commit replaces its input files.
        List<CommitMessage> laterMessages =
                replaceBeforeRewrite ? doCompact(laterBatch) : staleMessages;
        try (TableCommitImpl laterCommit =
                appendOnlyFileStoreTable.newCommit(UUID.randomUUID().toString())) {
            assertThatThrownBy(() -> laterCommit.commit(laterMessages))
                    .hasMessageContaining("File deletion conflicts detected");
        }
        assertThat(snapshotManager.latestSnapshotId()).isEqualTo(concurrentSnapshotId);
        assertThat(
                        appendOnlyFileStoreTable.store().newScan().plan().files().stream()
                                .map(entry -> entry.file().fileName())
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrderElementsOf(committedFiles);
        assertThat(readRows()).containsExactlyInAnyOrderElementsOf(expectedRows);
    }

    private List<String> readRows() throws Exception {
        List<String> rows = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                appendOnlyFileStoreTable
                        .newReadBuilder()
                        .newRead()
                        .createReader(appendOnlyFileStoreTable.newReadBuilder().newScan().plan())) {
            reader.forEachRemaining(
                    row ->
                            rows.add(
                                    row.getInt(0)
                                            + ","
                                            + row.getString(1)
                                            + ","
                                            + row.getString(2)
                                            + ","
                                            + row.getString(3)));
        }
        return rows;
    }

    @Test
    public void testBatchScanFileNumLimitNoRemainingFiles() throws Exception {
        commit(writeCommit(10));

        // default 100_000
        AppendCompactCoordinator batchCoordinator = createBatchCoordinator(Collections.emptyMap());

        assertThat(batchCoordinator.scan()).isTrue();
        assertThat(batchCoordinator.batchRemainFiles()).isFalse();
        assertThat(batchCoordinator.listRestoredFiles().size()).isEqualTo(10);
    }

    @Test
    public void testBatchScanFileNumLimitExactMatch() throws Exception {
        commit(writeCommit(5));

        Map<String, String> options = new HashMap<>();
        options.put("compaction.file-num-limit", "5");
        AppendCompactCoordinator batchCoordinator = createBatchCoordinator(options);

        assertThat(batchCoordinator.scan()).isTrue();
        assertThat(batchCoordinator.batchRemainFiles()).isFalse();
        assertThat(batchCoordinator.listRestoredFiles().size()).isEqualTo(5);
    }

    @Test
    public void testBatchScanEmptyTableBatchRemainFiles() {
        AppendCompactCoordinator batchCoordinator = createBatchCoordinator(Collections.emptyMap());

        assertThatThrownBy(batchCoordinator::scan)
                .satisfies(anyCauseMatches(EndOfScanException.class));
        assertThat(batchCoordinator.batchRemainFiles()).isFalse();
    }

    @Test
    public void testBatchScanAllFilesFilteredOut() throws Exception {
        commit(writeCommit(10));

        // Set target-file-size to 1 byte, making compactionFileSize = 0,
        // so shouldCompact returns false for all files
        Map<String, String> options = new HashMap<>();
        options.put("target-file-size", "1 b");
        AppendCompactCoordinator batchCoordinator = createBatchCoordinator(options);

        assertThatThrownBy(batchCoordinator::scan)
                .satisfies(anyCauseMatches(EndOfScanException.class));
        assertThat(batchCoordinator.batchRemainFiles()).isFalse();
    }

    @Test
    public void testBatchCompactsLeftoverFiles() throws Exception {
        // 2 files, fewer than compaction.min.file-num = 3
        commit(writeCommit(2));

        // streaming waits for more files or for COMPACT_AGE
        assertThat(coordinator.run()).isEmpty();

        // issue 5130: a batch compaction plans once, so it compacts the 2 files right away
        List<AppendCompactTask> tasks = createBatchCoordinator(Collections.emptyMap()).run();
        assertThat(tasks).hasSize(1);
        assertThat(tasks.get(0).compactBefore()).hasSize(2);
        commit(doCompact(tasks));
        List<DataFileMeta> files =
                appendOnlyFileStoreTable.store().newScan().plan().files().stream()
                        .map(ManifestEntry::file)
                        .collect(Collectors.toList());
        assertThat(files).hasSize(1);
        assertThat(files.get(0).rowCount()).isEqualTo(2);
    }

    private AppendCompactCoordinator createBatchCoordinator(Map<String, String> extraOptions) {
        TableSchema schema = tableSchema.copy(extraOptions);
        FileStoreTable table =
                FileStoreTableFactory.create(
                        LocalFileIO.create(),
                        new org.apache.paimon.fs.Path(tempDir.toString()),
                        schema);
        return new AppendCompactCoordinator(table, false);
    }

    private InternalRow randomRow() {
        return GenericRow.of(
                random.nextInt(100),
                BinaryString.fromString("A" + random.nextInt(100)),
                BinaryString.fromString("B" + random.nextInt(100)),
                BinaryString.fromString("C" + random.nextInt(100)));
    }
}
