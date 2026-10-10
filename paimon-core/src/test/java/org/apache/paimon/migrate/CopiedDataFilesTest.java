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

package org.apache.paimon.migrate;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.append.dataevolution.DataEvolutionCompactCoordinator;
import org.apache.paimon.append.dataevolution.DataEvolutionCompactTask;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.BlobData;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/**
 * Tests for {@link CopiedDataFiles}: data files copied from another table and committed the way
 * {@code CopyFilesCommitOperator} of {@code sys.copy} does.
 */
public class CopiedDataFilesTest extends TableTestBase {

    private static final Identifier SOURCE = new Identifier("default", "src");
    private static final Identifier TARGET = new Identifier("default", "dst");

    // ---------------------------------------------------------------------------------------------
    // row ids
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testCopyIntoTableWithoutRowsAssignsRowIdsInOrder() throws Exception {
        createTable(SOURCE, Options.ROW_TRACKING, false);
        write(table(SOURCE), row(1, "a", "p1"), row(2, "b", "p1"));
        write(table(SOURCE), row(3, "c", "p1"));
        createTable(TARGET, Options.ROW_TRACKING, false);

        copy(SOURCE, TARGET, null);

        assertThat(rowIds(table(TARGET)))
                .containsEntry(1, 0L)
                .containsEntry(2, 1L)
                .containsEntry(3, 2L);
        assertThat(table(TARGET).snapshotManager().latestSnapshot().nextRowId()).isEqualTo(3L);
        write(table(TARGET), row(4, "d", "p1"));
        assertThat(rowIds(table(TARGET))).containsEntry(4, 3L);
        assertUniqueRowIds(table(TARGET));
    }

    @Test
    public void testCopyIntoOnePartitionFollowsTheRowsOfOtherPartitions() throws Exception {
        createTable(SOURCE, Options.ROW_TRACKING, true);
        write(table(SOURCE), row(1, "a", "p1"), row(2, "b", "p1"));
        createTable(TARGET, Options.ROW_TRACKING, true);
        write(table(TARGET), row(10, "x", "p2"), row(11, "y", "p2"));

        // dynamic partition overwrite replaces p1 only: the rows of p2 keep row ids 0 and 1
        copy(SOURCE, TARGET, null);

        FileStoreTable target = table(TARGET);
        assertThat(values(target)).containsOnlyKeys(1, 2, 10, 11);
        assertThat(rowIds(target))
                .containsEntry(10, 0L)
                .containsEntry(11, 1L)
                .containsEntry(1, 2L)
                .containsEntry(2, 3L);
        assertThat(target.snapshotManager().latestSnapshot().nextRowId()).isEqualTo(4L);
        assertUniqueRowIds(target);
    }

    @Test
    public void testCopyWithPartitionFilterFollowsTheRowsOfOtherPartitions() throws Exception {
        createTable(SOURCE, Options.ROW_TRACKING, true);
        write(table(SOURCE), row(1, "a", "p1"), row(2, "b", "p2"));
        createTable(TARGET, Options.ROW_TRACKING, true);
        write(table(TARGET), row(10, "x", "p2"));

        copy(SOURCE, TARGET, "p1");

        assertThat(values(table(TARGET))).containsOnlyKeys(1, 10);
        assertThat(rowIds(table(TARGET))).containsEntry(10, 0L).containsEntry(1, 1L);
        assertUniqueRowIds(table(TARGET));
    }

    @Test
    public void testCopyOverEveryRowFollowsTheRowIdsGivenOut() throws Exception {
        createTable(SOURCE, Options.ROW_TRACKING, false);
        write(table(SOURCE), row(1, "a", "p1"), row(2, "b", "p1"));
        createTable(TARGET, Options.ROW_TRACKING, false);
        write(table(TARGET), row(10, "x", "p1"), row(11, "y", "p1"), row(12, "z", "p1"));

        copy(SOURCE, TARGET, null);

        // the row ids of the overwritten rows are not given out again, as for any overwrite
        assertThat(values(table(TARGET))).containsOnlyKeys(1, 2);
        assertThat(rowIds(table(TARGET))).containsEntry(1, 3L).containsEntry(2, 4L);
        assertThat(table(TARGET).snapshotManager().latestSnapshot().nextRowId()).isEqualTo(5L);
    }

    @Test
    public void testAppendCommittedWhileTheCopyIsPreparedGetsOtherRowIds() throws Exception {
        createTable(SOURCE, Options.ROW_TRACKING, true);
        write(table(SOURCE), row(1, "a", "p1"), row(2, "b", "p1"));
        createTable(TARGET, Options.ROW_TRACKING, true);
        write(table(TARGET), row(10, "x", "p1"), row(11, "y", "p1"));

        // the copy is prepared, then another writer appends to p2, then the copy commits
        Map<Pair<BinaryRow, Integer>, List<DataFileMeta>> files = prepareCopy(SOURCE, TARGET);
        write(table(TARGET), row(20, "u", "p2"), row(21, "v", "p2"));
        commitCopy(table(TARGET), files);

        Map<Integer, Long> rowIds = rowIds(table(TARGET));
        assertThat(rowIds).containsOnlyKeys(1, 2, 20, 21);
        assertThat(rowIds).containsEntry(20, 2L).containsEntry(21, 3L);
        assertThat(rowIds).containsEntry(1, 4L).containsEntry(2, 5L);
        assertUniqueRowIds(table(TARGET));
    }

    @Test
    public void testAppendToEmptyTargetWhileTheCopyIsPreparedGetsOtherRowIds() throws Exception {
        createTable(SOURCE, Options.ROW_TRACKING, true);
        write(table(SOURCE), row(1, "a", "p1"), row(2, "b", "p1"));
        createTable(TARGET, Options.ROW_TRACKING, true);

        Map<Pair<BinaryRow, Integer>, List<DataFileMeta>> files = prepareCopy(SOURCE, TARGET);
        write(table(TARGET), row(20, "u", "p2"), row(21, "v", "p2"));
        commitCopy(table(TARGET), files);

        assertThat(rowIds(table(TARGET)))
                .containsEntry(20, 0L)
                .containsEntry(21, 1L)
                .containsEntry(1, 2L)
                .containsEntry(2, 3L);
        assertUniqueRowIds(table(TARGET));
    }

    @Test
    public void testCopyCommitRetriedAfterAConcurrentAppendGetsOtherRowIds() throws Exception {
        createTable(SOURCE, Options.ROW_TRACKING, true);
        write(table(SOURCE), row(1, "a", "p1"), row(2, "b", "p1"));
        createTable(TARGET, Options.ROW_TRACKING, true);
        write(table(TARGET), row(10, "x", "p1"));
        Map<Pair<BinaryRow, Integer>, List<DataFileMeta>> files = prepareCopy(SOURCE, TARGET);

        // The copy commit reads the latest snapshot and stops at its first manifest write. An
        // append commits meanwhile, so the copy loses the next snapshot id and retries.
        CountDownLatch paused = new CountDownLatch(1);
        CountDownLatch released = new CountDownLatch(1);
        AtomicReference<Thread> committer = new AtomicReference<>();
        FileStoreTable pausing =
                FileStoreTableFactory.create(
                        new PausingFileIO(committer, paused, released), table(TARGET).location());
        AtomicReference<Throwable> error = new AtomicReference<>();
        Thread thread =
                new Thread(
                        () -> {
                            try {
                                commitCopy(pausing, files);
                            } catch (Throwable t) {
                                error.set(t);
                            }
                        });
        committer.set(thread);
        thread.start();
        assertThat(paused.await(30, TimeUnit.SECONDS)).isTrue();
        write(table(TARGET), row(20, "u", "p2"), row(21, "v", "p2"));
        released.countDown();
        thread.join(TimeUnit.SECONDS.toMillis(60));
        assertThat(error.get()).isNull();

        Map<Integer, Long> rowIds = rowIds(table(TARGET));
        assertThat(rowIds).containsOnlyKeys(1, 2, 20, 21);
        assertThat(rowIds).containsEntry(20, 1L).containsEntry(21, 2L);
        assertThat(rowIds).containsEntry(1, 3L).containsEntry(2, 4L);
        assertUniqueRowIds(table(TARGET));
    }

    @Test
    public void testColumnUpdatesOfTheSourceAreRefused() throws Exception {
        createTable(SOURCE, Options.DATA_EVOLUTION, false);
        write(table(SOURCE), row(1, "a", "p1"), row(2, "b", "p1"));
        // a column update of both rows: shares the row id range of the file above
        updateColumn(table(SOURCE), 0, "a2", "b2");
        createTable(TARGET, Options.DATA_EVOLUTION, false);

        assertThatThrownBy(() -> copy(SOURCE, TARGET, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("they hold the same rows")
                .hasMessageContaining("Compact the source table first");
        assertThat(table(TARGET).snapshotManager().latestSnapshot()).isNull();
    }

    @Test
    public void testCompactedDataEvolutionSourceIsCopied() throws Exception {
        createTable(
                SOURCE,
                Options.DATA_EVOLUTION,
                false,
                Collections.singletonMap(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "2"));
        write(table(SOURCE), row(1, "a", "p1"), row(2, "b", "p1"));
        updateColumn(table(SOURCE), 0, "a2", "b2");
        compactDataEvolution(table(SOURCE));
        assertThat(liveFiles(table(SOURCE))).hasSize(1);
        createTable(TARGET, Options.DATA_EVOLUTION, false);
        write(table(TARGET), row(10, "x", "p1"));

        copy(SOURCE, TARGET, null);

        FileStoreTable target = table(TARGET);
        assertThat(values(target)).containsEntry(1, "a2").containsEntry(2, "b2");
        assertThat(rowIds(target)).containsEntry(1, 1L).containsEntry(2, 2L);
        // later column updates of the copied rows win
        updateColumn(target, 1, "a3", "b3");
        assertThat(values(table(TARGET))).containsEntry(1, "a3").containsEntry(2, "b3");
    }

    @Test
    public void testBlobFilesKeepTheRowIdsOfTheirRows() throws Exception {
        createBlobTable(SOURCE);
        writeBlobs(table(SOURCE), 1, 2);
        writeBlobs(table(SOURCE), 3);
        assertThat(liveFiles(table(SOURCE))).anyMatch(file -> file.fileName().endsWith(".blob"));
        createBlobTable(TARGET);
        writeBlobs(table(TARGET), 10);

        copy(SOURCE, TARGET, null);

        // the copy replaces the row of the target; every copied row reads its own blob, under
        // row ids after the one the target gave out
        Map<Integer, Long> rowIds = new TreeMap<>();
        RowType readType =
                SpecialFields.rowTypeWithRowTracking(table(TARGET).rowType(), true, true);
        for (InternalRow row : read(table(TARGET), readType)) {
            int id = row.getInt(0);
            assertThat(row.getBlob(1).toData()).isEqualTo(blob(id));
            rowIds.put(id, row.getLong(2));
        }
        assertThat(rowIds).containsExactly(entry(1, 1L), entry(2, 2L), entry(3, 3L));
    }

    @Test
    public void testFilesThatStoreTheirRowIdsAreRefusedByRowTrackingTargets() throws Exception {
        createTable(SOURCE, Options.ROW_TRACKING, false);
        write(table(SOURCE), row(1, "a", "p1"), row(2, "b", "p1"));
        copyOnWriteUpdate(table(SOURCE));
        assertThat(liveFiles(table(SOURCE))).anyMatch(CopiedDataFilesTest::storesRowIds);

        createTable(TARGET, Options.ROW_TRACKING, false);
        assertThatThrownBy(() -> copy(SOURCE, TARGET, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("its rows store their row ids")
                .hasMessageContaining("INSERT OVERWRITE");
        assertThat(table(TARGET).snapshotManager().latestSnapshot()).isNull();

        // a table without row tracking does not use them
        Identifier plain = new Identifier("default", "plain");
        createTable(plain, Options.PLAIN, false);
        copy(SOURCE, plain, null);
        assertThat(liveFiles(table(plain))).anyMatch(CopiedDataFilesTest::storesRowIds);
    }

    // ---------------------------------------------------------------------------------------------
    // file metadata and sequence numbers
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testLaterColumnUpdatesWinOverCopiedRows() throws Exception {
        createTable(SOURCE, Options.DATA_EVOLUTION, false);
        for (int i = 0; i < 20; i++) {
            write(table(SOURCE), row(i, "v" + i, "p1"));
        }
        createTable(TARGET, Options.DATA_EVOLUTION, false);

        copy(SOURCE, TARGET, null);

        FileStoreTable target = table(TARGET);
        long copySnapshot = target.snapshotManager().latestSnapshotId();
        assertThat(liveFiles(target))
                .allMatch(
                        file ->
                                file.minSequenceNumber() == copySnapshot
                                        && file.maxSequenceNumber() == copySnapshot);
        updateColumn(target, 19, "updated");
        assertThat(values(table(TARGET))).containsEntry(19, "updated").containsEntry(18, "v18");
    }

    @Test
    public void testCopiedFilesAreCommittedAsFilesWrittenForTheTarget() throws Exception {
        createTable(SOURCE, Options.DATA_EVOLUTION, false);
        write(table(SOURCE), row(1, "a", "p1"));
        write(table(SOURCE), row(2, "b", "p1"));
        createTable(TARGET, Options.ROW_TRACKING, false);
        List<DataFileMeta> source = liveFiles(table(SOURCE));
        List<DataFileMeta> copied = new ArrayList<>();
        for (DataFileMeta file : source) {
            copied.add(file.withWriteColsSequences(new long[] {7L, 7L, 7L}));
        }

        CopiedDataFiles.adaptToTarget(table(TARGET), Collections.singletonList(copied));

        assertThat(copied).hasSize(source.size());
        for (DataFileMeta file : copied) {
            assertThat(file.firstRowId()).isNull();
            assertThat(file.fileSource()).contains(FileSource.APPEND);
            assertThat(file.minSequenceNumber()).isZero();
            assertThat(file.maxSequenceNumber()).isZero();
            assertThat(file.writeColsSequences()).isNull();
        }
        // in row id order
        assertThat(copied.get(0).fileName()).isEqualTo(fileWithFirstRowId(source, 0L));
    }

    @Test
    public void testTableWithoutRowTrackingDropsCopiedRowIds() throws Exception {
        createTable(SOURCE, Options.ROW_TRACKING, false);
        write(table(SOURCE), row(1, "a", "p1"), row(2, "b", "p1"));
        createTable(TARGET, Options.PLAIN, false);

        copy(SOURCE, TARGET, null);

        assertThat(values(table(TARGET))).containsOnlyKeys(1, 2);
        assertThat(liveFiles(table(TARGET))).allMatch(file -> file.firstRowId() == null);
    }

    @Test
    public void testCopyBetweenTablesWithoutRowTrackingChangesNothing() throws Exception {
        createTable(SOURCE, Options.PLAIN, false);
        write(table(SOURCE), row(1, "a", "p1"));
        createTable(TARGET, Options.PLAIN, false);
        write(table(TARGET), row(10, "x", "p1"));
        List<DataFileMeta> before = liveFiles(table(SOURCE));

        List<DataFileMeta> copied = new ArrayList<>(before);
        CopiedDataFiles.adaptToTarget(table(TARGET), Collections.singletonList(copied));

        assertThat(copied).isEqualTo(before);
    }

    // ---------------------------------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------------------------------

    private enum Options {
        PLAIN,
        ROW_TRACKING,
        DATA_EVOLUTION
    }

    private void createTable(Identifier identifier, Options options, boolean partitioned)
            throws Exception {
        createTable(identifier, options, partitioned, Collections.emptyMap());
    }

    private void createTable(
            Identifier identifier,
            Options options,
            boolean partitioned,
            Map<String, String> extraOptions)
            throws Exception {
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("v", DataTypes.STRING())
                        .column("pt", DataTypes.STRING())
                        .options(extraOptions);
        if (partitioned) {
            builder.partitionKeys("pt");
        }
        if (options != Options.PLAIN) {
            builder.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        }
        if (options == Options.DATA_EVOLUTION) {
            builder.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        }
        catalog.createTable(identifier, builder.build(), false);
    }

    private void createBlobTable(Identifier identifier) throws Exception {
        catalog.createTable(
                identifier,
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("b", DataTypes.BLOB())
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .build(),
                false);
    }

    private static byte[] blob(int id) {
        return ("blob-" + id).getBytes();
    }

    private static void writeBlobs(FileStoreTable table, int... ids) throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            for (int id : ids) {
                write.write(GenericRow.of(id, new BlobData(blob(id))));
            }
            commit.commit(write.prepareCommit());
        }
    }

    private FileStoreTable table(Identifier identifier) throws Exception {
        return (FileStoreTable) catalog.getTable(identifier);
    }

    private static GenericRow row(int id, String v, String pt) {
        return GenericRow.of(id, BinaryString.fromString(v), BinaryString.fromString(pt));
    }

    private static void write(FileStoreTable table, GenericRow... rows) throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            for (GenericRow row : rows) {
                write.write(row);
            }
            commit.commit(write.prepareCommit());
        }
    }

    /** A data-evolution update of column {@code v} of the rows from {@code firstRowId} on. */
    private static void updateColumn(FileStoreTable table, long firstRowId, String... values)
            throws Exception {
        RowType writeType = table.rowType().project(Collections.singletonList("v"));
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite().withWriteType(writeType);
                BatchTableCommit commit = builder.newCommit()) {
            for (String value : values) {
                write.write(GenericRow.of(BinaryString.fromString(value)));
            }
            List<CommitMessage> messages = write.prepareCommit();
            for (CommitMessage message : messages) {
                CommitMessageImpl impl = (CommitMessageImpl) message;
                List<DataFileMeta> files = new ArrayList<>(impl.newFilesIncrement().newFiles());
                impl.newFilesIncrement().newFiles().clear();
                files.forEach(
                        f ->
                                impl.newFilesIncrement()
                                        .newFiles()
                                        .add(f.assignFirstRowId(firstRowId)));
            }
            commit.commit(messages);
        }
    }

    private static void compactDataEvolution(FileStoreTable table) throws Exception {
        DataEvolutionCompactCoordinator coordinator =
                new DataEvolutionCompactCoordinator(
                        table, false, false, table.snapshotManager().latestSnapshot());
        List<CommitMessage> messages = new ArrayList<>();
        for (DataEvolutionCompactTask task : coordinator.plan()) {
            messages.add(task.doCompact(table, "compact"));
        }
        assertThat(messages).isNotEmpty();
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(messages);
        }
    }

    /**
     * Like a copy-on-write UPDATE of the first file of a row-tracking table: rewrites it with the
     * row ids of its rows stored in the new file.
     */
    private static void copyOnWriteUpdate(FileStoreTable table) throws Exception {
        DataFileMeta rewritten = null;
        BinaryRow partition = null;
        for (ManifestEntry entry : entries(table)) {
            if (entry.file().firstRowId() != null && entry.file().firstRowId() == 0L) {
                rewritten = entry.file();
                partition = entry.partition();
            }
        }
        assertThat(rewritten).isNotNull();
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write =
                        builder.newWrite()
                                .withWriteType(
                                        SpecialFields.rowTypeWithRowTracking(
                                                table.rowType(), false, true));
                BatchTableCommit commit = builder.newCommit()) {
            write.write(
                    GenericRow.of(
                            1,
                            BinaryString.fromString("A"),
                            BinaryString.fromString("p1"),
                            0L,
                            null));
            write.write(
                    GenericRow.of(
                            2,
                            BinaryString.fromString("b"),
                            BinaryString.fromString("p1"),
                            1L,
                            null));
            CommitMessageImpl message = (CommitMessageImpl) write.prepareCommit().get(0);
            commit.commit(
                    Collections.singletonList(
                            new CommitMessageImpl(
                                    partition,
                                    message.bucket(),
                                    message.totalBuckets(),
                                    new DataIncrement(
                                            message.newFilesIncrement().newFiles(),
                                            Collections.singletonList(rewritten),
                                            Collections.emptyList()),
                                    CompactIncrement.emptyIncrement())));
        }
    }

    /** Copies and commits like {@code CopyFilesCommitOperator}, optionally one partition only. */
    private void copy(Identifier source, Identifier target, String partition) throws Exception {
        commitCopy(table(target), prepareCopy(source, target, partition));
    }

    private Map<Pair<BinaryRow, Integer>, List<DataFileMeta>> prepareCopy(
            Identifier source, Identifier target) throws Exception {
        return prepareCopy(source, target, null);
    }

    /** Copies the data files physically and adapts their metadata to the target. */
    private Map<Pair<BinaryRow, Integer>, List<DataFileMeta>> prepareCopy(
            Identifier source, Identifier target, String partition) throws Exception {
        FileStoreTable from = table(source);
        FileStoreTable to = table(target);
        Map<Pair<BinaryRow, Integer>, List<DataFileMeta>> files = new LinkedHashMap<>();
        for (ManifestEntry entry : entries(from)) {
            if (partition != null
                    && (entry.partition().getFieldCount() == 0
                            || !partition.equals(entry.partition().getString(0).toString()))) {
                continue;
            }
            Path fromBucket =
                    from.store().pathFactory().bucketPath(entry.partition(), entry.bucket());
            Path toBucket = to.store().pathFactory().bucketPath(entry.partition(), entry.bucket());
            to.fileIO().mkdirs(toBucket);
            to.fileIO()
                    .copyFile(
                            new Path(fromBucket, entry.file().fileName()),
                            new Path(toBucket, entry.file().fileName()),
                            false);
            files.computeIfAbsent(
                            Pair.of(entry.partition().copy(), entry.bucket()),
                            k -> new ArrayList<>())
                    .add(entry.file());
        }
        CopiedDataFiles.adaptToTarget(to, files.values());
        return files;
    }

    /** The overwrite commit of {@code CopyFilesCommitOperator}. */
    private static void commitCopy(
            FileStoreTable target, Map<Pair<BinaryRow, Integer>, List<DataFileMeta>> files)
            throws Exception {
        List<CommitMessage> messages = new ArrayList<>();
        for (Map.Entry<Pair<BinaryRow, Integer>, List<DataFileMeta>> entry : files.entrySet()) {
            messages.add(
                    new CommitMessageImpl(
                            entry.getKey().getLeft(),
                            entry.getKey().getRight(),
                            target.coreOptions().bucket(),
                            new DataIncrement(
                                    entry.getValue(),
                                    Collections.emptyList(),
                                    Collections.emptyList()),
                            CompactIncrement.emptyIncrement()));
        }
        try (BatchTableCommit commit = target.newBatchWriteBuilder().withOverwrite().newCommit()) {
            commit.commit(messages);
        }
    }

    private static String fileWithFirstRowId(List<DataFileMeta> files, long firstRowId) {
        for (DataFileMeta file : files) {
            if (file.firstRowId() != null && file.firstRowId() == firstRowId) {
                return file.fileName();
            }
        }
        throw new AssertionError("no file with first row id " + firstRowId);
    }

    private static List<ManifestEntry> entries(FileStoreTable table) {
        List<ManifestEntry> entries = new ArrayList<>();
        table.newSnapshotReader().readFileIterator().forEachRemaining(entries::add);
        return entries;
    }

    private static List<DataFileMeta> liveFiles(FileStoreTable table) {
        List<DataFileMeta> files = new ArrayList<>();
        entries(table).forEach(entry -> files.add(entry.file()));
        return files;
    }

    private static boolean storesRowIds(DataFileMeta file) {
        return file.writeCols() != null && file.writeCols().contains(SpecialFields.ROW_ID.name());
    }

    private static Map<Integer, String> values(FileStoreTable table) throws Exception {
        Map<Integer, String> result = new TreeMap<>();
        for (InternalRow row : read(table, table.rowType())) {
            result.put(row.getInt(0), row.getString(1).toString());
        }
        return result;
    }

    private static Map<Integer, Long> rowIds(FileStoreTable table) throws Exception {
        Map<Integer, Long> result = new TreeMap<>();
        RowType readType = SpecialFields.rowTypeWithRowTracking(table.rowType(), true, true);
        for (InternalRow row : read(table, readType)) {
            result.put(row.getInt(0), row.isNullAt(3) ? null : row.getLong(3));
        }
        return result;
    }

    private static void assertUniqueRowIds(FileStoreTable table) throws Exception {
        List<Long> ids = new ArrayList<>(rowIds(table).values());
        assertThat(ids).doesNotContainNull();
        assertThat(new HashSet<>(ids)).hasSameSizeAs(ids);
    }

    private static List<InternalRow> read(FileStoreTable table, RowType readType) throws Exception {
        ReadBuilder readBuilder = table.newReadBuilder().withReadType(readType);
        TableRead read = readBuilder.newRead();
        InternalRowSerializer serializer = new InternalRowSerializer(readType);
        List<InternalRow> rows = new ArrayList<>();
        for (Split split : readBuilder.newScan().plan().splits()) {
            try (RecordReader<InternalRow> reader = read.createReader(split)) {
                reader.forEachRemaining(row -> rows.add(serializer.copy(row)));
            }
        }
        return rows;
    }

    /** Pauses the committer thread at its first manifest write. */
    private static class PausingFileIO extends LocalFileIO {

        private final AtomicReference<Thread> committer;
        private final CountDownLatch paused;
        private final CountDownLatch released;
        private final AtomicBoolean pausedOnce = new AtomicBoolean();

        private PausingFileIO(
                AtomicReference<Thread> committer, CountDownLatch paused, CountDownLatch released) {
            this.committer = committer;
            this.paused = paused;
            this.released = released;
        }

        @Override
        public PositionOutputStream newOutputStream(Path path, boolean overwrite)
                throws IOException {
            if (Thread.currentThread() == committer.get()
                    && path.getParent().getName().equals("manifest")
                    && pausedOnce.compareAndSet(false, true)) {
                paused.countDown();
                try {
                    released.await(60, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    throw new IOException(e);
                }
            }
            return super.newOutputStream(path, overwrite);
        }
    }
}
