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

package org.apache.paimon.operation;

import org.apache.paimon.KeyValue;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Recovery of primary-key data, compaction indexes and changelog. */
class FullCompactionRecoveryTest {

    @TempDir java.nio.file.Path directory;

    @ParameterizedTest
    @MethodSource("scenarios")
    void commitPreservesDataAndChangelogAcrossPublicationFailure(
            String producer, boolean deletionVectors, CrashPoint crash, boolean reuseCommit)
            throws Exception {
        Path root = new Path(directory.toUri());
        FailBetweenSnapshots io = new FailBetweenSnapshots();
        new FileSystemSchemaManager(io, root)
                .createTable(
                        Schema.newBuilder()
                                .column("k", DataTypes.INT())
                                .column("v", DataTypes.INT())
                                .primaryKey("k")
                                .option("bucket", "1")
                                .option("changelog-producer", producer)
                                .option(
                                        "deletion-vectors.enabled",
                                        Boolean.toString(deletionVectors))
                                .build());
        FileStoreTable table = FileStoreTableFactory.create(io, root);
        StreamTableScan scan = table.newReadBuilder().newStreamScan();
        scan.restore(1L);
        try (IOManager disk = IOManager.create(directory.toString());
                TableWriteImpl<?> write = table.newWrite("recovery").withIOManager(disk);
                TableCommitImpl commit = table.newCommit("recovery")) {
            write.write(GenericRow.of(1, 10));
            write.compact(BinaryRow.EMPTY_ROW, 0, true);
            List<CommitMessage> completedMessages = write.prepareCommit(true, 1);
            commit.commit(1, completedMessages);
            if (!producer.equals("none")) {
                assertThat(streamRows(table, scan)).containsExactly("+I 1|10");
            }

            write.write(GenericRow.of(1, 20));
            write.compact(BinaryRow.EMPTY_ROW, 0, true);
            List<CommitMessage> messages = write.prepareCommit(true, 2);
            assertThat(messages)
                    .anySatisfy(
                            message ->
                                    assertThat(
                                                    ((CommitMessageImpl) message)
                                                            .compactIncrement()
                                                            .isEmpty())
                                            .isFalse());
            if (!producer.equals("none")) {
                assertThat(messages)
                        .anySatisfy(
                                message ->
                                        assertThat(
                                                        ((CommitMessageImpl) message)
                                                                .compactIncrement()
                                                                .changelogFiles())
                                                .isNotEmpty());
            }
            Map<Long, List<CommitMessage>> pending = new LinkedHashMap<>();
            pending.put(1L, completedMessages);
            pending.put(2L, messages);
            long previousSnapshot = table.snapshotManager().latestSnapshotId();
            if (crash == CrashPoint.LEGACY_APPEND
                    || crash == CrashPoint.LEGACY_INDEX_ONLY
                    || crash == CrashPoint.LEGACY_EXPIRED_APPEND) {
                List<CommitMessage> appendOnly =
                        messages.stream()
                                .map(
                                        message -> {
                                            CommitMessageImpl original =
                                                    (CommitMessageImpl) message;
                                            return (CommitMessage)
                                                    new CommitMessageImpl(
                                                            original.partition(),
                                                            original.bucket(),
                                                            original.totalBuckets(),
                                                            original.newFilesIncrement(),
                                                            CompactIncrement.emptyIncrement());
                                        })
                                .collect(Collectors.toList());
                if (crash == CrashPoint.LEGACY_INDEX_ONLY) {
                    commit.commit(2, messages);
                    IndexFileHandler handler = table.store().newIndexFileHandler();
                    IndexFileMeta index =
                            handler.hashIndex(BinaryRow.EMPTY_ROW, 0).write(new int[] {1, 2, 7});
                    CommitMessage indexMessage =
                            new CommitMessageImpl(
                                    BinaryRow.EMPTY_ROW,
                                    0,
                                    1,
                                    DataIncrement.emptyIncrement(),
                                    new CompactIncrement(
                                            Collections.emptyList(),
                                            Collections.emptyList(),
                                            Collections.emptyList(),
                                            Collections.singletonList(index),
                                            Collections.emptyList()));
                    pending.clear();
                    pending.put(2L, messages);
                    pending.put(3L, Collections.singletonList(indexMessage));
                    commit.ignoreEmptyCommit(false);
                    commit.commit(3, Collections.emptyList());
                    commit.ignoreEmptyCommit(true);
                } else {
                    commit.commit(2, appendOnly);
                }
                assertThat(table.snapshotManager().latestSnapshot().commitKind())
                        .isEqualTo(Snapshot.CommitKind.APPEND);
                assertThat(table.snapshotManager().latestSnapshot().changelogManifestList())
                        .isNull();
                if (crash == CrashPoint.LEGACY_EXPIRED_APPEND) {
                    for (long id = 1; id < table.snapshotManager().latestSnapshotId(); id++) {
                        io.deleteQuietly(table.snapshotManager().snapshotPath(id));
                    }
                    table.snapshotManager().invalidateCache();
                }
                if (reuseCommit) {
                    commit.filterAndCommit(pending);
                } else {
                    try (TableCommitImpl recovered = table.newCommit("recovery")) {
                        recovered.filterAndCommit(pending);
                    }
                }
            } else if (crash == CrashPoint.BEFORE_PUBLICATION || crash == CrashPoint.AFTER_APPEND) {
                io.beforeAppend = crash == CrashPoint.BEFORE_PUBLICATION;
                io.appendSnapshot.set(table.snapshotManager().snapshotPath(previousSnapshot + 1));
                assertThatThrownBy(() -> commit.commit(2, messages)).isInstanceOf(Exception.class);
                assertThat(io.injected.get()).isTrue();
                assertThat(table.snapshotManager().latestSnapshotId())
                        .isEqualTo(previousSnapshot + (crash == CrashPoint.AFTER_APPEND ? 1 : 0));
                if (reuseCommit) {
                    commit.filterAndCommit(pending);
                } else {
                    try (TableCommitImpl recovered = table.newCommit("recovery")) {
                        recovered.filterAndCommit(pending);
                    }
                }
            } else {
                if (crash == CrashPoint.AFTER_COMPACT) {
                    io.compactSnapshot.set(
                            table.snapshotManager().snapshotPath(previousSnapshot + 2));
                }
                commit.commit(2, messages);
            }
            assertThat(table.snapshotManager().latestSnapshotId())
                    .isEqualTo(previousSnapshot + (crash == CrashPoint.LEGACY_INDEX_ONLY ? 4 : 2));
            if (crash != CrashPoint.LEGACY_APPEND
                    && crash != CrashPoint.LEGACY_INDEX_ONLY
                    && crash != CrashPoint.LEGACY_EXPIRED_APPEND) {
                assertThat(table.snapshotManager().latestSnapshot().properties())
                        .containsEntry("paimon.commit.compact-protocol", "recoverable-v1");
            }
            assertThat(io.injected.get())
                    .isEqualTo(
                            crash == CrashPoint.BEFORE_PUBLICATION
                                    || crash == CrashPoint.AFTER_APPEND
                                    || crash == CrashPoint.AFTER_COMPACT);
            if (crash == CrashPoint.LEGACY_INDEX_ONLY) {
                IndexFileHandler handler = table.store().newIndexFileHandler();
                IndexFileMeta index =
                        handler.scanHashIndex(
                                        table.snapshotManager().latestSnapshot(),
                                        BinaryRow.EMPTY_ROW,
                                        0)
                                .get();
                assertThat(handler.hashIndex(BinaryRow.EMPTY_ROW, 0).readList(index))
                        .containsExactly(1, 2, 7);
            }
            List<Integer> batchValues = new ArrayList<>();
            try (RecordReader<InternalRow> reader =
                    table.newRead().createReader(table.newScan().plan())) {
                reader.forEachRemaining(row -> batchValues.add(row.getInt(1)));
            }
            assertThat(batchValues).containsExactly(20);
            if (!producer.equals("none")) {
                assertThat(streamRows(table, scan)).containsExactlyInAnyOrder("-U 1|10", "+U 1|20");
            }
            long snapshot = table.snapshotManager().latestSnapshotId();
            try (TableCommitImpl recovered = table.newCommit("recovery")) {
                assertThat(recovered.filterAndCommit(pending)).isZero();
                assertThat(recovered.filterAndCommit(pending)).isZero();
            }
            assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(snapshot);
            if (!producer.equals("none")) {
                assertThat(streamRows(table, scan)).isEmpty();
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"false,true", "false,false", "true,true"})
    void competingCompactionRecoversRequiredChangelogWithoutDuplicates(
            boolean appendPending, boolean otherFull) throws Exception {
        Path root = new Path(directory.toUri());
        LocalFileIO io = LocalFileIO.create();
        new FileSystemSchemaManager(io, root)
                .createTable(
                        Schema.newBuilder()
                                .column("k", DataTypes.INT())
                                .column("v", DataTypes.INT())
                                .primaryKey("k")
                                .option("bucket", "1")
                                .option("changelog-producer", "full-compaction")
                                .option("deletion-vectors.enabled", "false")
                                .build());
        FileStoreTable table = FileStoreTableFactory.create(io, root);
        StreamTableScan scan = table.newReadBuilder().newStreamScan();
        scan.restore(1L);
        try (IOManager disk = IOManager.create(directory.toString());
                TableWriteImpl<?> write = table.newWrite("legacy").withIOManager(disk);
                TableCommitImpl commit = table.newCommit("legacy")) {
            write.write(GenericRow.of(1, 10));
            write.compact(BinaryRow.EMPTY_ROW, 0, true);
            commit.commit(1, write.prepareCommit(true, 1));
            assertThat(streamRows(table, scan)).containsExactly("+I 1|10");
            write.write(GenericRow.of(1, 20));
            write.compact(BinaryRow.EMPTY_ROW, 0, true);
            List<CommitMessage> pending = write.prepareCommit(true, 2);
            List<CommitMessage> appendOnly =
                    pending.stream()
                            .map(
                                    message -> {
                                        CommitMessageImpl original = (CommitMessageImpl) message;
                                        return (CommitMessage)
                                                new CommitMessageImpl(
                                                        original.partition(),
                                                        original.bucket(),
                                                        original.totalBuckets(),
                                                        original.newFilesIncrement(),
                                                        CompactIncrement.emptyIncrement());
                                    })
                            .collect(Collectors.toList());
            if (!appendPending) {
                commit.commit(2, appendOnly);
            }
            try (TableWriteImpl<?> other = table.newWrite("other").withIOManager(disk);
                    TableCommitImpl otherCommit = table.newCommit("other")) {
                other.write(GenericRow.of(2, 30));
                if (otherFull) {
                    other.compact(BinaryRow.EMPTY_ROW, 0, true);
                }
                List<CommitMessage> otherMessages = other.prepareCommit(true, 1);
                if (!otherFull) {
                    List<DataFileMeta> before =
                            pending.stream()
                                    .flatMap(
                                            message ->
                                                    ((CommitMessageImpl) message)
                                                            .compactIncrement().compactBefore()
                                                                    .stream())
                                    .filter(file -> file.level() == 0)
                                    .collect(Collectors.toList());
                    assertThat(before).isNotEmpty();
                    CommitMessageImpl appended = (CommitMessageImpl) otherMessages.get(0);
                    otherMessages =
                            Collections.singletonList(
                                    new CommitMessageImpl(
                                            appended.partition(),
                                            appended.bucket(),
                                            appended.totalBuckets(),
                                            appended.newFilesIncrement(),
                                            new CompactIncrement(
                                                    before,
                                                    before.stream()
                                                            .map(file -> file.upgrade(1))
                                                            .collect(Collectors.toList()),
                                                    Collections.emptyList(),
                                                    Collections.emptyList(),
                                                    Collections.emptyList())));
                }
                otherCommit.commit(1, otherMessages);
            }
            if (appendPending) {
                assertThat(streamRows(table, scan)).containsExactly("+I 2|30");
                assertThatThrownBy(() -> commit.commit(2, pending))
                        .isInstanceOf(RuntimeException.class)
                        .hasMessageContaining("refresh the writer");
                assertThat(commit.filterAndCommit(Collections.singletonMap(2L, pending))).isZero();
                assertThat(streamRows(table, scan)).containsExactlyInAnyOrder("-U 1|10", "+U 1|20");
                List<Integer> values = new ArrayList<>();
                try (RecordReader<InternalRow> reader =
                        table.newRead().createReader(table.newScan().plan())) {
                    reader.forEachRemaining(row -> values.add(row.getInt(1)));
                }
                assertThat(values).containsExactlyInAnyOrder(20, 30);
                return;
            }
            if (otherFull) {
                assertThat(streamRows(table, scan))
                        .containsExactlyInAnyOrder("-U 1|10", "+U 1|20", "+I 2|30");
            } else {
                assertThat(streamRows(table, scan)).isEmpty();
            }
            long durable = table.snapshotManager().latestSnapshotId();
            try (TableCommitImpl recovered = table.newCommit("legacy")) {
                assertThatThrownBy(
                                () ->
                                        recovered.filterAndCommit(
                                                Collections.singletonMap(2L, pending)))
                        .isInstanceOf(RuntimeException.class)
                        .hasMessageContaining("refresh the writer");
                assertThat(recovered.filterAndCommit(Collections.singletonMap(2L, pending)))
                        .isZero();
                assertThat(recovered.filterAndCommit(Collections.singletonMap(2L, pending)))
                        .isZero();
            }
            assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(durable + 1);
            durable++;
            if (otherFull) {
                assertThat(streamRows(table, scan)).isEmpty();
            } else {
                assertThat(streamRows(table, scan))
                        .containsExactlyInAnyOrder("-U 1|10", "+U 1|20", "+I 2|30");
            }
            List<Integer> values = new ArrayList<>();
            try (RecordReader<InternalRow> reader =
                    table.newRead().createReader(table.newScan().plan())) {
                reader.forEachRemaining(row -> values.add(row.getInt(1)));
            }
            assertThat(values).containsExactlyInAnyOrder(20, 30);
            List<CommitMessage> compactOnly =
                    pending.stream()
                            .map(
                                    message -> {
                                        CommitMessageImpl original = (CommitMessageImpl) message;
                                        return (CommitMessage)
                                                new CommitMessageImpl(
                                                        original.partition(),
                                                        original.bucket(),
                                                        original.totalBuckets(),
                                                        DataIncrement.emptyIncrement(),
                                                        original.compactIncrement());
                                    })
                            .collect(Collectors.toList());
            try (TableCommitImpl compactor = table.newCommit("new-compactor")) {
                assertThatThrownBy(() -> compactor.commit(99, compactOnly))
                        .isInstanceOf(RuntimeException.class)
                        .hasMessageContaining("refresh the writer");
                assertThat(compactor.filterAndCommit(Collections.singletonMap(99L, compactOnly)))
                        .isZero();
            }
            assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(durable + 1);
            assertThat(streamRows(table, scan)).isEmpty();
            List<Integer> afterCompaction = new ArrayList<>();
            try (RecordReader<InternalRow> reader =
                    table.newRead().createReader(table.newScan().plan())) {
                reader.forEachRemaining(row -> afterCompaction.add(row.getInt(1)));
            }
            assertThat(afterCompaction).containsExactlyInAnyOrder(20, 30);
        }
    }

    @ParameterizedTest
    @CsvSource({"false,false", "true,false", "false,true", "true,true"})
    @SuppressWarnings("unchecked")
    void restoredWriterPreservesPendingInputAfterDiscardedCompactionFilesAreRemoved(
            boolean pendingInput, boolean laterAppend) throws Exception {
        Path root = new Path(directory.toUri());
        LocalFileIO io = LocalFileIO.create();
        new FileSystemSchemaManager(io, root)
                .createTable(
                        Schema.newBuilder()
                                .column("k", DataTypes.INT())
                                .column("v", DataTypes.INT())
                                .primaryKey("k")
                                .option("bucket", "1")
                                .option("changelog-producer", "full-compaction")
                                .option("deletion-vectors.enabled", "false")
                                .build());
        FileStoreTable table = FileStoreTableFactory.create(io, root);
        StreamTableScan scan = table.newReadBuilder().newStreamScan();
        scan.restore(1L);
        List<FileStoreWrite.State<KeyValue>> checkpoint;
        List<CommitMessage> messages;
        try (IOManager disk = IOManager.create(directory.toString());
                TableWriteImpl<KeyValue> write =
                        (TableWriteImpl<KeyValue>) table.newWrite("recovery").withIOManager(disk);
                TableCommitImpl commit = table.newCommit("recovery")) {
            write.write(GenericRow.of(1, 10));
            write.compact(BinaryRow.EMPTY_ROW, 0, true);
            commit.commit(1, write.prepareCommit(true, 1));
            assertThat(streamRows(table, scan)).containsExactly("+I 1|10");
            write.write(GenericRow.of(1, 20));
            write.compact(BinaryRow.EMPTY_ROW, 0, true);
            messages = write.prepareCommit(true, 2);
            if (pendingInput) {
                write.write(GenericRow.of(1, 40));
            }
            checkpoint = write.checkpoint();
            try (TableWriteImpl<?> other = table.newWrite("other").withIOManager(disk);
                    TableCommitImpl otherCommit = table.newCommit("other")) {
                other.write(GenericRow.of(2, 30));
                other.compact(BinaryRow.EMPTY_ROW, 0, true);
                otherCommit.commit(1, other.prepareCommit(true, 1));
            }
            assertThat(streamRows(table, scan)).containsExactly("+I 2|30");
            assertThatThrownBy(() -> commit.commit(2, messages))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("refresh the writer");
            assertThat(commit.filterAndCommit(Collections.singletonMap(2L, messages))).isZero();
            assertThat(streamRows(table, scan)).containsExactlyInAnyOrder("-U 1|10", "+U 1|20");
        }
        List<String> committed =
                table.store().newScan().plan().files().stream()
                        .map(entry -> entry.file().fileName())
                        .collect(Collectors.toList());
        for (CommitMessage message : messages) {
            CommitMessageImpl original = (CommitMessageImpl) message;
            for (DataFileMeta file : original.compactIncrement().compactAfter()) {
                assertThat(committed).doesNotContain(file.fileName());
                io.deleteQuietly(
                        table.store()
                                .pathFactory()
                                .createDataFilePathFactory(original.partition(), original.bucket())
                                .toPath(file));
            }
        }
        if (laterAppend) {
            try (IOManager disk = IOManager.create(directory.toString());
                    TableWriteImpl<?> later = table.newWrite("recovery").withIOManager(disk);
                    TableCommitImpl commit = table.newCommit("recovery")) {
                later.write(GenericRow.of(3, 60));
                List<CommitMessage> append = later.prepareCommit(true, 3);
                assertThat(append)
                        .allSatisfy(
                                message ->
                                        assertThat(
                                                        ((CommitMessageImpl) message)
                                                                .compactIncrement()
                                                                .changelogFiles())
                                                .isEmpty());
                commit.commit(3, append);
            }
            assertThat(streamRows(table, scan)).isEmpty();
        }
        try (IOManager disk = IOManager.create(directory.toString());
                TableWriteImpl<KeyValue> write =
                        (TableWriteImpl<KeyValue>) table.newWrite("recovery").withIOManager(disk);
                TableCommitImpl commit = table.newCommit("recovery")) {
            write.restore(checkpoint);
            if (!pendingInput) {
                write.write(GenericRow.of(1, 50));
            }
            write.compact(BinaryRow.EMPTY_ROW, 0, true);
            commit.commit(laterAppend ? 4 : 3, write.prepareCommit(true, laterAppend ? 4 : 3));
        }
        List<String> expected = new ArrayList<>();
        expected.add("-U 1|20");
        expected.add("+U 1|" + (pendingInput ? 40 : 50));
        if (laterAppend) {
            expected.add("+I 3|60");
        }
        assertThat(streamRows(table, scan)).containsExactlyInAnyOrderElementsOf(expected);
        List<Integer> values = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                table.newRead().createReader(table.newScan().plan())) {
            reader.forEachRemaining(row -> values.add(row.getInt(1)));
        }
        List<Integer> expectedValues = new ArrayList<>(Arrays.asList(pendingInput ? 40 : 50, 30));
        if (laterAppend) {
            expectedValues.add(60);
        }
        assertThat(values).containsExactlyInAnyOrderElementsOf(expectedValues);
    }

    @Test
    @EnabledIfSystemProperty(named = "paimon.commit.benchmark", matches = "true")
    void measureProductionFullCompactionCommitBoundary() throws Exception {
        int warmups =
                Integer.parseInt(System.getProperties().getProperty("paimon.commit.warmups", "2"));
        int runs = Integer.parseInt(System.getProperties().getProperty("paimon.commit.runs", "3"));
        int keys = Integer.parseInt(System.getProperties().getProperty("paimon.commit.keys", "64"));
        int commits =
                Integer.parseInt(System.getProperties().getProperty("paimon.commit.count", "16"));
        assertThat(warmups).isGreaterThanOrEqualTo(1);
        assertThat(runs).isGreaterThanOrEqualTo(3);
        List<Double> seconds = new ArrayList<>();
        for (int run = 0; run < warmups + runs; run++) {
            Path root = new Path(directory.resolve("benchmark-" + run).toUri());
            LocalFileIO io = LocalFileIO.create();
            new FileSystemSchemaManager(io, root)
                    .createTable(
                            Schema.newBuilder()
                                    .column("k", DataTypes.INT())
                                    .column("v", DataTypes.INT())
                                    .primaryKey("k")
                                    .option("bucket", "1")
                                    .option("changelog-producer", "full-compaction")
                                    .option("deletion-vectors.enabled", "false")
                                    .build());
            FileStoreTable table = FileStoreTableFactory.create(io, root);
            StreamTableScan scan = table.newReadBuilder().newStreamScan();
            scan.restore(1L);
            long nanos = 0;
            try (IOManager disk = IOManager.create(directory.toString());
                    TableWriteImpl<?> write = table.newWrite("benchmark").withIOManager(disk);
                    TableCommitImpl commit = table.newCommit("benchmark")) {
                for (int id = 1; id <= commits; id++) {
                    for (int key = 0; key < keys; key++) {
                        write.write(GenericRow.of(key, id));
                    }
                    write.compact(BinaryRow.EMPTY_ROW, 0, true);
                    List<CommitMessage> messages = write.prepareCommit(true, id);
                    long start = System.nanoTime();
                    commit.commit(id, messages);
                    nanos += System.nanoTime() - start;
                }
            }
            List<Integer> values = new ArrayList<>();
            try (RecordReader<InternalRow> reader =
                    table.newRead().createReader(table.newScan().plan())) {
                reader.forEachRemaining(row -> values.add(row.getInt(1)));
            }
            assertThat(values).hasSize(keys).allMatch(value -> value == commits);
            assertThat(streamRows(table, scan)).hasSize(keys * (1 + 2 * (commits - 1)));
            double elapsed = nanos / 1e9;
            System.out.printf(
                    "[paimon-commit-trial] keys=%d commits=%d run=%d warmup=%s seconds=%.9f%n",
                    keys, commits, run, run < warmups, elapsed);
            if (run >= warmups) {
                seconds.add(elapsed);
            }
        }
        System.out.println(
                "[paimon-commit-job] keys=" + keys + " commits=" + commits + " seconds=" + seconds);
    }

    private enum CrashPoint {
        NONE,
        BEFORE_PUBLICATION,
        AFTER_APPEND,
        AFTER_COMPACT,
        LEGACY_APPEND,
        LEGACY_INDEX_ONLY,
        LEGACY_EXPIRED_APPEND
    }

    private static Stream<Arguments> scenarios() {
        List<Arguments> cases = new ArrayList<>();
        for (String producer : new String[] {"full-compaction", "lookup"}) {
            for (boolean vectors : new boolean[] {false, true}) {
                if (producer.equals("full-compaction") && vectors) {
                    continue;
                }
                for (CrashPoint crash : CrashPoint.values()) {
                    if (crash == CrashPoint.LEGACY_INDEX_ONLY
                            && !(producer.equals("lookup") && vectors)) {
                        continue;
                    }
                    for (boolean reuse : new boolean[] {false, true}) {
                        cases.add(Arguments.of(producer, vectors, crash, reuse));
                    }
                }
            }
        }
        return cases.stream();
    }

    private static List<String> streamRows(FileStoreTable table, StreamTableScan scan)
            throws Exception {
        List<String> rows = new ArrayList<>();
        long latest = table.snapshotManager().latestSnapshotId();
        while (scan.checkpoint() <= latest) {
            long before = scan.checkpoint();
            try (RecordReader<InternalRow> reader = table.newRead().createReader(scan.plan())) {
                reader.forEachRemaining(
                        row ->
                                rows.add(
                                        row.getRowKind().shortString()
                                                + " "
                                                + row.getInt(0)
                                                + "|"
                                                + row.getInt(1)));
            }
            assertThat(scan.checkpoint()).isGreaterThan(before);
        }
        return rows;
    }

    private static class FailBetweenSnapshots extends LocalFileIO {
        private final AtomicReference<Path> appendSnapshot = new AtomicReference<>();
        private final AtomicReference<Path> compactSnapshot = new AtomicReference<>();
        private final AtomicBoolean injected = new AtomicBoolean();
        private boolean beforeAppend;

        @Override
        public boolean rename(Path source, Path target) throws IOException {
            boolean success = super.rename(source, target);
            Path compact = compactSnapshot.get();
            if (success && target.equals(compact) && compactSnapshot.compareAndSet(compact, null)) {
                injected.set(true);
                throw new IOException("Injected lost response after durable COMPACT");
            }
            return success;
        }

        @Override
        public SeekableInputStream newInputStream(Path path) throws IOException {
            Path append = appendSnapshot.get();
            if (append != null
                    && path.toString().contains("/manifest/")
                    && (beforeAppend != super.exists(append))
                    && appendSnapshot.compareAndSet(append, null)) {
                injected.set(true);
                throw new IOException(
                        "Injected manifest-read failure at "
                                + (beforeAppend ? "before APPEND" : "after APPEND"));
            }
            return super.newInputStream(path);
        }
    }
}
