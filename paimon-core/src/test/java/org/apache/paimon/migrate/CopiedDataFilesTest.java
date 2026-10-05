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
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
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

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/**
 * Tests for {@link CopiedDataFiles}: data files copied from another table, committed the way {@code
 * CopyFilesCommitOperator} of {@code sys.copy} does.
 */
public class CopiedDataFilesTest extends TableTestBase {

    private static final Identifier SOURCE = new Identifier("default", "src");
    private static final Identifier SOURCE2 = new Identifier("default", "src2");
    private static final Identifier TARGET = new Identifier("default", "dst");

    // ---------------------------------------------------------------------------------------------
    // row ids
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testCopyIntoTableWithoutRowsKeepsTheRowIdsOfTheSource() throws Exception {
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
    public void testCopyIntoOnePartitionShiftsPastTheRowsOfOtherPartitions() throws Exception {
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
        write(target, row(3, "c", "p1"));
        assertThat(rowIds(table(TARGET))).containsEntry(3, 4L);
        assertUniqueRowIds(table(TARGET));
    }

    @Test
    public void testCopyWithPartitionFilterShiftsPastTheRowsOfOtherPartitions() throws Exception {
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
    public void testCopyOverEveryRowStillShiftsPastTheRowIdsGivenOut() throws Exception {
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
    public void testCopyShiftsPastCopiedRowIdsTheNextRowIdDoesNotCover() throws Exception {
        // A table without row tracking keeps no next row id, but files copied into it keep theirs.
        createTable(SOURCE, Options.ROW_TRACKING, true);
        write(table(SOURCE), row(1, "a", "p1"), row(2, "b", "p1"));
        createTable(SOURCE2, Options.ROW_TRACKING, true);
        write(table(SOURCE2), row(3, "c", "p2"), row(4, "d", "p2"));
        createTable(TARGET, Options.PLAIN, true);

        copy(SOURCE, TARGET, null);
        copy(SOURCE2, TARGET, null);

        Map<Long, Integer> byFirstRowId = new TreeMap<>();
        for (DataFileMeta file : liveFiles(table(TARGET))) {
            byFirstRowId.put(file.firstRowId(), (int) file.rowCount());
        }
        assertThat(byFirstRowId).containsOnlyKeys(0L, 2L);
    }

    @Test
    public void testShiftKeepsColumnUpdatesOnTheirRows() throws Exception {
        createTable(SOURCE, Options.DATA_EVOLUTION, false);
        write(table(SOURCE), row(1, "a", "p1"), row(2, "b", "p1"));
        // a column update of both rows: shares the row id range of the file above
        updateColumn(table(SOURCE), 0, "a2", "b2");
        createTable(TARGET, Options.DATA_EVOLUTION, false);
        write(table(TARGET), row(10, "x", "p1"), row(11, "y", "p1"));

        copy(SOURCE, TARGET, null);

        FileStoreTable target = table(TARGET);
        // both copied files moved by the same offset: the update still covers its rows
        assertThat(values(target)).containsOnly(entry(1, "a2"), entry(2, "b2"));
        assertThat(rowIds(target)).containsEntry(1, 2L).containsEntry(2, 3L);
        assertThat(liveFiles(target)).allMatch(file -> file.firstRowId() == 2L);
        assertUniqueRowIds(target);

        // and the shifted rows take later column updates by their new row ids
        updateColumn(target, 2, "a3", "b3");
        assertThat(values(table(TARGET))).containsOnly(entry(1, "a3"), entry(2, "b3"));
    }

    @Test
    public void testFilesThatStoreTheirRowIdsAreRefusedByRowTrackingTargets() throws Exception {
        createTable(SOURCE, Options.ROW_TRACKING, false);
        write(table(SOURCE), row(1, "a", "p1"), row(2, "b", "p1"));
        copyOnWriteUpdate(table(SOURCE));
        assertThat(liveFiles(table(SOURCE))).anyMatch(CopiedDataFilesTest::storesRowIds);

        // into a table with rows, whose row ids the stored ones collide with
        createTable(TARGET, Options.ROW_TRACKING, false);
        write(table(TARGET), row(10, "x", "p1"));
        long snapshot = table(TARGET).snapshotManager().latestSnapshotId();
        assertThatThrownBy(() -> copy(SOURCE, TARGET, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("its rows store their row ids")
                .hasMessageContaining("INSERT OVERWRITE");
        assertThat(table(TARGET).snapshotManager().latestSnapshotId()).isEqualTo(snapshot);

        // and into an empty one: the commit would not move the next row id past them
        Identifier empty = new Identifier("default", "empty");
        createTable(empty, Options.ROW_TRACKING, false);
        assertThatThrownBy(() -> copy(SOURCE, empty, null))
                .hasMessageContaining("its rows store their row ids");
        assertThat(table(empty).snapshotManager().latestSnapshot()).isNull();

        // a table without row tracking does not use them
        Identifier plain = new Identifier("default", "plain");
        createTable(plain, Options.PLAIN, false);
        copy(SOURCE, plain, null);
        assertThat(liveFiles(table(plain))).anyMatch(CopiedDataFilesTest::storesRowIds);
    }

    @Test
    public void testLaterColumnUpdatesWinOverCopiedSequenceNumbers() throws Exception {
        createTable(SOURCE, Options.DATA_EVOLUTION, false);
        for (int i = 0; i < 20; i++) {
            write(table(SOURCE), row(i, "v" + i, "p1"));
        }
        createTable(TARGET, Options.DATA_EVOLUTION, false);

        copy(SOURCE, TARGET, null);

        FileStoreTable target = table(TARGET);
        long copySnapshot = target.snapshotManager().latestSnapshotId();
        assertThat(liveFiles(target)).allMatch(file -> file.maxSequenceNumber() <= copySnapshot);
        updateColumn(target, 19, "updated");
        assertThat(values(table(TARGET))).containsEntry(19, "updated").containsEntry(18, "v18");
    }

    @Test
    public void testCopyKeepsTheOrderOfColumnUpdatesFromTheSource() throws Exception {
        createTable(SOURCE, Options.DATA_EVOLUTION, false);
        write(table(SOURCE), row(1, "a", "p1"));
        updateColumn(table(SOURCE), 0, "a2");
        updateColumn(table(SOURCE), 0, "a3");
        assertThat(values(table(SOURCE))).containsEntry(1, "a3");
        createTable(TARGET, Options.DATA_EVOLUTION, false);

        copy(SOURCE, TARGET, null);

        assertThat(values(table(TARGET))).containsEntry(1, "a3");
        updateColumn(table(TARGET), 0, "a4");
        assertThat(values(table(TARGET))).containsEntry(1, "a4");
    }

    @Test
    public void testSequenceNumbersAreMappedInOrderIncludingPerColumnOnes() throws Exception {
        createTable(SOURCE, Options.DATA_EVOLUTION, false);
        write(table(SOURCE), row(1, "a", "p1"));
        updateColumn(table(SOURCE), 0, "a2");
        createTable(TARGET, Options.DATA_EVOLUTION, false);

        List<DataFileMeta> files = new ArrayList<>(liveFiles(table(SOURCE)));
        files.sort((a, b) -> Long.compare(a.maxSequenceNumber(), b.maxSequenceNumber()));
        DataFileMeta older = files.get(0);
        DataFileMeta newer = files.get(1);
        // a compacted file records the sequence of each column
        long[] perColumn = {
            older.maxSequenceNumber(), newer.maxSequenceNumber(), older.maxSequenceNumber() - 1
        };
        List<DataFileMeta> copied =
                new ArrayList<>(Arrays.asList(older.withWriteColsSequences(perColumn), newer));

        CopiedDataFiles.adaptToTarget(table(TARGET), Collections.singletonList(copied));

        // newest -> 0, which the commit stamps with its snapshot id; older -> -1, -2, ...
        assertThat(copied.get(1).maxSequenceNumber()).isEqualTo(0L);
        assertThat(copied.get(1).minSequenceNumber()).isEqualTo(0L);
        assertThat(copied.get(0).maxSequenceNumber()).isEqualTo(-1L);
        assertThat(copied.get(0).writeColsSequences()).containsExactly(-1L, 0L, -2L);
    }

    @Test
    public void testRowTrackingOnlyTargetKeepsSequenceNumbers() throws Exception {
        createTable(SOURCE, Options.ROW_TRACKING, false);
        write(table(SOURCE), row(1, "a", "p1"), row(2, "b", "p1"));
        createTable(TARGET, Options.ROW_TRACKING, false);
        List<DataFileMeta> before = liveFiles(table(SOURCE));

        List<DataFileMeta> copied = new ArrayList<>(before);
        CopiedDataFiles.adaptToTarget(table(TARGET), Collections.singletonList(copied));

        assertThat(copied).isEqualTo(before);
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
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("v", DataTypes.STRING())
                        .column("pt", DataTypes.STRING());
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

    /**
     * Copies the live data files of {@code source}, or of one of its partitions, into {@code
     * target} and commits them as {@code CopyFilesCommitOperator} does: grouped by partition and
     * bucket, adapted by {@link CopiedDataFiles}, with an overwrite commit.
     */
    private void copy(Identifier source, Identifier target, @Nullable String partition)
            throws Exception {
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

        List<CommitMessage> messages = new ArrayList<>();
        for (Map.Entry<Pair<BinaryRow, Integer>, List<DataFileMeta>> entry : files.entrySet()) {
            messages.add(
                    new CommitMessageImpl(
                            entry.getKey().getLeft(),
                            entry.getKey().getRight(),
                            to.coreOptions().bucket(),
                            new DataIncrement(
                                    entry.getValue(),
                                    Collections.emptyList(),
                                    Collections.emptyList()),
                            CompactIncrement.emptyIncrement()));
        }
        try (BatchTableCommit commit = to.newBatchWriteBuilder().withOverwrite().newCommit()) {
            commit.commit(messages);
        }
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
        for (InternalRow row : read(table)) {
            result.put(row.getInt(0), row.getString(1).toString());
        }
        return result;
    }

    private static Map<Integer, Long> rowIds(FileStoreTable table) throws Exception {
        Map<Integer, Long> result = new TreeMap<>();
        for (InternalRow row : read(table)) {
            result.put(row.getInt(0), row.isNullAt(3) ? null : row.getLong(3));
        }
        return result;
    }

    private static void assertUniqueRowIds(FileStoreTable table) throws Exception {
        List<Long> ids = new ArrayList<>(rowIds(table).values());
        assertThat(ids).doesNotContainNull();
        assertThat(new HashSet<>(ids)).hasSameSizeAs(ids);
    }

    private static List<InternalRow> read(FileStoreTable table) throws Exception {
        RowType readType = SpecialFields.rowTypeWithRowTracking(table.rowType(), true, true);
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
}
