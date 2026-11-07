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
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.IncrementalSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.CloseableIterator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for generating changelog during reads. */
public class ChangelogOnReadTest extends TableTestBase {

    @Test
    public void testForceLookupCachedHistoryAfterFileUpgrade() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.FILE_FORMAT.key(), "avro");
        options.put(CoreOptions.NUM_LEVELS.key(), "3");
        options.put(CoreOptions.NUM_SORTED_RUNS_COMPACTION_TRIGGER.key(), "10");
        options.put(CoreOptions.LOOKUP_COMPACT.key(), "radical");
        FileStoreTable table =
                createTable(CoreOptions.MergeEngine.AGGREGATE, 2, true).copy(options);
        // Keep the history file large enough that the next small delta compacts only to L1.
        List<InternalRow> rows = new ArrayList<>();
        for (int key = 1; key <= 2000; key++) {
            rows.add(GenericRow.of(1, key, 1, 10));
        }
        write(table, ioManager, rows.toArray(new InternalRow[0]));
        ReadBuilder readBuilder = table.newReadBuilder().withProjection(new int[] {3});
        StreamTableScan scan = readBuilder.newStreamScan();
        DataSplit initial = (DataSplit) scan.plan().splits().get(0);
        assertThat(initial.dataFiles()).hasSize(1);
        String historyFile = initial.dataFiles().get(0).fileName();
        FileStoreTable compactionTable =
                table.copy(Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), "false"));
        BatchWriteBuilder compactBuilder = compactionTable.newBatchWriteBuilder();
        try (TableRead read = readBuilder.newRead().withIOManager(ioManager);
                BatchTableWrite compactWrite = compactBuilder.newWrite();
                BatchTableCommit compactCommit = compactBuilder.newCommit()) {
            compactWrite.withIOManager(ioManager);
            compactWrite.compact(initial.partition(), initial.bucket(), true);
            List<CommitMessage> upgrade = compactWrite.prepareCommit();

            // Cache the original file at L0 before publishing its same-name upgrade to L2.
            write(table, ioManager, GenericRow.of(1, 1, 2, 3));
            assertThat(read(read, scan.plan())).containsExactly("-U 10", "+U 13");
            compactCommit.commit(upgrade);
            compact(compactionTable, initial.partition(), initial.bucket(), ioManager, false);

            write(table, ioManager, GenericRow.of(1, 1, 3, 4));
            TableScan.Plan delta = scan.plan();
            IncrementalSplit split = (IncrementalSplit) delta.splits().get(0);
            assertThat(split.beforeFiles())
                    .extracting(DataFileMeta::level)
                    .containsExactlyInAnyOrder(1, 2);
            assertThat(split.beforeFiles())
                    .filteredOn(file -> file.fileName().equals(historyFile))
                    .hasSize(1)
                    .allSatisfy(file -> assertThat(file.level()).isEqualTo(2));
            // L1 contains 13, including the older L2 value 10; count that history only once.
            assertThat(read(read, delta)).containsExactly("-U 13", "+U 17");
            try (TableRead coldRead = readBuilder.newRead().withIOManager(ioManager)) {
                assertThat(read(coldRead, delta)).containsExactly("-U 13", "+U 17");
            }
        }
    }

    @Test
    public void testDeduplicateMultipleLevel0() throws Exception {
        FileStoreTable table = createTable(CoreOptions.MergeEngine.DEDUPLICATE, 2, false);
        write(table, GenericRow.of(1, 1, 1, 10));

        StreamTableScan scan = table.newStreamScan();
        ReadBuilder readBuilder = table.newReadBuilder().withProjection(new int[] {3});
        try (IOManager lookupIO = IOManager.create(tempPath.resolve("lookup").toString());
                TableRead read = readBuilder.newRead().withIOManager(lookupIO)) {
            assertThat(read(read, scan.plan())).containsExactly("+I 10");

            write(table, GenericRow.of(1, 1, 3, 30));
            assertThat(read(read, scan.plan())).containsExactly("-U 10", "+U 30");

            write(table, GenericRow.ofKind(RowKind.DELETE, 1, 1, 4, null));
            assertThat(read(read, scan.plan())).containsExactly("-D 30");
        }
    }

    @ParameterizedTest
    @CsvSource({
        "false,INSERT,DELETE",
        "true,INSERT,DELETE",
        "false,UPDATE_AFTER,UPDATE_BEFORE",
        "true,UPDATE_AFTER,UPDATE_BEFORE"
    })
    public void testHistoryWithSameSequenceAndDifferentRowKinds(
            boolean withSequenceField, RowKind addKind, RowKind retractKind) throws Exception {
        Schema.Builder schema =
                Schema.newBuilder()
                        .column("k", DataTypes.INT())
                        .column("seq", DataTypes.INT())
                        .column("v", DataTypes.INT())
                        .primaryKey("k")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.WRITE_ONLY.key(), "true")
                        .option(CoreOptions.CHANGELOG_PRODUCER.key(), "on-read");
        if (withSequenceField) {
            schema.option(CoreOptions.SEQUENCE_FIELD.key(), "seq");
        }
        catalog.createTable(identifier(), schema.build(), false);
        FileStoreTable table = getTableDefault();
        BatchWriteBuilder first = table.newBatchWriteBuilder();
        BatchWriteBuilder second = table.newBatchWriteBuilder();
        try (BatchTableWrite firstWrite = first.newWrite();
                BatchTableWrite secondWrite = second.newWrite();
                BatchTableCommit firstCommit = first.newCommit();
                BatchTableCommit secondCommit = second.newCommit()) {
            // Both writers start before either commit, so each key has equal internal sequences.
            // Reverse the row kinds for key 2 to cover either history file iteration order.
            firstWrite.write(GenericRow.ofKind(addKind, 1, 1, 10));
            firstWrite.write(GenericRow.ofKind(retractKind, 2, 1, 20));
            secondWrite.write(GenericRow.ofKind(retractKind, 1, 1, 10));
            secondWrite.write(GenericRow.ofKind(addKind, 2, 1, 20));
            List<CommitMessage> firstMessages = firstWrite.prepareCommit();
            List<CommitMessage> secondMessages = secondWrite.prepareCommit();
            firstCommit.commit(firstMessages);
            secondCommit.commit(secondMessages);
        }

        ReadBuilder readBuilder = table.newReadBuilder().withProjection(new int[] {2});
        StreamTableScan scan = readBuilder.newStreamScan();
        try (TableRead read = readBuilder.newRead().withIOManager(ioManager)) {
            assertThat(read(read, scan.plan()))
                    .containsExactlyInAnyOrder(
                            addKind.shortString() + " 10", addKind.shortString() + " 20");
            write(table, GenericRow.of(1, 2, 11), GenericRow.of(2, 2, 21));
            TableScan.Plan delta = scan.plan();
            IncrementalSplit split = (IncrementalSplit) delta.splits().get(0);
            assertThat(split.beforeFiles())
                    .hasSize(2)
                    .allSatisfy(
                            file -> {
                                assertThat(file.minSequenceNumber()).isZero();
                                assertThat(file.maxSequenceNumber()).isEqualTo(1);
                            });
            assertThat(read(read, delta)).containsExactly("-U 10", "+U 11", "-U 20", "+U 21");
        }
    }

    @Test
    public void testLookupStateEvictionAndReload() throws Exception {
        FileStoreTable table = createTable(CoreOptions.MergeEngine.AGGREGATE, 2, false);
        write(
                table,
                GenericRow.of(1, 1, 1, 10),
                GenericRow.of(2, 1, 1, 20),
                GenericRow.of(3, 1, 1, 30));

        StreamTableScan scan = table.newStreamScan();
        Path lookupDir = tempPath.resolve("lookup");
        Path reloaded;
        try (IOManager lookupIO = IOManager.create(lookupDir.toString())) {
            try (TableRead read =
                    table.newReadBuilder()
                            .withProjection(new int[] {3})
                            .newRead()
                            .withIOManager(lookupIO)) {
                TableScan.Plan initial = scan.plan();
                Map<Integer, String> dataFiles = new HashMap<>();
                for (Split split : initial.splits()) {
                    DataSplit dataSplit = (DataSplit) split;
                    dataFiles.put(
                            dataSplit.partition().getInt(0),
                            dataSplit.dataFiles().get(0).fileName());
                }
                assertThat(read(read, initial))
                        .containsExactlyInAnyOrder("+I 10", "+I 20", "+I 30");

                // Access A, B, A, C. B must be evicted even though A was loaded first.
                write(table, GenericRow.of(1, 1, 2, 1));
                assertThat(read(read, scan.plan())).containsExactly("-U 10", "+U 11");
                Path lookupA = findLookupFile(lookupDir, dataFiles.get(1));
                write(table, GenericRow.of(2, 1, 2, 2));
                assertThat(read(read, scan.plan())).containsExactly("-U 20", "+U 22");
                Path lookupB = findLookupFile(lookupDir, dataFiles.get(2));
                write(table, GenericRow.of(1, 1, 3, 3));
                assertThat(read(read, scan.plan())).containsExactly("-U 11", "+U 14");
                write(table, GenericRow.of(3, 1, 2, 4));
                assertThat(read(read, scan.plan())).containsExactly("-U 30", "+U 34");
                assertThat(lookupA).exists();
                assertThat(lookupB).doesNotExist();

                write(table, GenericRow.of(2, 1, 3, 5));
                assertThat(read(read, scan.plan())).containsExactly("-U 22", "+U 27");
                reloaded = findLookupFile(lookupDir, dataFiles.get(2));
                assertThat(reloaded).exists().isNotEqualTo(lookupB);
            }
            assertThat(reloaded).doesNotExist();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1})
    public void testRejectNonPositiveLookupStateMaxSize(int maxStates) throws Exception {
        FileStoreTable table = createTable(CoreOptions.MergeEngine.DEDUPLICATE, maxStates, false);
        assertThatThrownBy(() -> ((PrimaryKeyFileStoreTable) table).store().newRead())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(CoreOptions.CHANGELOG_ON_READ_LOOKUP_STATE_MAX_SIZE.key());
    }

    @ParameterizedTest
    @CsvSource({"min-heap,false", "min-heap,true", "loser-tree,false", "loser-tree,true"})
    public void testProjectedChangelogWithSpill(String sortEngine, boolean forceLookup)
            throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("k", DataTypes.INT())
                        .column("seq", DataTypes.INT())
                        .column("v", DataTypes.STRING())
                        .primaryKey("k")
                        .option("bucket", "1")
                        .option("write-only", "true")
                        .option("changelog-producer", "on-read")
                        .option("force-lookup", String.valueOf(forceLookup))
                        .option("sequence.field", "seq")
                        .option("sort-engine", sortEngine)
                        // Only the per-key merge buffer should create spill files in this test.
                        .option("sort-spill-threshold", "1000")
                        .option("lookup.merge-records-threshold", "0")
                        .option("lookup.merge-buffer-size", "4 kb")
                        .option("page-size", "4 kb")
                        .build();
        catalog.createTable(identifier(), schema, false);
        FileStoreTable table = getTableDefault();
        BinaryString value =
                BinaryString.fromString(String.join("", Collections.nCopies(1000, "x")));
        for (int seq = 0; seq < 8; seq++) {
            write(table, ioManager, GenericRow.of(1, seq, value));
        }
        ReadBuilder builder = table.newReadBuilder().withProjection(new int[] {2, 1});
        StreamTableScan scan = builder.newStreamScan();
        scan.plan();

        // Commit overlapping files together so the new versions of this key also spill.
        BatchWriteBuilder writes = table.newBatchWriteBuilder();
        List<CommitMessage> messages = new ArrayList<>();
        for (int seq = 8; seq < 16; seq++) {
            try (BatchTableWrite write = writes.newWrite()) {
                write.withIOManager(ioManager);
                write.write(GenericRow.of(1, seq, value));
                messages.addAll(write.prepareCommit());
            }
        }
        try (BatchTableCommit commit = writes.newCommit()) {
            commit.commit(messages);
        }
        Path lookupDir = tempPath.resolve("lookup");
        try (IOManager lookupIO = IOManager.create(lookupDir.toString());
                TableRead read = builder.newRead().withIOManager(lookupIO);
                CloseableIterator<InternalRow> rows =
                        read.createReader(scan.plan()).toCloseableIterator()) {
            InternalRow before = rows.next();
            assertThat(before.getRowKind()).isEqualTo(RowKind.UPDATE_BEFORE);
            assertThat(before.getString(0)).isEqualTo(value);
            assertThat(before.getInt(1)).isEqualTo(7);
            try (Stream<Path> files = Files.walk(lookupDir)) {
                assertThat(files)
                        .anyMatch(
                                file ->
                                        file.getFileName()
                                                .toString()
                                                .matches("[0-9a-f]{32}[.]channel"));
            }
            InternalRow after = rows.next();
            assertThat(after.getRowKind()).isEqualTo(RowKind.UPDATE_AFTER);
            assertThat(after.getString(0)).isEqualTo(value);
            assertThat(after.getInt(1)).isEqualTo(15);
            assertThat(rows.hasNext()).isFalse();
        }
    }

    private FileStoreTable createTable(
            CoreOptions.MergeEngine mergeEngine, int maxStates, boolean forceLookup)
            throws Exception {
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("p", DataTypes.INT())
                        .column("k", DataTypes.INT())
                        .column("seq", DataTypes.INT())
                        .column("v", DataTypes.INT())
                        .primaryKey("p", "k")
                        .partitionKeys("p")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.WRITE_ONLY.key(), "true")
                        .option(CoreOptions.CHANGELOG_PRODUCER.key(), "on-read")
                        .option(CoreOptions.FORCE_LOOKUP.key(), String.valueOf(forceLookup))
                        .option(CoreOptions.MERGE_ENGINE.key(), mergeEngine.toString())
                        .option(CoreOptions.SEQUENCE_FIELD.key(), "seq")
                        .option(
                                CoreOptions.CHANGELOG_ON_READ_LOOKUP_STATE_MAX_SIZE.key(),
                                String.valueOf(maxStates));
        if (mergeEngine == CoreOptions.MergeEngine.AGGREGATE) {
            builder.option("fields.v.aggregate-function", "sum");
        }
        catalog.createTable(identifier(), builder.build(), false);
        return getTableDefault();
    }

    private List<String> read(TableRead read, TableScan.Plan plan) throws IOException {
        List<String> result = new ArrayList<>();
        try (RecordReader<InternalRow> reader = read.createReader(plan)) {
            reader.forEachRemaining(
                    row -> result.add(row.getRowKind().shortString() + " " + row.getInt(0)));
        }
        return result;
    }

    private Path findLookupFile(Path lookupDir, String dataFile) throws IOException {
        try (Stream<Path> paths = Files.walk(lookupDir)) {
            return paths.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().contains(dataFile))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Lookup file was not cached."));
        }
    }
}
