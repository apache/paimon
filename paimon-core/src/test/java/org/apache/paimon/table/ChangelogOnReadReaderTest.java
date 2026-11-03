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
import org.apache.paimon.KeyValue;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.operation.MergeFileSplitRead;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.IncrementalSplit;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
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
public class ChangelogOnReadReaderTest extends TableTestBase {

    @Test
    public void testForceLookupCachedHistoryAfterFileUpgrade() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.FILE_FORMAT.key(), "avro");
        options.put(CoreOptions.NUM_LEVELS.key(), "3");
        options.put(CoreOptions.NUM_SORTED_RUNS_COMPACTION_TRIGGER.key(), "10");
        options.put(CoreOptions.LOOKUP_COMPACT.key(), "radical");
        FileStoreTable table = createTable(CoreOptions.MergeEngine.AGGREGATE, true).copy(options);
        // Keep the history file large enough that the next small delta compacts only to L1.
        List<InternalRow> rows = new ArrayList<>();
        for (int key = 1; key <= 2000; key++) {
            rows.add(GenericRow.of(1, key, 1, 10));
        }
        write(table, ioManager, rows.toArray(new InternalRow[0]));
        DataSplit initial = table.newSnapshotReader().read().dataSplits().get(0);
        assertThat(initial.dataFiles()).hasSize(1);
        String historyFile = initial.dataFiles().get(0).fileName();
        FileStoreTable compactionTable =
                table.copy(Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), "false"));
        BatchWriteBuilder compactBuilder = compactionTable.newBatchWriteBuilder();
        try (MergeFileSplitRead read = newReader(table, ioManager, "v");
                BatchTableWrite compactWrite = compactBuilder.newWrite();
                BatchTableCommit compactCommit = compactBuilder.newCommit()) {
            compactWrite.withIOManager(ioManager);
            compactWrite.compact(initial.partition(), initial.bucket(), true);
            List<CommitMessage> upgrade = compactWrite.prepareCommit();

            // Cache the original file at L0 before publishing its same-name upgrade to L2.
            IncrementalSplit warm = append(table, GenericRow.of(1, 1, 2, 3));
            assertThat(read(read, warm)).containsExactly("-U 10", "+U 13");
            compactCommit.commit(upgrade);
            compact(compactionTable, initial.partition(), initial.bucket(), ioManager, false);

            IncrementalSplit split = append(table, GenericRow.of(1, 1, 3, 4));
            assertThat(split.beforeFiles())
                    .extracting(DataFileMeta::level)
                    .containsExactlyInAnyOrder(1, 2);
            assertThat(split.beforeFiles())
                    .filteredOn(file -> file.fileName().equals(historyFile))
                    .hasSize(1)
                    .allSatisfy(file -> assertThat(file.level()).isEqualTo(2));
            // L1 contains 13, including the older L2 value 10; count that history only once.
            assertThat(read(read, split)).containsExactly("-U 13", "+U 17");
            try (MergeFileSplitRead coldRead = newReader(table, ioManager, "v")) {
                assertThat(read(coldRead, split)).containsExactly("-U 13", "+U 17");
            }
        }
    }

    @Test
    public void testDeduplicateMultipleLevel0() throws Exception {
        FileStoreTable table = createTable(CoreOptions.MergeEngine.DEDUPLICATE, false);
        try (MergeFileSplitRead read = newReader(table, ioManager, "v")) {
            assertThat(read(read, append(table, GenericRow.of(1, 1, 1, 10))))
                    .containsExactly("+I 10");
            assertThat(read(read, append(table, GenericRow.of(1, 1, 3, 30))))
                    .containsExactly("-U 10", "+U 30");
            assertThat(read(read, append(table, GenericRow.ofKind(RowKind.DELETE, 1, 1, 4, null))))
                    .containsExactly("-D 30");
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
                        .option(CoreOptions.WRITE_ONLY.key(), "true");
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

        try (MergeFileSplitRead read = newReader(table, ioManager, "v")) {
            IncrementalSplit split =
                    append(table, GenericRow.of(1, 2, 11), GenericRow.of(2, 2, 21));
            assertThat(split.beforeFiles())
                    .hasSize(2)
                    .allSatisfy(
                            file -> {
                                assertThat(file.minSequenceNumber()).isZero();
                                assertThat(file.maxSequenceNumber()).isEqualTo(1);
                            });
            assertThat(read(read, split)).containsExactly("-U 10", "+U 11", "-U 20", "+U 21");
        }
    }

    @Test
    public void testLookupStateEvictionAndReload() throws Exception {
        FileStoreTable table =
                createTable(CoreOptions.MergeEngine.AGGREGATE, false)
                        .copy(
                                Collections.singletonMap(
                                        CoreOptions.CHANGELOG_ON_READ_LOOKUP_STATE_MAX_SIZE.key(),
                                        "2"));
        write(
                table,
                GenericRow.of(1, 1, 1, 10),
                GenericRow.of(2, 1, 1, 20),
                GenericRow.of(3, 1, 1, 30));

        Path lookupDir = tempPath.resolve("lookup");
        Path reloaded;
        try (IOManager lookupIO = IOManager.create(lookupDir.toString())) {
            try (MergeFileSplitRead read = newReader(table, lookupIO, "v")) {
                Map<Integer, String> dataFiles = new HashMap<>();
                for (DataSplit dataSplit : table.newSnapshotReader().read().dataSplits()) {
                    dataFiles.put(
                            dataSplit.partition().getInt(0),
                            dataSplit.dataFiles().get(0).fileName());
                }
                // Access A, B, A, C. B must be evicted even though A was loaded first.
                assertThat(read(read, append(table, GenericRow.of(1, 1, 2, 1))))
                        .containsExactly("-U 10", "+U 11");
                Path lookupA = findLookupFile(lookupDir, dataFiles.get(1));
                assertThat(read(read, append(table, GenericRow.of(2, 1, 2, 2))))
                        .containsExactly("-U 20", "+U 22");
                Path lookupB = findLookupFile(lookupDir, dataFiles.get(2));
                assertThat(read(read, append(table, GenericRow.of(1, 1, 3, 3))))
                        .containsExactly("-U 11", "+U 14");
                assertThat(read(read, append(table, GenericRow.of(3, 1, 2, 4))))
                        .containsExactly("-U 30", "+U 34");
                assertThat(lookupA).exists();
                assertThat(lookupB).doesNotExist();

                assertThat(read(read, append(table, GenericRow.of(2, 1, 3, 5))))
                        .containsExactly("-U 22", "+U 27");
                reloaded = findLookupFile(lookupDir, dataFiles.get(2));
                assertThat(reloaded).exists().isNotEqualTo(lookupB);
            }
            assertThat(reloaded).doesNotExist();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1})
    public void testRejectNonPositiveLookupStateMaxSize(int maxStates) throws Exception {
        FileStoreTable table =
                createTable(CoreOptions.MergeEngine.DEDUPLICATE, false)
                        .copy(
                                Collections.singletonMap(
                                        CoreOptions.CHANGELOG_ON_READ_LOOKUP_STATE_MAX_SIZE.key(),
                                        String.valueOf(maxStates)));
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
        List<DataSplit> history = table.newSnapshotReader().read().dataSplits();

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
                MergeFileSplitRead read = newReader(table, lookupIO, "v", "seq");
                CloseableIterator<KeyValue> rows =
                        read.createChangelogOnReadReader(split(table, history))
                                .toCloseableIterator()) {
            KeyValue before = rows.next();
            assertThat(before.valueKind()).isEqualTo(RowKind.UPDATE_BEFORE);
            assertThat(before.value().getString(0)).isEqualTo(value);
            assertThat(before.value().getInt(1)).isEqualTo(7);
            try (Stream<Path> files = Files.walk(lookupDir)) {
                assertThat(files)
                        .anyMatch(
                                file ->
                                        file.getFileName()
                                                .toString()
                                                .matches("[0-9a-f]{32}[.]channel"));
            }
            KeyValue after = rows.next();
            assertThat(after.valueKind()).isEqualTo(RowKind.UPDATE_AFTER);
            assertThat(after.value().getString(0)).isEqualTo(value);
            assertThat(after.value().getInt(1)).isEqualTo(15);
            assertThat(rows.hasNext()).isFalse();
        }
    }

    private FileStoreTable createTable(CoreOptions.MergeEngine mergeEngine, boolean forceLookup)
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
                        .option(CoreOptions.FORCE_LOOKUP.key(), String.valueOf(forceLookup))
                        .option(CoreOptions.MERGE_ENGINE.key(), mergeEngine.toString())
                        .option(CoreOptions.SEQUENCE_FIELD.key(), "seq");
        if (mergeEngine == CoreOptions.MergeEngine.AGGREGATE) {
            builder.option("fields.v.aggregate-function", "sum");
        }
        catalog.createTable(identifier(), builder.build(), false);
        return getTableDefault();
    }

    private static MergeFileSplitRead newReader(
            FileStoreTable table, IOManager io, String... projection) {
        return ((PrimaryKeyFileStoreTable) table)
                .store()
                .newRead()
                .withReadKeyType(RowType.of())
                .withReadType(table.rowType().project(projection))
                .withIOManager(io);
    }

    private IncrementalSplit append(FileStoreTable table, InternalRow... rows) throws Exception {
        List<DataSplit> history = table.newSnapshotReader().read().dataSplits();
        write(table, ioManager, rows);
        return split(table, history);
    }

    // The fixture writes one bucket per increment. Supply the history captured before the write.
    private static IncrementalSplit split(FileStoreTable table, List<DataSplit> history) {
        List<DataSplit> deltas =
                table.newSnapshotReader().withMode(ScanMode.DELTA).read().dataSplits();
        assertThat(deltas).hasSize(1);
        DataSplit delta = deltas.get(0);
        List<DataFileMeta> before = new ArrayList<>();
        for (DataSplit previous : history) {
            if (previous.partition().equals(delta.partition())
                    && previous.bucket() == delta.bucket()) {
                before.addAll(previous.dataFiles());
            }
        }
        return new IncrementalSplit(
                delta.snapshotId(),
                delta.partition(),
                delta.bucket(),
                table.coreOptions().bucket(),
                before,
                null,
                delta.dataFiles(),
                null,
                true);
    }

    private static List<String> read(MergeFileSplitRead read, IncrementalSplit split)
            throws IOException {
        List<String> result = new ArrayList<>();
        try (RecordReader<KeyValue> reader = read.createChangelogOnReadReader(split)) {
            reader.forEachRemaining(
                    kv -> result.add(kv.valueKind().shortString() + " " + kv.value().getInt(0)));
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
