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
import org.apache.paimon.data.BinaryRow;
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
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for generating changelog during reads. */
public class ChangelogOnReadTest extends TableTestBase {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testAggregationProjectionMultipleLevel0AndCacheLifecycle(boolean forceLookup)
            throws Exception {
        FileStoreTable table =
                createTable(
                        CoreOptions.MergeEngine.AGGREGATE,
                        CoreOptions.CHANGELOG_ON_READ_LOOKUP_STATE_MAX_SIZE.defaultValue(),
                        forceLookup);
        write(table, GenericRow.of(1, 1, 1, 1));

        StreamTableScan scan = table.newStreamScan();
        ReadBuilder readBuilder = table.newReadBuilder().withProjection(new int[] {3});
        Path lookupDir = tempPath.resolve("lookup");
        IOManager lookupIO = IOManager.create(lookupDir.toString());
        TableRead read = readBuilder.newRead().withIOManager(lookupIO);
        Path firstLookupFile = null;
        try {
            TableScan.Plan initial = scan.plan();
            String firstDataFile =
                    ((DataSplit) initial.splits().get(0)).dataFiles().get(0).fileName();
            assertThat(read(read, initial)).containsExactly("+I 1");

            write(table, GenericRow.of(1, 1, 2, 2));
            assertThat(readAfterLookupPrepared(read, scan.plan(), lookupDir, firstDataFile))
                    .containsExactly("-U 1", "+U 3");
            firstLookupFile = findLookupFile(lookupDir, firstDataFile);
            long modified = Files.getLastModifiedTime(firstLookupFile).toMillis();

            write(table, GenericRow.of(1, 1, 0, 3));
            assertThat(read(read, scan.plan())).containsExactly("-U 3", "+U 6");
            assertThat(firstLookupFile).exists();
            assertThat(Files.getLastModifiedTime(firstLookupFile).toMillis()).isEqualTo(modified);

            // The temporary history filter for partition 1 must not affect the next delta scan.
            write(table, GenericRow.of(2, 1, 1, 4));
            assertThat(read(read, scan.plan())).containsExactly("+I 4");
        } finally {
            read.close();
            if (firstLookupFile != null) {
                assertThat(firstLookupFile).doesNotExist();
            }
            lookupIO.close();
        }
    }

    @Test
    public void testForceLookupPreservesCompactedHistoryWithProjection() throws Exception {
        FileStoreTable table =
                createTable(
                                CoreOptions.MergeEngine.AGGREGATE,
                                CoreOptions.CHANGELOG_ON_READ_LOOKUP_STATE_MAX_SIZE.defaultValue(),
                                true)
                        .copy(Collections.singletonMap("lookup.merge-records-threshold", "0"));
        write(table, ioManager, GenericRow.of(1, 1, 1, 10));
        DataSplit first = (DataSplit) table.newReadBuilder().newScan().plan().splits().get(0);
        String olderFile = first.dataFiles().get(0).fileName();
        write(table, ioManager, GenericRow.of(1, 1, 2, 13));
        StreamTableScan scan = table.newStreamScan();
        scan.plan();
        write(table, ioManager, GenericRow.of(1, 1, 3, 4));
        IncrementalSplit delta = (IncrementalSplit) scan.plan().splits().get(0);
        assertThat(delta.beforeFiles()).hasSize(2);
        // Model lookup-compacted history: L2 has 10, and the newer L1 value 13 includes it.
        List<DataFileMeta> history =
                delta.beforeFiles().stream()
                        .map(file -> file.upgrade(file.fileName().equals(olderFile) ? 2 : 1))
                        .collect(Collectors.toList());
        IncrementalSplit split =
                new IncrementalSplit(
                        delta.snapshotId(),
                        delta.partition(),
                        delta.bucket(),
                        delta.totalBuckets(),
                        history,
                        null,
                        delta.afterFiles(),
                        null,
                        true);
        try (IOManager lookupIO = IOManager.create(tempPath.resolve("lookup").toString());
                TableRead read =
                        table.newReadBuilder()
                                .withProjection(new int[] {3})
                                .newRead()
                                .withIOManager(lookupIO);
                RecordReader<InternalRow> reader = read.createReader(split)) {
            List<String> result = new ArrayList<>();
            reader.forEachRemaining(
                    row -> result.add(row.getRowKind().shortString() + " " + row.getInt(0)));
            // History is 13, not 10 + 13; then apply the new delta 4.
            assertThat(result).containsExactly("-U 13", "+U 17");
        }
    }

    @Test
    public void testForceLookupCachedHistoryAfterFileUpgrade() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.FILE_FORMAT.key(), "avro");
        options.put(CoreOptions.NUM_LEVELS.key(), "3");
        options.put(CoreOptions.NUM_SORTED_RUNS_COMPACTION_TRIGGER.key(), "10");
        options.put(CoreOptions.LOOKUP_COMPACT.key(), "radical");
        FileStoreTable table =
                createTable(
                                CoreOptions.MergeEngine.AGGREGATE,
                                CoreOptions.CHANGELOG_ON_READ_LOOKUP_STATE_MAX_SIZE.defaultValue(),
                                true)
                        .copy(options);
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
        FileStoreTable table = createTable(CoreOptions.MergeEngine.DEDUPLICATE);
        write(table, GenericRow.of(1, 1, 1, 10));

        StreamTableScan scan = table.newStreamScan();
        ReadBuilder readBuilder = table.newReadBuilder().withProjection(new int[] {3});
        try (IOManager lookupIO = IOManager.create(tempPath.resolve("lookup").toString());
                TableRead read = readBuilder.newRead().withIOManager(lookupIO)) {
            assertThat(read(read, scan.plan())).containsExactly("+I 10");

            write(table, GenericRow.of(1, 1, 3, 30));
            assertThat(read(read, scan.plan())).containsExactly("-U 10", "+U 30");

            write(table, GenericRow.of(1, 1, 4, 40));
            assertThat(read(read, scan.plan())).containsExactly("-U 30", "+U 40");
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
        FileStoreTable table = createTable(CoreOptions.MergeEngine.AGGREGATE, 2);
        write(
                table,
                GenericRow.of(1, 1, 1, 10),
                GenericRow.of(2, 1, 1, 20),
                GenericRow.of(3, 1, 1, 30));

        StreamTableScan scan = table.newStreamScan();
        Path lookupDir = tempPath.resolve("lookup");
        List<Path> cachedFiles = new ArrayList<>();
        try (IOManager lookupIO = IOManager.create(lookupDir.toString())) {
            TableRead read =
                    table.newReadBuilder()
                            .withProjection(new int[] {3})
                            .newRead()
                            .withIOManager(lookupIO);
            try {
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

                // Access A, B, A, C: B is the least recently used state, not A.
                write(table, GenericRow.of(1, 1, 2, 1));
                assertThat(read(read, scan.plan())).containsExactly("-U 10", "+U 11");
                Path lookupA = findLookupFile(lookupDir, dataFiles.get(1));
                cachedFiles.add(lookupA);

                write(table, GenericRow.of(2, 1, 2, 2));
                assertThat(read(read, scan.plan())).containsExactly("-U 20", "+U 22");
                Path lookupB = findLookupFile(lookupDir, dataFiles.get(2));
                cachedFiles.add(lookupB);

                write(table, GenericRow.of(1, 1, 3, 3));
                assertThat(read(read, scan.plan())).containsExactly("-U 11", "+U 14");
                assertThat(lookupA).exists();
                assertThat(lookupB).exists();

                write(table, GenericRow.of(3, 1, 2, 4));
                assertThat(read(read, scan.plan())).containsExactly("-U 30", "+U 34");
                Path lookupC = findLookupFile(lookupDir, dataFiles.get(3));
                cachedFiles.add(lookupC);
                assertThat(lookupA).exists();
                assertThat(lookupB).doesNotExist();
                assertThat(lookupC).exists();

                // Reconstruct the evicted partition from both historical level-0 files.
                write(table, GenericRow.of(2, 1, 3, 5));
                assertThat(read(read, scan.plan())).containsExactly("-U 22", "+U 27");
                Path reloadedLookupB = findLookupFile(lookupDir, dataFiles.get(2));
                cachedFiles.add(reloadedLookupB);
                assertThat(reloadedLookupB).exists().isNotEqualTo(lookupB);
                assertThat(lookupA).doesNotExist();
                assertThat(lookupB).doesNotExist();
                assertThat(lookupC).exists();
            } finally {
                read.close();
                for (Path cachedFile : cachedFiles) {
                    assertThat(cachedFile).doesNotExist();
                }
            }
        }
    }

    @Test
    public void testLookupStateCountBoundedAcrossPartitionsAndBuckets() throws Exception {
        int maxStates = 2;
        FileStoreTable table = createTable(CoreOptions.MergeEngine.DEDUPLICATE, maxStates);
        try (IOManager lookupIO = IOManager.create(tempPath.resolve("lookup").toString())) {
            MergeFileSplitRead read =
                    ((PrimaryKeyFileStoreTable) table).store().newRead().withIOManager(lookupIO);
            Field lookupLevelsField = MergeFileSplitRead.class.getDeclaredField("lookupLevelsMap");
            lookupLevelsField.setAccessible(true);
            Map<?, ?> lookupLevels = (Map<?, ?>) lookupLevelsField.get(read);
            try {
                int statesCreated = 0;
                for (int partition = 0; partition < 32; partition++) {
                    for (int bucket = 0; bucket < 4; bucket++) {
                        // Even states without cached files own locks and must be bounded.
                        IncrementalSplit split =
                                new IncrementalSplit(
                                        1L,
                                        BinaryRow.singleColumn(partition),
                                        bucket,
                                        4,
                                        Collections.emptyList(),
                                        null,
                                        Collections.emptyList(),
                                        null,
                                        true);
                        try (RecordReader<KeyValue> reader =
                                read.createChangelogOnReadReader(split)) {
                            assertThat(reader.readBatch()).isNull();
                        }
                        statesCreated++;
                        assertThat(lookupLevels).hasSize(Math.min(maxStates, statesCreated));
                    }
                }
            } finally {
                read.close();
            }
            assertThat(lookupLevels).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1})
    public void testRejectNonPositiveLookupStateMaxSize(int maxStates) throws Exception {
        FileStoreTable table = createTable(CoreOptions.MergeEngine.DEDUPLICATE, maxStates);
        assertThatThrownBy(() -> ((PrimaryKeyFileStoreTable) table).store().newRead())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(CoreOptions.CHANGELOG_ON_READ_LOOKUP_STATE_MAX_SIZE.key());
    }

    @ParameterizedTest
    @CsvSource({"min-heap,false", "min-heap,true", "loser-tree,false", "loser-tree,true"})
    public void testCloseReleasesMergeSpills(String sortEngine, boolean forceLookup)
            throws Exception {
        testMergeSpillCleanup(sortEngine, forceLookup, false, false);
    }

    @ParameterizedTest
    @CsvSource({"min-heap,false", "min-heap,true", "loser-tree,false", "loser-tree,true"})
    public void testEarlyCloseReleasesMergeSpills(String sortEngine, boolean forceLookup)
            throws Exception {
        testMergeSpillCleanup(sortEngine, forceLookup, true, false);
    }

    @ParameterizedTest
    @CsvSource({"min-heap,false", "min-heap,true", "loser-tree,false", "loser-tree,true"})
    public void testProjectedReaderReleasesMergeSpills(String sortEngine, boolean forceLookup)
            throws Exception {
        testMergeSpillCleanup(sortEngine, forceLookup, false, true);
    }

    private void testMergeSpillCleanup(
            String sortEngine, boolean forceLookup, boolean earlyClose, boolean projected)
            throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("k", DataTypes.INT())
                        .column("seq", DataTypes.INT())
                        .column("v", DataTypes.STRING())
                        .primaryKey("k")
                        .option("bucket", "1")
                        .option("write-only", "true")
                        .option("file.format", "avro")
                        .option("changelog-producer", "on-read")
                        .option("force-lookup", String.valueOf(forceLookup))
                        .option("sequence.field", "seq")
                        .option("sort-engine", sortEngine)
                        .option("sort-spill-threshold", "1000")
                        .option("lookup.merge-records-threshold", "0")
                        .option("lookup.merge-buffer-size", "4 kb")
                        .option("page-size", "4 kb")
                        .build();
        catalog.createTable(identifier(), schema, false);
        FileStoreTable table = getTableDefault();
        char[] chars = new char[1000];
        Arrays.fill(chars, 'x');
        BinaryString value = BinaryString.fromString(new String(chars));
        // Keep enough historical versions to spill historyMerge when force-lookup is enabled.
        for (int i = 0; i < 12; i++) {
            write(table, ioManager, GenericRow.of(1, i, value));
        }
        ReadBuilder builder = table.newReadBuilder();
        if (projected) {
            // Drop the primary key and reorder values so spill serialization must use readType.
            builder.withProjection(new int[] {2, 1});
        }
        StreamTableScan scan = builder.newStreamScan();
        DataSplit initial = (DataSplit) scan.plan().splits().get(0);
        String firstDataFile = initial.dataFiles().get(0).fileName();
        Path lookupDir = tempPath.resolve("lookup");
        try (IOManager lookupIO = IOManager.create(lookupDir.toString())) {
            TableRead read = builder.newRead().withIOManager(lookupIO);
            Path firstLookupFile = null;
            try {
                for (int round = 1; round <= 2; round++) {
                    // Multiple writers contribute overlapping files to one APPEND snapshot.
                    List<CommitMessage> messages = new ArrayList<>();
                    BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
                    for (int i = 0; i < 12; i++) {
                        try (BatchTableWrite write = writeBuilder.newWrite()) {
                            write.withIOManager(ioManager);
                            write.write(GenericRow.of(1, round * 100 + i, value));
                            messages.addAll(write.prepareCommit());
                        }
                    }
                    try (BatchTableCommit commit = writeBuilder.newCommit()) {
                        commit.commit(messages);
                    }
                    // The table's merge-function factory is shared by writes and reads.
                    read.withIOManager(lookupIO);
                    TableScan.Plan plan = scan.plan();
                    assertThat(plan.splits()).hasSize(1);
                    List<String> result = new ArrayList<>();
                    try (RecordReader<InternalRow> reader = read.createReader(plan)) {
                        RecordReader.RecordIterator<InternalRow> batch = reader.readBatch();
                        assertThat(batch).isNotNull();
                        try {
                            InternalRow before = batch.next();
                            assertThat(before).isNotNull();
                            assertThat(before.getFieldCount()).isEqualTo(projected ? 2 : 3);
                            assertThat(before.getString(projected ? 0 : 2)).isEqualTo(value);
                            result.add(before.getRowKind().shortString() + " " + before.getInt(1));
                            assertThat(mergeSpillFiles(lookupDir)).isNotEmpty();
                            if (!earlyClose) {
                                InternalRow row;
                                while ((row = batch.next()) != null) {
                                    result.add(
                                            row.getRowKind().shortString() + " " + row.getInt(1));
                                }
                            }
                        } finally {
                            batch.releaseBatch();
                        }
                        if (!earlyClose) {
                            while ((batch = reader.readBatch()) != null) {
                                try {
                                    InternalRow row;
                                    while ((row = batch.next()) != null) {
                                        result.add(
                                                row.getRowKind().shortString()
                                                        + " "
                                                        + row.getInt(1));
                                    }
                                } finally {
                                    batch.releaseBatch();
                                }
                            }
                        }
                    }
                    int beforeSeq = (round - 1) * 100 + 11;
                    if (earlyClose) {
                        assertThat(result).containsExactly("-U " + beforeSeq);
                    } else {
                        assertThat(result)
                                .containsExactly("-U " + beforeSeq, "+U " + (round * 100 + 11));
                    }
                    assertThat(mergeSpillFiles(lookupDir)).isEmpty();
                    Path cachedFile = findLookupFile(lookupDir, firstDataFile);
                    if (firstLookupFile == null) {
                        firstLookupFile = cachedFile;
                    } else {
                        assertThat(cachedFile).isEqualTo(firstLookupFile);
                    }
                    assertThat(firstLookupFile).exists();
                }
            } finally {
                read.close();
            }
            assertThat(firstLookupFile).doesNotExist();
            assertThat(mergeSpillFiles(lookupDir)).isEmpty();
        }
    }

    private List<Path> mergeSpillFiles(Path lookupDir) throws IOException {
        try (Stream<Path> paths = Files.walk(lookupDir)) {
            // Lookup files have a data-file prefix; only merge buffers use unnamed channels.
            return paths.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().matches("[0-9a-f]{32}[.]channel"))
                    .collect(Collectors.toList());
        }
    }

    private FileStoreTable createTable(CoreOptions.MergeEngine mergeEngine) throws Exception {
        return createTable(
                mergeEngine, CoreOptions.CHANGELOG_ON_READ_LOOKUP_STATE_MAX_SIZE.defaultValue());
    }

    private FileStoreTable createTable(CoreOptions.MergeEngine mergeEngine, int maxStates)
            throws Exception {
        return createTable(mergeEngine, maxStates, false);
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
                        .option(CoreOptions.MERGE_ENGINE.key(), mergeEngine.toString())
                        .option(CoreOptions.FORCE_LOOKUP.key(), String.valueOf(forceLookup))
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

    private List<String> readAfterLookupPrepared(
            TableRead read, TableScan.Plan plan, Path lookupDir, String dataFile)
            throws IOException {
        List<String> result = new ArrayList<>();
        try (RecordReader<InternalRow> reader = read.createReader(plan)) {
            RecordReader.RecordIterator<InternalRow> batch = reader.readBatch();
            assertThat(batch).isNotNull();
            assertThat(findLookupFile(lookupDir, dataFile)).exists();
            consume(batch, result);
            while ((batch = reader.readBatch()) != null) {
                consume(batch, result);
            }
        }
        return result;
    }

    private void consume(RecordReader.RecordIterator<InternalRow> batch, List<String> result)
            throws IOException {
        InternalRow row;
        while ((row = batch.next()) != null) {
            result.add(row.getRowKind().shortString() + " " + row.getInt(0));
        }
        batch.releaseBatch();
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
