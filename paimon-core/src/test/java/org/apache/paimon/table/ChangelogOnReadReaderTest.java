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
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.IncrementalSplit;
import org.apache.paimon.types.DataType;
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
import java.util.List;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.nCopies;
import static java.util.Collections.singletonList;
import static org.apache.paimon.io.DataFileTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for generating changelog from explicit history and delta files. */
public class ChangelogOnReadReaderTest extends TableTestBase {

    private FileStoreTable table;

    @Test
    public void testForceLookupCachedHistoryAfterFileUpgrade() throws Exception {
        createTable(sumSchema().option("force-lookup", "true"));
        DataFileMeta base = writeFile(GenericRow.of(1, 1, 1, 10));
        DataFileMeta delta = writeFile(GenericRow.of(1, 1, 2, 3));
        // L1 already includes the base value, whose file has since been upgraded from L0 to L2.
        DataFileMeta compacted = writeFile(GenericRow.of(1, 1, 2, 13)).upgrade(1);
        DataFileMeta next = writeFile(GenericRow.of(1, 1, 3, 4));
        IncrementalSplit split = split(1, asList(compacted, base.upgrade(2)), singletonList(next));
        try (MergeFileSplitRead reader = newReader("v")) {
            assertThat(read(reader, split(1, singletonList(base), singletonList(delta))))
                    .containsExactly("-U 10", "+U 13");
            assertThat(read(reader, split)).containsExactly("-U 13", "+U 17");
        }
        try (MergeFileSplitRead cold = newReader("v")) {
            assertThat(read(cold, split)).containsExactly("-U 13", "+U 17");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testHistoryWithSameSequence(boolean withSequenceField) throws Exception {
        Schema.Builder schema = schema(DataTypes.INT());
        if (withSequenceField) {
            schema.option("sequence.field", "seq");
        }
        createTable(schema);
        // Independent writers assign equal internal sequences. Reverse add/retract order for key 2.
        CommitMessageImpl first =
                prepareFile(
                        GenericRow.of(1, 1, 1, 10),
                        GenericRow.ofKind(RowKind.UPDATE_BEFORE, 1, 2, 1, 20));
        CommitMessageImpl second =
                prepareFile(
                        GenericRow.ofKind(RowKind.DELETE, 1, 1, 1, 10),
                        GenericRow.ofKind(RowKind.UPDATE_AFTER, 1, 2, 1, 20));
        commitDefault(asList(first, second));
        List<DataFileMeta> history =
                table.newSnapshotReader().read().dataSplits().get(0).dataFiles();
        DataFileMeta delta =
                writeFile(
                        GenericRow.of(1, 1, 2, 11),
                        GenericRow.ofKind(RowKind.DELETE, 1, 2, 2, null),
                        GenericRow.of(1, 3, 2, 30));
        assertThat(history).extracting(DataFileMeta::maxSequenceNumber).containsExactly(1L, 1L);
        assertThat(delta.minSequenceNumber()).isGreaterThan(1L);
        try (MergeFileSplitRead reader = newReader("v")) {
            assertThat(read(reader, split(1, history, singletonList(delta))))
                    .containsExactly("-U 10", "+U 11", "-D 20", "+I 30");
        }
    }

    @Test
    public void testLookupStateEvictionAndReload() throws Exception {
        createTable(
                sumSchema().option(CoreOptions.CHANGELOG_ON_READ_LOOKUP_STATE_MAX_SIZE.key(), "2"));
        IncrementalSplit a = updateSplit(1);
        IncrementalSplit b = updateSplit(2);
        IncrementalSplit c = updateSplit(3);
        Path reloaded;
        try (MergeFileSplitRead reader = newReader("v")) {
            read(reader, a);
            Path lookupA = lookupFile(a);
            read(reader, b);
            Path lookupB = lookupFile(b);
            read(reader, a);
            read(reader, c);
            assertThat(lookupA).exists();
            assertThat(lookupB).doesNotExist();
            List<DataFileMeta> history = asList(b.beforeFiles().get(0), b.afterFiles().get(0));
            DataFileMeta delta = writeFile(GenericRow.of(2, 1, 3, 5));
            assertThat(read(reader, split(2, history, singletonList(delta))))
                    .containsExactly("-U 21", "+U 26");
            reloaded = lookupFile(b);
            assertThat(reloaded).isNotEqualTo(lookupB);
        }
        assertThat(reloaded).doesNotExist();
    }

    private IncrementalSplit updateSplit(int partition) throws Exception {
        int value = partition * 10;
        DataFileMeta before = writeFile(GenericRow.of(partition, 1, 1, value));
        DataFileMeta after = writeFile(GenericRow.of(partition, 1, 2, 1));
        return split(partition, singletonList(before), singletonList(after));
    }

    private Path lookupFile(IncrementalSplit split) throws IOException {
        String dataFile = split.beforeFiles().get(0).fileName();
        try (Stream<Path> paths = Files.walk(tempPath)) {
            return paths.filter(p -> p.getFileName().toString().contains(dataFile + "-"))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Missing lookup file for " + dataFile));
        }
    }

    @ParameterizedTest
    @CsvSource({"min-heap,false", "min-heap,true", "loser-tree,false", "loser-tree,true"})
    public void testProjectedChangelogWithSpill(String sortEngine, boolean forceLookup)
            throws Exception {
        createTable(
                schema(DataTypes.STRING())
                        .option("sequence.field", "seq")
                        .option("force-lookup", String.valueOf(forceLookup))
                        .option("sort-engine", sortEngine)
                        .option("sort-spill-threshold", "1000")
                        .option("lookup.merge-records-threshold", "0")
                        .option("lookup.merge-buffer-size", "4 kb")
                        .option("page-size", "4 kb"));
        BinaryString value = BinaryString.fromString(String.join("", nCopies(1000, "x")));
        List<DataFileMeta> files = new ArrayList<>();
        for (int seq = 0; seq < 16; seq++) {
            files.add(writeFile(GenericRow.of(1, 1, seq, value)));
        }
        IncrementalSplit split = split(1, files.subList(0, 8), files.subList(8, 16));
        Path lookupDir = tempPath.resolve("lookup");
        try (IOManager lookupIO = IOManager.create(lookupDir.toString());
                MergeFileSplitRead reader = newReader("v", "seq").withIOManager(lookupIO);
                CloseableIterator<KeyValue> rows =
                        reader.createChangelogOnReadReader(split).toCloseableIterator()) {
            assertProjectedRow(rows.next(), RowKind.UPDATE_BEFORE, value, 7);
            // Lookup files have a prefix. An anonymous channel is a spill from the per-key buffer.
            try (Stream<Path> paths = Files.walk(lookupDir)) {
                assertThat(paths)
                        .anyMatch(
                                p -> p.getFileName().toString().matches("[0-9a-f]{32}[.]channel"));
            }
            assertProjectedRow(rows.next(), RowKind.UPDATE_AFTER, value, 15);
            assertThat(rows.hasNext()).isFalse();
        }
    }

    private static void assertProjectedRow(
            KeyValue row, RowKind kind, BinaryString value, int seq) {
        assertThat(row.valueKind()).isEqualTo(kind);
        assertThat(row.value().getString(0)).isEqualTo(value);
        assertThat(row.value().getInt(1)).isEqualTo(seq);
    }

    private static Schema.Builder sumSchema() {
        return schema(DataTypes.INT())
                .option("merge-engine", "aggregation")
                .option("fields.v.aggregate-function", "sum")
                .option("sequence.field", "seq");
    }

    private static Schema.Builder schema(DataType valueType) {
        return Schema.newBuilder()
                .column("p", DataTypes.INT())
                .column("k", DataTypes.INT())
                .column("seq", DataTypes.INT())
                .column("v", valueType)
                .primaryKey("p", "k")
                .partitionKeys("p")
                .option("bucket", "1")
                .option("write-only", "true");
    }

    private void createTable(Schema.Builder schema) throws Exception {
        catalog.createTable(identifier(), schema.build(), false);
        table = getTableDefault();
    }

    // Each writer produces one real L0 file for an explicit split, without committing a snapshot.
    private DataFileMeta writeFile(InternalRow... rows) throws Exception {
        List<DataFileMeta> files = prepareFile(rows).newFilesIncrement().newFiles();
        assertThat(files).hasSize(1);
        return files.get(0);
    }

    private CommitMessageImpl prepareFile(InternalRow... rows) throws Exception {
        try (BatchTableWrite write = table.newBatchWriteBuilder().newWrite()) {
            write.withIOManager(ioManager);
            for (InternalRow row : rows) {
                write.write(row);
            }
            List<CommitMessage> messages = write.prepareCommit();
            assertThat(messages).hasSize(1);
            return (CommitMessageImpl) messages.get(0);
        }
    }

    private static IncrementalSplit split(
            int partition, List<DataFileMeta> before, List<DataFileMeta> after) {
        return new IncrementalSplit(1, row(partition), 0, 1, before, null, after, null, true);
    }

    private MergeFileSplitRead newReader(String... projection) {
        return ((PrimaryKeyFileStoreTable) table)
                .store()
                .newRead()
                .withReadKeyType(RowType.of())
                .withReadType(table.rowType().project(projection))
                .withIOManager(ioManager);
    }

    private static List<String> read(MergeFileSplitRead reader, IncrementalSplit split)
            throws IOException {
        List<String> result = new ArrayList<>();
        reader.createChangelogOnReadReader(split)
                .forEachRemaining(
                        kv ->
                                result.add(
                                        kv.valueKind().shortString() + " " + kv.value().getInt(0)));
        return result;
    }
}
