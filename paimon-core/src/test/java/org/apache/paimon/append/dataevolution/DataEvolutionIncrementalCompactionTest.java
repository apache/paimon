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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.BlobData;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.BitmapDeletionVector;
import org.apache.paimon.deletionvectors.append.BaseAppendDeleteFileMaintainer;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.TraceableFileIO;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Behavioral tests for incremental normal-column compaction. */
public class DataEvolutionIncrementalCompactionTest extends TableTestBase {

    private static final int ROW_COUNT = 128;

    @Test
    public void testDroppedNestedLeafPreservesParentNullness() throws Exception {
        catalog.createTable(
                identifier(),
                Schema.newBuilder()
                        .column("id", DataTypes.INT().notNull())
                        .column("cold", DataTypes.STRING().notNull())
                        .column("x", DataTypes.INT())
                        .column(
                                "nest",
                                DataTypes.ROW(
                                        DataTypes.FIELD(0, "a", DataTypes.INT()),
                                        DataTypes.FIELD(1, "b", DataTypes.INT())))
                        .options(schemaDefault().options())
                        .option(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "2")
                        .option(CoreOptions.DATA_EVOLUTION_NESTED_FIELD_ENABLED.key(), "true")
                        .build(),
                false);
        FileStoreTable table = incrementalTable();
        Random random = new Random(42);
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            for (int i = 0; i < ROW_COUNT; i++) {
                write.write(
                        GenericRow.of(
                                i, coldValue(random), i, i % 2 == 0 ? null : GenericRow.of(i, i)));
            }
            commit.commit(write.prepareCommit());
        }
        writeNestedUpdate(table, "nest.b", i -> GenericRow.of(i + 100));
        update(table, "x", 200);
        catalog.alterTable(
                identifier(), SchemaChange.dropColumn(new String[] {"nest", "b"}), false);
        table = incrementalTable();
        List<InternalRow> expected = read(table);
        assertThat(expected)
                .allSatisfy(
                        row -> {
                            assertThat(row.isNullAt(3)).isFalse();
                            if (row.getInt(0) % 2 == 0) {
                                assertThat(row.getRow(3, 1).isNullAt(0)).isTrue();
                            } else {
                                assertThat(row.getRow(3, 1).getInt(0)).isEqualTo(row.getInt(0));
                            }
                        });
        DataEvolutionCompactTask task = plan(table);
        compact(table, task);
        assertThat(read(table)).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testDeepAddedLeafAllowsCompactionOfOlderWholeStruct() throws Exception {
        catalog.createTable(
                identifier(),
                Schema.newBuilder()
                        .column("id", DataTypes.INT().notNull())
                        .column("cold", DataTypes.STRING().notNull())
                        .column("x", DataTypes.INT())
                        .column(
                                "nest",
                                DataTypes.ROW(
                                        DataTypes.FIELD(
                                                0,
                                                "sub",
                                                DataTypes.ROW(
                                                        DataTypes.FIELD(1, "a", DataTypes.INT()))),
                                        DataTypes.FIELD(2, "kept", DataTypes.STRING())))
                        .options(schemaDefault().options())
                        .option(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "2")
                        .option(CoreOptions.DATA_EVOLUTION_NESTED_FIELD_ENABLED.key(), "true")
                        .build(),
                false);
        FileStoreTable table = incrementalTable();
        Random random = new Random(42);
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            for (int i = 0; i < ROW_COUNT; i++) {
                write.write(
                        GenericRow.of(
                                i,
                                coldValue(random),
                                i,
                                GenericRow.of(
                                        GenericRow.of(i), BinaryString.fromString("kept_" + i))));
            }
            commit.commit(write.prepareCommit());
        }
        writeNestedUpdate(table, "nest.sub", i -> GenericRow.of(GenericRow.of(i + 100)));
        update(table, "x", 200);
        catalog.alterTable(
                identifier(),
                SchemaChange.addColumn(
                        new String[] {"nest", "sub", "b"}, DataTypes.INT(), null, null),
                false);
        table = incrementalTable();
        List<InternalRow> expected = read(table);
        DataEvolutionCompactTask task = plan(table);
        compact(table, task);
        assertThat(read(table)).containsExactlyInAnyOrderElementsOf(expected);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testProjectionReadsEachSchemaVersionOnce(boolean renameColumn) throws Exception {
        FileStoreTable table = writeVersions(true);
        if (renameColumn) {
            catalog.alterTable(identifier(), SchemaChange.renameColumn("b", "renamed_b"), false);
            table = incrementalTable();
            update(table, "c", 400);
        }
        DataEvolutionNormalCompactTask task = (DataEvolutionNormalCompactTask) plan(table);
        SchemaCountingFileIO fileIO = new SchemaCountingFileIO();
        FileStoreTable countingTable =
                new AppendOnlyFileStoreTable(
                        fileIO, table.location(), table.schema(), CatalogEnvironment.empty());
        assertThat(task.readWriteType(countingTable).getFieldNames())
                .containsExactly(renameColumn ? "renamed_b" : "b", "c");
        assertThat(fileIO.schemaReads)
                .isEqualTo(
                        (int)
                                task.compactBefore().stream()
                                        .map(DataFileMeta::schemaId)
                                        .distinct()
                                        .count());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testWholeStructWithDottedNamesAfterAddedLeaf(boolean topLevelCollision)
            throws Exception {
        Schema.Builder schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT().notNull())
                        .column("cold", DataTypes.STRING().notNull())
                        .column("x", DataTypes.INT());
        if (topLevelCollision) {
            schema.column("nest.a", DataTypes.INT());
        }
        schema.column(
                        "nest",
                        DataTypes.ROW(
                                DataTypes.FIELD(
                                        0, topLevelCollision ? "a" : "a.b", DataTypes.INT()),
                                DataTypes.FIELD(1, "kept", DataTypes.STRING())))
                .options(schemaDefault().options())
                .option(CoreOptions.METADATA_STATS_MODE.key(), "none")
                .option(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "2")
                .option(CoreOptions.DATA_EVOLUTION_NESTED_FIELD_ENABLED.key(), "true");
        catalog.createTable(identifier(), schema.build(), false);
        FileStoreTable table = incrementalTable();
        Random random = new Random(42);
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            for (int i = 0; i < ROW_COUNT; i++) {
                GenericRow nested = GenericRow.of(i, BinaryString.fromString("kept_" + i));
                write.write(
                        topLevelCollision
                                ? GenericRow.of(i, coldValue(random), i, i + 1000, nested)
                                : GenericRow.of(i, coldValue(random), i, nested));
            }
            commit.commit(write.prepareCommit());
        }
        writeNestedUpdate(
                table,
                "nest",
                i -> GenericRow.of(i + 100, BinaryString.fromString("updated_" + i)));
        update(table, "x", 200);
        catalog.alterTable(
                identifier(),
                SchemaChange.addColumn(new String[] {"nest", "added"}, DataTypes.INT(), null, null),
                false);
        table = incrementalTable();
        List<InternalRow> expected = read(table);
        DataEvolutionCompactTask task = plan(table);
        compact(table, task);
        assertThat(read(table)).containsExactlyInAnyOrderElementsOf(expected);
    }

    private static class SchemaCountingFileIO extends TraceableFileIO {

        private static final long serialVersionUID = 1L;

        private int schemaReads;

        @Override
        public SeekableInputStream newInputStream(Path path) throws IOException {
            if (path.getName().startsWith("schema-")) {
                schemaReads++;
            }
            return super.newInputStream(path);
        }
    }

    @Override
    public Schema schemaDefault() {
        return Schema.newBuilder()
                .column("id", DataTypes.INT().notNull())
                .column("cold", DataTypes.STRING().notNull())
                .column("b", DataTypes.INT())
                .column("c", DataTypes.INT())
                .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                .option(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true")
                .option(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "3")
                .option(CoreOptions.GLOBAL_INDEX_COLUMN_UPDATE_ACTION.key(), "ignore")
                .build();
    }

    @Test
    public void testConcurrentUpdateWinsOverStagedCompaction() throws Exception {
        FileStoreTable table = writeVersions(true);
        DataEvolutionCompactTask task = plan(table);
        Snapshot snapshot = table.snapshotManager().latestSnapshot();
        List<CommitMessage> messages = new ArrayList<>();
        messages.add(task.doCompact(table, "staged-incremental-compact"));
        update(table, "b", 500);
        List<InternalRow> expected = read(table);
        messages.addAll(
                new DataEvolutionCompactionCommitPreparation(table, snapshot).prepare(messages));
        commit(table, messages);
        assertThat(read(table)).containsExactlyInAnyOrderElementsOf(expected);
        assertThat(files(table)).hasSize(3);
    }

    @Test
    public void testRenameAndNewColumnDoNotChangeColumnProviders() throws Exception {
        writeVersions(true);
        catalog.alterTable(
                identifier(),
                Arrays.asList(
                        SchemaChange.renameColumn("b", "renamed_b"),
                        SchemaChange.addColumn("b", DataTypes.INT())),
                false);
        FileStoreTable table = incrementalTable();
        List<InternalRow> expected = read(table);
        DataEvolutionCompactTask task = plan(table);
        compact(table, task);
        assertThat(task.compactAfter().get(0).writeCols()).containsExactly("renamed_b", "c");
        assertThat(read(table)).containsExactlyInAnyOrderElementsOf(expected);
        assertThat(read(table)).allSatisfy(row -> assertThat(row.isNullAt(4)).isTrue());
    }

    @Test
    public void testRetainedAnchorKeepsDeletionVector() throws Exception {
        FileStoreTable table = writeVersions(true);
        DataFileMeta anchor =
                files(table).stream()
                        .min(java.util.Comparator.comparingLong(DataFileMeta::maxSequenceNumber))
                        .get();
        deletePosition(table, anchor, 2);
        List<InternalRow> expected = read(table);
        assertThat(expected).hasSize(ROW_COUNT - 1);
        List<IndexManifestEntry> before = table.store().newIndexFileHandler().scanEntries();
        DataEvolutionCompactTask task = plan(table);
        compact(table, task);
        assertThat(files(table)).contains(anchor).hasSize(2);
        assertThat(task.compactAfter().get(0).rowCount()).isEqualTo(ROW_COUNT);
        assertThat(read(table)).containsExactlyInAnyOrderElementsOf(expected);
        assertThat(table.store().newIndexFileHandler().scanEntries())
                .containsExactlyInAnyOrderElementsOf(before);
    }

    @Test
    public void testSpaceAmplificationFallsBackToFullCompaction() throws Exception {
        FileStoreTable table =
                writeVersions(true)
                        .copy(
                                Collections.singletonMap(
                                        CoreOptions.COMPACTION_MAX_SIZE_AMPLIFICATION_PERCENT.key(),
                                        "0"));
        List<DataFileMeta> input = files(table);
        List<InternalRow> expected = read(table);
        DataEvolutionCompactTask task = plan(table);
        assertThat(task.compactBefore()).containsExactlyInAnyOrderElementsOf(input);
        compact(table, task);
        assertThat(files(table)).hasSize(1);
        assertThat(read(table)).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testDifferentColumnsDoNotCountAsRedundantSpace() throws Exception {
        FileStoreTable table =
                writeBase(true)
                        .copy(
                                Collections.singletonMap(
                                        CoreOptions.COMPACTION_MAX_SIZE_AMPLIFICATION_PERCENT.key(),
                                        "0"));
        update(table, "b", 100);
        update(table, "c", 200);
        DataEvolutionCompactTask task = plan(table);
        assertThat(task.compactBefore()).hasSize(2);
        List<InternalRow> expected = read(table);
        compact(table, task);
        assertThat(files(table)).hasSize(2);
        assertThat(read(table)).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testPartialTaskKeepsItsRangeWhenExecutionOptionsChange() throws Exception {
        FileStoreTable table = writeVersions(true);
        DataEvolutionCompactTask task = plan(table);
        List<InternalRow> expected = read(table);
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.TARGET_FILE_SIZE.key(), "1 b");
        options.put(CoreOptions.TARGET_FILE_ROW_NUM.key(), "1");
        options.put(CoreOptions.DATA_EVOLUTION_COMPACTION_SPLIT_LARGE_FILES.key(), "true");
        options.put(CoreOptions.DATA_EVOLUTION_COMPACTION_INCREMENTAL_ENABLED.key(), "false");
        compact(table.copy(options), task);
        assertThat(task.compactAfter()).hasSize(1);
        assertThat(task.compactAfter().get(0).rowCount()).isEqualTo(ROW_COUNT);
        assertThat(files(table)).hasSize(2);
        assertThat(read(table)).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testDroppedColumnPrefixUsesFullCompaction() throws Exception {
        writeVersions(true);
        catalog.alterTable(
                identifier(),
                Arrays.asList(SchemaChange.dropColumn("b"), SchemaChange.dropColumn("c")),
                false);
        FileStoreTable table = incrementalTable();
        List<InternalRow> expected = read(table);
        DataEvolutionCompactTask task = plan(table);
        assertThat(task.compactBefore()).hasSize(4);
        compact(table, task);
        assertThat(files(table)).hasSize(1);
        assertThat(read(table)).containsExactlyInAnyOrderElementsOf(expected);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testPartitionedCompactionRetainsDedicatedFiles(boolean splitLargeFiles)
            throws Exception {
        catalog.createTable(
                identifier(),
                Schema.newBuilder()
                        .column("p", DataTypes.STRING())
                        .column("id", DataTypes.INT().notNull())
                        .column("cold", DataTypes.STRING().notNull())
                        .column("b", DataTypes.INT())
                        .column("c", DataTypes.INT())
                        .column("blob", DataTypes.BLOB())
                        .partitionKeys("p")
                        .options(schemaDefault().options())
                        .build(),
                false);
        FileStoreTable table = incrementalTable();
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        Random random = new Random(42);
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            for (int i = 0; i < ROW_COUNT; i++) {
                write.write(
                        GenericRow.of(
                                BinaryString.fromString("p0"),
                                i,
                                coldValue(random),
                                i,
                                i,
                                new BlobData(new byte[] {(byte) i})));
            }
            commit.commit(write.prepareCommit());
        }
        List<DataFileMeta> original = files(table);
        assertThat(original).hasSize(2);
        update(table, "b", 100);
        update(table, "c", 200);
        update(table, "b", 300);
        if (splitLargeFiles) {
            Map<String, String> options = new HashMap<>();
            options.put(CoreOptions.DATA_EVOLUTION_COMPACTION_SPLIT_LARGE_FILES.key(), "true");
            options.put(CoreOptions.TARGET_FILE_SIZE.key(), "1 kb");
            table = table.copy(options);
        }
        List<InternalRow> expected = read(table);
        DataEvolutionCompactTask task = plan(table);
        assertThat(task.compactBefore()).hasSize(3).doesNotContainAnyElementsOf(original);
        compact(table, task);
        assertThat(files(table)).hasSize(3).containsAll(original);
        assertThat(task.compactAfter().get(0).writeCols())
                .contains("b", "c")
                .doesNotContain("id", "cold", "blob");
        assertThat(read(table)).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testAdjacentVersionGroupsRemainIndependent() throws Exception {
        FileStoreTable table = writeBase(true);
        appendBase(table);
        List<DataFileMeta> bases = files(table);
        for (long firstRowId : new long[] {0L, ROW_COUNT}) {
            update(table, "b", 100, firstRowId);
            update(table, "c", 200, firstRowId);
            update(table, "b", 300, firstRowId);
        }
        List<InternalRow> expected = read(table);
        Snapshot snapshot = table.snapshotManager().latestSnapshot();
        List<DataEvolutionCompactTask> tasks =
                new DataEvolutionCompactCoordinator(table, false, false, snapshot).plan();
        assertThat(tasks).hasSize(2);
        List<CommitMessage> messages = new ArrayList<>();
        for (DataEvolutionCompactTask task : tasks) {
            assertThat(task.compactBefore()).hasSize(3).doesNotContainAnyElementsOf(bases);
            messages.add(task.doCompact(table, "adjacent-incremental-compact"));
            assertThat(task.compactAfter()).hasSize(1);
            assertThat(task.compactAfter().get(0).rowCount()).isEqualTo(ROW_COUNT);
        }
        messages.addAll(
                new DataEvolutionCompactionCommitPreparation(table, snapshot).prepare(messages));
        commit(table, messages);
        assertThat(files(table)).hasSize(4).containsAll(bases);
        assertThat(read(table))
                .hasSize(2 * ROW_COUNT)
                .containsExactlyInAnyOrderElementsOf(expected);
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "data-evolution.compaction.split-large-files",
                "data-evolution.nested-field.enabled"
            })
    public void testIncrementalSelectionWithNestedAndSplitOptions(String option) throws Exception {
        FileStoreTable table = writeVersions(true).copy(Collections.singletonMap(option, "true"));
        List<InternalRow> expected = read(table);
        DataEvolutionCompactTask task = plan(table);
        assertThat(task.compactBefore()).hasSize(3);
        compact(table, task);
        assertThat(files(table)).hasSize(2);
        assertThat(read(table)).containsExactlyInAnyOrderElementsOf(expected);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testOversizedBaseSplitsAllVersionsBelowFileCountThreshold(boolean updateColumns)
            throws Exception {
        FileStoreTable table = updateColumns ? writeVersions(true) : writeBase(true);
        DataFileMeta anchor =
                files(table).stream()
                        .min(java.util.Comparator.comparingLong(DataFileMeta::maxSequenceNumber))
                        .get();
        deletePosition(table, anchor, 2);
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.DATA_EVOLUTION_COMPACTION_SPLIT_LARGE_FILES.key(), "true");
        options.put(CoreOptions.TARGET_FILE_SIZE.key(), "1 kb");
        options.put(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "10");
        table = table.copy(options);
        List<DataFileMeta> input = files(table);
        List<InternalRow> expected = read(table);
        assertThat(expected).hasSize(ROW_COUNT - 1);
        DataEvolutionCompactTask task = plan(table);
        assertThat(task.compactBefore()).containsExactlyInAnyOrderElementsOf(input);
        compact(table, task);
        assertThat(task.compactAfter().size()).isGreaterThan(1);
        assertThat(files(table)).doesNotContainAnyElementsOf(input);
        assertThat(task.compactAfter().stream().mapToLong(DataFileMeta::rowCount).sum())
                .isEqualTo(ROW_COUNT);
        assertThat(read(table)).containsExactlyInAnyOrderElementsOf(expected);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testNestedLeafProjectionPreservesRetainedSiblings(boolean splitLargeFiles)
            throws Exception {
        FileStoreTable table = writeNestedVersions();
        Map<String, String> options = new HashMap<>();
        options.put(
                CoreOptions.DATA_EVOLUTION_COMPACTION_SPLIT_LARGE_FILES.key(),
                String.valueOf(splitLargeFiles));
        options.put(CoreOptions.COMPACTION_MAX_SIZE_AMPLIFICATION_PERCENT.key(), "0");
        table = table.copy(options);
        List<InternalRow> expected = read(table);
        DataEvolutionCompactTask task = plan(table);
        assertThat(task.compactBefore()).hasSize(2);
        compact(table, task);
        assertThat(task.compactAfter().get(0).writeCols()).containsExactly("nest.a", "nest.b");
        assertThat(files(table)).hasSize(2);
        assertThat(read(table)).containsExactlyInAnyOrderElementsOf(expected);
        // Both update files have a null parent for row 0. The retained sibling keeps the
        // logical struct non-null even when a query projects only an updated leaf.
        RowType projection = table.rowType().projectByPaths(Collections.singletonList("nest.a"));
        int[] projectedRows = {0};
        ReadBuilder readBuilder = table.newReadBuilder().withReadType(projection);
        try (RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan())) {
            reader.forEachRemaining(
                    row -> {
                        assertThat(row.isNullAt(0)).isFalse();
                        int position = projectedRows[0]++;
                        InternalRow nested = row.getRow(0, 1);
                        if (position < 2) {
                            assertThat(nested.isNullAt(0)).isTrue();
                        } else {
                            assertThat(nested.getInt(0)).isEqualTo(position + 100);
                        }
                    });
        }
        assertThat(projectedRows[0]).isEqualTo(ROW_COUNT);
    }

    @Test
    public void testNestedRenameAndSameNameNewLeafKeepFieldIds() throws Exception {
        writeNestedVersions();
        catalog.alterTable(
                identifier(),
                Arrays.asList(
                        SchemaChange.renameColumn(new String[] {"nest", "a"}, "renamed_a"),
                        SchemaChange.addColumn(
                                new String[] {"nest", "a"}, DataTypes.INT(), null, null)),
                false);
        FileStoreTable table = incrementalTable();
        List<InternalRow> expected = read(table);
        DataEvolutionCompactTask task = plan(table);
        assertThat(task.compactBefore()).hasSize(2);
        compact(table, task);
        assertThat(task.compactAfter().get(0).writeCols())
                .containsExactly("nest.renamed_a", "nest.b");
        assertThat(read(table)).containsExactlyInAnyOrderElementsOf(expected);
        assertThat(read(table))
                .allSatisfy(row -> assertThat(row.getRow(2, 4).isNullAt(3)).isTrue());
    }

    @Test
    public void testNestedVersionsSplitTogetherWithOversizedBase() throws Exception {
        FileStoreTable table = writeNestedVersions();
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.DATA_EVOLUTION_COMPACTION_SPLIT_LARGE_FILES.key(), "true");
        options.put(CoreOptions.TARGET_FILE_SIZE.key(), "16 kb");
        options.put(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "10");
        table = table.copy(options);
        List<DataFileMeta> input = files(table);
        List<InternalRow> expected = read(table);
        DataEvolutionCompactTask task = plan(table);
        assertThat(task.compactBefore()).containsExactlyInAnyOrderElementsOf(input);
        compact(table, task);
        assertThat(task.compactAfter().size()).isGreaterThan(1);
        assertThat(files(table)).doesNotContainAnyElementsOf(input);
        assertThat(read(table)).hasSize(ROW_COUNT).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testNestedCollectionColumnGroupsStayWhole() throws Exception {
        catalog.createTable(
                identifier(),
                Schema.newBuilder()
                        .column("id", DataTypes.INT().notNull())
                        .column("cold", DataTypes.STRING().notNull())
                        .column(
                                "nest",
                                DataTypes.ROW(
                                        DataTypes.FIELD(0, "a", DataTypes.INT()),
                                        DataTypes.FIELD(1, "kept", DataTypes.STRING()),
                                        DataTypes.FIELD(
                                                2,
                                                "payload",
                                                DataTypes.ROW(
                                                        DataTypes.FIELD(
                                                                3,
                                                                "values",
                                                                DataTypes.ARRAY(DataTypes.INT())),
                                                        DataTypes.FIELD(
                                                                4,
                                                                "labels",
                                                                DataTypes.MAP(
                                                                        DataTypes.STRING(),
                                                                        DataTypes.INT()))))))
                        .options(schemaDefault().options())
                        .option(CoreOptions.DATA_EVOLUTION_NESTED_FIELD_ENABLED.key(), "true")
                        .build(),
                false);
        FileStoreTable table = incrementalTable();
        Random random = new Random(42);
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            for (int i = 0; i < ROW_COUNT; i++) {
                write.write(
                        GenericRow.of(
                                i,
                                coldValue(random),
                                GenericRow.of(i, BinaryString.fromString("kept_" + i), null)));
            }
            commit.commit(write.prepareCommit());
        }
        writeNestedUpdate(
                table,
                "nest.payload",
                i ->
                        GenericRow.of(
                                GenericRow.of(
                                        new GenericArray(new int[] {i + 100}),
                                        new GenericMap(
                                                Collections.singletonMap(
                                                        BinaryString.fromString("new"),
                                                        i + 200)))));
        writeNestedUpdate(table, "nest.a", i -> GenericRow.of(i + 300));
        table =
                table.copy(
                        Collections.singletonMap(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "2"));
        List<InternalRow> expected = read(table);
        DataEvolutionCompactTask task = plan(table);
        assertThat(task.compactBefore()).hasSize(2);
        compact(table, task);
        assertThat(task.compactAfter().get(0).writeCols())
                .containsExactly("nest.a", "nest.payload");
        assertThat(files(table)).hasSize(2);
        assertThat(read(table)).containsExactlyInAnyOrderElementsOf(expected);
    }

    private FileStoreTable writeNestedVersions() throws Exception {
        catalog.createTable(
                identifier(),
                Schema.newBuilder()
                        .column("id", DataTypes.INT().notNull())
                        .column("cold", DataTypes.STRING().notNull())
                        .column(
                                "nest",
                                DataTypes.ROW(
                                        DataTypes.FIELD(0, "a", DataTypes.INT()),
                                        DataTypes.FIELD(1, "b", DataTypes.INT()),
                                        DataTypes.FIELD(2, "kept", DataTypes.STRING())))
                        .options(schemaDefault().options())
                        .option(CoreOptions.DATA_EVOLUTION_NESTED_FIELD_ENABLED.key(), "true")
                        .build(),
                false);
        FileStoreTable table = incrementalTable();
        Random random = new Random(42);
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            for (int i = 0; i < ROW_COUNT; i++) {
                write.write(
                        GenericRow.of(
                                i,
                                coldValue(random),
                                GenericRow.of(i, i, BinaryString.fromString("kept_" + i))));
            }
            commit.commit(write.prepareCommit());
        }
        for (String path : Arrays.asList("nest.a", "nest.b")) {
            writeNestedUpdate(
                    table, path, i -> i == 0 ? null : GenericRow.of(i == 1 ? null : i + 100));
        }
        return table;
    }

    private void writeNestedUpdate(
            FileStoreTable table, String path, IntFunction<InternalRow> values) throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write =
                        builder.newWrite()
                                .withWriteType(
                                        table.rowType()
                                                .projectByPaths(Collections.singletonList(path)));
                BatchTableCommit commit = builder.newCommit()) {
            for (int i = 0; i < ROW_COUNT; i++) {
                write.write(GenericRow.of(values.apply(i)));
            }
            List<CommitMessage> messages = write.prepareCommit();
            for (CommitMessage message : messages) {
                ((CommitMessageImpl) message)
                        .newFilesIncrement()
                        .newFiles()
                        .replaceAll(file -> file.assignFirstRowId(0L));
            }
            commit.commit(messages);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testRetainBaseAndCompactLatestColumns(boolean incremental) throws Exception {
        FileStoreTable table = writeVersions(incremental);
        List<DataFileMeta> input = files(table);
        DataFileMeta base =
                input.stream()
                        .min(java.util.Comparator.comparingLong(DataFileMeta::maxSequenceNumber))
                        .get();
        List<InternalRow> expected = read(table);
        long historicalSnapshot = table.snapshotManager().latestSnapshotId();
        DataEvolutionCompactTask task = plan(table);
        assertThat(task.type()).isEqualTo(DataEvolutionCompactTask.TaskType.NORMAL);
        assertThat(((DataEvolutionNormalCompactTask) task).partialRewrite()).isEqualTo(incremental);
        if (incremental) {
            assertThat(task.compactBefore()).hasSize(3).doesNotContain(base);
            assertThat(task.compactBefore().stream().mapToLong(DataFileMeta::fileSize).sum())
                    .isLessThan(base.fileSize());
        } else {
            assertThat(task.compactBefore()).containsExactlyInAnyOrderElementsOf(input);
        }

        compact(table, task);
        assertThat(read(table)).containsExactlyInAnyOrderElementsOf(expected);
        assertThat(
                        read(
                                table.copy(
                                        Collections.singletonMap(
                                                CoreOptions.SCAN_SNAPSHOT_ID.key(),
                                                String.valueOf(historicalSnapshot)))))
                .containsExactlyInAnyOrderElementsOf(expected);
        assertThat(task.compactAfter()).hasSize(1);
        DataFileMeta output = task.compactAfter().get(0);
        assertThat(output.nonNullRowIdRange()).isEqualTo(base.nonNullRowIdRange());
        if (incremental) {
            assertThat(files(table)).contains(base).hasSize(2);
            assertThat(output.writeCols()).containsExactly("b", "c");
            assertThat(output.writeColsSequences())
                    .containsExactly(output.maxSequenceNumber(), output.maxSequenceNumber() - 1);
            assertThat(
                            new DataEvolutionCompactCoordinator(
                                            table,
                                            false,
                                            false,
                                            table.snapshotManager().latestSnapshot())
                                    .plan())
                    .isEmpty();
        } else {
            assertThat(files(table)).containsExactly(output);
        }
    }

    private FileStoreTable writeVersions(boolean incremental) throws Exception {
        FileStoreTable table = writeBase(incremental);
        update(table, "b", 100);
        update(table, "c", 200);
        update(table, "b", 300);
        return table;
    }

    private FileStoreTable incrementalTable() throws Exception {
        return getTableDefault()
                .copy(
                        Collections.singletonMap(
                                CoreOptions.DATA_EVOLUTION_COMPACTION_INCREMENTAL_ENABLED.key(),
                                "true"));
    }

    private FileStoreTable writeBase(boolean incremental) throws Exception {
        createTableDefault();
        FileStoreTable table = incremental ? incrementalTable() : getTableDefault();
        appendBase(table);
        return table;
    }

    private void appendBase(FileStoreTable table) throws Exception {
        Random random = new Random(42);
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            for (int i = 0; i < ROW_COUNT; i++) {
                write.write(GenericRow.of(i, coldValue(random), i, i));
            }
            commit.commit(write.prepareCommit());
        }
    }

    private void update(FileStoreTable table, String column, int offset) throws Exception {
        update(table, column, offset, 0L);
    }

    private void update(FileStoreTable table, String column, int offset, long firstRowId)
            throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        List<String> columns = new ArrayList<>(table.partitionKeys());
        columns.add(column);
        try (BatchTableWrite write =
                        builder.newWrite().withWriteType(table.rowType().project(columns));
                BatchTableCommit commit = builder.newCommit()) {
            for (int i = 0; i < ROW_COUNT; i++) {
                Object value = i == 0 ? null : i + offset;
                write.write(
                        columns.size() == 1
                                ? GenericRow.of(value)
                                : GenericRow.of(BinaryString.fromString("p0"), value));
            }
            List<CommitMessage> messages = write.prepareCommit();
            for (CommitMessage message : messages) {
                List<DataFileMeta> newFiles =
                        ((CommitMessageImpl) message).newFilesIncrement().newFiles();
                newFiles.replaceAll(file -> file.assignFirstRowId(firstRowId));
            }
            commit.commit(messages);
        }
    }

    private DataEvolutionCompactTask plan(FileStoreTable table) throws Exception {
        List<DataEvolutionCompactTask> tasks =
                new DataEvolutionCompactCoordinator(
                                table, false, false, table.snapshotManager().latestSnapshot())
                        .plan();
        assertThat(tasks).hasSize(1);
        DataEvolutionCompactTaskSerializer serializer = new DataEvolutionCompactTaskSerializer();
        return serializer.deserialize(serializer.getVersion(), serializer.serialize(tasks.get(0)));
    }

    private void compact(FileStoreTable table, DataEvolutionCompactTask task) throws Exception {
        Snapshot snapshot = table.snapshotManager().latestSnapshot();
        List<CommitMessage> messages = new ArrayList<>();
        messages.add(task.doCompact(table, "incremental-compact"));
        messages.addAll(
                new DataEvolutionCompactionCommitPreparation(table, snapshot).prepare(messages));
        commit(table, messages);
    }

    private void commit(FileStoreTable table, List<CommitMessage> messages) throws Exception {
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(messages);
        }
    }

    private void deletePosition(FileStoreTable table, DataFileMeta anchor, long position)
            throws Exception {
        BaseAppendDeleteFileMaintainer maintainer =
                BaseAppendDeleteFileMaintainer.forUnawareAppend(
                        table.store().newIndexFileHandler(),
                        table.snapshotManager().latestSnapshot(),
                        BinaryRow.EMPTY_ROW);
        BitmapDeletionVector vector = new BitmapDeletionVector();
        vector.delete(position);
        maintainer.notifyNewDeletionVector(anchor.fileName(), vector);
        List<IndexFileMeta> indexes =
                maintainer.persist().stream()
                        .map(IndexManifestEntry::indexFile)
                        .collect(Collectors.toList());
        commit(
                table,
                Collections.singletonList(
                        new CommitMessageImpl(
                                BinaryRow.EMPTY_ROW,
                                0,
                                null,
                                DataIncrement.indexIncrement(indexes),
                                CompactIncrement.emptyIncrement())));
    }

    private List<DataFileMeta> files(FileStoreTable table) {
        return table.store().newScan().plan().files().stream()
                .map(ManifestEntry::file)
                .collect(Collectors.toList());
    }

    private BinaryString coldValue(Random random) {
        StringBuilder cold = new StringBuilder();
        for (int j = 0; j < 128; j++) {
            cold.append(Long.toHexString(random.nextLong()));
        }
        return BinaryString.fromString(cold.toString());
    }
}
