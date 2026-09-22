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
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.fileindex.bloomfilter.BloomFilterFileIndexFactory;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.rest.RESTCatalog;
import org.apache.paimon.rest.RESTCatalogInternalOptions;
import org.apache.paimon.rest.RESTCatalogOptions;
import org.apache.paimon.rest.RESTCatalogServer;
import org.apache.paimon.rest.auth.AuthProviderEnum;
import org.apache.paimon.rest.auth.BearTokenAuthProvider;
import org.apache.paimon.rest.responses.ConfigResponse;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.paimon.table.SpecialFields.rowTypeWithRowId;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/** Tests for {@link DataEvolutionEnabler}. */
public class DataEvolutionEnablerTest extends TableTestBase {

    private static final Identifier TABLE = new Identifier("default", "t");
    private static final RowType ROW_TYPE =
            RowType.of(
                    new org.apache.paimon.types.DataType[] {
                        DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING()
                    },
                    new String[] {"id", "v", "pt"});

    // ---------------------------------------------------------------------------------------------
    // conversion
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testEmptyTableOnlyFlipsSchema() throws Exception {
        createTable(Collections.emptyMap());

        DataEvolutionEnabler.Result result = enabler().run(false);

        assertThat(result.skipped).isFalse();
        assertThat(result.schemaBefore).isEqualTo(0L);
        assertThat(result.schemaAfter).isEqualTo(1L);
        assertThat(result.snapshotBefore).isNull();
        assertThat(result.snapshotAfter).isNull();
        assertThat(result.assignedFileCount).isZero();
        assertThat(result.nextRowId).isNull();
        assertThat(result.describe(TABLE)).startsWith("Success.");

        FileStoreTable table = loadTable();
        assertThat(table.coreOptions().rowTrackingEnabled()).isTrue();
        assertThat(table.coreOptions().dataEvolutionEnabled()).isTrue();
        writeRows(table, row(1, "a", "p1"), row(2, "b", "p1"));
        assertThat(rowIdsById(table)).containsEntry(1, 0L).containsEntry(2, 1L);
    }

    @Test
    public void testAssignsContiguousRowIdsInSequenceOrder() throws Exception {
        FileStoreTable table = createTable(Collections.emptyMap());
        writeRows(table, row(1, "a", "p1"), row(2, "b", "p1"));
        writeRows(table, row(3, "c", "p1"));
        writeRows(table, row(4, "d", "p1"), row(5, "e", "p1"), row(6, "f", "p1"));
        long snapshotBefore = table.snapshotManager().latestSnapshotId();

        DataEvolutionEnabler.Result result = enabler().run(false);

        assertThat(result.assignedFileCount).isEqualTo(3L);
        assertThat(result.assignedRowCount).isEqualTo(6L);
        assertThat(result.nextRowId).isEqualTo(6L);
        assertThat(result.snapshotBefore).isEqualTo(snapshotBefore);
        assertThat(result.snapshotAfter).isEqualTo(snapshotBefore + 1);

        table = loadTable();
        Snapshot latest = table.snapshotManager().latestSnapshot();
        assertThat(latest.commitKind()).isEqualTo(Snapshot.CommitKind.OVERWRITE);
        assertThat(latest.nextRowId()).isEqualTo(6L);
        assertThat(latest.schemaId()).isEqualTo(0L);
        assertThat(latest.totalRecordCount()).isEqualTo(6L);

        // files in commit order, ids contiguous, row id = first row id + position
        assertThat(liveFiles(table).stream().map(DataFileMeta::firstRowId))
                .containsExactlyInAnyOrder(0L, 2L, 3L);
        Map<Integer, Long> rowIds = rowIdsById(table);
        for (int id = 1; id <= 6; id++) {
            assertThat(rowIds.get(id)).isEqualTo((long) id - 1);
        }

        // later writes continue after the assigned range
        writeRows(table, row(7, "g", "p1"));
        assertThat(rowIdsById(table)).containsEntry(7, 6L);
        assertThat(table.snapshotManager().latestSnapshot().nextRowId()).isEqualTo(7L);
    }

    @Test
    public void testPartitionsGetContiguousRanges() throws Exception {
        FileStoreTable table = createPartitionedTable(Collections.emptyMap());
        // interleave partitions across commits
        writeRows(table, row(1, "a", "p2"), row(2, "b", "p1"));
        writeRows(table, row(3, "c", "p3"), row(4, "d", "p1"));
        writeRows(table, row(5, "e", "p2"));

        enabler().run(false);
        table = loadTable();

        // each partition holds one contiguous range, partitions in order
        Map<String, List<Long>> rangesByPartition = new TreeMap<>();
        for (DataFileMeta file : liveFiles(table)) {
            String partition = partitionOf(table, file);
            rangesByPartition
                    .computeIfAbsent(partition, k -> new ArrayList<>())
                    .add(file.firstRowId());
            rangesByPartition.get(partition).add(file.firstRowId() + file.rowCount());
        }
        long expectedStart = 0;
        for (List<Long> bounds : rangesByPartition.values()) {
            long min = Collections.min(bounds);
            long max = Collections.max(bounds);
            assertThat(min).isEqualTo(expectedStart);
            expectedStart = max;
        }
        assertThat(expectedStart).isEqualTo(5L);

        // hence reassign_row_id has nothing to do
        DataEvolutionRowIdReassigner.Result reassigned =
                new DataEvolutionRowIdReassigner(table).reassign();
        assertThat(reassigned.reassigned).isFalse();
    }

    @Test
    public void testOnlyManifestsHoldingConvertedFilesAreRewritten() throws Exception {
        FileStoreTable table = createTable(Collections.emptyMap());
        writeRows(table, row(1, "a", "p1"));
        enabler().run(false);
        table = loadTable();
        writeRows(table, row(2, "b", "p1"));
        Set<String> manifestsBefore = manifestNames(table);

        // a second run has nothing to assign: no manifest is touched
        DataEvolutionEnabler.Result result = enabler().run(false);
        assertThat(result.skipped).isTrue();
        assertThat(result.describe(TABLE)).startsWith("Skipped.");
        assertThat(manifestNames(loadTable())).isEqualTo(manifestsBefore);
    }

    @Test
    public void testKeepsEveryOtherFileAttribute() throws Exception {
        // an external data path and an embedded file index are the attributes a manifest rewrite
        // is most likely to lose
        Map<String, String> options = new HashMap<>();
        options.put(
                CoreOptions.DATA_FILE_EXTERNAL_PATHS.key(),
                "file://" + tempPath.resolve("external"));
        options.put(CoreOptions.DATA_FILE_EXTERNAL_PATHS_STRATEGY.key(), "round-robin");
        options.put(
                FileIndexOptions.FILE_INDEX
                        + "."
                        + BloomFilterFileIndexFactory.BLOOM_FILTER
                        + "."
                        + CoreOptions.COLUMNS,
                "id");
        options.put(CoreOptions.FILE_INDEX_IN_MANIFEST_THRESHOLD.key(), "1 MB");
        FileStoreTable table = createTable(options);
        writeRows(table, row(1, "a", "p1"), row(2, "b", "p1"));
        writeRows(table, row(3, "c", "p1"));
        Map<String, DataFileMeta> before = new HashMap<>();
        for (DataFileMeta file : liveFiles(table)) {
            assertThat(file.externalPath()).isPresent();
            assertThat(file.embeddedIndex()).isNotNull();
            assertThat(file.firstRowId()).isNull();
            before.put(file.fileName(), file);
        }
        assertThat(before).hasSize(2);

        enabler().run(false);

        table = loadTable();
        List<DataFileMeta> after = liveFiles(table);
        assertThat(after).hasSize(2);
        for (DataFileMeta file : after) {
            assertThat(file.firstRowId()).isNotNull();
            // the first row id is the only difference
            assertThat(file)
                    .isEqualTo(before.get(file.fileName()).assignFirstRowId(file.firstRowId()));
        }

        // the files are still read from their external path, statistics still prune
        assertThat(valuesById(table)).containsExactly(entry(1, "a"), entry(2, "b"), entry(3, "c"));
        assertNoDuplicateOrMissingRowIds(table, 3);
        List<Split> splits =
                table.newReadBuilder()
                        .withFilter(new PredicateBuilder(ROW_TYPE).equal(0, 3))
                        .newScan()
                        .plan()
                        .splits();
        assertThat(splits).hasSize(1);
        // only the one-row file holds id 3
        assertThat(((DataSplit) splits.get(0)).dataFiles())
                .extracting(DataFileMeta::rowCount)
                .containsExactly(1L);
    }

    @Test
    public void testDryRunChangesNothing() throws Exception {
        FileStoreTable table = createTable(Collections.emptyMap());
        writeRows(table, row(1, "a", "p1"), row(2, "b", "p1"));
        long snapshotBefore = table.snapshotManager().latestSnapshotId();

        DataEvolutionEnabler.Result result = enabler().run(true);

        assertThat(result.dryRun).isTrue();
        assertThat(result.assignedFileCount).isEqualTo(1L);
        assertThat(result.assignedRowCount).isEqualTo(2L);
        assertThat(result.nextRowId).isEqualTo(2L);
        assertThat(result.describe(TABLE)).startsWith("Dry run.");
        table = loadTable();
        assertThat(table.schema().id()).isEqualTo(0L);
        assertThat(table.coreOptions().dataEvolutionEnabled()).isFalse();
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(snapshotBefore);
    }

    @Test
    public void testConvertedTableSupportsPartialColumnWriteAndCompaction() throws Exception {
        FileStoreTable table =
                createTable(
                        Collections.singletonMap(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "2"));
        writeRows(table, row(1, "a", "p1"), row(2, "b", "p1"));
        enabler().run(false);
        table = loadTable();

        // overwrite column v over the converted file's row id range
        RowType writeType = table.rowType().project(Collections.singletonList("v"));
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite().withWriteType(writeType);
                BatchTableCommit commit = builder.newCommit()) {
            write.write(GenericRow.of(BinaryString.fromString("A")));
            write.write(GenericRow.of(BinaryString.fromString("B")));
            List<CommitMessage> messages = write.prepareCommit();
            for (CommitMessage message : messages) {
                CommitMessageImpl impl = (CommitMessageImpl) message;
                List<DataFileMeta> files = new ArrayList<>(impl.newFilesIncrement().newFiles());
                impl.newFilesIncrement().newFiles().clear();
                files.forEach(f -> impl.newFilesIncrement().newFiles().add(f.assignFirstRowId(0)));
            }
            commit.commit(messages);
        }
        assertThat(valuesById(loadTable())).containsEntry(1, "A").containsEntry(2, "B");

        // data-evolution compaction merges the column file into the converted one
        FileStoreTable latest = loadTable();
        DataEvolutionCompactCoordinator coordinator =
                new DataEvolutionCompactCoordinator(
                        latest, false, false, latest.snapshotManager().latestSnapshot());
        List<CommitMessage> compactMessages = new ArrayList<>();
        for (DataEvolutionCompactTask task : coordinator.plan()) {
            compactMessages.add(task.doCompact(latest, "test-compact"));
        }
        assertThat(compactMessages).isNotEmpty();
        latest.newBatchWriteBuilder().newCommit().commit(compactMessages);
        assertThat(valuesById(loadTable())).containsEntry(1, "A").containsEntry(2, "B");
        assertThat(rowIdsById(loadTable())).containsEntry(1, 0L).containsEntry(2, 1L);
    }

    @Test
    public void testConvertsOneBranchOnly() throws Exception {
        FileStoreTable table = createTable(Collections.emptyMap());
        writeRows(table, row(1, "a", "p1"));
        table.createTag("base", 1);
        table.createBranch("b1", "base");
        Identifier branch = new Identifier("default", "t", "b1");

        DataEvolutionEnabler.Result result = new DataEvolutionEnabler(catalog, branch).run(false);

        assertThat(result.assignedFileCount).isEqualTo(1L);
        FileStoreTable branchTable = (FileStoreTable) catalog.getTable(branch);
        assertThat(branchTable.coreOptions().dataEvolutionEnabled()).isTrue();
        assertThat(rowIdsById(branchTable)).containsEntry(1, 0L);
        FileStoreTable main = loadTable();
        assertThat(main.coreOptions().dataEvolutionEnabled()).isFalse();
        assertThat(main.schema().id()).isEqualTo(0L);
    }

    // ---------------------------------------------------------------------------------------------
    // rejections
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testRejectsPrimaryKeyTable() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.PRIMARY_KEY.key(), "id");
        options.put(CoreOptions.BUCKET.key(), "1");
        createTable(options);

        assertThatThrownBy(() -> enabler().run(false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot enable data evolution on table default.t")
                .hasMessageContaining("primary-key");
    }

    @Test
    public void testRejectsBucketedAppendTable() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BUCKET.key(), "2");
        options.put(CoreOptions.BUCKET_KEY.key(), "id");
        createTable(options);

        assertThatThrownBy(() -> enabler().run(false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot enable data evolution on table default.t")
                .hasMessageContaining("bucket = -1");
    }

    @Test
    public void testRejectsIncrementalClustering() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.CLUSTERING_INCREMENTAL.key(), "true");
        options.put(CoreOptions.CLUSTERING_COLUMNS.key(), "id");
        createTable(options);

        assertThatThrownBy(() -> enabler().run(false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot enable data evolution on table default.t")
                .hasMessageContaining("clustering.incremental");
    }

    @Test
    public void testRejectsRESTCatalog() throws Exception {
        String restWarehouse = UUID.randomUUID().toString();
        String token = UUID.randomUUID().toString();
        ConfigResponse config =
                new ConfigResponse(
                        ImmutableMap.of(
                                RESTCatalogInternalOptions.PREFIX.key(),
                                "paimon",
                                CatalogOptions.WAREHOUSE.key(),
                                restWarehouse),
                        ImmutableMap.of());
        RESTCatalogServer server =
                new RESTCatalogServer(
                        tempPath.resolve("rest").toString(),
                        new BearTokenAuthProvider(token),
                        config,
                        restWarehouse);
        server.start();
        try {
            Options options = new Options();
            options.set(CatalogOptions.WAREHOUSE.key(), restWarehouse);
            options.set(RESTCatalogOptions.URI, server.getUrl());
            options.set(RESTCatalogOptions.TOKEN, token);
            options.set(RESTCatalogOptions.TOKEN_PROVIDER, AuthProviderEnum.BEAR.identifier());
            try (RESTCatalog restCatalog = new RESTCatalog(CatalogContext.create(options))) {
                restCatalog.createDatabase(TABLE.getDatabaseName(), true);
                restCatalog.createTable(
                        TABLE, Schema.newBuilder().column("id", DataTypes.INT()).build(), false);

                DataEvolutionEnabler enabler = new DataEvolutionEnabler(restCatalog, TABLE);
                for (boolean dryRun : new boolean[] {true, false}) {
                    assertThatThrownBy(() -> enabler.run(dryRun))
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessage(
                                    "Enabling data evolution on table default.t of a REST catalog "
                                            + "is not supported yet.");
                }
                assertThat(
                                ((FileStoreTable) restCatalog.getTable(TABLE))
                                        .coreOptions()
                                        .rowTrackingEnabled())
                        .isFalse();
            }
        } finally {
            server.shutdown();
        }
    }

    @Test
    public void testAlterTableStillCannotEnableRowTracking() throws Exception {
        FileStoreTable table = createTable(Collections.emptyMap());
        writeRows(table, row(1, "a", "p1"));

        assertThatThrownBy(
                        () ->
                                catalog.alterTable(
                                        TABLE,
                                        SchemaChange.setOption(
                                                CoreOptions.ROW_TRACKING_ENABLED.key(), "true"),
                                        false))
                .hasMessageContaining("Change 'row-tracking.enabled' is not supported yet");
        assertThatThrownBy(
                        () ->
                                catalog.alterTable(
                                        TABLE,
                                        SchemaChange.setOption(
                                                CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true"),
                                        false))
                .hasMessageContaining("Change 'data-evolution.enabled' is not supported yet");
    }

    // ---------------------------------------------------------------------------------------------
    // concurrency
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testConcurrentAppendBeforeRowIdCommitIsAssignedOnRetry() throws Exception {
        FileStoreTable table = createTable(Collections.emptyMap());
        writeRows(table, row(1, "a", "p1"), row(2, "b", "p1"));
        FileStoreTable concurrentWriter = loadTable();
        AtomicInteger commits = new AtomicInteger();

        DataEvolutionEnabler enabler =
                new DataEvolutionEnabler(
                        catalog,
                        TABLE,
                        () -> {
                            // lands between planning and the first commit attempt only
                            if (commits.getAndIncrement() == 0) {
                                writeRowsUnchecked(concurrentWriter, row(3, "c", "p1"));
                            }
                        },
                        () -> {});
        DataEvolutionEnabler.Result result = enabler.run(false);

        assertThat(commits.get()).isEqualTo(2);
        assertThat(result.assignedFileCount).isEqualTo(2L);
        assertThat(result.assignedRowCount).isEqualTo(3L);
        assertThat(result.nextRowId).isEqualTo(3L);
        assertNoDuplicateOrMissingRowIds(loadTable(), 3);
    }

    @Test
    public void testConcurrentCompactionBeforeRowIdCommitIsAssignedOnRetry() throws Exception {
        FileStoreTable table = createTable(Collections.emptyMap());
        writeRows(table, row(1, "a", "p1"));
        writeRows(table, row(2, "b", "p1"));
        FileStoreTable compactor = loadTable();
        AtomicInteger commits = new AtomicInteger();

        DataEvolutionEnabler enabler =
                new DataEvolutionEnabler(
                        catalog,
                        TABLE,
                        () -> {
                            if (commits.getAndIncrement() == 0) {
                                try {
                                    compact(compactor);
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        },
                        () -> {});
        DataEvolutionEnabler.Result result = enabler.run(false);

        assertThat(commits.get()).isEqualTo(2);
        // the compacted file replaced the two originals
        assertThat(result.assignedFileCount).isEqualTo(1L);
        assertThat(result.assignedRowCount).isEqualTo(2L);
        assertNoDuplicateOrMissingRowIds(loadTable(), 2);
    }

    @Test
    public void testStaleWriterCommitBetweenStepsIsRepaired() throws Exception {
        FileStoreTable table = createTable(Collections.emptyMap());
        writeRows(table, row(1, "a", "p1"));
        FileStoreTable staleWriter = loadTable();

        DataEvolutionEnabler enabler =
                new DataEvolutionEnabler(
                        catalog,
                        TABLE,
                        () -> {},
                        // after the row ids were committed, before the schema changes
                        () -> writeRowsUnchecked(staleWriter, row(2, "b", "p1")));
        DataEvolutionEnabler.Result result = enabler.run(false);

        assertThat(result.assignedFileCount).isEqualTo(2L);
        assertThat(result.nextRowId).isEqualTo(2L);
        table = loadTable();
        assertThat(table.coreOptions().dataEvolutionEnabled()).isTrue();
        assertNoDuplicateOrMissingRowIds(table, 2);
        assertThat(table.snapshotManager().latestSnapshot().schemaId()).isEqualTo(1L);

        // and from now on the stale writer is refused
        assertThatThrownBy(() -> writeRows(staleWriter, row(3, "c", "p1")))
                .hasStackTraceContaining("enabled row tracking in schema 1");
    }

    @Test
    public void testGivesUpWhenSnapshotsKeepMoving() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.COMMIT_MAX_RETRIES.key(), "1");
        FileStoreTable table = createTable(options);
        writeRows(table, row(1, "a", "p1"));
        FileStoreTable concurrentWriter = loadTable();
        AtomicInteger commits = new AtomicInteger();

        DataEvolutionEnabler enabler =
                new DataEvolutionEnabler(
                        catalog,
                        TABLE,
                        () ->
                                writeRowsUnchecked(
                                        concurrentWriter,
                                        row(commits.incrementAndGet() + 1, "x", "p1")),
                        () -> {});

        assertThatThrownBy(() -> enabler.run(false))
                .hasMessageContaining("Failed to assign row ids to table default.t");
        // nothing changed
        table = loadTable();
        assertThat(table.schema().id()).isEqualTo(0L);
        assertThat(table.coreOptions().dataEvolutionEnabled()).isFalse();
        assertThat(liveFiles(table)).allMatch(file -> file.firstRowId() == null);
    }

    // ---------------------------------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------------------------------

    private DataEvolutionEnabler enabler() {
        return new DataEvolutionEnabler(catalog, TABLE);
    }

    private FileStoreTable createTable(Map<String, String> options) throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("v", DataTypes.STRING())
                        .column("pt", DataTypes.STRING())
                        .options(options)
                        .build();
        catalog.createTable(TABLE, schema, false);
        return loadTable();
    }

    private FileStoreTable createPartitionedTable(Map<String, String> options) throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("v", DataTypes.STRING())
                        .column("pt", DataTypes.STRING())
                        .partitionKeys("pt")
                        .options(options)
                        .build();
        catalog.createTable(TABLE, schema, false);
        return loadTable();
    }

    private FileStoreTable loadTable() throws Exception {
        return (FileStoreTable) catalog.getTable(TABLE);
    }

    private static GenericRow row(int id, String v, String pt) {
        return GenericRow.of(id, BinaryString.fromString(v), BinaryString.fromString(pt));
    }

    private void writeRows(FileStoreTable table, GenericRow... rows) throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            for (GenericRow row : rows) {
                write.write(row);
            }
            commit.commit(write.prepareCommit());
        }
    }

    private void writeRowsUnchecked(FileStoreTable table, GenericRow... rows) {
        try {
            writeRows(table, rows);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** Rewrites every live file of the (unpartitioned) table into one, as a compaction does. */
    private void compact(FileStoreTable table) throws Exception {
        List<DataFileMeta> before = liveFiles(table);
        List<InternalRow> rows = read(table, (RowType) null);
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        List<DataFileMeta> after = new ArrayList<>();
        try (BatchTableWrite write = writeBuilder.newWrite()) {
            for (InternalRow row : rows) {
                write.write(row);
            }
            for (CommitMessage message : write.prepareCommit()) {
                after.addAll(((CommitMessageImpl) message).newFilesIncrement().newFiles());
            }
        }
        CommitMessage compaction =
                new CommitMessageImpl(
                        org.apache.paimon.data.BinaryRow.EMPTY_ROW,
                        0,
                        table.coreOptions().bucket(),
                        org.apache.paimon.io.DataIncrement.emptyIncrement(),
                        new org.apache.paimon.io.CompactIncrement(
                                before, after, Collections.emptyList()));
        try (BatchTableCommit commit = writeBuilder.newCommit()) {
            commit.commit(Collections.singletonList(compaction));
        }
    }

    private List<DataFileMeta> liveFiles(FileStoreTable table) {
        List<DataFileMeta> files = new ArrayList<>();
        table.newSnapshotReader().readFileIterator().forEachRemaining(e -> files.add(e.file()));
        return files;
    }

    private Set<String> manifestNames(FileStoreTable table) {
        Snapshot latest = table.snapshotManager().latestSnapshot();
        return table.store().manifestListFactory().create().readDataManifests(latest).stream()
                .map(ManifestFileMeta::fileName)
                .collect(Collectors.toSet());
    }

    private String partitionOf(FileStoreTable table, DataFileMeta file) {
        // the partition of a file is not on the meta; read it through the manifest entries
        List<String> partitions = new ArrayList<>();
        table.newSnapshotReader()
                .readFileIterator()
                .forEachRemaining(
                        entry -> {
                            if (entry.file().fileName().equals(file.fileName())) {
                                partitions.add(entry.partition().getString(0).toString());
                            }
                        });
        assertThat(partitions).hasSize(1);
        return partitions.get(0);
    }

    private Map<Integer, Long> rowIdsById(FileStoreTable table) throws Exception {
        Map<Integer, Long> result = new HashMap<>();
        for (InternalRow row : read(table, rowTypeWithRowId(ROW_TYPE))) {
            result.put(row.getInt(0), row.isNullAt(3) ? null : row.getLong(3));
        }
        return result;
    }

    private Map<Integer, String> valuesById(FileStoreTable table) throws Exception {
        Map<Integer, String> result = new HashMap<>();
        for (InternalRow row : read(table, ROW_TYPE)) {
            result.put(row.getInt(0), row.getString(1).toString());
        }
        return result;
    }

    private void assertNoDuplicateOrMissingRowIds(FileStoreTable table, int rowCount)
            throws Exception {
        Map<Integer, Long> rowIds = rowIdsById(table);
        assertThat(rowIds).hasSize(rowCount);
        assertThat(rowIds.values()).doesNotContainNull();
        assertThat(rowIds.values().stream().sorted().collect(Collectors.toList()))
                .isEqualTo(
                        java.util.stream.LongStream.range(0, rowCount)
                                .boxed()
                                .collect(Collectors.toList()));
        assertThat(liveFiles(table)).allMatch(file -> file.firstRowId() != null);
    }

    private List<InternalRow> read(FileStoreTable table, @Nullable RowType readType)
            throws Exception {
        ReadBuilder readBuilder = table.newReadBuilder();
        if (readType != null) {
            readBuilder.withReadType(readType);
        }
        TableRead read = readBuilder.newRead();
        InternalRowSerializer serializer =
                new InternalRowSerializer(readType == null ? table.rowType() : readType);
        List<InternalRow> rows = new ArrayList<>();
        for (Split split : readBuilder.newScan().plan().splits()) {
            try (RecordReader<InternalRow> reader = read.createReader(split)) {
                reader.forEachRemaining(row -> rows.add(serializer.copy(row)));
            }
        }
        return rows;
    }
}
