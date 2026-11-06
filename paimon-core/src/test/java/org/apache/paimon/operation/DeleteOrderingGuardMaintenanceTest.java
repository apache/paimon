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
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.PositionOutputStreamWrapper;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.index.DeleteOrderingGuardReaderFactory;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.mergetree.compact.DeleteOrderingGuardReader;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.partition.PartitionExpireStrategy;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.partition.PartitionUpdateTimeExpireStrategy;
import org.apache.paimon.partition.PartitionValuesTimeExpireStrategy;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.ExpireSnapshotsImpl;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.PrimaryKeyFileStoreTable;
import org.apache.paimon.table.PrimaryKeyTableUtils;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.TraceableFileIO;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.paimon.index.DeleteOrderingGuardFile.DELETE_ORDERING_GUARD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Native maintenance must preserve hidden ordering state and partition lifecycle semantics. */
class DeleteOrderingGuardMaintenanceTest {

    private static final String OLD = "2020-01-01";
    private static final String RECENT = "2099-01-01";
    private static final String USER = "maintenance";

    @TempDir java.nio.file.Path tempDir;
    private final List<IOManager> ioManagers = new ArrayList<>();

    @AfterEach
    void close() throws Exception {
        IOUtils.closeAll(ioManagers);
    }

    @Test
    void testValuesTimeExpirationDiscoversGuardOnlyPartitionsAndFencesWriters() throws Exception {
        PrimaryKeyFileStoreTable table = createTable(Collections.emptyMap());
        append(table, 0, row(RowKind.DELETE, OLD, 1, 4), row(RowKind.DELETE, RECENT, 2, 4));
        compact(table, 1);
        Snapshot original = table.snapshotManager().latestSnapshot();
        assertThat(table.store().newScan().readPartitionEntries()).isEmpty();
        assertThat(
                        table.store()
                                .newScan()
                                .withDeleteOrderingGuardPartitions()
                                .readPartitionEntries())
                .hasSize(2)
                .allSatisfy(entry -> assertThat(entry.fileCount()).isZero());
        List<CommitMessage> stale = prepare(table, 2, row(RowKind.INSERT, OLD, 1, 2));
        List<CommitMessage> unaffected = prepare(table, 3, row(RowKind.INSERT, RECENT, 2, 2));
        assertThat(
                        expire(
                                table,
                                new PartitionValuesTimeExpireStrategy(
                                        table.coreOptions(), table.schema().logicalPartitionType()),
                                LocalDateTime.of(2026, 11, 2, 0, 0),
                                4))
                .containsExactly(Collections.singletonMap("pt", OLD));
        assertThat(guards(table))
                .allSatisfy(entry -> assertThat(entry.partition()).isEqualTo(partition(RECENT)));
        assertThatThrownBy(() -> commit(table, 5, stale))
                .hasMessageContaining("generation changed");
        commit(table, 6, unaffected);
        assertThat(read(table)).isEmpty();
        assertThat(
                        table.store()
                                .newScan()
                                .withSnapshot(original)
                                .withDeleteOrderingGuardPartitions()
                                .withPartitionFilter(
                                        PartitionPredicate.fromMap(
                                                table.schema().logicalPartitionType(),
                                                Collections.singletonMap("pt", OLD),
                                                table.coreOptions().partitionDefaultName()))
                                .readPartitionEntries())
                .extracting(PartitionEntry::partition)
                .containsExactly(partition(OLD));
        append(table, 7, row(RowKind.INSERT, OLD, 1, 2));
        assertThat(read(table)).hasSize(1);
    }

    @Test
    void testConsolidationDoesNotPostponeUpdateTimeExpiration() throws Exception {
        PrimaryKeyFileStoreTable table = createTable(Collections.emptyMap());
        append(table, 0, row(RowKind.DELETE, OLD, 1, 4));
        compact(table, 1);
        append(table, 2, row(RowKind.DELETE, OLD, 1, 7));
        compact(table, 3);
        assertThat(guards(table)).hasSize(2);
        long activity =
                DeleteOrderingGuardPartitions.read(table.snapshotManager().latestSnapshot())
                        .get(partition(OLD))
                        .lastFileCreationTime();
        compact(table, 4);
        assertThat(guards(table)).hasSize(1);
        assertThat(
                        DeleteOrderingGuardPartitions.read(table.snapshotManager().latestSnapshot())
                                .get(partition(OLD))
                                .lastFileCreationTime())
                .isEqualTo(activity);
        LocalDateTime now =
                LocalDateTime.ofInstant(Instant.ofEpochMilli(activity), ZoneId.systemDefault())
                        .plusDays(2);
        assertThat(
                        expire(
                                table,
                                new PartitionUpdateTimeExpireStrategy(
                                        table.coreOptions(), table.schema().logicalPartitionType()),
                                now,
                                5))
                .containsExactly(Collections.singletonMap("pt", OLD));
        assertThat(guards(table)).isEmpty();
        assertThat(DeleteOrderingGuardPartitions.read(table.snapshotManager().latestSnapshot()))
                .isEmpty();
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 5})
    void testRescaleGuardOnlyPartitionsPreservesBoundsAndInternalWatermark(int buckets)
            throws Exception {
        PrimaryKeyFileStoreTable table = createTable(Collections.emptyMap());
        for (int key = 0; key < 12; key++) {
            append(table, key, row(RowKind.DELETE, OLD, key, 4));
        }
        compact(table, 12);
        List<String> bounds = bounds(table);
        List<CommitMessage> stale = prepare(table, 13, row(RowKind.INSERT, OLD, 1, 2));
        long sequence =
                SequenceSnapshotProperties.guardMaxSequenceNumber(
                                table.snapshotManager().latestSnapshot())
                        .getAsLong();
        table = setOption(table, "bucket", String.valueOf(buckets));
        PrimaryKeyFileStoreTable rescaled = table;
        assertThatThrownBy(() -> prepare(rescaled, 14, row(RowKind.INSERT, OLD, 1, 2)))
                .hasStackTraceContaining("previous bucket num is 2")
                .hasStackTraceContaining("sys.rescale");
        DeleteOrderingGuardRescale migration =
                new DeleteOrderingGuardRescale(
                        table, table.snapshotManager().latestSnapshot(), buckets, null);
        assertThat(migration.plan())
                .allSatisfy(
                        split -> {
                            assertThat(split.dataFiles()).isEmpty();
                            assertThat(split.totalBuckets()).isEqualTo(2);
                        });
        migration.commit("rescale", 0, rewrite(migration));
        assertThat(bounds(table)).containsExactlyInAnyOrderElementsOf(bounds);
        assertThat(read(table)).isEmpty();
        assertThat(table.snapshotManager().latestSnapshot().commitKind())
                .isEqualTo(Snapshot.CommitKind.OVERWRITE);
        assertThat(
                        SequenceSnapshotProperties.guardMaxSequenceNumber(
                                        table.snapshotManager().latestSnapshot())
                                .getAsLong())
                .isGreaterThanOrEqualTo(sequence);
        assertThat(DeleteOrderingGuardPartitions.read(table.snapshotManager().latestSnapshot()))
                .allSatisfy(
                        (p, metadata) -> assertThat(metadata.totalBuckets()).isEqualTo(buckets));
        assertThatThrownBy(() -> commit(rescaled, 15, stale))
                .hasMessageContaining("generation changed");
        append(table, 16, row(RowKind.INSERT, OLD, 1, 2));
        assertThat(read(table)).isEmpty();
        // Equal source versions still use the preserved internal sequence watermark.
        append(table, 17, row(RowKind.INSERT, OLD, 1, 4));
        assertThat(read(table))
                .singleElement()
                .satisfies(r -> assertThat(r.getInt(2)).isEqualTo(4));
    }

    @ParameterizedTest
    @ValueSource(strings = {"default", "mod", "hive"})
    void testRescaleMixedDataKeepsNativeSequencesAndUnselectedPartitions(String bucketFunction)
            throws Exception {
        PrimaryKeyFileStoreTable table =
                createTable(Collections.singletonMap("bucket-function.type", bucketFunction));
        append(table, 0, row(RowKind.DELETE, OLD, 1, 7), row(RowKind.DELETE, RECENT, 3, 7));
        compact(table, 1);
        append(table, 2, row(RowKind.INSERT, OLD, 1, 2), row(RowKind.INSERT, OLD, 2, 3));
        List<String> physical = physicalRecords(table, partition(OLD));
        List<IndexManifestEntry> unaffected =
                guards(table).stream()
                        .filter(entry -> entry.partition().equals(partition(RECENT)))
                        .collect(Collectors.toList());
        table = setOption(table, "bucket", "5");
        PartitionPredicate selected =
                PartitionPredicate.fromMap(
                        table.schema().logicalPartitionType(),
                        Collections.singletonMap("pt", OLD),
                        table.coreOptions().partitionDefaultName());
        DeleteOrderingGuardRescale migration =
                new DeleteOrderingGuardRescale(
                        table, table.snapshotManager().latestSnapshot(), 5, selected);
        migration.commit("rescale", 0, rewrite(migration));
        assertThat(physicalRecords(table, partition(OLD)))
                .containsExactlyInAnyOrderElementsOf(physical);
        assertThat(guards(table)).containsAll(unaffected);
        assertThat(
                        DeleteOrderingGuardPartitions.read(table.snapshotManager().latestSnapshot())
                                .get(partition(RECENT))
                                .totalBuckets())
                .isEqualTo(2);
        assertThat(read(table))
                .singleElement()
                .satisfies(r -> assertThat(r.getInt(1)).isEqualTo(2));
        append(table, 3, row(RowKind.INSERT, OLD, 1, 4));
        assertThat(read(table)).hasSize(1);
        append(table, 4, row(RowKind.INSERT, OLD, 1, 8));
        assertThat(read(table)).hasSize(2);
    }

    @Test
    void testRescalePreservesSnapshotOrderingOfAppendAndCompactedRecords() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("sequence.snapshot-ordering", "true");
        options.put("write-only", "true");
        PrimaryKeyFileStoreTable table =
                createTable(
                        options,
                        RowType.builder()
                                .field("pt", DataTypes.STRING())
                                .field("k", DataTypes.INT())
                                .field("version", DataTypes.INT())
                                .field("payload", DataTypes.STRING())
                                .build());
        append(table, 0, row(RowKind.DELETE, OLD, 1, 9), row(RowKind.INSERT, OLD, 2, 100));
        compact(
                (PrimaryKeyFileStoreTable)
                        table.copy(Collections.singletonMap("write-only", "false")),
                1);
        assertThat(guards(table)).isNotEmpty();
        // APPEND metadata supplies the snapshot sequence; compacted records carry it physically.
        append(table, 2, row(RowKind.INSERT, OLD, 2, 1));
        List<String> records = physicalRecords(table, partition(OLD));
        table = setOption(table, "bucket", "5");
        DeleteOrderingGuardRescale migration =
                new DeleteOrderingGuardRescale(
                        table, table.snapshotManager().latestSnapshot(), 5, null);
        migration.commit("rescale", 0, rewrite(migration));
        assertThat(physicalRecords(table, partition(OLD)))
                .containsExactlyInAnyOrderElementsOf(records);
        assertThat(read(table))
                .singleElement()
                .satisfies(row -> assertThat(row.getInt(2)).isEqualTo(1));
        append(table, 3, row(RowKind.INSERT, OLD, 1, 1));
        assertThat(read(table))
                .hasSize(2)
                .allSatisfy(row -> assertThat(row.getInt(2)).isEqualTo(1));
        assertThat(read(table)).extracting(row -> row.getInt(1)).containsExactlyInAnyOrder(1, 2);
        compact(
                (PrimaryKeyFileStoreTable)
                        table.copy(Collections.singletonMap("write-only", "false")),
                4);
        assertThat(read(table))
                .hasSize(2)
                .allSatisfy(row -> assertThat(row.getInt(2)).isEqualTo(1));
    }

    @Test
    void testRescalePreservesIndependentPartialUpdateGroupsWithCaptureDisabled() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("merge-engine", "partial-update");
        options.put("fields.seqA.sequence-group", "a");
        options.put("fields.seqB.sequence-group", "b");
        RowType type =
                RowType.builder()
                        .field("pt", DataTypes.STRING())
                        .field("k", DataTypes.INT())
                        .field("seqA", DataTypes.INT())
                        .field("a", DataTypes.STRING())
                        .field("seqB", DataTypes.INT())
                        .field("b", DataTypes.STRING())
                        .field("ungrouped", DataTypes.STRING())
                        .build();
        PrimaryKeyFileStoreTable table = createTable(options, type);
        append(table, 0, GenericRow.ofKind(RowKind.DELETE, str(OLD), 1, 4, null, 7, null, null));
        compact(table, 1);
        append(table, 2, GenericRow.ofKind(RowKind.DELETE, str(OLD), 1, 6, null, 3, null, null));
        compact(table, 3);
        table = setOption(table, "delete-ordering-guards.enabled", "false");
        table = setOption(table, "bucket", "5");
        DeleteOrderingGuardRescale migration =
                new DeleteOrderingGuardRescale(
                        table, table.snapshotManager().latestSnapshot(), 5, null);
        migration.commit("rescale", 0, rewrite(migration));
        append(table, 4, GenericRow.of(str(OLD), 1, 5, str("stale"), 8, str("fresh"), str("raw")));
        assertThat(read(table))
                .singleElement()
                .satisfies(
                        r -> {
                            assertThat(r.getInt(2)).isEqualTo(6);
                            assertThat(r.isNullAt(3)).isTrue();
                            assertThat(r.getInt(4)).isEqualTo(8);
                            assertThat(r.getString(5).toString()).isEqualTo("fresh");
                            assertThat(r.getString(6).toString()).isEqualTo("raw");
                        });
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testConcurrentChangesRejectRescaleAndCleanUnpublishedOutputs(boolean schemaChange)
            throws Exception {
        PrimaryKeyFileStoreTable table = createTable(Collections.emptyMap());
        append(table, 0, row(RowKind.DELETE, OLD, 1, 4), row(RowKind.INSERT, OLD, 2, 3));
        compact(table, 1);
        DeleteOrderingGuardRescale migration =
                new DeleteOrderingGuardRescale(
                        table, table.snapshotManager().latestSnapshot(), 5, null);
        List<String> original = files();
        List<CommitMessage> messages = rewrite(migration);
        List<String> outputs = files();
        outputs.removeAll(original);
        assertThat(outputs).isNotEmpty();
        if (schemaChange) {
            setOption(table, "write-buffer-size", "32 mb");
        } else {
            append(table, 2, row(RowKind.INSERT, OLD, 3, 8));
        }
        assertThatThrownBy(() -> migration.commit("rescale", 0, messages))
                .hasMessageContaining("changed during delete ordering guard rescaling");
        for (String output : outputs) {
            assertThat(java.nio.file.Paths.get(output)).doesNotExist();
        }
        assertThat(guards(table)).isNotEmpty();
        assertThat(read(table)).hasSize(schemaChange ? 1 : 2);
    }

    @Test
    void testGuardFinalizationFailureAbortsAlreadyWrittenRescaleData() throws Exception {
        PrimaryKeyFileStoreTable table = createTable(Collections.emptyMap());
        append(table, 0, row(RowKind.DELETE, OLD, 1, 4), row(RowKind.INSERT, OLD, 2, 3));
        compact(table, 1);
        List<String> original = files();
        TraceableFileIO failing =
                new TraceableFileIO() {
                    @Override
                    public PositionOutputStream newOutputStream(Path path, boolean overwrite)
                            throws IOException {
                        PositionOutputStream stream = super.newOutputStream(path, overwrite);
                        if (!path.getName().startsWith("index-")) {
                            return stream;
                        }
                        return new PositionOutputStreamWrapper(stream) {
                            @Override
                            public void close() throws IOException {
                                out.close();
                                throw new IOException(
                                        "Injected rescale guard finalization failure");
                            }
                        };
                    }
                };
        PrimaryKeyFileStoreTable faultTable =
                (PrimaryKeyFileStoreTable)
                        FileStoreTableFactory.create(failing, table.location(), table.schema());
        DeleteOrderingGuardRescale migration =
                new DeleteOrderingGuardRescale(
                        faultTable, table.snapshotManager().latestSnapshot(), 5, null);
        assertThatThrownBy(() -> rewrite(migration))
                .hasStackTraceContaining("Injected rescale guard finalization failure");
        assertThat(files()).containsExactlyInAnyOrderElementsOf(original);
        assertThat(read(table)).hasSize(1);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testOtherBranchRetainsGuardsAfterMainConsolidationAndTagDeletion(boolean snapshotOnly)
            throws Exception {
        PrimaryKeyFileStoreTable table = createTable(Collections.emptyMap());
        append(table, 0, row(RowKind.DELETE, OLD, 1, 4), row(RowKind.INSERT, OLD, 2, 3));
        compact(table, 1);
        List<IndexManifestEntry> original = guards(table);
        table.createTag("base", table.snapshotManager().latestSnapshotId());
        table.createBranch("audit", "base");
        PrimaryKeyFileStoreTable branch = (PrimaryKeyFileStoreTable) table.switchToBranch("audit");
        if (snapshotOnly) {
            branch.deleteTag("base");
        }
        append(table, 2, row(RowKind.DELETE, OLD, 1, 7), row(RowKind.DELETE, OLD, 2, 7));
        compact(table, 3);
        compact(table, 4);
        expireOldSnapshots(table);
        table.deleteTag("base");
        new LocalOrphanFilesClean(table, Long.MAX_VALUE).clean();
        for (IndexManifestEntry guard : original) {
            assertThat(
                            table.fileIO()
                                    .exists(
                                            table.store()
                                                    .pathFactory()
                                                    .indexFileFactory(
                                                            guard.partition(), guard.bucket())
                                                    .toPath(guard.indexFile())))
                    .isTrue();
        }
        append(branch, 5, row(RowKind.INSERT, OLD, 1, 2));
        assertThat(read(branch))
                .singleElement()
                .satisfies(row -> assertThat(row.getInt(1)).isEqualTo(2));
        append(branch, 6, row(RowKind.INSERT, OLD, 1, 5));
        assertThat(read(branch)).hasSize(2);
        assertThat(read(table)).isEmpty();
        try (FileStoreCommit commit = branch.store().newCommit(USER, null)) {
            commit.truncateTable(7);
        }
        expireOldSnapshots(branch);
        branch.deleteTag("base");
        new LocalOrphanFilesClean(table, Long.MAX_VALUE).clean();
        for (IndexManifestEntry guard : original) {
            assertThat(
                            table.fileIO()
                                    .exists(
                                            table.store()
                                                    .pathFactory()
                                                    .indexFileFactory(
                                                            guard.partition(), guard.bucket())
                                                    .toPath(guard.indexFile())))
                    .isFalse();
        }
        append(table, 8, row(RowKind.INSERT, OLD, 1, 5));
        assertThat(read(table)).isEmpty();
    }

    private PrimaryKeyFileStoreTable createTable(Map<String, String> options) throws Exception {
        return createTable(
                options,
                RowType.builder()
                        .field("pt", DataTypes.STRING())
                        .field("k", DataTypes.INT())
                        .field("seq", DataTypes.INT())
                        .field("payload", DataTypes.STRING())
                        .build());
    }

    private PrimaryKeyFileStoreTable createTable(Map<String, String> extra, RowType type)
            throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("bucket", "2");
        options.put("delete-ordering-guards.enabled", "true");
        options.put("delete-ordering-guards.compaction.min-file-num", "100");
        options.put("num-sorted-run.compaction-trigger", "100");
        options.put("num-levels", "3");
        options.put("partition.timestamp-pattern", "$pt");
        options.put("partition.timestamp-formatter", "yyyy-MM-dd");
        if (type.getFieldNames().contains("seq")) {
            options.put("sequence.field", "seq");
        }
        options.putAll(extra);
        Path path = new Path(tempDir.resolve("table").toUri());
        TableSchema schema =
                SchemaUtils.forceCommit(
                        new FileSystemSchemaManager(LocalFileIO.create(), path),
                        new Schema(
                                type.getFields(),
                                Collections.singletonList("pt"),
                                Arrays.asList("pt", "k"),
                                options,
                                ""));
        return (PrimaryKeyFileStoreTable)
                FileStoreTableFactory.create(LocalFileIO.create(), path, schema);
    }

    private PrimaryKeyFileStoreTable setOption(
            PrimaryKeyFileStoreTable table, String key, String value) throws Exception {
        TableSchema schema =
                new FileSystemSchemaManager(table.fileIO(), table.location(), table.currentBranch())
                        .commitChanges(SchemaChange.setOption(key, value));
        return (PrimaryKeyFileStoreTable)
                FileStoreTableFactory.create(table.fileIO(), table.location(), schema);
    }

    private TableWriteImpl<KeyValue> writer(PrimaryKeyFileStoreTable table) throws IOException {
        IOManager io = new IOManagerImpl(tempDir.resolve("spill").toString());
        ioManagers.add(io);
        return table.newWrite(USER).withIOManager(io);
    }

    private List<CommitMessage> prepare(
            PrimaryKeyFileStoreTable table, long identifier, InternalRow... rows) throws Exception {
        try (TableWriteImpl<KeyValue> write = writer(table)) {
            for (InternalRow row : rows) {
                write.write(row);
            }
            return write.prepareCommit(true, identifier);
        }
    }

    private void append(PrimaryKeyFileStoreTable table, long identifier, InternalRow... rows)
            throws Exception {
        commit(table, identifier, prepare(table, identifier, rows));
    }

    private void commit(
            PrimaryKeyFileStoreTable table, long identifier, List<CommitMessage> messages) {
        ManifestCommittable committable = new ManifestCommittable(identifier);
        messages.forEach(committable::addFileCommittable);
        try (FileStoreCommit commit = table.store().newCommit(USER, null)) {
            commit.commit(committable, false);
        }
    }

    private void compact(PrimaryKeyFileStoreTable table, long identifier) throws Exception {
        try (TableWriteImpl<KeyValue> write = writer(table)) {
            for (DataSplit split :
                    table.newSnapshotReader()
                            .withDeleteOrderingGuardBuckets()
                            .read()
                            .dataSplits()) {
                write.compact(split.partition(), split.bucket(), true);
            }
            commit(table, identifier, write.prepareCommit(true, identifier));
        }
    }

    private List<CommitMessage> rewrite(DeleteOrderingGuardRescale migration) throws Exception {
        List<CommitMessage> messages = new ArrayList<>();
        try {
            for (DataSplit split : migration.plan()) {
                messages.addAll(migration.rewrite(split));
            }
        } catch (Exception failure) {
            migration.abort("rescale", 0, messages);
            throw failure;
        }
        return messages;
    }

    private List<Map<String, String>> expire(
            PrimaryKeyFileStoreTable table,
            PartitionExpireStrategy strategy,
            LocalDateTime now,
            long identifier) {
        try (FileStoreCommit commit = table.store().newCommit("expire", null)) {
            return new NormalPartitionExpire(
                            Duration.ofDays(1),
                            Duration.ZERO,
                            strategy,
                            table.store().newScan(),
                            commit,
                            null,
                            100,
                            0)
                    .expire(now, identifier);
        }
    }

    private void expireOldSnapshots(PrimaryKeyFileStoreTable table) {
        ((ExpireSnapshotsImpl) table.newExpireSnapshots())
                .expireUntil(
                        table.snapshotManager().earliestSnapshotId(),
                        table.snapshotManager().latestSnapshotId());
    }

    private List<InternalRow> read(PrimaryKeyFileStoreTable table) throws Exception {
        List<InternalRow> rows = new ArrayList<>();
        InternalRowSerializer serializer = new InternalRowSerializer(table.rowType());
        table.newRead()
                .createReader(table.newScan().plan())
                .forEachRemaining(row -> rows.add(serializer.copy(row)));
        return rows;
    }

    private List<IndexManifestEntry> guards(PrimaryKeyFileStoreTable table) {
        return table.store().newIndexFileHandler().scan(DELETE_ORDERING_GUARD);
    }

    private List<String> bounds(PrimaryKeyFileStoreTable table) throws Exception {
        List<String> result = new ArrayList<>();
        RowType keyType =
                new RowType(
                        PrimaryKeyTableUtils.PrimaryKeyFieldsExtractor.EXTRACTOR.keyFields(
                                table.schema()));
        try (CacheManager cache = new CacheManager(MemorySize.ofMebiBytes(1), 0.2)) {
            DeleteOrderingGuardReaderFactory factory =
                    new DeleteOrderingGuardReaderFactory(
                            table.fileIO(),
                            new FileSystemSchemaManager(
                                    table.fileIO(), table.location(), table.currentBranch()),
                            table.coreOptions(),
                            keyType,
                            table.rowType(),
                            table.store().pathFactory(),
                            cache);
            for (IndexManifestEntry entry : guards(table)) {
                try (DeleteOrderingGuardReader reader =
                        factory.open(
                                entry.partition(),
                                entry.bucket(),
                                Collections.singletonList(entry.indexFile()),
                                table.rowType(),
                                table.store().newKeyComparator())) {
                    List<KeyValue> key;
                    while ((key = reader.nextKey()) != null) {
                        for (KeyValue kv : key) {
                            result.add(
                                    kv.key().getInt(0)
                                            + "/"
                                            + kv.sequenceNumber()
                                            + "/"
                                            + kv.valueKind()
                                            + "/"
                                            + kv.level()
                                            + "/"
                                            + kv.value().getInt(2));
                        }
                    }
                }
            }
        }
        return result;
    }

    private List<String> physicalRecords(PrimaryKeyFileStoreTable table, BinaryRow partition)
            throws Exception {
        List<String> result = new ArrayList<>();
        for (DataSplit split : table.newSnapshotReader().read().dataSplits()) {
            if (!split.partition().equals(partition)) {
                continue;
            }
            for (DataFileMeta file : split.dataFiles()) {
                try (RecordReaderIterator<KeyValue> rows =
                        new RecordReaderIterator<>(
                                table.store()
                                        .newReaderFactoryBuilder()
                                        .buildWithoutDeletionVector(partition, split.bucket())
                                        .createRecordReader(file))) {
                    while (rows.hasNext()) {
                        KeyValue row = rows.next();
                        result.add(
                                row.key().getInt(0)
                                        + "/"
                                        + row.sequenceNumber()
                                        + "/"
                                        + row.valueKind()
                                        + "/"
                                        + row.value().getInt(2));
                    }
                }
            }
        }
        return result;
    }

    private List<String> files() throws IOException {
        try (Stream<java.nio.file.Path> paths = Files.walk(tempDir.resolve("table"))) {
            return paths.filter(Files::isRegularFile)
                    .map(Object::toString)
                    .collect(Collectors.toList());
        }
    }

    private static InternalRow row(RowKind kind, String partition, int key, int sequence) {
        return GenericRow.ofKind(kind, str(partition), key, sequence, str("payload"));
    }

    private static BinaryString str(String value) {
        return BinaryString.fromString(value);
    }

    private static BinaryRow partition(String value) {
        return BinaryRow.singleColumn(str(value));
    }
}
