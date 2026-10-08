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

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.RenamingSnapshotCommit;
import org.apache.paimon.catalog.SnapshotCommit;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.iceberg.IcebergOptions;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileMetaSerializer;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.BucketEntry;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.operation.RemoveUnexistingManifests;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.IncrementalSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.PartitionStatisticsReporter;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.paimon.data.BinaryString.fromString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/** Tests unknown file row counts without a file-import producer. */
public class UnknownRowCountTest extends TableTestBase {

    @Override
    protected Schema schemaDefault() {
        return Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("dt", DataTypes.STRING())
                .partitionKeys("dt")
                .option("bucket", "-1")
                .option("file.format", "parquet")
                .build();
    }

    @Test
    public void testUnknownCountSurvivesSerializationAndMixedFiles() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        commit(table, true, 1);
        Snapshot first = table.snapshotManager().latestSnapshot();
        assertThat(first.deltaRecordCountKnown()).isFalse();
        assertThat(Snapshot.fromJson(first.toJson()).deltaRecordCountKnown()).isFalse();
        commit(table, false, 2, 3);
        DataSplit split = (DataSplit) table.newScan().plan().splits().get(0);
        assertThat(split.rowCount()).isEqualTo(DataFileMeta.UNKNOWN_ROW_COUNT);
        assertThat(split.mergedRowCount()).isEmpty();
        DataFileMeta unknown =
                split.dataFiles().stream().filter(f -> f.rowCount() < 0).findFirst().get();
        DataFileMetaSerializer serializer = new DataFileMetaSerializer();
        assertThat(serializer.fromRow(serializer.toRow(unknown)).rowCount())
                .isEqualTo(DataFileMeta.UNKNOWN_ROW_COUNT);
        Snapshot snapshot = table.snapshotManager().latestSnapshot();
        assertThat(snapshot.totalRecordCount()).isEqualTo(DataFileMeta.UNKNOWN_ROW_COUNT);
        assertThat(snapshot.deltaRecordCount()).isEqualTo(2);
        assertThat(snapshot.deltaRecordCountKnown()).isTrue();
        assertThat(table.newScan().listPartitionEntries().get(0).recordCount())
                .isEqualTo(DataFileMeta.UNKNOWN_ROW_COUNT);
        List<ManifestEntry> entries =
                split.dataFiles().stream()
                        .map(f -> ManifestEntry.create(FileKind.ADD, split.partition(), 0, -1, f))
                        .collect(Collectors.toList());
        assertThat(BucketEntry.merge(entries).iterator().next().recordCount())
                .isEqualTo(DataFileMeta.UNKNOWN_ROW_COUNT);
        assertThat(
                        new IncrementalSplit(
                                        1,
                                        split.partition(),
                                        0,
                                        -1,
                                        Arrays.asList(unknown),
                                        null,
                                        split.dataFiles(),
                                        null,
                                        false)
                                .rowCount())
                .isEqualTo(DataFileMeta.UNKNOWN_ROW_COUNT);
        assertThat(read(table).stream().map(row -> row.getInt(0)))
                .containsExactlyInAnyOrder(1, 2, 3);
    }

    @Test
    public void testKnownDeleteOfOneRowIsNotUnknown() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        commit(table, false, 1);
        DataSplit split = (DataSplit) table.newScan().plan().splits().get(0);
        DataFileMeta file = split.dataFiles().get(0);
        ManifestEntry add = ManifestEntry.create(FileKind.ADD, split.partition(), 0, -1, file);
        ManifestEntry delete =
                ManifestEntry.create(FileKind.DELETE, split.partition(), 0, -1, file);
        assertThat(
                        PartitionEntry.fromManifestEntry(delete)
                                .merge(PartitionEntry.fromManifestEntry(add))
                                .recordCount())
                .isZero();
        assertThat(
                        BucketEntry.fromManifestEntry(delete)
                                .merge(BucketEntry.fromManifestEntry(add))
                                .recordCount())
                .isZero();
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.truncatePartitions(
                    Collections.singletonList(Collections.singletonMap("dt", "p1")));
        }
        Snapshot latest = table.snapshotManager().latestSnapshot();
        assertThat(latest.deltaRecordCount()).isEqualTo(-1);
        assertThat(latest.deltaRecordCountKnown()).isTrue();
        assertThat(latest.totalRecordCount()).isZero();
        assertThat(table.newScan().plan().splits()).isEmpty();
    }

    @Test
    public void testUnknownCountsDoNotPruneNewNullableColumn() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        commit(table, true, 1, 2, 3);
        catalog.alterTable(
                identifier(),
                Collections.singletonList(SchemaChange.addColumn("added", DataTypes.INT())),
                false);
        table = getTableDefault();
        org.apache.paimon.predicate.PredicateBuilder predicates =
                new org.apache.paimon.predicate.PredicateBuilder(table.rowType());
        ReadBuilder builder = table.newReadBuilder().withFilter(predicates.isNull(2));
        List<Integer> ids = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                builder.newRead().createReader(builder.newScan().plan())) {
            reader.forEachRemaining(row -> ids.add(row.getInt(0)));
        }
        assertThat(ids).containsExactlyInAnyOrder(1, 2, 3);
        assertThat(table.newReadBuilder().withLimit(2).newScan().plan().splits()).isNotEmpty();
    }

    @Test
    public void testPartitionReporterKeepsUnknownAndOtherMeasurements() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        commit(table, true, 1);
        commit(table, false, 2, 3, 4, 5, 6);
        PartitionModification modification = mock(PartitionModification.class);
        try (PartitionStatisticsReporter reporter =
                new PartitionStatisticsReporter(table, modification)) {
            reporter.report("dt=p1/", 1000);
        }
        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<PartitionStatistics>> captor = ArgumentCaptor.forClass(List.class);
        verify(modification).alterPartitions(captor.capture());
        PartitionStatistics stats = captor.getValue().get(0);
        assertThat(stats.recordCount()).isEqualTo(DataFileMeta.UNKNOWN_ROW_COUNT);
        assertThat(stats.fileCount()).isEqualTo(2);
        assertThat(stats.fileSizeInBytes()).isGreaterThan(0);
        assertThat(stats.lastFileCreationTime()).isEqualTo(1000);
    }

    @Test
    public void testSnapshotTotalRemainsConservativelyUnknownAfterTruncate() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        commit(table, true, 1);
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.truncatePartitions(
                    Collections.singletonList(Collections.singletonMap("dt", "p1")));
        }
        assertThat(table.newScan().plan().splits()).isEmpty();
        assertThat(table.snapshotManager().latestSnapshot().totalRecordCount())
                .isEqualTo(DataFileMeta.UNKNOWN_ROW_COUNT);
    }

    @Test
    public void testBackendRejectsUnknownBeforePublicationButAllowsKnownDelete() throws Exception {
        createTableDefault();
        FileStoreTable original = getTableDefault();
        SnapshotCommit backend =
                spy(new RenamingSnapshotCommit(original.snapshotManager(), Lock.empty()));
        doReturn(false).when(backend).supportsUnknownRowCount();
        CatalogEnvironment environment = spy(CatalogEnvironment.empty());
        doReturn(backend).when(environment).snapshotCommit(any());
        FileStoreTable table =
                FileStoreTableFactory.create(
                        original.fileIO(), original.location(), original.schema(), environment);
        assertThatThrownBy(() -> commit(table, true, 1))
                .hasStackTraceContaining("Snapshot backend does not support unknown row counts");
        assertThat(table.snapshotManager().latestSnapshot()).isNull();
        commit(table, false, 2);
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.truncatePartitions(
                    Collections.singletonList(Collections.singletonMap("dt", "p1")));
        }
        assertThat(table.snapshotManager().latestSnapshot().totalRecordCount()).isZero();
        assertThat(table.snapshotManager().latestSnapshot().deltaRecordCount()).isEqualTo(-1);
        assertThat(table.snapshotManager().latestSnapshot().deltaRecordCountKnown()).isTrue();
    }

    @Test
    public void testRepairDoesNotInventCountsForSurvivingUnknownFiles() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        commit(table, false, 2, 3, 4, 5, 6);
        commit(table, false, 7);
        commit(table, true, 1);
        List<ManifestFileMeta> manifests =
                table.store()
                        .newScan()
                        .manifestsReader()
                        .read(table.snapshotManager().latestSnapshot(), ScanMode.ALL)
                        .allManifests;
        ManifestFileMeta dropped =
                manifests.stream()
                        .filter(
                                meta ->
                                        table.store().newScan().readManifest(meta).stream()
                                                .anyMatch(e -> e.file().rowCount() == 1))
                        .reduce((left, right) -> right)
                        .get();
        table.fileIO()
                .delete(table.store().pathFactory().toManifestFilePath(dropped.fileName()), false);
        assertThat(new RemoveUnexistingManifests(table).execute()).isTrue();
        assertThat(table.snapshotManager().latestSnapshot().totalRecordCount())
                .isEqualTo(DataFileMeta.UNKNOWN_ROW_COUNT);
        assertThat(table.snapshotManager().latestSnapshot().deltaRecordCountKnown()).isTrue();
    }

    @Test
    public void testManifestCompactionClearsUnknownDeltaMarker() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        commit(table, false, 1);
        commit(table, true, 2);
        Snapshot before = table.snapshotManager().latestSnapshot();
        assertThat(before.deltaRecordCountKnown()).isFalse();
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.compactManifests();
        }
        Snapshot latest = table.snapshotManager().latestSnapshot();
        assertThat(latest.id()).isGreaterThan(before.id());
        assertThat(latest.deltaRecordCount()).isZero();
        assertThat(latest.deltaRecordCountKnown()).isTrue();
        assertThat(latest.totalRecordCount()).isEqualTo(DataFileMeta.UNKNOWN_ROW_COUNT);
    }

    @Test
    public void testEnablingIcebergSyncRejectsKnownAppendAfterUnknownSnapshot() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        commit(table, true, 1);
        long snapshotId = table.snapshotManager().latestSnapshotId();
        catalog.alterTable(
                identifier(),
                Collections.singletonList(
                        SchemaChange.setOption(
                                IcebergOptions.METADATA_ICEBERG_STORAGE.key(), "table-location")),
                false);
        FileStoreTable synced = getTableDefault();
        assertThatThrownBy(() -> commit(synced, false, 2))
                .hasStackTraceContaining(
                        "Unknown row counts do not support Iceberg metadata synchronization");
        assertThat(synced.snapshotManager().latestSnapshotId()).isEqualTo(snapshotId);
        assertThat(read(synced).stream().map(row -> row.getInt(0))).containsExactly(1);
    }

    @Test
    public void testRollbackPreservesKnownTotalAndUnknownDelta() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        commit(table, false, 1);
        Snapshot target = table.snapshotManager().latestSnapshot();
        commit(table, true, 2);
        try (org.apache.paimon.operation.FileStoreCommitImpl commit =
                (org.apache.paimon.operation.FileStoreCommitImpl)
                        table.store().newCommit("rollback", table)) {
            assertThat(commit.rollbackToAsLatest(target)).isTrue();
        }
        Snapshot latest = table.snapshotManager().latestSnapshot();
        assertThat(latest.totalRecordCount()).isEqualTo(1);
        assertThat(latest.deltaRecordCount()).isEqualTo(DataFileMeta.UNKNOWN_ROW_COUNT);
        assertThat(latest.deltaRecordCountKnown()).isFalse();
        assertThat(read(table).stream().map(row -> row.getInt(0))).containsExactly(1);
    }

    private void commit(FileStoreTable table, boolean unknownCount, int... ids) throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            for (int id : ids) {
                write.write(GenericRow.of(id, fromString("p1")));
            }
            List<CommitMessage> messages = write.prepareCommit();
            if (unknownCount) {
                messages =
                        messages.stream()
                                .map(
                                        message -> {
                                            CommitMessageImpl m = (CommitMessageImpl) message;
                                            List<DataFileMeta> files =
                                                    m.newFilesIncrement().newFiles().stream()
                                                            .map(
                                                                    file ->
                                                                            DataFileMeta.forAppend(
                                                                                    file.fileName(),
                                                                                    file.fileSize(),
                                                                                    DataFileMeta
                                                                                            .UNKNOWN_ROW_COUNT,
                                                                                    SimpleStats
                                                                                            .EMPTY_STATS,
                                                                                    file
                                                                                            .minSequenceNumber(),
                                                                                    file
                                                                                            .maxSequenceNumber(),
                                                                                    file.schemaId(),
                                                                                    file
                                                                                            .extraFiles(),
                                                                                    null,
                                                                                    FileSource
                                                                                            .APPEND,
                                                                                    Collections
                                                                                            .emptyList(),
                                                                                    file.externalPath()
                                                                                            .orElse(
                                                                                                    null),
                                                                                    null,
                                                                                    null))
                                                            .collect(Collectors.toList());
                                            return new CommitMessageImpl(
                                                    m.partition(),
                                                    m.bucket(),
                                                    m.totalBuckets(),
                                                    new DataIncrement(
                                                            files,
                                                            Collections.emptyList(),
                                                            Collections.emptyList()),
                                                    m.compactIncrement());
                                        })
                                .collect(Collectors.toList());
            }
            commit.commit(messages);
        }
    }
}
