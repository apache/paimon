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

package org.apache.paimon.table.source;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.pk.PrimaryKeyIndexDefinition;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceFile;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Answers.CALLS_REAL_METHODS;
import static org.mockito.Answers.RETURNS_SELF;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests snapshot-consistent planning for primary-key full-text search. */
class PrimaryKeyFullTextScanTest {

    private static final int FIELD_ID = 7;

    @Test
    void testSeparatesCurrentCoverageAndExcludesIneligibleFiles() {
        DataFileMeta indexed = dataFile("indexed", 1, FileSource.COMPACT);
        DataFileMeta uncovered = dataFile("uncovered", 2, FileSource.COMPACT);
        DataFileMeta levelZero = dataFile("level-zero", 0, FileSource.COMPACT);
        DataFileMeta appended = dataFile("appended", 1, FileSource.APPEND);
        DeletionFile indexedDv = new DeletionFile("indexed.dv", 0, 10, 1L);
        DeletionFile uncoveredDv = new DeletionFile("uncovered.dv", 10, 10, 1L);
        DataSplit source =
                dataSplit(
                        Arrays.asList(indexed, uncovered, levelZero, appended),
                        Arrays.asList(indexedDv, uncoveredDv, null, null));

        List<IndexManifestEntry> payloads =
                Arrays.asList(
                        payloadEntry("indexed", FIELD_ID, "current"),
                        payloadEntry("uncovered", 8, "other-field"));

        PrimaryKeyFullTextScan.Plan plan =
                PrimaryKeyFullTextScan.plan(
                        11, Collections.singletonList(source), payloads, FIELD_ID);

        assertThat(plan.snapshotId()).isEqualTo(11);
        assertThat(plan.splits()).hasSize(1);
        PrimaryKeyFullTextSearchSplit split = (PrimaryKeyFullTextSearchSplit) plan.splits().get(0);
        assertThat(split.dataSplit().dataFiles()).containsExactly(indexed, uncovered);
        assertThat(split.dataSplit().deletionFiles()).isPresent();
        assertThat(split.dataSplit().deletionFiles().get()).containsExactly(indexedDv, uncoveredDv);
        assertThat(split.payloadFiles())
                .extracting(IndexFileMeta::fileName)
                .containsExactly("current");
        assertThat(split.uncoveredDataFiles()).containsExactly("uncovered");
    }

    @Test
    void testPlansMultiSourceArchiveOnce() {
        DataFileMeta first = dataFile("data-1", 1, FileSource.COMPACT);
        DataFileMeta second = dataFile("data-2", 1, FileSource.COMPACT);
        IndexManifestEntry payload =
                payloadEntry(Arrays.asList("data-1", "data-2"), FIELD_ID, "multi-source");

        PrimaryKeyFullTextScan.Plan plan =
                PrimaryKeyFullTextScan.plan(
                        11,
                        Collections.singletonList(dataSplit(Arrays.asList(first, second), null)),
                        Collections.singletonList(payload),
                        FIELD_ID);

        PrimaryKeyFullTextSearchSplit split = (PrimaryKeyFullTextSearchSplit) plan.splits().get(0);
        assertThat(split.payloadFiles()).containsExactly(payload.indexFile());
        assertThat(split.uncoveredDataFiles()).isEmpty();
    }

    @Test
    void testRejectsPartialLevelCoverage() {
        DataFileMeta first = dataFile("data-1", 1, FileSource.COMPACT);
        DataFileMeta second = dataFile("data-2", 1, FileSource.COMPACT);

        PrimaryKeyFullTextScan.Plan plan =
                PrimaryKeyFullTextScan.plan(
                        11,
                        Collections.singletonList(dataSplit(Arrays.asList(first, second), null)),
                        Collections.singletonList(
                                payloadEntry("data-1", FIELD_ID, "partial-level")),
                        FIELD_ID);

        PrimaryKeyFullTextSearchSplit split = (PrimaryKeyFullTextSearchSplit) plan.splits().get(0);
        assertThat(split.payloadFiles()).isEmpty();
        assertThat(split.uncoveredDataFiles()).containsExactly("data-1", "data-2");
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void testCapturesOneSnapshotAndPrunesPartitions() {
        FileStoreTable table = mock(FileStoreTable.class);
        Snapshot snapshot = mock(Snapshot.class);
        when(snapshot.id()).thenReturn(11L);
        Options tableOptions = new Options();
        tableOptions.set(CoreOptions.PK_FULL_TEXT_INDEX_COLUMNS, "content");
        when(table.coreOptions()).thenReturn(new CoreOptions(tableOptions));
        when(table.copy(Collections.singletonMap(CoreOptions.SCAN_SNAPSHOT_ID.key(), "11")))
                .thenReturn(table);

        SnapshotReader reader = mock(SnapshotReader.class, RETURNS_SELF);
        SnapshotReader.Plan snapshotPlan = mock(SnapshotReader.Plan.class, CALLS_REAL_METHODS);
        when(snapshotPlan.snapshotId()).thenReturn(11L);
        when(snapshotPlan.splits())
                .thenReturn(
                        Collections.singletonList(
                                dataSplit(
                                        Collections.singletonList(
                                                dataFile("indexed", 1, FileSource.COMPACT)),
                                        null)));
        when(reader.read()).thenReturn(snapshotPlan);
        when(table.newSnapshotReader()).thenReturn(reader);

        PartitionPredicate partitionFilter = mock(PartitionPredicate.class);
        when(partitionFilter.test(BinaryRow.EMPTY_ROW)).thenReturn(true);
        PrimaryKeyIndexDefinition definition = definition();
        List<IndexManifestEntry> entries =
                Arrays.asList(
                        payloadEntry("indexed", FIELD_ID, "current"),
                        payloadEntry("indexed", FIELD_ID + 1, "other-field"));
        IndexFileHandler indexFileHandler = mock(IndexFileHandler.class);
        when(indexFileHandler.scan(eq(snapshot), any(Filter.class)))
                .thenAnswer(
                        invocation -> {
                            Filter<IndexManifestEntry> filter = invocation.getArgument(1);
                            List<IndexManifestEntry> filtered = new ArrayList<>();
                            for (IndexManifestEntry entry : entries) {
                                if (filter.test(entry)) {
                                    filtered.add(entry);
                                }
                            }
                            return filtered;
                        });
        when(reader.indexFileHandler()).thenReturn(indexFileHandler);
        configureBatchScan(table, reader, snapshot);

        PrimaryKeyFullTextScan.Plan plan =
                new PrimaryKeyFullTextScan(table, definition, partitionFilter, snapshot).scan();

        assertThat(plan.snapshotId()).isEqualTo(11);
        PrimaryKeyFullTextSearchSplit split = (PrimaryKeyFullTextSearchSplit) plan.splits().get(0);
        assertThat(split.payloadFiles())
                .extracting(IndexFileMeta::fileName)
                .containsExactly("current");
        assertThat(split.uncoveredDataFiles()).isEmpty();
        verify(table).copy(Collections.singletonMap(CoreOptions.SCAN_SNAPSHOT_ID.key(), "11"));
        verify(reader).withPartitionFilter(partitionFilter);
        verify(reader).indexFileHandler();
    }

    @Test
    void testSplitSerialization() throws Exception {
        PrimaryKeyFullTextSearchSplit split =
                new PrimaryKeyFullTextSearchSplit(
                        dataSplit(
                                Arrays.asList(
                                        dataFile("indexed", 1, FileSource.COMPACT),
                                        dataFile("raw", 1, FileSource.COMPACT)),
                                Arrays.asList(new DeletionFile("indexed.dv", 0, 10, 1L), null)),
                        Collections.singletonList(
                                payloadEntry("indexed", FIELD_ID, "current").indexFile()),
                        Collections.singletonList("raw"));

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(split);
        }
        PrimaryKeyFullTextSearchSplit restored;
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            restored = (PrimaryKeyFullTextSearchSplit) input.readObject();
        }

        assertThat(restored).isEqualTo(split);
    }

    private static PrimaryKeyIndexDefinition definition() {
        return new PrimaryKeyIndexDefinition(
                "content",
                FIELD_ID,
                "full-text",
                new Options(),
                PrimaryKeyIndexDefinition.Family.FULL_TEXT);
    }

    private static DataSplit dataSplit(
            List<DataFileMeta> dataFiles, List<DeletionFile> deletionFiles) {
        DataSplit.Builder builder =
                DataSplit.builder()
                        .withSnapshot(11)
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath("bucket-0")
                        .withTotalBuckets(1)
                        .withDataFiles(dataFiles);
        if (deletionFiles != null) {
            builder.withDataDeletionFiles(deletionFiles);
        }
        return builder.build();
    }

    private static IndexManifestEntry payloadEntry(
            String sourceFile, int fieldId, String payloadFile) {
        return payloadEntry(Collections.singletonList(sourceFile), fieldId, payloadFile);
    }

    private static IndexManifestEntry payloadEntry(
            List<String> sourceFiles, int fieldId, String payloadFile) {
        List<PrimaryKeyIndexSourceFile> sources = new ArrayList<>();
        for (String sourceFile : sourceFiles) {
            sources.add(new PrimaryKeyIndexSourceFile(sourceFile, 2));
        }
        byte[] sourceMeta = new PrimaryKeyIndexSourceMeta(1, sources).serialize();
        long rowCount = 2L * sources.size();
        return new IndexManifestEntry(
                FileKind.ADD,
                BinaryRow.EMPTY_ROW,
                0,
                new IndexFileMeta(
                        "full-text",
                        payloadFile,
                        100,
                        rowCount,
                        new GlobalIndexMeta(0, rowCount - 1, fieldId, null, null, sourceMeta),
                        null));
    }

    private static void configureBatchScan(
            FileStoreTable table, SnapshotReader snapshotReader, Snapshot snapshot) {
        TableSchema schema = mock(TableSchema.class);
        when(schema.primaryKeys()).thenReturn(Collections.singletonList("id"));
        when(schema.logicalRowType())
                .thenReturn(RowType.of(new DataField(1, "id", DataTypes.INT().notNull())));
        when(table.schema()).thenReturn(schema);
        when(table.schemaManager()).thenReturn(mock(SchemaManager.class));
        SnapshotManager snapshotManager = mock(SnapshotManager.class);
        when(snapshotManager.latestSnapshot()).thenReturn(snapshot);
        when(snapshotManager.snapshot(snapshot.id())).thenReturn(snapshot);
        when(snapshotReader.snapshotManager()).thenReturn(snapshotManager);
        when(table.newScan(any(FileStoreTable.SnapshotReaderFactory.class)))
                .thenAnswer(
                        invocation -> {
                            FileStoreTable.SnapshotReaderFactory factory =
                                    invocation.getArgument(0);
                            return new PrimaryKeyBatchScan(
                                    table, factory.create(table), mock(TableQueryAuth.class), null);
                        });
    }

    private static DataFileMeta dataFile(String fileName, int level, FileSource fileSource) {
        return DataFileMeta.create(
                fileName,
                100,
                2,
                BinaryRow.EMPTY_ROW,
                BinaryRow.EMPTY_ROW,
                SimpleStats.EMPTY_STATS,
                SimpleStats.EMPTY_STATS,
                0,
                0,
                0,
                level,
                Collections.emptyList(),
                0L,
                null,
                fileSource,
                null,
                null,
                null,
                null);
    }
}
