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
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceFile;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Range;
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
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Answers.CALLS_REAL_METHODS;
import static org.mockito.Answers.RETURNS_SELF;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests snapshot-consistent planning for bucket-local primary-key vector search. */
class PrimaryKeyVectorScanTest {

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void testPostponeTableOnlyScansRealBuckets() {
        Options options = new Options();
        options.set(CoreOptions.BUCKET, BucketMode.POSTPONE_BUCKET);
        options.set(CoreOptions.PK_VECTOR_INDEX_COLUMNS, "embedding");
        options.setString("fields.embedding.pk-vector.index.type", "ivf-pq");

        FileStoreTable table = mock(FileStoreTable.class);
        Snapshot snapshot = mock(Snapshot.class);
        when(snapshot.id()).thenReturn(11L);
        when(table.coreOptions()).thenReturn(new CoreOptions(options));
        when(table.latestSnapshot()).thenReturn(Optional.of(snapshot));

        SnapshotReader reader = mock(SnapshotReader.class, RETURNS_SELF);
        SnapshotReader.Plan snapshotPlan = mock(SnapshotReader.Plan.class, CALLS_REAL_METHODS);
        when(snapshotPlan.snapshotId()).thenReturn(11L);
        when(snapshotPlan.splits()).thenReturn(Collections.emptyList());
        when(reader.read()).thenReturn(snapshotPlan);
        when(table.newSnapshotReader()).thenReturn(reader);

        IndexFileHandler indexFileHandler = mock(IndexFileHandler.class);
        when(indexFileHandler.scan(eq(snapshot), any(Filter.class)))
                .thenReturn(Collections.emptyList());
        when(reader.indexFileHandler()).thenReturn(indexFileHandler);
        configureBatchScan(table, reader, snapshot);

        new PrimaryKeyVectorScan(table, 7, "ivf-pq", null).scan();

        verify(reader).onlyReadRealBuckets();
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void testScansOneSnapshotAndFiltersVectorIdentity() {
        CoreOptions coreOptions = coreOptions();
        FileStoreTable table = mock(FileStoreTable.class);
        Snapshot snapshot = mock(Snapshot.class);
        when(snapshot.id()).thenReturn(11L);
        when(table.coreOptions()).thenReturn(coreOptions);
        when(table.latestSnapshot()).thenReturn(Optional.of(snapshot));
        when(table.copy(Collections.singletonMap(CoreOptions.SCAN_SNAPSHOT_ID.key(), "11")))
                .thenReturn(table);

        SnapshotReader snapshotReader = mock(SnapshotReader.class, RETURNS_SELF);
        SnapshotReader.Plan snapshotPlan = mock(SnapshotReader.Plan.class, CALLS_REAL_METHODS);
        when(snapshotPlan.snapshotId()).thenReturn(11L);
        when(snapshotPlan.splits())
                .thenReturn(Collections.singletonList(dataSplit(dataFile("data-1"))));
        when(snapshotReader.read()).thenReturn(snapshotPlan);
        when(table.newSnapshotReader()).thenReturn(snapshotReader);

        List<IndexManifestEntry> entries =
                Arrays.asList(
                        payloadEntry("ivf-pq", 7, "ann-match"),
                        payloadEntry("ivf-pq", 8, "ann-other-field"),
                        payloadEntry("hnsw", 7, "ann-other-type"));
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
        when(snapshotReader.indexFileHandler()).thenReturn(indexFileHandler);
        configureBatchScan(table, snapshotReader, snapshot);

        PrimaryKeyVectorScan.Plan plan =
                new PrimaryKeyVectorScan(table, 7, "ivf-pq", null, null, snapshot).scan();

        assertThat(plan.snapshotId()).isEqualTo(11);
        assertThat(plan.splits()).hasSize(1);
        BucketVectorSearchSplit bucketSplit = (BucketVectorSearchSplit) plan.splits().get(0);
        assertThat(bucketSplit.payloadFiles())
                .extracting(IndexFileMeta::fileName)
                .containsExactly("ann-match");
        verify(table).copy(Collections.singletonMap(CoreOptions.SCAN_SNAPSHOT_ID.key(), "11"));
    }

    @Test
    void testMergesDataSplitsForCompleteBucketCoverage() {
        DataFileMeta data1 = dataFile("data-1");
        DataFileMeta data2 = dataFile("data-2");
        DeletionFile deletion = new DeletionFile("dv", 10, 20, 1L);
        DataSplit split1 = dataSplit(data1, Collections.singletonList(deletion));
        DataSplit split2 = dataSplit(data2, null);

        PrimaryKeyVectorScan.Plan plan =
                PrimaryKeyVectorScan.plan(
                        11,
                        Arrays.asList(split1, split2),
                        Collections.singletonList(payloadEntry()));

        assertThat(plan.snapshotId()).isEqualTo(11);
        assertThat(plan.splits()).hasSize(1);
        BucketVectorSearchSplit bucketSplit = (BucketVectorSearchSplit) plan.splits().get(0);
        assertThat(bucketSplit.dataSplit().dataFiles()).containsExactly(data1, data2);
        assertThat(bucketSplit.dataSplit().deletionFiles()).isPresent();
        assertThat(bucketSplit.dataSplit().deletionFiles().get()).containsExactly(deletion, null);
        assertThat(bucketSplit.payloadFiles()).containsExactly(payloadFile());
    }

    @Test
    void testBucketSplitSerialization() throws Exception {
        IndexFileMeta payload = payloadFile();
        BucketVectorSearchSplit split =
                new BucketVectorSearchSplit(
                        dataSplit(dataFile("data-1")),
                        Collections.singletonList(payload),
                        Collections.singletonMap(
                                "data-1", Collections.singletonList(new Range(1, 1))));

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(split);
        }
        BucketVectorSearchSplit restored;
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            restored = (BucketVectorSearchSplit) input.readObject();
        }

        assertThat(restored).isEqualTo(split);
        assertThat(restored.payloadFiles().get(0).globalIndexMeta().sourceMeta())
                .isEqualTo(payload.globalIndexMeta().sourceMeta());
    }

    private static DataSplit dataSplit(DataFileMeta dataFile) {
        return dataSplit(dataFile, null);
    }

    private static DataSplit dataSplit(
            DataFileMeta dataFile, java.util.List<DeletionFile> deletionFiles) {
        DataSplit.Builder builder =
                DataSplit.builder()
                        .withSnapshot(11)
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath("bucket-0")
                        .withTotalBuckets(1)
                        .withDataFiles(Collections.singletonList(dataFile));
        if (deletionFiles != null) {
            builder.withDataDeletionFiles(deletionFiles);
        }
        return builder.build();
    }

    private static IndexManifestEntry payloadEntry() {
        return new IndexManifestEntry(FileKind.ADD, BinaryRow.EMPTY_ROW, 0, payloadFile());
    }

    private static IndexManifestEntry payloadEntry(String indexType, int fieldId, String fileName) {
        return new IndexManifestEntry(
                FileKind.ADD, BinaryRow.EMPTY_ROW, 0, payloadFile(indexType, fieldId, fileName));
    }

    private static IndexFileMeta payloadFile() {
        return payloadFile("ivf-pq", 7, "ann");
    }

    private static IndexFileMeta payloadFile(String indexType, int fieldId, String fileName) {
        byte[] sourceMeta =
                new PrimaryKeyIndexSourceMeta(
                                Collections.singletonList(
                                        new PrimaryKeyIndexSourceFile("data-1", 2)))
                        .serialize();
        return new IndexFileMeta(
                indexType,
                fileName,
                100,
                2,
                new GlobalIndexMeta(0, 1, fieldId, null, null, sourceMeta),
                null);
    }

    private static CoreOptions coreOptions() {
        Options options = new Options();
        options.set(CoreOptions.PK_VECTOR_INDEX_COLUMNS, "embedding");
        options.setString("fields.embedding.pk-vector.index.type", "ivf-pq");
        return new CoreOptions(options);
    }

    private static void configureBatchScan(
            FileStoreTable table, SnapshotReader snapshotReader, Snapshot snapshot) {
        TableSchema schema = mock(TableSchema.class);
        when(schema.primaryKeys()).thenReturn(Collections.singletonList("id"));
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

    private static DataFileMeta dataFile(String fileName) {
        return DataFileMeta.forAppend(
                fileName,
                100,
                2,
                SimpleStats.EMPTY_STATS,
                0,
                0,
                1,
                Collections.emptyList(),
                null,
                FileSource.COMPACT,
                null,
                null,
                null,
                null);
    }
}
