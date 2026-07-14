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
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.globalindex.btree.BTreeGlobalIndexerFactory;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceFile;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests automatic source-backed BTree/Bitmap evaluation in ordinary batch planning. */
class PrimaryKeySortedIndexBatchScanTest {

    @Test
    void testOrdinaryBatchScanUsesSnapshotScopedSortedIndex() {
        ScanFixture fixture = fixture(reader(2));

        TableScan.Plan result = fixture.scan.plan();

        assertThat(result.splits()).singleElement().isInstanceOf(IndexedSplit.class);
        IndexedSplit indexedSplit = (IndexedSplit) result.splits().get(0);
        assertThat(indexedSplit.dataSplit().dataFiles()).containsExactly(fixture.dataFile);
        assertThat(indexedSplit.rowRanges()).containsExactly(new Range(2, 2));
    }

    @Test
    void testOrdinaryBatchScanFailsWhenApplyingSortedIndexFails() {
        ScanFixture fixture = fixture(reader(2));
        when(fixture.scan
                        .snapshotReader
                        .indexFileHandler()
                        .scan(
                                any(Snapshot.class),
                                org.mockito.ArgumentMatchers.<Filter<IndexManifestEntry>>any()))
                .thenThrow(new RuntimeException("corrupt index"));

        assertThatThrownBy(fixture.scan::plan)
                .isInstanceOf(RuntimeException.class)
                .hasMessage("corrupt index");
    }

    private static TableSchema tableSchema() {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BUCKET.key(), "2");
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        options.put(CoreOptions.DELETION_VECTORS_MERGE_ON_READ.key(), "false");
        options.put(CoreOptions.PK_BTREE_INDEX_COLUMNS.key(), "f7");
        return new TableSchema(
                5,
                Arrays.asList(
                        new DataField(1, "id", DataTypes.INT().notNull()),
                        new DataField(7, "f7", DataTypes.INT())),
                7,
                Collections.emptyList(),
                Collections.singletonList("id"),
                options,
                null);
    }

    private static ScanFixture fixture(GlobalIndexReader reader) {
        TableSchema schema = tableSchema();
        CoreOptions options = new CoreOptions(schema.options());
        DataFileMeta dataFile = dataFile("data-1", 4);
        DataSplit dataSplit = dataSplit(11, dataFile);
        Snapshot snapshot = mock(Snapshot.class);
        when(snapshot.id()).thenReturn(11L);
        when(snapshot.schemaId()).thenReturn(schema.id());
        SnapshotManager snapshotManager = mock(SnapshotManager.class);
        when(snapshotManager.latestSnapshot()).thenReturn(snapshot);
        when(snapshotManager.snapshot(11)).thenReturn(snapshot);
        SnapshotReader snapshotReader = mock(SnapshotReader.class, RETURNS_SELF);
        when(snapshotReader.snapshotManager()).thenReturn(snapshotManager);
        when(snapshotReader.hasNonPartitionFilter()).thenReturn(true);
        when(snapshotReader.read())
                .thenReturn(new PlanImpl(null, 11L, Collections.<Split>singletonList(dataSplit)));
        IndexFileHandler indexFileHandler = mock(IndexFileHandler.class);
        when(snapshotReader.indexFileHandler()).thenReturn(indexFileHandler);
        when(indexFileHandler.scan(
                        eq(snapshot),
                        org.mockito.ArgumentMatchers.<Filter<IndexManifestEntry>>any()))
                .thenReturn(
                        Collections.singletonList(
                                new IndexManifestEntry(
                                        FileKind.ADD,
                                        BinaryRow.EMPTY_ROW,
                                        0,
                                        payload("btree-0", "data-1", 4))));
        SchemaManager schemaManager = mock(SchemaManager.class);
        when(schemaManager.schema(schema.id())).thenReturn(schema);
        Predicate predicate = new PredicateBuilder(schema.logicalRowType()).equal(1, 42);
        FileStoreTable table = mock(FileStoreTable.class);
        when(table.schema()).thenReturn(schema);
        when(table.schemaManager()).thenReturn(schemaManager);
        when(table.coreOptions()).thenReturn(options);
        PrimaryKeyBatchScan scan =
                new PrimaryKeyBatchScan(
                        table,
                        snapshotReader,
                        mock(TableQueryAuth.class),
                        (ignoredFile, ignoredDefinition, ignoredPayloads) -> reader);
        scan.withFilter(predicate);
        return new ScanFixture(dataFile, scan);
    }

    private static GlobalIndexReader reader(long rowPosition) {
        RoaringNavigableMap64 positions = new RoaringNavigableMap64();
        positions.add(rowPosition);
        GlobalIndexReader reader = mock(GlobalIndexReader.class);
        when(reader.visitEqual(any(), eq(42)))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                Optional.of(GlobalIndexResult.create(positions))));
        return reader;
    }

    private static DataSplit dataSplit(long snapshotId, DataFileMeta dataFile) {
        return DataSplit.builder()
                .withSnapshot(snapshotId)
                .withPartition(BinaryRow.EMPTY_ROW)
                .withBucket(0)
                .withBucketPath("bucket-0")
                .withTotalBuckets(2)
                .withDataFiles(Collections.singletonList(dataFile))
                .withDataDeletionFiles(Collections.singletonList(null))
                .isStreaming(false)
                .rawConvertible(true)
                .build();
    }

    private static DataFileMeta dataFile(String fileName, long rowCount) {
        return DataFileMeta.forAppend(
                fileName,
                100,
                rowCount,
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

    private static IndexFileMeta payload(String fileName, String sourceName, long sourceRowCount) {
        byte[] sourceMeta =
                new PrimaryKeyIndexSourceMeta(
                                new PrimaryKeyIndexSourceFile(sourceName, sourceRowCount))
                        .serialize();
        return new IndexFileMeta(
                BTreeGlobalIndexerFactory.IDENTIFIER,
                fileName,
                100,
                sourceRowCount,
                new GlobalIndexMeta(0, sourceRowCount - 1, 7, null, new byte[] {1}, sourceMeta),
                null);
    }

    private static class ScanFixture {

        private final DataFileMeta dataFile;
        private final PrimaryKeyBatchScan scan;

        private ScanFixture(DataFileMeta dataFile, PrimaryKeyBatchScan scan) {
            this.dataFile = dataFile;
            this.scan = scan;
        }
    }
}
