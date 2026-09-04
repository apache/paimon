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

package org.apache.paimon.globalindex.generic;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.globalindex.ScanResult;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link GenericGlobalIndexScanner}. */
public class GenericGlobalIndexScannerTest extends TableTestBase {

    @Override
    public Schema schemaDefault() {
        return Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("v", DataTypes.INT())
                .option(CoreOptions.BUCKET.key(), "-1")
                .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                .build();
    }

    @Test
    public void testScan() throws Exception {
        FileStoreTable table = writeRows();
        long snapshotId = table.snapshotManager().latestSnapshot().id();

        ScanResult<ManifestEntry> scanResult =
                new GenericGlobalIndexScanner(table)
                        .scan()
                        .orElseThrow(() -> new IllegalStateException("Expected scan result."));

        assertThat(scanResult.scanSnapshotId()).isEqualTo(snapshotId);
        assertThat(scanResult.entries()).isNotEmpty();
        assertThat(scanResult.rowRangeIndex().ranges()).containsExactly(new Range(0, 9));
        assertThat(scanResult.deletedIndexEntries()).isEmpty();
    }

    @Test
    public void testIncrementalScanWithoutExistingIndex() throws Exception {
        FileStoreTable table = writeRows();

        ScanResult<ManifestEntry> scanResult =
                new GenericGlobalIndexScanner(table)
                        .withIndex("test-index", Collections.singletonList("v"), new Options())
                        .incrementalScan()
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Expected incremental scan result."));

        assertThat(scanResult.entries()).isNotEmpty();
        assertThat(scanResult.rowRangeIndex().ranges()).containsExactly(new Range(0, 9));
        assertThat(scanResult.deletedIndexEntries()).isEmpty();
    }

    @Test
    public void testIncrementalScanReplacesLegacyIndex() throws Exception {
        FileStoreTable table = writeRows();
        IndexFileMeta legacyIndex = globalIndex("legacy-index", null);
        commitIndexes(table, Collections.singletonList(legacyIndex), Collections.emptyList());

        ScanResult<ManifestEntry> scanResult =
                new GenericGlobalIndexScanner(table)
                        .withIndex("test-index", Collections.singletonList("v"), new Options())
                        .incrementalScan()
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Expected legacy index replacement plan."));

        assertThat(scanResult.rowRangeIndex().ranges()).containsExactly(new Range(0, 9));
        assertThat(scanResult.deletedIndexEntries())
                .extracting(entry -> entry.indexFile().fileName())
                .containsExactly("legacy-index");

        IndexFileMeta replacement = globalIndex("replacement-index", table.schema().id());
        commitIndexes(
                table,
                Collections.singletonList(replacement),
                scanResult.deletedIndexEntries().stream()
                        .map(IndexManifestEntry::indexFile)
                        .collect(java.util.stream.Collectors.toList()));

        List<IndexManifestEntry> currentIndexes =
                table.store()
                        .newIndexFileHandler()
                        .scan(table.snapshotManager().latestSnapshot(), "test-index");
        assertThat(currentIndexes).hasSize(1);
        assertThat(currentIndexes.get(0).indexFile().fileName()).isEqualTo("replacement-index");
        assertThat(currentIndexes.get(0).schemaId()).isEqualTo(table.schema().id());
    }

    @Test
    public void testIncrementalScanReplacesIndexAfterIndexedTypeChange() throws Exception {
        FileStoreTable table = writeRows();
        long buildSchemaId = table.schema().id();
        IndexFileMeta oldIndex = globalIndex("old-index", buildSchemaId);
        commitIndexes(table, Collections.singletonList(oldIndex), Collections.emptyList());

        table.schemaManager().commitChanges(SchemaChange.updateColumnType("v", DataTypes.BIGINT()));
        table = table.copyWithLatestSchema();

        ScanResult<ManifestEntry> scanResult =
                new GenericGlobalIndexScanner(table)
                        .withIndex("test-index", Collections.singletonList("v"), new Options())
                        .incrementalScan()
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Expected incompatible index replacement plan."));

        assertThat(table.schema().id()).isNotEqualTo(buildSchemaId);
        assertThat(scanResult.rowRangeIndex().ranges()).containsExactly(new Range(0, 9));
        assertThat(scanResult.deletedIndexEntries())
                .extracting(entry -> entry.indexFile().fileName())
                .containsExactly("old-index");
    }

    @Test
    public void testScanEmptyTable() throws Exception {
        createTableDefault();

        assertThat(new GenericGlobalIndexScanner(getTableDefault()).scan()).isEmpty();
    }

    private FileStoreTable writeRows() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite()) {
            for (int i = 0; i < 10; i++) {
                write.write(GenericRow.of(i, i));
            }
            try (BatchTableCommit commit = builder.newCommit()) {
                commit.commit(write.prepareCommit());
            }
        }
        return table;
    }

    private IndexFileMeta globalIndex(String fileName, Long schemaId) {
        return new IndexFileMeta(
                "test-index",
                fileName,
                1L,
                10L,
                null,
                null,
                new GlobalIndexMeta(0, 9, 1, null, null),
                schemaId);
    }

    private void commitIndexes(
            FileStoreTable table, List<IndexFileMeta> additions, List<IndexFileMeta> deletions)
            throws Exception {
        DataIncrement increment =
                new DataIncrement(
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        additions,
                        deletions);
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(
                    Collections.singletonList(
                            new CommitMessageImpl(
                                    BinaryRow.EMPTY_ROW,
                                    0,
                                    null,
                                    increment,
                                    CompactIncrement.emptyIncrement())));
        }
    }
}
