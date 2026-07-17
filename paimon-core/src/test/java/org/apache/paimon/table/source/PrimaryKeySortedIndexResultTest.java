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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.globalindex.btree.BTreeGlobalIndexerFactory;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.pk.PrimaryKeyIndexDefinition;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceFile;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests conversion from file-local sorted-index results to physical-position splits. */
class PrimaryKeySortedIndexResultTest {

    @Test
    void testIndexedEmptyRawAndInvalidFiles() {
        DataFileMeta indexed = dataFile("indexed", 5, 1);
        DataFileMeta empty = dataFile("empty", 5, 2);
        DataFileMeta raw = dataFile("raw", 5, 3);
        DataFileMeta invalid = dataFile("invalid", 5, 4);
        List<DeletionFile> deletionFiles =
                Arrays.asList(
                        new DeletionFile("dv-indexed", 0, 1, 1L),
                        new DeletionFile("dv-empty", 1, 1, 1L),
                        new DeletionFile("dv-raw", 2, 1, 1L),
                        new DeletionFile("dv-invalid", 3, 1, 1L));
        DataSplit split = dataSplit(11, Arrays.asList(indexed, empty, raw, invalid), deletionFiles);
        PrimaryKeyIndexDefinition definition =
                new PrimaryKeyIndexDefinition(
                        "f7",
                        7,
                        BTreeGlobalIndexerFactory.IDENTIFIER,
                        new Options(),
                        PrimaryKeyIndexDefinition.Family.BTREE);
        PrimaryKeySortedIndexScan.Plan plan =
                PrimaryKeySortedIndexScan.plan(
                        11,
                        Collections.singletonList(split),
                        Collections.singletonList(definition),
                        Arrays.asList(
                                payloadEntry(payload("btree-indexed", "indexed", 5, 1)),
                                payloadEntry(payload("btree-empty", "empty", 5, 2)),
                                payloadEntry(payload("btree-invalid", "invalid", 5, 4))));
        RowType rowType = RowType.of(new DataField(7, "f7", DataTypes.INT()));
        Predicate predicate = new PredicateBuilder(rowType).equal(0, 42);
        PrimaryKeySortedIndexScan.EvaluatedPlan evaluated =
                PrimaryKeySortedIndexScan.evaluate(
                        plan,
                        rowType,
                        predicate,
                        Collections.singletonList(definition),
                        (file, ignoredDefinition, ignoredPayloads) -> {
                            if (file.dataFile().fileName().equals("indexed")) {
                                return reader(1, 2, 4);
                            } else if (file.dataFile().fileName().equals("empty")) {
                                return reader();
                            }
                            return reader(5);
                        });

        PrimaryKeySortedIndexResult result = new PrimaryKeySortedIndexResult(evaluated);

        assertThat(result.snapshotId()).isEqualTo(11);
        assertThat(result.splits()).hasSize(3);

        assertThat(result.splits().get(0)).isInstanceOf(IndexedSplit.class);
        IndexedSplit indexedSplit = (IndexedSplit) result.splits().get(0);
        assertThat(indexedSplit.dataSplit().dataFiles()).containsExactly(indexed);
        assertThat(indexedSplit.dataSplit().deletionFiles().get())
                .containsExactly(deletionFiles.get(0));
        assertThat(indexedSplit.rowRanges()).containsExactly(new Range(1, 2), new Range(4, 4));

        assertRawSplit(result.splits().get(1), raw, deletionFiles.get(2));
        assertRawSplit(result.splits().get(2), invalid, deletionFiles.get(3));
    }

    @Test
    void testNonRawConvertibleSplitPreservesMergeBoundary() {
        DataFileMeta first = dataFile("first", 5, 1);
        DataFileMeta second = dataFile("second", 5, 2);
        List<DeletionFile> deletionFiles =
                Arrays.asList(
                        new DeletionFile("dv-first", 0, 1, 1L),
                        new DeletionFile("dv-second", 1, 1, 1L));
        DataSplit split = dataSplit(11, Arrays.asList(first, second), deletionFiles, false);
        PrimaryKeyIndexDefinition definition =
                new PrimaryKeyIndexDefinition(
                        "f7",
                        7,
                        BTreeGlobalIndexerFactory.IDENTIFIER,
                        new Options(),
                        PrimaryKeyIndexDefinition.Family.BTREE);
        PrimaryKeySortedIndexScan.Plan plan =
                PrimaryKeySortedIndexScan.plan(
                        11,
                        Collections.singletonList(split),
                        Collections.singletonList(definition),
                        Arrays.asList(
                                payloadEntry(payload("btree-first", "first", 5, 1)),
                                payloadEntry(payload("btree-second", "second", 5, 2))));
        RowType rowType = RowType.of(new DataField(7, "f7", DataTypes.INT()));
        Predicate predicate = new PredicateBuilder(rowType).equal(0, 42);
        PrimaryKeySortedIndexScan.EvaluatedPlan evaluated =
                PrimaryKeySortedIndexScan.evaluate(
                        plan,
                        rowType,
                        predicate,
                        Collections.singletonList(definition),
                        (file, ignoredDefinition, ignoredPayloads) ->
                                file.dataFile().fileName().equals("first") ? reader(1) : reader(2));

        PrimaryKeySortedIndexResult result = new PrimaryKeySortedIndexResult(evaluated);

        assertThat(result.splits()).singleElement().isSameAs(split);
    }

    @Test
    void testFragmentedIndexResultFallsBackToRawSplit() {
        DataFileMeta file = dataFile("fragmented", 8193);
        DeletionFile deletionFile = new DeletionFile("dv-fragmented", 0, 1, 1L);
        DataSplit split =
                dataSplit(
                        11,
                        Collections.singletonList(file),
                        Collections.singletonList(deletionFile));
        PrimaryKeyIndexDefinition definition =
                new PrimaryKeyIndexDefinition(
                        "f7",
                        7,
                        BTreeGlobalIndexerFactory.IDENTIFIER,
                        new Options(),
                        PrimaryKeyIndexDefinition.Family.BTREE);
        PrimaryKeySortedIndexScan.Plan plan =
                PrimaryKeySortedIndexScan.plan(
                        11,
                        Collections.singletonList(split),
                        Collections.singletonList(definition),
                        Collections.singletonList(
                                payloadEntry(payload("btree-fragmented", "fragmented", 8193))));
        RowType rowType = RowType.of(new DataField(7, "f7", DataTypes.INT()));
        Predicate predicate = new PredicateBuilder(rowType).equal(0, 42);
        long[] positions = new long[4097];
        for (int i = 0; i < positions.length; i++) {
            positions[i] = i * 2L;
        }
        PrimaryKeySortedIndexScan.EvaluatedPlan evaluated =
                PrimaryKeySortedIndexScan.evaluate(
                        plan,
                        rowType,
                        predicate,
                        Collections.singletonList(definition),
                        (ignoredFile, ignoredDefinition, ignoredPayloads) -> reader(positions));

        PrimaryKeySortedIndexResult result = new PrimaryKeySortedIndexResult(evaluated);

        assertThat(result.splits()).singleElement().isInstanceOf(DataSplit.class);
        assertRawSplit(result.splits().get(0), file, deletionFile);
    }

    private static void assertRawSplit(
            Split split, DataFileMeta expectedFile, DeletionFile expectedDeletionFile) {
        assertThat(split).isInstanceOf(DataSplit.class);
        DataSplit rawSplit = (DataSplit) split;
        assertThat(rawSplit.snapshotId()).isEqualTo(11);
        assertThat(rawSplit.dataFiles()).containsExactly(expectedFile);
        assertThat(rawSplit.deletionFiles().get()).containsExactly(expectedDeletionFile);
        assertThat(rawSplit.rawConvertible()).isFalse();
    }

    private static GlobalIndexReader reader(long... rowPositions) {
        RoaringNavigableMap64 positions = new RoaringNavigableMap64();
        for (long rowPosition : rowPositions) {
            positions.add(rowPosition);
        }
        GlobalIndexReader reader = mock(GlobalIndexReader.class);
        when(reader.visitEqual(any(), any()))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                Optional.of(GlobalIndexResult.createExact(positions))));
        return reader;
    }

    private static DataSplit dataSplit(
            long snapshotId, List<DataFileMeta> dataFiles, List<DeletionFile> deletionFiles) {
        return dataSplit(snapshotId, dataFiles, deletionFiles, true);
    }

    private static DataSplit dataSplit(
            long snapshotId,
            List<DataFileMeta> dataFiles,
            List<DeletionFile> deletionFiles,
            boolean rawConvertible) {
        return DataSplit.builder()
                .withSnapshot(snapshotId)
                .withPartition(BinaryRow.EMPTY_ROW)
                .withBucket(0)
                .withBucketPath("bucket-0")
                .withTotalBuckets(2)
                .withDataFiles(dataFiles)
                .withDataDeletionFiles(deletionFiles)
                .isStreaming(false)
                .rawConvertible(rawConvertible)
                .build();
    }

    private static DataFileMeta dataFile(String fileName, long rowCount) {
        return dataFile(fileName, rowCount, 1);
    }

    private static DataFileMeta dataFile(String fileName, long rowCount, int dataLevel) {
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
                        null)
                .upgrade(dataLevel);
    }

    private static IndexManifestEntry payloadEntry(IndexFileMeta payload) {
        return new IndexManifestEntry(FileKind.ADD, BinaryRow.EMPTY_ROW, 0, payload);
    }

    private static IndexFileMeta payload(String fileName, String sourceName, long sourceRowCount) {
        return payload(fileName, sourceName, sourceRowCount, 1);
    }

    private static IndexFileMeta payload(
            String fileName, String sourceName, long sourceRowCount, int dataLevel) {
        byte[] sourceMeta =
                new PrimaryKeyIndexSourceMeta(
                                dataLevel,
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
}
