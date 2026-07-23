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

package org.apache.paimon.globalindex;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.DataEvolutionIndexSourceMeta;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link DataEvolutionGlobalIndexRefreshPlanner}. */
class DataEvolutionGlobalIndexRefreshPlannerTest {

    private static final DataField VECTOR_FIELD =
            new DataField(1, "vector", new ArrayType(new FloatType()));
    private static final DataField OTHER_FIELD = new DataField(2, "other", new IntType());
    private static final DataField UNRELATED_FIELD = new DataField(3, "unrelated", new IntType());

    private SchemaManager schemaManager;

    @BeforeEach
    void beforeEach() {
        schemaManager = mock(SchemaManager.class);
        when(schemaManager.schema(0L))
                .thenReturn(
                        new TableSchema(
                                0L,
                                Collections.singletonList(OTHER_FIELD),
                                1,
                                Collections.emptyList(),
                                Collections.emptyList(),
                                new HashMap<>(),
                                ""));
        when(schemaManager.schema(1L))
                .thenReturn(
                        new TableSchema(
                                1L,
                                Arrays.asList(VECTOR_FIELD, OTHER_FIELD, UNRELATED_FIELD),
                                3,
                                Collections.emptyList(),
                                Collections.emptyList(),
                                new HashMap<>(),
                                ""));
        when(schemaManager.schema(2L))
                .thenReturn(
                        new TableSchema(
                                2L,
                                Arrays.asList(
                                        new DataField(
                                                1,
                                                "renamed_vector",
                                                new ArrayType(new FloatType())),
                                        OTHER_FIELD,
                                        UNRELATED_FIELD),
                                3,
                                Collections.emptyList(),
                                Collections.emptyList(),
                                new HashMap<>(),
                                ""));
    }

    @Test
    void testRefreshesOnlyForNewPhysicalIndexColumnFile() {
        IndexManifestEntry index = index("index", 0, 99, 5L, BinaryRow.EMPTY_ROW, 0);

        assertThat(
                        plan(
                                Collections.singletonList(
                                        data("vector-update", 0, 100, 6, 1, "vector")),
                                index))
                .containsExactly(index);
        assertThat(
                        plan(
                                Collections.singletonList(
                                        data("old-vector", 0, 100, 5, 1, "vector")),
                                index))
                .isEmpty();
        assertThat(
                        plan(
                                Collections.singletonList(
                                        data("other-update", 0, 100, 6, 1, "other")),
                                index))
                .isEmpty();
        assertThat(
                        plan(
                                Collections.singletonList(
                                        data("outside", 100, 100, 6, 1, "vector")),
                                index))
                .isEmpty();
    }

    @Test
    void testLegacyIndexIsNotRefreshedAutomatically() {
        IndexManifestEntry legacy = index("legacy", 0, 99, null, BinaryRow.EMPTY_ROW, 0);

        assertThat(plan(Collections.singletonList(data("base", 0, 100, 0, 1, "vector")), legacy))
                .isEmpty();
    }

    @Test
    void testUsesStableFieldIdAcrossRenameAndFullWrites() {
        IndexManifestEntry index = index("index", 0, 99, 5L, BinaryRow.EMPTY_ROW, 0);

        assertThat(
                        plan(
                                Collections.singletonList(
                                        data("renamed", 0, 100, 6, 2, "renamed_vector")),
                                index))
                .containsExactly(index);
        assertThat(plan(Collections.singletonList(data("full", 0, 100, 6, 1)), index))
                .containsExactly(index);
    }

    @Test
    void testRefreshesFromUpdateLayerOverBaseSchemaWithoutIndexColumn() {
        IndexManifestEntry index = index("index", 0, 99, 5L, BinaryRow.EMPTY_ROW, 0);
        ManifestEntry baseWithoutVector = data("base", 0, 100, 1, 0);
        ManifestEntry vectorUpdate = data("vector-update", 0, 100, 6, 1, "vector");

        assertThat(plan(Arrays.asList(baseWithoutVector, vectorUpdate), index))
                .containsExactly(index);
    }

    @Test
    void testHandlesSystemAndEmptyPhysicalColumns() {
        IndexManifestEntry index = index("index", 0, 99, 5L, BinaryRow.EMPTY_ROW, 0);

        assertThat(
                        plan(
                                Collections.singletonList(
                                        data(
                                                "vector-with-system-fields",
                                                0,
                                                100,
                                                6,
                                                1,
                                                SpecialFields.ROW_ID.name(),
                                                "vector",
                                                SpecialFields.SEQUENCE_NUMBER.name())),
                                index))
                .containsExactly(index);
        assertThat(
                        plan(
                                Collections.singletonList(
                                        data(
                                                "system-only",
                                                0,
                                                100,
                                                6,
                                                1,
                                                SpecialFields.ROW_ID.name(),
                                                SpecialFields.SEQUENCE_NUMBER.name())),
                                index))
                .isEmpty();
        assertThat(
                        plan(
                                Collections.singletonList(
                                        dataWithWriteCols(
                                                "empty", 0, 100, 6, 1, Collections.emptyList())),
                                index))
                .isEmpty();
    }

    @Test
    void testMultiColumnIndexRefreshesForEitherIndexedField() {
        List<DataField> indexedFields = Arrays.asList(VECTOR_FIELD, OTHER_FIELD);
        IndexManifestEntry index =
                index(
                        "multi-column",
                        0,
                        99,
                        5L,
                        BinaryRow.EMPTY_ROW,
                        0,
                        new int[] {OTHER_FIELD.id()});

        assertThat(
                        plan(
                                Collections.singletonList(
                                        data("vector-update", 0, 100, 6, 1, "vector")),
                                Collections.singletonList(index),
                                indexedFields))
                .containsExactly(index);
        assertThat(
                        plan(
                                Collections.singletonList(
                                        data("extra-field-update", 0, 100, 6, 1, "other")),
                                Collections.singletonList(index),
                                indexedFields))
                .containsExactly(index);
        assertThat(
                        plan(
                                Collections.singletonList(
                                        data("unrelated-update", 0, 100, 6, 1, "unrelated")),
                                Collections.singletonList(index),
                                indexedFields))
                .isEmpty();
    }

    @Test
    void testIsolatesPartitionAndBucket() {
        BinaryRow partition = BinaryRow.singleColumn(1);
        IndexManifestEntry index = index("index", 0, 99, 5L, partition, 2);

        ManifestEntry wrongPartition =
                data("wrong-partition", 0, 100, 6, 1, BinaryRow.singleColumn(2), 2, "vector");
        ManifestEntry wrongBucket = data("wrong-bucket", 0, 100, 6, 1, partition, 1, "vector");
        assertThat(plan(Arrays.asList(wrongPartition, wrongBucket), index)).isEmpty();

        ManifestEntry matching = data("matching", 0, 100, 6, 1, partition, 2, "vector");
        assertThat(plan(Collections.singletonList(matching), index)).containsExactly(index);
    }

    @Test
    void testSequenceSweepHandlesOverlappingRangesAndPreservesOrder() {
        IndexManifestEntry equalSequence = index("equal", 20, 29, 5L, BinaryRow.EMPTY_ROW, 0);
        IndexManifestEntry boundary = index("boundary", 10, 19, 6L, BinaryRow.EMPTY_ROW, 0);
        IndexManifestEntry first = index("first", 0, 9, 8L, BinaryRow.EMPTY_ROW, 0);

        List<ManifestEntry> dataEntries =
                Arrays.asList(
                        data("wide-equal", 0, 30, 5, 1, "vector"),
                        data("high-outside", 30, 10, 100, 1, "vector"),
                        data("boundary-update", 9, 2, 7, 1, "vector"),
                        data("first-update", 0, 10, 9, 1, "vector"));

        assertThat(plan(dataEntries, Arrays.asList(equalSequence, boundary, first)))
                .containsExactly(boundary, first);
    }

    @Test
    void testSequenceSweepMatchesBruteForceForOverlappingRanges() {
        Random random = new Random(123456L);
        for (int round = 0; round < 10; round++) {
            List<ManifestEntry> dataEntries = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                dataEntries.add(
                        data(
                                "data-" + round + "-" + i,
                                random.nextInt(1000),
                                random.nextInt(100) + 1,
                                random.nextInt(25),
                                1,
                                "vector"));
            }

            List<IndexManifestEntry> indexEntries = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                long from = random.nextInt(1000);
                indexEntries.add(
                        index(
                                "index-" + round + "-" + i,
                                from,
                                from + random.nextInt(100),
                                (long) random.nextInt(24) + 1,
                                BinaryRow.EMPTY_ROW,
                                0));
            }
            Collections.shuffle(dataEntries, random);
            Collections.shuffle(indexEntries, random);

            assertThat(plan(dataEntries, indexEntries))
                    .containsExactlyElementsOf(bruteForcePlan(dataEntries, indexEntries));
        }
    }

    private List<IndexManifestEntry> plan(
            List<ManifestEntry> dataEntries, IndexManifestEntry indexEntry) {
        return plan(dataEntries, Collections.singletonList(indexEntry));
    }

    private List<IndexManifestEntry> plan(
            List<ManifestEntry> dataEntries, List<IndexManifestEntry> indexEntries) {
        return plan(dataEntries, indexEntries, Collections.singletonList(VECTOR_FIELD));
    }

    private List<IndexManifestEntry> plan(
            List<ManifestEntry> dataEntries,
            List<IndexManifestEntry> indexEntries,
            List<DataField> indexedFields) {
        return DataEvolutionGlobalIndexRefreshPlanner.findIndexesToRefresh(
                schemaManager, dataEntries, indexEntries, indexedFields);
    }

    private List<IndexManifestEntry> bruteForcePlan(
            List<ManifestEntry> dataEntries, List<IndexManifestEntry> indexEntries) {
        List<IndexManifestEntry> result = new ArrayList<>();
        for (IndexManifestEntry indexEntry : indexEntries) {
            GlobalIndexMeta indexMeta = indexEntry.indexFile().globalIndexMeta();
            long scanSnapshotId =
                    DataEvolutionIndexSourceMeta.deserialize(indexMeta.sourceMeta())
                            .scanSnapshotId();
            for (ManifestEntry dataEntry : dataEntries) {
                DataFileMeta file = dataEntry.file();
                if (file.maxSequenceNumber() > scanSnapshotId
                        && file.nonNullRowIdRange().hasIntersection(indexMeta.rowRange())) {
                    result.add(indexEntry);
                    break;
                }
            }
        }
        return result;
    }

    private IndexManifestEntry index(
            String fileName,
            long from,
            long to,
            Long scanSnapshotId,
            BinaryRow partition,
            int bucket) {
        return index(fileName, from, to, scanSnapshotId, partition, bucket, null);
    }

    private IndexManifestEntry index(
            String fileName,
            long from,
            long to,
            Long scanSnapshotId,
            BinaryRow partition,
            int bucket,
            int[] extraFieldIds) {
        byte[] sourceMeta =
                scanSnapshotId == null
                        ? null
                        : new DataEvolutionIndexSourceMeta(scanSnapshotId).serialize();
        return new IndexManifestEntry(
                FileKind.ADD,
                partition,
                bucket,
                new IndexFileMeta(
                        "lumina",
                        fileName,
                        1L,
                        to - from + 1,
                        new GlobalIndexMeta(
                                from, to, VECTOR_FIELD.id(), extraFieldIds, null, sourceMeta),
                        null));
    }

    private ManifestEntry data(
            String fileName,
            long firstRowId,
            long rowCount,
            long maxSequenceNumber,
            long schemaId,
            String... writeCols) {
        return data(
                fileName,
                firstRowId,
                rowCount,
                maxSequenceNumber,
                schemaId,
                BinaryRow.EMPTY_ROW,
                0,
                writeCols);
    }

    private ManifestEntry data(
            String fileName,
            long firstRowId,
            long rowCount,
            long maxSequenceNumber,
            long schemaId,
            BinaryRow partition,
            int bucket,
            String... writeCols) {
        List<String> physicalColumns = writeCols.length == 0 ? null : Arrays.asList(writeCols);
        DataFileMeta file =
                DataFileMeta.forAppend(
                        fileName,
                        1L,
                        rowCount,
                        SimpleStats.EMPTY_STATS,
                        maxSequenceNumber,
                        maxSequenceNumber,
                        schemaId,
                        Collections.emptyList(),
                        null,
                        null,
                        null,
                        null,
                        firstRowId,
                        physicalColumns);
        return ManifestEntry.create(FileKind.ADD, partition, bucket, 1, file);
    }

    private ManifestEntry dataWithWriteCols(
            String fileName,
            long firstRowId,
            long rowCount,
            long maxSequenceNumber,
            long schemaId,
            List<String> writeCols) {
        DataFileMeta file =
                DataFileMeta.forAppend(
                        fileName,
                        1L,
                        rowCount,
                        SimpleStats.EMPTY_STATS,
                        maxSequenceNumber,
                        maxSequenceNumber,
                        schemaId,
                        Collections.emptyList(),
                        null,
                        null,
                        null,
                        null,
                        firstRowId,
                        writeCols);
        return ManifestEntry.create(FileKind.ADD, BinaryRow.EMPTY_ROW, 0, 1, file);
    }
}
