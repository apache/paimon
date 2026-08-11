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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.stats.StatsTestUtils;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Queue;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DataEvolutionCompactRangePlanner}. */
class DataEvolutionCompactRangePlannerTest extends TableTestBase {

    @Override
    protected Schema schemaDefault() {
        return Schema.newBuilder()
                .column("f0", DataTypes.INT())
                .column("f1", DataTypes.INT())
                .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                .build();
    }

    @Test
    void testPlansLiveLogicalRangesInBoundedFileBatches() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        ManifestFile manifestFile = table.store().manifestFileFactory().create();

        ManifestEntry normal0 = add("normal-0.parquet", 0L, 10L, 60L);
        ManifestEntry normal1 = add("normal-1.parquet", 10L, 10L, 60L);
        ManifestEntry deletedNormal = add("deleted.parquet", 20L, 10L, 60L);
        ManifestEntry normal3 = add("normal-3.parquet", 30L, 10L, 60L);
        ManifestEntry normal4 = add("normal-4.parquet", 40L, 10L, 60L);
        List<ManifestFileMeta> manifests =
                manifestFile.write(
                        Arrays.asList(
                                normal0,
                                normal1,
                                deletedNormal,
                                normal3,
                                normal4,
                                delete(deletedNormal)));

        Queue<DataEvolutionCompactRangePlanner.RangeBatch> batches =
                plan(
                        new DataEvolutionCompactRangePlanner(
                                manifestFile, null, 2, candidateOptions(table, false, false)),
                        manifests);

        assertThat(batches).hasSize(2);
        DataEvolutionCompactRangePlanner.RangeBatch first = batches.poll();
        assertThat(first.fileCount()).isEqualTo(2);
        assertThat(first.toRanges()).containsExactly(new Range(0L, 19L));
        DataEvolutionCompactRangePlanner.RangeBatch second = batches.poll();
        assertThat(second.fileCount()).isEqualTo(2);
        assertThat(second.toRanges()).containsExactly(new Range(30L, 49L));
    }

    @Test
    void testDoesNotSplitOneLogicalRangeAtBatchBoundary() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        ManifestFile manifestFile = table.store().manifestFileFactory().create();

        List<ManifestEntry> entries = new ArrayList<>();
        entries.add(add("normal-0.parquet", 0L, 10L, 60L));
        entries.add(add("updated-0.parquet", 0L, 10L, 60L));
        entries.add(add("newer-0.parquet", 0L, 10L, 60L));
        List<ManifestFileMeta> manifests = manifestFile.write(entries);

        Queue<DataEvolutionCompactRangePlanner.RangeBatch> batches =
                plan(
                        new DataEvolutionCompactRangePlanner(
                                manifestFile, null, 2, candidateOptions(table, false, false)),
                        manifests);

        assertThat(batches).hasSize(1);
        DataEvolutionCompactRangePlanner.RangeBatch batch = batches.poll();
        assertThat(batch.fileCount()).isEqualTo(3);
        assertThat(batch.toRanges()).containsExactly(new Range(0L, 9L));
    }

    @Test
    void testPreservesLogicalRangesInsideOneBatch() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        ManifestFile manifestFile = table.store().manifestFileFactory().create();

        List<ManifestFileMeta> manifests =
                manifestFile.write(
                        Arrays.asList(
                                add("normal-0.parquet", 0L, 10L, 200L),
                                add("updated-0.parquet", 0L, 10L, 200L),
                                add("normal-2.parquet", 20L, 10L, 200L),
                                add("updated-2.parquet", 20L, 10L, 200L)));

        Queue<DataEvolutionCompactRangePlanner.RangeBatch> batches =
                plan(
                        new DataEvolutionCompactRangePlanner(
                                manifestFile, null, 4, candidateOptions(table, false, false)),
                        manifests);

        assertThat(batches).hasSize(1);
        DataEvolutionCompactRangePlanner.RangeBatch batch = batches.poll();
        assertThat(batch.fileCount()).isEqualTo(4);
        assertThat(batch.toRanges()).containsExactly(new Range(0L, 9L), new Range(20L, 29L));
    }

    @Test
    void testOneBatchCarriesAllManifestGroupsNeededForOneScan() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        ManifestFile manifestFile = table.store().manifestFileFactory().create();

        List<ManifestFileMeta> firstManifest =
                manifestFile.write(
                        Arrays.asList(
                                add("normal-0.parquet", 0L, 10L, 200L),
                                add("updated-0.parquet", 0L, 10L, 200L)));
        List<ManifestFileMeta> secondManifest =
                manifestFile.write(
                        Arrays.asList(
                                add("normal-2.parquet", 20L, 10L, 200L),
                                add("updated-2.parquet", 20L, 10L, 200L)));
        List<ManifestFileMeta> manifests = new ArrayList<>();
        manifests.addAll(firstManifest);
        manifests.addAll(secondManifest);

        Queue<DataEvolutionCompactRangePlanner.RangeBatch> batches =
                plan(
                        new DataEvolutionCompactRangePlanner(
                                manifestFile, null, 4, candidateOptions(table, false, false)),
                        manifests);

        assertThat(batches).hasSize(1);
        DataEvolutionCompactRangePlanner.RangeBatch batch = batches.poll();
        assertThat(batch.fileCount()).isEqualTo(4);
        assertThat(batch.manifestFiles()).containsExactlyElementsOf(manifests);
        assertThat(batch.toRanges()).containsExactly(new Range(0L, 9L), new Range(20L, 29L));
    }

    @Test
    void testPlansOnlyRangesWhichCanProduceNormalCompaction() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        ManifestFile manifestFile = table.store().manifestFileFactory().create();

        List<ManifestFileMeta> manifests =
                manifestFile.write(
                        Arrays.asList(
                                add("large-0.parquet", 0L, 10L, 200L),
                                add("small-1.parquet", 20L, 10L, 60L),
                                add("small-2.parquet", 30L, 10L, 60L),
                                add("large-3.parquet", 50L, 10L, 200L)));

        DataEvolutionCompactRangePlanner.CandidateOptions options =
                candidateOptions(table, false, false);
        Queue<DataEvolutionCompactRangePlanner.RangeBatch> batches =
                plan(
                        new DataEvolutionCompactRangePlanner(manifestFile, null, 10, options),
                        manifests);

        assertThat(batches).hasSize(1);
        DataEvolutionCompactRangePlanner.RangeBatch batch = batches.poll();
        assertThat(batch.fileCount()).isEqualTo(2);
        assertThat(batch.toRanges()).containsExactly(new Range(20L, 39L));
    }

    @Test
    void testPlansBlobCandidatesByFieldAndFileSize() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        ManifestFile manifestFile = table.store().manifestFileFactory().create();

        List<ManifestFileMeta> manifests =
                manifestFile.write(
                        Arrays.asList(
                                add("normal-0.parquet", 0L, 20L, 200L),
                                blob("f0-0.blob", 0L, 10L, 40L, "f0"),
                                blob("f0-1.blob", 10L, 10L, 40L, "f0"),
                                add("normal-1.parquet", 30L, 20L, 200L),
                                blob("different-0.blob", 30L, 10L, 40L, "f0"),
                                blob("different-1.blob", 40L, 10L, 40L, "f1")));

        Queue<DataEvolutionCompactRangePlanner.RangeBatch> batches =
                plan(
                        new DataEvolutionCompactRangePlanner(
                                manifestFile, null, 10, candidateOptions(table, true, false)),
                        manifests);

        assertThat(batches).hasSize(1);
        DataEvolutionCompactRangePlanner.RangeBatch batch = batches.poll();
        assertThat(batch.fileCount()).isEqualTo(3);
        assertThat(batch.toRanges()).containsExactly(new Range(0L, 19L));
    }

    @Test
    void testPlansUpdatedBlobFilesSpanningAdjacentNormalRanges() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        ManifestFile manifestFile = table.store().manifestFileFactory().create();

        List<ManifestFileMeta> manifests =
                manifestFile.write(
                        Arrays.asList(
                                add("normal-0.parquet", 0L, 10L, 200L),
                                add("normal-1.parquet", 10L, 10L, 200L),
                                blob("old.blob", 5L, 10L, 40L, "f0"),
                                blob("updated.blob", 5L, 10L, 40L, "f0")));

        Queue<DataEvolutionCompactRangePlanner.RangeBatch> batches =
                plan(
                        new DataEvolutionCompactRangePlanner(
                                manifestFile, null, 10, candidateOptions(table, true, false)),
                        manifests);

        assertThat(batches).hasSize(1);
        DataEvolutionCompactRangePlanner.RangeBatch batch = batches.poll();
        assertThat(batch.fileCount()).isEqualTo(3);
        assertThat(batch.toRanges()).containsExactly(new Range(0L, 9L));
    }

    @Test
    void testPlansNormalCompactionWithIgnoredBlobSpanningAdjacentRanges() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        ManifestFile manifestFile = table.store().manifestFileFactory().create();

        List<ManifestFileMeta> manifests =
                manifestFile.write(
                        Arrays.asList(
                                add("normal-0.parquet", 0L, 10L, 40L),
                                add("normal-1.parquet", 10L, 10L, 40L),
                                blob("ignored.blob", 5L, 10L, 40L, "f0")));

        Queue<DataEvolutionCompactRangePlanner.RangeBatch> batches =
                plan(
                        new DataEvolutionCompactRangePlanner(
                                manifestFile, null, 10, candidateOptions(table, false, false)),
                        manifests);

        assertThat(batches).hasSize(1);
        DataEvolutionCompactRangePlanner.RangeBatch batch = batches.poll();
        assertThat(batch.fileCount()).isEqualTo(3);
        assertThat(batch.toRanges()).containsExactly(new Range(0L, 19L));
    }

    @Test
    void testDeletedFileCannotCreateCandidate() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        ManifestFile manifestFile = table.store().manifestFileFactory().create();
        ManifestEntry first = add("first.parquet", 0L, 10L, 60L);
        ManifestEntry deleted = add("deleted.parquet", 10L, 10L, 60L);
        List<ManifestFileMeta> manifests =
                manifestFile.write(Arrays.asList(first, deleted, delete(deleted)));

        Queue<DataEvolutionCompactRangePlanner.RangeBatch> batches =
                plan(
                        new DataEvolutionCompactRangePlanner(
                                manifestFile, null, 10, candidateOptions(table, false, false)),
                        manifests);

        assertThat(batches).isEmpty();
    }

    @Test
    void testPlansVectorCandidatesWithinTheirNormalRange() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        ManifestFile manifestFile = table.store().manifestFileFactory().create();
        List<ManifestFileMeta> manifests =
                manifestFile.write(
                        Arrays.asList(
                                add("normal.parquet", 0L, 10L, 200L),
                                add("first.vector.json", 0L, 10L, 200L),
                                add("second.vector.json", 0L, 10L, 200L)));

        Queue<DataEvolutionCompactRangePlanner.RangeBatch> batches =
                plan(
                        new DataEvolutionCompactRangePlanner(
                                manifestFile, null, 10, candidateOptions(table, false, true)),
                        manifests);

        assertThat(batches).hasSize(1);
        DataEvolutionCompactRangePlanner.RangeBatch batch = batches.poll();
        assertThat(batch.fileCount()).isEqualTo(3);
        assertThat(batch.toRanges()).containsExactly(new Range(0L, 9L));
    }

    private DataEvolutionCompactRangePlanner.CandidateOptions candidateOptions(
            FileStoreTable table, boolean compactBlob, boolean compactVector) {
        return new DataEvolutionCompactRangePlanner.CandidateOptions(
                compactBlob,
                compactVector,
                100L,
                100L,
                1L,
                2L,
                schemaId -> table.schema().logicalRowType(),
                null);
    }

    private Queue<DataEvolutionCompactRangePlanner.RangeBatch> plan(
            DataEvolutionCompactRangePlanner planner, List<ManifestFileMeta> manifests) {
        Queue<DataEvolutionCompactRangePlanner.RangeBatch> batches = new ArrayDeque<>();
        planner.plan(manifests).forEachRemaining(batches::add);
        return batches;
    }

    private ManifestEntry add(String fileName, long firstRowId, long rowCount) {
        return add(fileName, firstRowId, rowCount, 100L);
    }

    private ManifestEntry add(String fileName, long firstRowId, long rowCount, long fileSize) {
        return ManifestEntry.create(
                FileKind.ADD,
                BinaryRow.EMPTY_ROW,
                0,
                0,
                dataFile(fileName, firstRowId, rowCount, fileSize));
    }

    private ManifestEntry delete(ManifestEntry add) {
        return ManifestEntry.create(
                FileKind.DELETE, add.partition(), add.bucket(), add.totalBuckets(), add.file());
    }

    private ManifestEntry blob(
            String fileName, long firstRowId, long rowCount, long fileSize, String fieldName) {
        return ManifestEntry.create(
                FileKind.ADD,
                BinaryRow.EMPTY_ROW,
                0,
                0,
                dataFile(
                        fileName,
                        firstRowId,
                        rowCount,
                        fileSize,
                        Collections.singletonList(fieldName)));
    }

    private DataFileMeta dataFile(String fileName, long firstRowId, long rowCount) {
        return dataFile(fileName, firstRowId, rowCount, 100L);
    }

    private DataFileMeta dataFile(String fileName, long firstRowId, long rowCount, long fileSize) {
        return dataFile(fileName, firstRowId, rowCount, fileSize, null);
    }

    private DataFileMeta dataFile(
            String fileName,
            long firstRowId,
            long rowCount,
            long fileSize,
            List<String> writeColumns) {
        return DataFileMeta.create(
                fileName,
                fileSize,
                rowCount,
                BinaryRow.EMPTY_ROW,
                BinaryRow.EMPTY_ROW,
                StatsTestUtils.newEmptySimpleStats(),
                StatsTestUtils.newEmptySimpleStats(),
                0L,
                0L,
                0L,
                0,
                Collections.emptyList(),
                Timestamp.fromEpochMillis(0L),
                0L,
                null,
                FileSource.APPEND,
                null,
                null,
                firstRowId,
                writeColumns);
    }
}
