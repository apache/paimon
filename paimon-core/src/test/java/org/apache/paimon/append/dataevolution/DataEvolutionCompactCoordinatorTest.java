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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.stats.StatsTestUtils;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DataEvolutionCompactCoordinator.CompactPlanner}. */
public class DataEvolutionCompactCoordinatorTest {

    @Test
    public void testCompactPlannerSingleFile() {
        // Single file should not produce compaction tasks
        List<ManifestEntry> entries = new ArrayList<>();
        entries.add(makeEntry("file1.parquet", 0L, 100L, 100));

        DataEvolutionCompactCoordinator.CompactPlanner planner =
                new DataEvolutionCompactCoordinator.CompactPlanner(
                        false, 128 * 1024 * 1024, 4 * 1024 * 1024, 2);

        List<DataEvolutionCompactTask> tasks = planner.compactPlan(entries);

        assertThat(tasks).isEmpty();
    }

    @Test
    public void testCompactPlannerContiguousFiles() {
        // Multiple contiguous files should be grouped together
        List<ManifestEntry> entries = new ArrayList<>();
        entries.add(makeEntry("file1.parquet", 0L, 100L, 100));
        entries.add(makeEntry("file2.parquet", 100L, 100L, 100));
        entries.add(makeEntry("file3.parquet", 200L, 100L, 100));

        // Use small target file size to trigger compaction
        DataEvolutionCompactCoordinator.CompactPlanner planner =
                new DataEvolutionCompactCoordinator.CompactPlanner(false, 199, 1, 2);

        List<DataEvolutionCompactTask> tasks = planner.compactPlan(entries);

        assertThat(tasks).isNotEmpty();
        assertThat(tasks.get(0).compactBefore())
                .containsExactly(entries.get(0).file(), entries.get(1).file());

        planner = new DataEvolutionCompactCoordinator.CompactPlanner(false, 200, 1, 2);
        tasks = planner.compactPlan(entries);
        assertThat(tasks).isNotEmpty();
        assertThat(tasks.get(0).compactBefore())
                .containsExactly(
                        entries.get(0).file(), entries.get(1).file(), entries.get(2).file());
    }

    @Test
    public void testCompactPlannerWithRowIdGap() {
        // Files with a gap in row IDs should trigger compaction of previous group
        List<ManifestEntry> entries = new ArrayList<>();
        entries.add(makeEntry("file1.parquet", 0L, 100L, 100));
        entries.add(makeEntry("file2.parquet", 100L, 100L, 100));
        // Gap: row IDs 200-999 are missing
        entries.add(makeEntry("file3.parquet", 1000L, 100L, 100));
        entries.add(makeEntry("file3.parquet", 1100L, 100L, 100));

        // Use large target file size so compaction is triggered by gap, not size
        DataEvolutionCompactCoordinator.CompactPlanner planner =
                new DataEvolutionCompactCoordinator.CompactPlanner(
                        false, 128 * 1024 * 1024, 4 * 1024 * 1024, 2);

        List<DataEvolutionCompactTask> tasks = planner.compactPlan(entries);

        // Gap should trigger compaction of the first group (file1 + file2)
        assertThat(tasks).hasSize(2);
        assertThat(tasks.get(0).compactBefore()).hasSize(2);
        assertThat(tasks.get(1).compactBefore()).hasSize(2);
    }

    @Test
    public void testCompactPlannerSkipsLargeFiles() {
        // Files larger than target size should be skipped
        List<ManifestEntry> entries = new ArrayList<>();
        // This file is larger than target
        entries.add(makeEntryWithSize("large.parquet", 0L, 100L, 100, 200 * 1024 * 1024));
        entries.add(makeEntry("file1.parquet", 100L, 100L, 100));
        entries.add(makeEntry("file2.parquet", 200L, 100L, 100));
        entries.add(makeEntryWithSize("large2.parquet", 300L, 100L, 100, 200 * 1024 * 1024));
        entries.add(makeEntryWithSize("large2-1.blob", 300L, 50L, 100, 200 * 1024 * 1024));
        entries.add(makeEntryWithSize("large2-2.blob", 350L, 50L, 100, 200 * 1024 * 1024));
        entries.add(makeEntry("file3.parquet", 400L, 100L, 100));
        entries.add(makeEntry("file4.parquet", 500L, 100L, 100));
        entries.add(makeEntry(BinaryRow.singleColumn(0), "file5.parquet", 600L, 100L, 100));
        entries.add(makeEntry(BinaryRow.singleColumn(0), "file6.parquet", 700L, 100L, 100));
        entries.add(makeEntry(BinaryRow.singleColumn(1), "file7.parquet", 800L, 100L, 100));
        entries.add(makeEntry(BinaryRow.singleColumn(1), "file8.parquet", 900L, 100L, 100));

        DataEvolutionCompactCoordinator.CompactPlanner planner =
                new DataEvolutionCompactCoordinator.CompactPlanner(
                        false, 100 * 1024 * 1024, 4 * 1024 * 1024, 2);

        List<DataEvolutionCompactTask> tasks = planner.compactPlan(entries);

        assertThat(tasks.size()).isEqualTo(4);
        assertThat(tasks.get(0).compactBefore())
                .containsExactly(entries.get(1).file(), entries.get(2).file());
        assertThat(tasks.get(1).compactBefore())
                .containsExactly(entries.get(6).file(), entries.get(7).file());
        assertThat(tasks.get(2).compactBefore())
                .containsExactly(entries.get(8).file(), entries.get(9).file());
        assertThat(tasks.get(3).compactBefore())
                .containsExactly(entries.get(10).file(), entries.get(11).file());
    }

    @Test
    public void testCompactPlannerWithBlobFiles() {
        // Test blob file compaction when enabled
        List<ManifestEntry> entries = new ArrayList<>();
        entries.add(makeEntry("file1.parquet", 0L, 100L, 100));
        entries.add(makeBlobEntry("file1.blob", 0L, 100L, 100));
        entries.add(makeBlobEntry("file1b.blob", 0L, 100L, 100));
        entries.add(makeEntry("file2.parquet", 100L, 100L, 100));
        entries.add(makeBlobEntry("file2.blob", 100L, 100L, 100));
        entries.add(makeBlobEntry("file2b.blob", 100L, 100L, 100));

        // Use small target to trigger compaction, with blob compaction enabled
        DataEvolutionCompactCoordinator.CompactPlanner planner =
                new DataEvolutionCompactCoordinator.CompactPlanner(true, 1024, 1024, 2);

        List<DataEvolutionCompactTask> tasks = planner.compactPlan(entries);

        // Should have compaction tasks for both data files and blob files
        assertThat(tasks.size()).isEqualTo(2);

        assertThat(tasks.get(0).compactBefore())
                .containsExactly(entries.get(0).file(), entries.get(3).file());
        assertThat(tasks.get(1).compactBefore())
                .containsExactly(
                        entries.get(1).file(),
                        entries.get(2).file(),
                        entries.get(4).file(),
                        entries.get(5).file());
    }

    private ManifestEntry makeEntry(
            String fileName, long firstRowId, long rowCount, long fileSize) {
        return makeEntryWithSize(
                BinaryRow.EMPTY_ROW, fileName, firstRowId, rowCount, fileSize, fileSize);
    }

    private ManifestEntry makeEntry(
            BinaryRow partition, String fileName, long firstRowId, long rowCount, long fileSize) {
        return makeEntryWithSize(partition, fileName, firstRowId, rowCount, fileSize, fileSize);
    }

    private ManifestEntry makeEntryWithSize(
            String fileName, long firstRowId, long rowCount, long minSeq, long fileSize) {
        return makeEntryWithSize(
                BinaryRow.EMPTY_ROW, fileName, firstRowId, rowCount, minSeq, fileSize);
    }

    private ManifestEntry makeEntryWithSize(
            BinaryRow partition,
            String fileName,
            long firstRowId,
            long rowCount,
            long minSeq,
            long fileSize) {
        return ManifestEntry.create(
                FileKind.ADD,
                partition,
                0,
                0,
                createDataFileMeta(fileName, firstRowId, rowCount, minSeq, fileSize));
    }

    private ManifestEntry makeBlobEntry(
            String fileName, long firstRowId, long rowCount, long fileSize) {
        // Blob files have .blob extension
        String blobFileName = fileName.endsWith(".blob") ? fileName : fileName + ".blob";
        return ManifestEntry.create(
                FileKind.ADD,
                BinaryRow.EMPTY_ROW,
                0,
                0,
                createDataFileMeta(blobFileName, firstRowId, rowCount, 0, fileSize));
    }

    private DataFileMeta createDataFileMeta(
            String fileName, long firstRowId, long rowCount, long maxSeq, long fileSize) {
        return DataFileMeta.create(
                fileName,
                fileSize,
                rowCount,
                BinaryRow.EMPTY_ROW,
                BinaryRow.EMPTY_ROW,
                StatsTestUtils.newEmptySimpleStats(),
                StatsTestUtils.newEmptySimpleStats(),
                0,
                maxSeq,
                0,
                0,
                Collections.emptyList(),
                Timestamp.fromEpochMillis(System.currentTimeMillis()),
                0L,
                null,
                FileSource.APPEND,
                null,
                null,
                firstRowId,
                null);
    }

    @Test
    public void testSerializerBasic() throws IOException {
        DataEvolutionCompactTaskSerializer serializer = new DataEvolutionCompactTaskSerializer();

        List<DataFileMeta> files =
                Arrays.asList(
                        createDataFileMeta("file1.parquet", 0L, 100L, 0, 1024),
                        createDataFileMeta("file2.parquet", 100L, 100L, 0, 1024));

        DataEvolutionCompactTask task =
                new DataEvolutionCompactTask(BinaryRow.EMPTY_ROW, files, false);

        byte[] bytes = serializer.serialize(task);
        DataEvolutionCompactTask deserialized =
                serializer.deserialize(serializer.getVersion(), bytes);

        assertThat(deserialized).isEqualTo(task);
    }

    @Test
    public void testSerializerBlobTask() throws IOException {
        DataEvolutionCompactTaskSerializer serializer = new DataEvolutionCompactTaskSerializer();

        List<DataFileMeta> files =
                Arrays.asList(
                        createDataFileMeta("file1.blob", 0L, 100L, 0, 1024),
                        createDataFileMeta("file2.blob", 0L, 100L, 0, 1024));

        DataEvolutionCompactTask task =
                new DataEvolutionCompactTask(BinaryRow.EMPTY_ROW, files, true);

        byte[] bytes = serializer.serialize(task);
        DataEvolutionCompactTask deserialized =
                serializer.deserialize(serializer.getVersion(), bytes);

        assertThat(deserialized).isEqualTo(task);
        assertThat(deserialized.isBlobTask()).isTrue();
    }

    @Test
    public void testSerializerWithPartition() throws IOException {
        DataEvolutionCompactTaskSerializer serializer = new DataEvolutionCompactTaskSerializer();

        List<DataFileMeta> files =
                Arrays.asList(
                        createDataFileMeta("file1.parquet", 0L, 100L, 0, 1024),
                        createDataFileMeta("file2.parquet", 100L, 100L, 0, 1024));

        BinaryRow partition = BinaryRow.singleColumn(42);
        DataEvolutionCompactTask task = new DataEvolutionCompactTask(partition, files, false);

        byte[] bytes = serializer.serialize(task);
        DataEvolutionCompactTask deserialized =
                serializer.deserialize(serializer.getVersion(), bytes);

        assertThat(deserialized).isEqualTo(task);
        assertThat(deserialized.partition()).isEqualTo(partition);
    }
}
