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

package org.apache.paimon.append;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.append.AppendDeleteFileMaintainer;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.EndOfScanException;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;

import static org.apache.paimon.CoreOptions.COMPACTION_MIN_FILE_NUM;
import static org.apache.paimon.CoreOptions.DELETION_VECTORS_ENABLED;
import static org.apache.paimon.mergetree.compact.MergeTreeCompactManagerTest.row;
import static org.apache.paimon.stats.StatsTestUtils.newSimpleStats;
import static org.apache.paimon.testutils.assertj.PaimonAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link AppendCompactCoordinator}. */
public class AppendCompactCoordinatorTest {

    @TempDir Path tempDir;
    private FileStoreTable appendOnlyFileStoreTable;
    private AppendCompactCoordinator compactionCoordinator;
    private BinaryRow partition;

    @Test
    public void testForCompactPlan() {
        List<DataFileMeta> files = generateNewFiles(200, 0);
        assertTasks(files, 2);
    }

    @Test
    public void testNoCompactTask() {
        List<DataFileMeta> files = generateNewFiles(100, Long.MAX_VALUE);
        assertTasks(files, 0);
    }

    @Test
    public void testMinSizeCompactTask() {
        List<DataFileMeta> files =
                generateNewFiles(
                        100, appendOnlyFileStoreTable.coreOptions().targetFileSize(false) / 3 + 1);
        assertTasks(files, 17);
    }

    @Test
    public void testFilterMiddleFile() {
        List<DataFileMeta> files =
                generateNewFiles(
                        100, appendOnlyFileStoreTable.coreOptions().targetFileSize(false) / 10 * 8);
        assertTasks(files, 0);
    }

    @Test
    public void testCompactGroupSplit() {
        List<DataFileMeta> files =
                generateNewFiles(
                        1000, appendOnlyFileStoreTable.coreOptions().targetFileSize(false) / 10);
        compactionCoordinator.notifyNewFiles(partition, files);

        assertThat(compactionCoordinator.compactPlan().size()).isEqualTo(56);
        files.clear();

        files =
                generateNewFiles(
                        1050, appendOnlyFileStoreTable.coreOptions().targetFileSize(false) / 5);
        compactionCoordinator.notifyNewFiles(partition, files);
        assertThat(compactionCoordinator.compactPlan().size()).isEqualTo(105);
    }

    @Test
    public void testCompactGroupSplit2() {
        List<DataFileMeta> files =
                generateNewFiles(
                        1089, appendOnlyFileStoreTable.coreOptions().targetFileSize(false) / 5);
        compactionCoordinator.notifyNewFiles(partition, files);
        assertThat(compactionCoordinator.compactPlan().size()).isEqualTo(109);
    }

    @Test
    public void testEliminatePartitionCoordinator() {
        List<DataFileMeta> files = generateNewFiles(1, 0);
        compactionCoordinator.notifyNewFiles(partition, files);

        for (int i = 0; i < AppendCompactCoordinator.REMOVE_AGE; i++) {
            assertThat(compactionCoordinator.compactPlan().size()).isEqualTo(0);
            assertThat(compactionCoordinator.subCoordinators.size()).isEqualTo(1);
        }

        // age enough, eliminate partitionCoordinator
        assertThat(compactionCoordinator.compactPlan().size()).isEqualTo(0);
        assertThat(compactionCoordinator.subCoordinators.size()).isEqualTo(0);
    }

    @Test
    public void testCompactLessFile() {
        List<DataFileMeta> files = generateNewFiles(2, 0);
        compactionCoordinator.notifyNewFiles(partition, files);

        for (int i = 0; i < AppendCompactCoordinator.COMPACT_AGE; i++) {
            assertThat(compactionCoordinator.compactPlan().size()).isEqualTo(0);
            assertThat(compactionCoordinator.subCoordinators.size()).isEqualTo(1);
        }

        // age enough, generate less file compaction
        List<AppendCompactTask> tasks = compactionCoordinator.compactPlan();
        assertThat(tasks.size()).isEqualTo(1);
        assertThat(new HashSet<>(files))
                .containsExactlyInAnyOrderElementsOf(tasks.get(0).compactBefore());
        assertThat(compactionCoordinator.subCoordinators.size()).isEqualTo(0);
    }

    @Test
    public void testAgeGrowUp() {
        List<DataFileMeta> files = generateNewFiles(1, 0);
        compactionCoordinator.notifyNewFiles(partition, files);

        for (int i = 0; i < AppendCompactCoordinator.REMOVE_AGE; i++) {
            compactionCoordinator.compactPlan();
            assertThat(compactionCoordinator.subCoordinators.size()).isEqualTo(1);
            assertThat(compactionCoordinator.subCoordinators.get(partition).age).isEqualTo(i + 1);
        }

        // clear age
        compactionCoordinator.notifyNewFiles(partition, generateNewFiles(1, 0));
        assertThat(compactionCoordinator.subCoordinators.size()).isEqualTo(1);

        // check whether age goes to zero again
        assertThat(compactionCoordinator.subCoordinators.get(partition).age).isEqualTo(0);
    }

    @Test
    public void testBatchScanEmptyTable() {
        compactionCoordinator = new AppendCompactCoordinator(appendOnlyFileStoreTable, false);
        assertThatThrownBy(() -> compactionCoordinator.scan())
                .satisfies(anyCauseMatches(EndOfScanException.class));
    }

    @Test
    public void testDeleteRatio() {
        List<DataFileMeta> files =
                generateNewFiles(2, appendOnlyFileStoreTable.coreOptions().targetFileSize(false));
        AppendDeleteFileMaintainer dvMaintainer = compactionCoordinator.dvMaintainer(partition);
        DeletionFile deletionFile = new DeletionFile("", 0, 0, 21L);
        dvMaintainer.putDeletionFile(files.get(0).fileName(), deletionFile);
        assertTasks(files, 1);
    }

    @Test
    public void testDeleteRatioNoCompact() {
        List<DataFileMeta> files =
                generateNewFiles(2, appendOnlyFileStoreTable.coreOptions().targetFileSize(false));
        AppendDeleteFileMaintainer dvMaintainer = compactionCoordinator.dvMaintainer(partition);
        DeletionFile deletionFile = new DeletionFile("", 0, 0, 11L);
        dvMaintainer.putDeletionFile(files.get(0).fileName(), deletionFile);
        assertTasks(files, 0);
    }

    @Test
    public void testDeleteRatioMergeTask() {
        List<DataFileMeta> files = generateNewFiles(100, 1);
        AppendDeleteFileMaintainer dvMaintainer = compactionCoordinator.dvMaintainer(partition);
        for (int i = 0; i < files.size(); i++) {
            DeletionFile deletionFile = new DeletionFile("" + i, 0, 0, 21L);
            dvMaintainer.putDeletionFile(files.get(i).fileName(), deletionFile);
        }

        // with merging, should be 1
        assertTasks(files, 1);
    }

    private void assertTasks(List<DataFileMeta> files, int taskNum) {
        compactionCoordinator.notifyNewFiles(partition, files);
        List<AppendCompactTask> tasks = compactionCoordinator.compactPlan();
        assertThat(tasks.size()).isEqualTo(taskNum);
    }

    private static Schema schema() {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.STRING());
        schemaBuilder.column("f2", DataTypes.STRING());
        schemaBuilder.column("f3", DataTypes.STRING());
        schemaBuilder.option(COMPACTION_MIN_FILE_NUM.key(), "3");
        schemaBuilder.option(DELETION_VECTORS_ENABLED.key(), "true");
        return schemaBuilder.build();
    }

    @BeforeEach
    public void createCoordinator() throws Exception {
        FileIO fileIO = new LocalFileIO();
        org.apache.paimon.fs.Path path = new org.apache.paimon.fs.Path(tempDir.toString());
        SchemaManager schemaManager = new SchemaManager(fileIO, path);
        TableSchema tableSchema = schemaManager.createTable(schema());

        appendOnlyFileStoreTable =
                FileStoreTableFactory.create(
                        fileIO, new org.apache.paimon.fs.Path(tempDir.toString()), tableSchema);
        compactionCoordinator = new AppendCompactCoordinator(appendOnlyFileStoreTable, true);
        partition = BinaryRow.EMPTY_ROW;
    }

    private List<DataFileMeta> generateNewFiles(int fileNum, long fileSize) {
        List<DataFileMeta> files = new ArrayList<>();
        for (int i = 0; i < fileNum; i++) {
            files.add(newFile(fileSize));
        }
        return files;
    }

    private DataFileMeta newFile(long fileSize) {
        return new DataFileMeta(
                UUID.randomUUID().toString(),
                fileSize,
                100,
                row(0),
                row(0),
                newSimpleStats(0, 1),
                newSimpleStats(0, 1),
                0,
                0,
                0,
                0,
                0L,
                null,
                FileSource.APPEND,
                null,
                null,
                null);
    }
}
