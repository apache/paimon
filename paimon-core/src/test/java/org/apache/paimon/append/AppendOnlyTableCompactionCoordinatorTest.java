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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;

import static org.apache.paimon.mergetree.compact.MergeTreeCompactManagerTest.row;
import static org.apache.paimon.stats.StatsTestUtils.newTableStats;

/** Tests for {@link AppendOnlyTableCompactionCoordinator}. */
public class AppendOnlyTableCompactionCoordinatorTest {

    @TempDir Path tempDir;
    private AppendOnlyFileStoreTable appendOnlyFileStoreTable;
    private AppendOnlyTableCompactionCoordinator compactionCoordinator;
    private BinaryRow partition;

    @Test
    public void testForCompactPlan() {
        List<DataFileMeta> files = generateNewFiles(200, 0);
        assertTasks(files, 200 / 6);
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
                        100, appendOnlyFileStoreTable.coreOptions().targetFileSize() / 3 + 1);
        assertTasks(files, 100 / 3);
    }

    @Test
    public void testFilterMiddleFile() {
        List<DataFileMeta> files =
                generateNewFiles(
                        100, appendOnlyFileStoreTable.coreOptions().targetFileSize() / 10 * 8);
        assertTasks(files, 0);
    }

    @Test
    public void testEliminatePartitionCoordinator() {
        List<DataFileMeta> files = generateNewFiles(1, 0);
        compactionCoordinator.notifyNewFiles(partition, files);

        for (int i = 0; i < AppendOnlyTableCompactionCoordinator.REMOVE_AGE; i++) {
            Assertions.assertEquals(compactionCoordinator.compactPlan().size(), 0);
            Assertions.assertEquals(compactionCoordinator.partitionCompactCoordinators.size(), 1);
        }

        // age enough, eliminate partitionCoordinator
        Assertions.assertEquals(compactionCoordinator.compactPlan().size(), 0);
        Assertions.assertEquals(compactionCoordinator.partitionCompactCoordinators.size(), 0);
    }

    @Test
    public void testCompactLessFile() {
        List<DataFileMeta> files = generateNewFiles(2, 0);
        compactionCoordinator.notifyNewFiles(partition, files);

        for (int i = 0; i < AppendOnlyTableCompactionCoordinator.COMPACT_AGE; i++) {
            Assertions.assertEquals(compactionCoordinator.compactPlan().size(), 0);
            Assertions.assertEquals(compactionCoordinator.partitionCompactCoordinators.size(), 1);
        }

        // age enough, generate less file comaction
        List<AppendOnlyCompactionTask> tasks = compactionCoordinator.compactPlan();
        Assertions.assertEquals(tasks.size(), 1);
        Assertions.assertIterableEquals(new HashSet<>(files), tasks.get(0).compactBefore());
        Assertions.assertEquals(compactionCoordinator.partitionCompactCoordinators.size(), 0);
    }

    @Test
    public void testAgeGrowUp() {
        List<DataFileMeta> files = generateNewFiles(1, 0);
        compactionCoordinator.notifyNewFiles(partition, files);

        for (int i = 0; i < AppendOnlyTableCompactionCoordinator.REMOVE_AGE; i++) {
            compactionCoordinator.compactPlan();
            Assertions.assertEquals(compactionCoordinator.partitionCompactCoordinators.size(), 1);
            Assertions.assertEquals(
                    compactionCoordinator.partitionCompactCoordinators.get(partition).age, i + 1);
        }

        // clear age
        compactionCoordinator.notifyNewFiles(partition, generateNewFiles(1, 0));
        Assertions.assertEquals(compactionCoordinator.partitionCompactCoordinators.size(), 1);
        // check whether age goes to zero again
        Assertions.assertEquals(
                compactionCoordinator.partitionCompactCoordinators.get(partition).age, 0);
    }

    private void assertTasks(List<DataFileMeta> files, int taskNum) {
        compactionCoordinator.notifyNewFiles(partition, files);
        List<AppendOnlyCompactionTask> tasks = compactionCoordinator.compactPlan();
        Assertions.assertEquals(tasks.size(), taskNum);
    }

    private static Schema schema() {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.STRING());
        schemaBuilder.column("f2", DataTypes.STRING());
        schemaBuilder.column("f3", DataTypes.STRING());
        schemaBuilder.option(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "3");
        schemaBuilder.option(CoreOptions.COMPACTION_MAX_FILE_NUM.key(), "6");
        return schemaBuilder.build();
    }

    @BeforeEach
    public void createCoordinator() throws Exception {
        FileIO fileIO = new LocalFileIO();
        org.apache.paimon.fs.Path path = new org.apache.paimon.fs.Path(tempDir.toString());
        SchemaManager schemaManager = new SchemaManager(fileIO, path);
        TableSchema tableSchema = schemaManager.createTable(schema());

        appendOnlyFileStoreTable =
                (AppendOnlyFileStoreTable)
                        FileStoreTableFactory.create(
                                fileIO,
                                new org.apache.paimon.fs.Path(tempDir.toString()),
                                tableSchema);
        compactionCoordinator = new AppendOnlyTableCompactionCoordinator(appendOnlyFileStoreTable);
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
                1,
                row(0),
                row(0),
                newTableStats(0, 1),
                newTableStats(0, 1),
                0,
                0,
                0,
                0);
    }
}
