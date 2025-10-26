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

package org.apache.paimon.append.cluster;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.mergetree.LevelSortedRun;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link IncrementalClusterManager}. */
public class IncrementalClusterManagerTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testNonUnAwareBucketTable() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BUCKET.key(), "1");
        options.put(CoreOptions.BUCKET_KEY.key(), "f0");

        assertThatThrownBy(() -> createTable(options, Collections.emptyList()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Cannot define bucket for incremental clustering  table, it only support bucket = -1");
    }

    @Test
    public void testNonClusterIncremental() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BUCKET.key(), "-1");
        options.put(CoreOptions.CLUSTERING_INCREMENTAL.key(), "false");
        FileStoreTable table = createTable(options, Collections.emptyList());
        assertThatThrownBy(() -> new IncrementalClusterManager(table))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Only support incremental clustering when 'clustering.incremental' is true.");
    }

    @Test
    public void testConstructPartitionLevels() throws Exception {
        // Create a valid table for IncrementalClusterManager
        Map<String, String> options = new HashMap<>();
        FileStoreTable table = createTable(options, Collections.emptyList());
        IncrementalClusterManager incrementalClusterManager = new IncrementalClusterManager(table);

        // Create test files with different levels
        List<DataFileMeta> partitionFiles = new ArrayList<>();

        // Level 0 files (should be individual LevelSortedRuns)
        DataFileMeta level0File1 = createFile(100, 1, 0);
        DataFileMeta level0File2 = createFile(200, 1, 0);
        partitionFiles.add(level0File1);
        partitionFiles.add(level0File2);

        // Level 1 files (should be grouped into one LevelSortedRun)
        DataFileMeta level1File1 = createFile(300, 1, 1);
        DataFileMeta level1File2 = createFile(400, 1, 1);
        partitionFiles.add(level1File1);
        partitionFiles.add(level1File2);

        // Level 2 files (should be grouped into one LevelSortedRun)
        DataFileMeta level2File1 = createFile(500, 1, 2);
        partitionFiles.add(level2File1);

        // Call the method under test
        List<LevelSortedRun> result =
                incrementalClusterManager.constructPartitionLevels(partitionFiles);

        // Verify the results
        assertThat(result).hasSize(4); // 2 level-0 runs + 1 level-1 run + 1 level-2 run

        // Verify sorting by level
        assertThat(result.get(0).level()).isEqualTo(0);
        assertThat(result.get(1).level()).isEqualTo(0);
        assertThat(result.get(2).level()).isEqualTo(1);
        assertThat(result.get(3).level()).isEqualTo(2);

        // Verify level 0 files are individual runs
        assertThat(result.get(0).run().files()).hasSize(1);
        assertThat(result.get(1).run().files()).hasSize(1);

        // Verify level 1 files are grouped together
        assertThat(result.get(2).run().files()).hasSize(2);
        assertThat(result.get(2).run().files()).containsExactlyInAnyOrder(level1File1, level1File2);

        // Verify level 2 file
        assertThat(result.get(3).run().files()).hasSize(1);
        assertThat(result.get(3).run().files()).containsExactly(level2File1);
    }

    @Test
    public void testUpgrade() throws Exception {
        // Create a valid table for IncrementalClusterManager
        Map<String, String> options = new HashMap<>();
        FileStoreTable table = createTable(options, Collections.emptyList());
        IncrementalClusterManager incrementalClusterManager = new IncrementalClusterManager(table);

        // Create test files with different levels
        List<DataFileMeta> filesAfterCluster = new ArrayList<>();
        DataFileMeta file1 = createFile(100, 1, 0);
        DataFileMeta file2 = createFile(200, 1, 1);
        DataFileMeta file3 = createFile(300, 1, 2);
        filesAfterCluster.add(file1);
        filesAfterCluster.add(file2);
        filesAfterCluster.add(file3);

        // Test upgrading to level 3
        int outputLevel = 3;
        List<DataFileMeta> upgradedFiles =
                IncrementalClusterManager.upgrade(filesAfterCluster, outputLevel);

        // Verify the results
        assertThat(upgradedFiles).hasSize(3);

        // Verify all files are upgraded to the specified output level
        for (DataFileMeta upgradedFile : upgradedFiles) {
            assertThat(upgradedFile.level()).isEqualTo(outputLevel);
        }
    }

    private FileStoreTable createTable(
            Map<String, String> customOptions, List<String> partitionKeys) throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BUCKET.key(), "-1");
        options.put(CoreOptions.CLUSTERING_COLUMNS.key(), "f0,f1");
        options.put(CoreOptions.CLUSTERING_INCREMENTAL.key(), "true");
        options.putAll(customOptions);

        Schema schema =
                new Schema(
                        RowType.of(
                                        DataTypes.INT(),
                                        DataTypes.INT(),
                                        DataTypes.STRING(),
                                        DataTypes.STRING())
                                .getFields(),
                        partitionKeys,
                        Collections.emptyList(),
                        options,
                        "");

        SchemaManager schemaManager =
                new SchemaManager(LocalFileIO.create(), new Path(tempDir.toString()));
        return FileStoreTableFactory.create(
                LocalFileIO.create(),
                new Path(tempDir.toString()),
                schemaManager.createTable(schema));
    }

    private static DataFileMeta createFile(long size, long schemaId, int level) {
        return DataFileMeta.create(
                "",
                size,
                1,
                null,
                null,
                null,
                null,
                0,
                0,
                schemaId,
                level,
                null,
                null,
                FileSource.APPEND,
                null,
                null,
                null);
    }
}
