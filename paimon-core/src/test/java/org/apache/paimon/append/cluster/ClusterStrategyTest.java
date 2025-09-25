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
import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.mergetree.LevelSortedRun;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ClusterStrategy}. */
public class ClusterStrategyTest {

    @TempDir static java.nio.file.Path tempDir;

    private static SchemaManager schemaManager;
    private static ClusterStrategy clusterStrategy;

    @BeforeAll
    public static void setUp() throws Exception {
        schemaManager = new SchemaManager(LocalFileIO.create(), new Path(tempDir.toString()));
        prepareSchema();
        clusterStrategy = new ClusterStrategy(schemaManager, Arrays.asList("f0", "f1"), 25, 1, 3);
    }

    @Test
    public void testPickFullCompactionWithEmptyRuns() {
        // Test case: empty runs should return empty
        Optional<CompactUnit> result =
                clusterStrategy.pickFullCompaction(3, Collections.emptyList());
        assertThat(result.isPresent()).isFalse();
    }

    @Test
    public void testPickFullCompactionWithSingleRunSameClusterKey() {
        // Test case: single run at max level with same cluster key should return empty
        // Using schema-0 which has clustering columns "f0,f1" (same as clusterKeys)
        int maxLevel = 2;
        DataFileMeta file = createFile(1, 0L, maxLevel);
        LevelSortedRun run = new LevelSortedRun(maxLevel, SortedRun.fromSingle(file));
        List<LevelSortedRun> runs = Collections.singletonList(run);

        Optional<CompactUnit> result = clusterStrategy.pickFullCompaction(3, runs);
        assertThat(result.isPresent()).isFalse();
    }

    @Test
    public void testPickFullCompactionWithSingleRunDifferentClusterKey() {
        // Test case: single run at max level with different cluster key should return compaction
        // Using schema-1 which has clustering columns "f2,f3" (different from clusterKeys "f0,f1")
        int maxLevel = 2;
        DataFileMeta file = createFile(1, 1L, maxLevel); // Use schema-1
        LevelSortedRun run = new LevelSortedRun(maxLevel, SortedRun.fromSingle(file));
        List<LevelSortedRun> runs = Collections.singletonList(run);

        Optional<CompactUnit> result = clusterStrategy.pickFullCompaction(3, runs);
        assertThat(result.isPresent()).isTrue();
        assertThat(result.get().outputLevel()).isEqualTo(maxLevel);
        assertThat(result.get().files()).hasSize(1);
        assertThat(result.get().files().get(0).fileSize()).isEqualTo(1);
    }

    @Test
    public void testPickFullCompactionWithSingleRunNotAtMaxLevel() {
        // Test case: single run not at max level should return compaction
        int maxLevel = 2;
        int runLevel = 1;
        DataFileMeta file = createFile(1, 0L, runLevel);
        LevelSortedRun run = new LevelSortedRun(runLevel, SortedRun.fromSingle(file));
        List<LevelSortedRun> runs = Collections.singletonList(run);

        Optional<CompactUnit> result = clusterStrategy.pickFullCompaction(3, runs);
        assertThat(result.isPresent()).isTrue();
        assertThat(result.get().outputLevel()).isEqualTo(maxLevel);
        assertThat(result.get().files()).hasSize(1);
        assertThat(result.get().files().get(0).fileSize()).isEqualTo(1);
    }

    @Test
    public void testPickFullCompactionWithMultipleRuns() {
        // Test case: multiple runs should return compaction
        int maxLevel = 2;
        DataFileMeta file1 = createFile(1, 0L, 0);
        DataFileMeta file2 = createFile(2, 1L, 1);
        DataFileMeta file3 = createFile(3, 0L, maxLevel);

        LevelSortedRun run1 = new LevelSortedRun(0, SortedRun.fromSingle(file1));
        LevelSortedRun run2 = new LevelSortedRun(1, SortedRun.fromSingle(file2));
        LevelSortedRun run3 = new LevelSortedRun(maxLevel, SortedRun.fromSingle(file3));

        List<LevelSortedRun> runs = Arrays.asList(run1, run2, run3);

        Optional<CompactUnit> result = clusterStrategy.pickFullCompaction(3, runs);
        assertThat(result.isPresent()).isTrue();
        assertThat(result.get().outputLevel()).isEqualTo(maxLevel);
        assertThat(result.get().files()).hasSize(3);

        long[] fileSizes =
                result.get().files().stream().mapToLong(DataFileMeta::fileSize).toArray();
        assertThat(fileSizes).isEqualTo(new long[] {1, 2, 3});
    }

    @Test
    public void testPickFullCompactionWithDifferentNumLevels() {
        // Test case: different number of levels
        DataFileMeta file1 = createFile(1, 0L, 0);
        DataFileMeta file2 = createFile(2, 1L, 1);

        LevelSortedRun run1 = new LevelSortedRun(0, SortedRun.fromSingle(file1));
        LevelSortedRun run2 = new LevelSortedRun(1, SortedRun.fromSingle(file2));

        List<LevelSortedRun> runs = Arrays.asList(run1, run2);

        // Test with numLevels = 5, maxLevel should be 4
        Optional<CompactUnit> result = clusterStrategy.pickFullCompaction(5, runs);
        assertThat(result.isPresent()).isTrue();
        assertThat(result.get().outputLevel()).isEqualTo(4); // maxLevel = numLevels - 1
        assertThat(result.get().files()).hasSize(2);
    }

    @Test
    public void testPickFullCompactionWithMixedSchemas() {
        // Test case: runs with mixed schemas (some same, some different cluster keys)
        int maxLevel = 2;
        DataFileMeta file1 = createFile(1, 0L, 0); // schema-0: f0,f1 (same as clusterKeys)
        DataFileMeta file2 = createFile(2, 1L, 1); // schema-1: f2,f3 (different from clusterKeys)
        DataFileMeta file3 = createFile(3, 0L, maxLevel); // schema-0: f0,f1 (same as clusterKeys)

        LevelSortedRun run1 = new LevelSortedRun(0, SortedRun.fromSingle(file1));
        LevelSortedRun run2 = new LevelSortedRun(1, SortedRun.fromSingle(file2));
        LevelSortedRun run3 = new LevelSortedRun(maxLevel, SortedRun.fromSingle(file3));

        List<LevelSortedRun> runs = Arrays.asList(run1, run2, run3);

        Optional<CompactUnit> result = clusterStrategy.pickFullCompaction(3, runs);
        assertThat(result.isPresent()).isTrue();
        assertThat(result.get().outputLevel()).isEqualTo(maxLevel);
        assertThat(result.get().files()).hasSize(3);
    }

    private static void prepareSchema() throws Exception {
        // schema-0
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.CLUSTERING_COLUMNS.key(), "f0,f1");
        Schema schema =
                new Schema(
                        RowType.of(
                                        VarCharType.STRING_TYPE,
                                        VarCharType.STRING_TYPE,
                                        VarCharType.STRING_TYPE,
                                        VarCharType.STRING_TYPE)
                                .getFields(),
                        emptyList(),
                        emptyList(),
                        options,
                        "");
        schemaManager.createTable(schema);
        // schema-1
        schemaManager.commitChanges(
                SchemaChange.setOption(CoreOptions.CLUSTERING_COLUMNS.key(), "f2,f3"));
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
