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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.LevelSortedRun;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link LevelCompaction}. */
public class LevelCompactionTest extends CompactionTestBase {

    private final RecordComparator comparator = (o1, o2) -> o1.getInt(0) - o2.getInt(0);

    @Test
    public void testPickLevel0() {
        // only for level 0
        LevelCompaction compaction = new LevelCompaction(10L, 10, 3, comparator, 5);
        String input0 = "[1, 80, 100], [2, 120, 150], [3, 100, 210], [4, 150, 240]";
        List<LevelSortedRun> level0SortedRuns = level(0, input0);
        Optional<CompactUnit> pick = compaction.pick(2, level0SortedRuns);
        assertThat(pick.isPresent()).isTrue();
        assertThat(pick.get().outputLevel()).isEqualTo(1);
        long[] results = pick.get().files().stream().mapToLong(DataFileMeta::fileSize).toArray();
        assertThat(results).isEqualTo(new long[] {1, 2, 3, 4});

        // the number of the level 0 files do not reach the level0FileNumCompactionTrigger
        pick = compaction.pick(1, level0(1, 2));
        assertThat(pick.isPresent()).isFalse();
    }

    /** the input level only contains level 0 and level 1, no level 2. */
    @Test
    public void testPickLevel0AndNoLevel0() {
        int maxLevel = 5;
        LevelCompaction compaction = new LevelCompaction(10L, 10, 3, comparator, maxLevel);
        List<LevelSortedRun> runs = new ArrayList<>();

        // the score of level 1 gt the score of level 0, we choose level 1 to compaction.
        String input0 = "[1, 80, 100], [10, 120, 150], [3, 100, 210], [4, 150, 240]";
        List<LevelSortedRun> level0SortedRuns = level(1, input0);
        runs.addAll(level0SortedRuns);
        // [size, minKey, maxKey]
        String input1 = "[1, 100, 200], [10, 201, 300], [3, 301, 400], [4, 500, 700]";
        List<LevelSortedRun> level1SortedRuns = level(1, input1);
        runs.addAll(level1SortedRuns);
        Optional<CompactUnit> pick = compaction.pick(maxLevel, runs);
        assertThat(pick.isPresent()).isTrue();
        assertThat(pick.get().outputLevel()).isEqualTo(2);
        long[] results = pick.get().files().stream().mapToLong(DataFileMeta::fileSize).toArray();
        assertThat(results).isEqualTo(new long[] {1});

        // the SortRun number of level 0 do not reach the level0_file_num_compaction_trigger.
        runs.clear();
        level0SortedRuns = level0(10, 11);
        runs.addAll(level0SortedRuns);
        runs.addAll(level1SortedRuns);
        pick = compaction.pick(maxLevel, runs);
        assertThat(pick.isPresent()).isTrue();
        assertThat(pick.get().outputLevel()).isEqualTo(2);
        results = pick.get().files().stream().mapToLong(DataFileMeta::fileSize).toArray();
        assertThat(results).isEqualTo(new long[] {1});

        // the score of level 0 gt the score of level1.
        runs.clear();
        input0 = "[20, 80, 100], [21, 120, 150], [22, 100, 210]";
        level0SortedRuns = level(0, input0);
        runs.addAll(level0SortedRuns);
        runs.addAll(level1SortedRuns);
        pick = compaction.pick(maxLevel, runs);
        assertThat(pick.isPresent()).isTrue();
        assertThat(pick.get().outputLevel()).isEqualTo(1);
        results = pick.get().files().stream().mapToLong(DataFileMeta::fileSize).toArray();
        assertThat(results).isEqualTo(new long[] {20, 21, 22, 1, 10});
    }

    /** the input contains the level N and level N+1. */
    @Test
    public void testPickMultipleLevel() {
        int maxLevel = 5;

        LevelCompaction compaction = new LevelCompaction(10L, 10, 3, comparator, maxLevel);
        List<LevelSortedRun> level0SortedRuns = level0(1, 2, 3, 3);
        List<LevelSortedRun> runs = new ArrayList<>(level0SortedRuns);

        // [size, minKey, maxKey]
        String input = "[1, 100, 200], [10, 201, 300], [3, 301, 400], [4, 500, 700]";
        List<LevelSortedRun> level1SortedRuns = level(1, input);
        runs.addAll(level1SortedRuns);

        input = "[5, 80, 150], [6, 180, 300], [7, 600, 700]";
        List<LevelSortedRun> level2SortedRuns = level(2, input);
        runs.addAll(level2SortedRuns);

        Optional<CompactUnit> pick = compaction.pick(maxLevel, runs);
        assertThat(pick.isPresent()).isTrue();
        assertThat(pick.get().outputLevel()).isEqualTo(2);
        long[] results = pick.get().files().stream().mapToLong(DataFileMeta::fileSize).toArray();
        assertThat(results).isEqualTo(new long[] {1, 5, 6});
    }

    @Test
    public void testNotTriggerCompaction() {
        int maxLevel = 5;
        LevelCompaction compaction = new LevelCompaction(10L, 10, 3, comparator, maxLevel);

        // [size, minKey, maxKey]
        String input = "[1, 100, 200], [10, 201, 300], [3, 301, 400], [4, 500, 700]";
        List<LevelSortedRun> levelSortedRuns = level(4, input);
        List<LevelSortedRun> runs = new ArrayList<>(levelSortedRuns);

        // the file size of level 4 do not reach the max limit.
        Optional<CompactUnit> pick = compaction.pick(maxLevel, runs);
        assertThat(pick.isPresent()).isFalse();
    }
}
