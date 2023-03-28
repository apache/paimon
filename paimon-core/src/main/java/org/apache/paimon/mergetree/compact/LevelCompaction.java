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

import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.LevelSortedRun;
import org.apache.paimon.mergetree.SortedRun;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Classic Leveled compaction, minimizes space amplification at the cost of read and write
 * amplification.
 *
 * <p>Each level is one sorted run (except level 0) that can be range partitioned into many files.
 * Each level is many times larger than the previous level.
 *
 * <p>When the size of level N exceeds its limit, we compact it. The compaction picks a file from
 * level N and all overlapping files from the next level N+1.
 *
 * <p>When multiple levels trigger the compaction condition, we need to pick which level to compact
 * first. A score is generated for each level:
 *
 * <p>For non-zero levels, the score is total size of the level divided by the target size.
 *
 * <p>for level-0, the score is the total number of files, divided by
 * level0_file_num_compaction_trigger, or total size over max_bytes_for_level_base, which ever is
 * larger. (if the file size is smaller than level0_file_num_compaction_trigger, compaction won't
 * trigger from level 0, no matter how big the score is.)
 *
 * <p>We compare the score of each level, and the level with highest score takes the priority to
 * compact.
 */
public class LevelCompaction implements CompactStrategy {

    private final long maxBytesForLevelBase;
    private final int maxBytesForLevelMultiplier;
    private final int level0FileNumCompactionTrigger;
    private final Comparator<InternalRow> keyComparator;
    private final int maxLevel;

    public LevelCompaction(
            long maxBytesForLevelBase,
            int maxBytesForLevelMultiplier,
            int level0FileNumCompactionTrigger,
            Comparator<InternalRow> keyComparator,
            int maxLevel) {
        this.maxBytesForLevelBase = maxBytesForLevelBase;
        this.maxBytesForLevelMultiplier = maxBytesForLevelMultiplier;
        this.level0FileNumCompactionTrigger = level0FileNumCompactionTrigger;
        this.keyComparator = keyComparator;
        this.maxLevel = maxLevel;
    }

    @Override
    public Optional<CompactUnit> pick(int numLevels, List<LevelSortedRun> runs) {
        Optional<Integer> levelOptional = chooseCompactionLevel(runs);
        if (levelOptional.isPresent()) {
            int level = levelOptional.get();
            List<LevelSortedRun> levelSortedRuns =
                    runs.stream().filter(m -> m.level() == level).collect(Collectors.toList());
            if (level == 0) {
                List<DataFileMeta> dataFileMetas =
                        levelSortedRuns.stream()
                                .map(m -> m.run().files())
                                .flatMap(Collection::stream)
                                .collect(Collectors.toList());
                BinaryRow minKey = dataFileMetas.get(0).minKey();
                BinaryRow maxKey = dataFileMetas.get(0).maxKey();
                // get the min key and max key in level 1
                for (int i = 1; i < dataFileMetas.size(); i++) {
                    int left = keyComparator.compare(minKey, dataFileMetas.get(i).minKey());
                    if (left > 0) {
                        minKey = dataFileMetas.get(i).minKey();
                    }

                    int right = keyComparator.compare(maxKey, dataFileMetas.get(i).maxKey());
                    if (right < 0) {
                        maxKey = dataFileMetas.get(i).maxKey();
                    }
                }

                // find the overlap file in level 1
                List<LevelSortedRun> level1SortedRuns =
                        runs.stream().filter(m -> m.level() == 1).collect(Collectors.toList());
                if (!level1SortedRuns.isEmpty()) {
                    List<DataFileMeta> level1DataFileMetas = level1SortedRuns.get(0).run().files();
                    levelSortedRuns.add(
                            new LevelSortedRun(
                                    1,
                                    SortedRun.fromSorted(
                                            getOverlapDataFiles(
                                                    minKey, maxKey, level1DataFileMetas))));
                }
                return Optional.of(CompactUnit.fromLevelRuns(1, levelSortedRuns));
            } else {
                List<DataFileMeta> dataFileMetas =
                        new ArrayList<>(levelSortedRuns.get(0).run().files());
                sort(dataFileMetas);

                List<DataFileMeta> candidateList = new ArrayList<>();

                // TODO: we could looking for a better way to find the first file for candidate
                // files.
                DataFileMeta dataFileMeta = dataFileMetas.get(0);
                candidateList.add(dataFileMeta);

                // the min and max key of level N, we use them to find files on Level N+1, the file
                // which has overlap with
                // the min and max key will be added to the candidate files to compaction.
                BinaryRow minKey = dataFileMeta.minKey();
                BinaryRow maxKey = dataFileMeta.maxKey();

                int nextLevel = level + 1;
                // get input file in Level N+1
                List<LevelSortedRun> nextLevelSortedRuns =
                        runs.stream()
                                .filter(m -> m.level() == nextLevel)
                                .collect(Collectors.toList());
                if (!nextLevelSortedRuns.isEmpty()) {
                    List<DataFileMeta> nextLevelDataFileMetas =
                            nextLevelSortedRuns.get(0).run().files();
                    candidateList.addAll(
                            getOverlapDataFiles(minKey, maxKey, nextLevelDataFileMetas));
                }

                return Optional.of(CompactUnit.fromFiles(level + 1, candidateList));
            }
        } else {
            return Optional.empty();
        }
    }

    private List<DataFileMeta> getOverlapDataFiles(
            BinaryRow minKey, BinaryRow maxKey, List<DataFileMeta> dataFileMetas) {
        List<DataFileMeta> candidateList = new ArrayList<>();
        for (DataFileMeta meta : dataFileMetas) {
            boolean overlap = checkOverlap(meta.minKey(), meta.maxKey(), minKey, maxKey);
            if (overlap) {
                candidateList.add(meta);
            }
        }
        return candidateList;
    }

    private void sort(List<DataFileMeta> dataFileMetas) {
        dataFileMetas.sort(
                (o1, o2) -> {
                    int leftResult = keyComparator.compare(o1.minKey(), o2.minKey());
                    return leftResult == 0
                            ? keyComparator.compare(o1.maxKey(), o2.maxKey())
                            : leftResult;
                });
    }

    private boolean checkOverlap(
            BinaryRow leftMinKey,
            BinaryRow leftMaxKey,
            BinaryRow rightMinKey,
            BinaryRow rightMaxKey) {
        int left1 = keyComparator.compare(leftMinKey, rightMinKey);
        int left2 = keyComparator.compare(leftMinKey, rightMaxKey);

        int right = keyComparator.compare(rightMinKey, leftMaxKey);
        return (left1 > 0 && left2 < 0) || (left1 < 0 && right < 0);
    }

    private Optional<Integer> chooseCompactionLevel(List<LevelSortedRun> runs) {
        // key: score , value : level
        SortedMap<Double, Integer> sortedMap = new TreeMap<>();

        List<LevelSortedRun> level0SortedRuns =
                runs.stream().filter(m -> m.level() == 0).collect(Collectors.toList());
        List<DataFileMeta> level0DataFiles =
                level0SortedRuns.stream()
                        .map(m -> m.run().files())
                        .flatMap(Collection::stream)
                        .collect(Collectors.toList());
        if (!level0SortedRuns.isEmpty()
                && level0DataFiles.size() >= level0FileNumCompactionTrigger) {
            double level0ScoreFileNum =
                    level0SortedRuns.size() / (double) level0FileNumCompactionTrigger;
            double level0ScoreFileSize =
                    level0SortedRuns.stream().mapToLong(m -> m.run().totalSize()).sum()
                            / (double) maxBytesForLevelBase;
            sortedMap.put(Math.max(level0ScoreFileNum, level0ScoreFileSize), 0);
        }

        runs.stream()
                .filter(m -> m.level() != 0 && m.level() != maxLevel)
                .forEach(
                        m -> {
                            double score =
                                    m.run().totalSize()
                                            / (maxBytesForLevelBase
                                                    * Math.pow(
                                                            maxBytesForLevelMultiplier,
                                                            m.level() - 1));
                            // total size gather than the threshold, trigger the compaction
                            if (score > 1) {
                                sortedMap.put(score, m.level());
                            }
                        });
        if (!sortedMap.isEmpty()) {
            return Optional.of(sortedMap.get(sortedMap.lastKey()));
        } else {
            return Optional.empty();
        }
    }
}
