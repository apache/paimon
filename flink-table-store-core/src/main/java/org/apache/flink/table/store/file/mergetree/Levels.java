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

package org.apache.flink.table.store.file.mergetree;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.mergetree.sst.SstFileMeta;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static org.apache.flink.util.Preconditions.checkArgument;

/** A class which stores all level files of merge tree. */
public class Levels {

    private final Comparator<RowData> keyComparator;

    private final TreeSet<SstFileMeta> level0;

    private final List<SortedRun> levels;

    public Levels(Comparator<RowData> keyComparator, List<SstFileMeta> inputFiles, int numLevels) {
        this.keyComparator = keyComparator;
        checkArgument(numLevels > 1, "levels must be at least 2.");
        this.level0 =
                new TreeSet<>(Comparator.comparing(SstFileMeta::maxSequenceNumber).reversed());
        this.levels = new ArrayList<>();
        for (int i = 1; i < numLevels; i++) {
            levels.add(SortedRun.empty());
        }

        Map<Integer, List<SstFileMeta>> levelMap = new HashMap<>();
        for (SstFileMeta file : inputFiles) {
            levelMap.computeIfAbsent(file.level(), level -> new ArrayList<>()).add(file);
        }
        levelMap.forEach((level, files) -> updateLevel(level, emptyList(), files));
    }

    public void addLevel0File(SstFileMeta file) {
        checkArgument(file.level() == 0);
        level0.add(file);
    }

    public SortedRun runOfLevel(int level) {
        checkArgument(level > 0, "Level0 dose not have one single sorted run.");
        return levels.get(level - 1);
    }

    public int numberOfLevels() {
        return levels.size() + 1;
    }

    public int numberOfSortedRuns() {
        int numberOfSortedRuns = level0.size();
        for (SortedRun run : levels) {
            if (run.nonEmpty()) {
                numberOfSortedRuns++;
            }
        }
        return numberOfSortedRuns;
    }

    /** @return the highest non-empty level or -1 if all levels empty. */
    public int nonEmptyHighestLevel() {
        int i;
        for (i = levels.size() - 1; i >= 0; i--) {
            if (levels.get(i).nonEmpty()) {
                return i + 1;
            }
        }
        return level0.isEmpty() ? -1 : 0;
    }

    public List<SstFileMeta> allFiles() {
        List<SstFileMeta> files = new ArrayList<>();
        List<LevelSortedRun> runs = levelSortedRuns();
        for (LevelSortedRun run : runs) {
            files.addAll(run.run().files());
        }
        return files;
    }

    public List<LevelSortedRun> levelSortedRuns() {
        List<LevelSortedRun> runs = new ArrayList<>();
        level0.forEach(file -> runs.add(new LevelSortedRun(0, SortedRun.fromSingle(file))));
        for (int i = 0; i < levels.size(); i++) {
            SortedRun run = levels.get(i);
            if (run.nonEmpty()) {
                runs.add(new LevelSortedRun(i + 1, run));
            }
        }
        return runs;
    }

    public void update(List<SstFileMeta> before, List<SstFileMeta> after) {
        Map<Integer, List<SstFileMeta>> groupedBefore = groupByLevel(before);
        Map<Integer, List<SstFileMeta>> groupedAfter = groupByLevel(after);
        for (int i = 0; i < numberOfLevels(); i++) {
            updateLevel(
                    i,
                    groupedBefore.getOrDefault(i, emptyList()),
                    groupedAfter.getOrDefault(i, emptyList()));
        }
    }

    private void updateLevel(int level, List<SstFileMeta> before, List<SstFileMeta> after) {
        if (before.isEmpty() && after.isEmpty()) {
            return;
        }

        if (level == 0) {
            before.forEach(level0::remove);
            level0.addAll(after);
        } else {
            List<SstFileMeta> files = new ArrayList<>(runOfLevel(level).files());
            files.removeAll(before);
            files.addAll(after);
            levels.set(level - 1, SortedRun.fromUnsorted(files, keyComparator));
        }
    }

    private Map<Integer, List<SstFileMeta>> groupByLevel(List<SstFileMeta> files) {
        return files.stream()
                .collect(Collectors.groupingBy(SstFileMeta::level, Collectors.toList()));
    }
}
