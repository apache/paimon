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

package org.apache.paimon.operation;

import org.apache.paimon.utils.Preconditions;

import java.util.ArrayList;
import java.util.List;

/**
 * Pick strategy for manifest LSM Tree compaction.
 *
 * <p>Requires more than {@link #MAX_LEVEL} runs; otherwise returns an empty pick. Strategy priority
 * when that precondition holds:
 *
 * <ol>
 *   <li><b>SizeAmp</b>: if the total size of all lower-level runs is greater than {@code
 *       sizeAmpThreshold} percent of the highest-level run's size (check: {@code
 *       lowerLevelTotalSize * 100 > sizeAmpThreshold * highestRunSize}), trigger full compaction
 *       (pick all runs).
 *   <li><b>SizeRatio</b>: from low to high, pick adjacent runs whose amplification factor is less
 *       than {@code sizeRatioThreshold}.
 *   <li><b>Forced pick</b>: on the SizeRatio path, level0 and level1 runs are included without a
 *       SizeRatio check; the result is returned only when at least two runs are selected.
 * </ol>
 */
public class ManifestPickStrategy {

    public static final int MAX_LEVEL = 4;

    private final int sizeAmpThreshold;
    private final int sizeRatioThreshold;

    public ManifestPickStrategy(int sizeAmpThreshold, int sizeRatioThreshold) {
        Preconditions.checkArgument(sizeAmpThreshold > 0, "sizeAmpThreshold must be positive");
        Preconditions.checkArgument(sizeRatioThreshold > 0, "sizeRatioThreshold must be positive");
        this.sizeAmpThreshold = sizeAmpThreshold;
        this.sizeRatioThreshold = sizeRatioThreshold;
    }

    /**
     * Pick runs that need compaction from the given level runs.
     *
     * <p>Returns an empty list when {@code levelRuns} is empty or has at most {@link #MAX_LEVEL}
     * runs. Otherwise tries SizeAmp first, then SizeRatio with forced inclusion of level0/level1.
     *
     * @param levelRuns runs with assigned levels (level 0~4)
     * @return list of picked runs to compact
     */
    public List<ManifestAdjacentSortedRun> pick(List<ManifestAdjacentSortedRun> levelRuns) {
        if (levelRuns.isEmpty() || levelRuns.size() <= MAX_LEVEL) {
            return new ArrayList<>();
        }

        // Try SizeAmp first
        List<ManifestAdjacentSortedRun> sizeAmpResult = pickForSizeAmp(levelRuns);
        if (sizeAmpResult != null) {
            return sizeAmpResult;
        }

        // SizeRatio + forced pick
        return pickForSizeRatioAndForce(levelRuns);
    }

    /**
     * SizeAmp check: if the total size of all lower-level (0~3) runs is greater than {@code
     * sizeAmpThreshold} percent of the highest-level run's size, pick all runs for full compaction.
     *
     * <p>{@code sizeAmpThreshold} is a percentage of the highest-level run's size, not an
     * additional-size percentage and not a multiplier. Formula (consistent with {@code
     * UniversalCompaction#pickForSizeAmp}): {@code lowerLevelTotalSize * 100 > sizeAmpThreshold *
     * highestRunSize}
     */
    private List<ManifestAdjacentSortedRun> pickForSizeAmp(
            List<ManifestAdjacentSortedRun> levelRuns) {
        if (levelRuns.isEmpty()) {
            return null;
        }

        // The last run has the highest level (set by buildLevelSortedRuns)
        ManifestAdjacentSortedRun highestRun = levelRuns.get(levelRuns.size() - 1);
        int maxLevel = highestRun.level();

        if (maxLevel <= 0) {
            return null;
        }

        long lowerLevelTotalSize = 0;
        for (ManifestAdjacentSortedRun run : levelRuns) {
            if (run.level() < maxLevel) {
                lowerLevelTotalSize += run.totalSize();
            }
        }

        // size amplification = percentage of additional size
        if (lowerLevelTotalSize * 100 > (long) sizeAmpThreshold * highestRun.totalSize()) {
            return new ArrayList<>(levelRuns);
        }
        return null;
    }

    /**
     * SizeRatio + forced pick. Invoked only after {@link #pick} has verified {@code
     * levelRuns.size() > MAX_LEVEL} and SizeAmp did not trigger.
     *
     * <ul>
     *   <li>Level0 and level1 runs are included without a SizeRatio check.
     *   <li>From low to high, if the cumulative picked size with ratio amplification covers the
     *       next run's size, continue picking.
     *   <li>If only one run remains picked after the scan, return an empty list (no compaction).
     * </ul>
     *
     * <p>Formula (consistent with {@code UniversalCompaction#pickForSizeRatio}): {@code pickedSize
     * * (100.0 + sizeRatioThreshold) / 100.0 >= nextRunSize}
     */
    private List<ManifestAdjacentSortedRun> pickForSizeRatioAndForce(
            List<ManifestAdjacentSortedRun> levelRuns) {
        // levelRuns is already sorted by level ascending (set by buildLevelSortedRuns)
        List<ManifestAdjacentSortedRun> picked = new ArrayList<>();

        // Seed with the first run; a single-run pick is discarded below.
        picked.add(levelRuns.get(0));
        long pickedSize = levelRuns.get(0).totalSize();

        // From the second run onward: force-include level0/level1, then SizeRatio for the rest.
        for (int i = 1; i < levelRuns.size(); i++) {
            ManifestAdjacentSortedRun run = levelRuns.get(i);
            if (run.level() <= 1) {
                picked.add(run);
                pickedSize += run.totalSize();
            } else {
                long nextRunSize = run.totalSize();
                if (pickedSize * (100 + sizeRatioThreshold) >= nextRunSize * 100L) {
                    picked.add(run);
                    pickedSize += nextRunSize;
                }
            }
        }
        if (picked.size() == 1) {
            return new ArrayList<>();
        }
        return picked;
    }
}
