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
import org.apache.paimon.mergetree.LevelSortedRun;

import java.util.List;
import java.util.Optional;

/** Compact strategy to decide which files to select for compaction. */
public interface CompactStrategy {

    /**
     * Pick compaction unit from runs.
     *
     * <ul>
     *   <li>compaction is runs-based, not file-based.
     *   <li>level 0 is special, one run per file; all other levels are one run per level.
     *   <li>compaction is sequential from small level to large level.
     * </ul>
     */
    Optional<CompactUnit> pick(int numLevels, List<LevelSortedRun> runs);

    /** Pick a compaction unit consisting of all existing files. */
    static Optional<CompactUnit> pickFullCompaction(int numLevels, List<LevelSortedRun> runs) {
        int maxLevel = numLevels - 1;
        if (runs.isEmpty() || (runs.size() == 1 && runs.get(0).level() == maxLevel)) {
            // no sorted run or only 1 sorted run on the max level, no need to compact
            return Optional.empty();
        } else {
            return Optional.of(CompactUnit.fromLevelRuns(maxLevel, runs));
        }
    }
}
