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

package org.apache.flink.table.store.file.mergetree.compact;

import org.apache.flink.table.store.file.compact.CompactUnit;
import org.apache.flink.table.store.file.mergetree.LevelSortedRun;

import java.util.List;
import java.util.Optional;

/** A {@link CompactStrategy} to force compacting level 0 files. */
public class LookupCompaction implements CompactStrategy {

    private final UniversalCompaction universalCompaction;

    public LookupCompaction(UniversalCompaction universalCompaction) {
        this.universalCompaction = universalCompaction;
    }

    @Override
    public Optional<CompactUnit> pick(int numLevels, List<LevelSortedRun> runs) {
        Optional<CompactUnit> pick = universalCompaction.pick(numLevels, runs);
        if (pick.isPresent()) {
            return pick;
        }

        if (runs.isEmpty() || runs.get(0).level() > 0) {
            return Optional.empty();
        }

        // collect all level 0 files
        int candidateCount = 1;
        for (int i = candidateCount; i < runs.size(); i++) {
            if (runs.get(i).level() > 0) {
                break;
            }
            candidateCount++;
        }

        return Optional.of(
                universalCompaction.pickForSizeRatio(numLevels - 1, runs, candidateCount, true));
    }
}
