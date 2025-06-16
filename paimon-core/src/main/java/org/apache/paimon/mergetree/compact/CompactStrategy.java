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
import org.apache.paimon.deletionvectors.DeletionVectorsMaintainer;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.RecordLevelExpire;
import org.apache.paimon.mergetree.LevelSortedRun;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** Compact strategy to decide which files to select for compaction. */
public interface CompactStrategy {

    Logger LOG = LoggerFactory.getLogger(CompactStrategy.class);

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
    static Optional<CompactUnit> pickFullCompaction(
            int numLevels,
            List<LevelSortedRun> runs,
            @Nullable RecordLevelExpire recordLevelExpire,
            @Nullable DeletionVectorsMaintainer dvMaintainer,
            boolean forceRewriteAllFiles) {
        int maxLevel = numLevels - 1;
        if (runs.isEmpty()) {
            // no sorted run, no need to compact
            return Optional.empty();
        }

        // only max level files
        if ((runs.size() == 1 && runs.get(0).level() == maxLevel)) {
            List<DataFileMeta> filesToBeCompacted = new ArrayList<>();

            for (DataFileMeta file : runs.get(0).run().files()) {
                if (forceRewriteAllFiles) {
                    // add all files when force compacted
                    filesToBeCompacted.add(file);
                } else if (recordLevelExpire != null && recordLevelExpire.isExpireFile(file)) {
                    // check record level expire for large files
                    filesToBeCompacted.add(file);
                } else if (dvMaintainer != null
                        && dvMaintainer.deletionVectorOf(file.fileName()).isPresent()) {
                    // check deletion vector for large files
                    filesToBeCompacted.add(file);
                }
            }

            if (filesToBeCompacted.isEmpty()) {
                return Optional.empty();
            }

            return Optional.of(CompactUnit.fromFiles(maxLevel, filesToBeCompacted, true));
        }

        // full compaction
        return Optional.of(CompactUnit.fromLevelRuns(maxLevel, runs));
    }
}
