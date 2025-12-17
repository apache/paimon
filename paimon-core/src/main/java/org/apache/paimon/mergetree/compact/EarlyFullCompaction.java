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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.mergetree.LevelSortedRun;
import org.apache.paimon.options.MemorySize;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

/** Early trigger full compaction. */
public class EarlyFullCompaction {

    private static final Logger LOG = LoggerFactory.getLogger(EarlyFullCompaction.class);

    @Nullable private final Long fullCompactionInterval;
    @Nullable private final Long totalSizeThreshold;
    @Nullable private final Long incrementalSizeThreshold;

    @Nullable private Long lastFullCompaction;

    public EarlyFullCompaction(
            @Nullable Long fullCompactionInterval,
            @Nullable Long totalSizeThreshold,
            @Nullable Long incrementalSizeThreshold) {
        this.fullCompactionInterval = fullCompactionInterval;
        this.totalSizeThreshold = totalSizeThreshold;
        this.incrementalSizeThreshold = incrementalSizeThreshold;
    }

    @Nullable
    public static EarlyFullCompaction create(CoreOptions options) {
        Duration interval = options.optimizedCompactionInterval();
        MemorySize totalThreshold = options.compactionTotalSizeThreshold();
        MemorySize incrementalThreshold = options.compactionIncrementalSizeThreshold();
        if (interval == null && totalThreshold == null && incrementalThreshold == null) {
            return null;
        }
        return new EarlyFullCompaction(
                interval == null ? null : interval.toMillis(),
                totalThreshold == null ? null : totalThreshold.getBytes(),
                incrementalThreshold == null ? null : incrementalThreshold.getBytes());
    }

    public Optional<CompactUnit> tryFullCompact(int numLevels, List<LevelSortedRun> runs) {
        if (runs.size() == 1) {
            return Optional.empty();
        }

        int maxLevel = numLevels - 1;
        if (fullCompactionInterval != null) {
            if (lastFullCompaction == null
                    || currentTimeMillis() - lastFullCompaction > fullCompactionInterval) {
                LOG.debug("Universal compaction due to full compaction interval");
                updateLastFullCompaction();
                return Optional.of(CompactUnit.fromLevelRuns(maxLevel, runs));
            }
        }
        if (totalSizeThreshold != null) {
            long totalSize = 0;
            for (LevelSortedRun run : runs) {
                totalSize += run.run().totalSize();
            }
            if (totalSize < totalSizeThreshold) {
                return Optional.of(CompactUnit.fromLevelRuns(maxLevel, runs));
            }
        }
        if (incrementalSizeThreshold != null) {
            long incrementalSize = 0;
            for (LevelSortedRun run : runs) {
                if (run.level() != maxLevel) {
                    incrementalSize += run.run().totalSize();
                }
            }
            if (incrementalSize > incrementalSizeThreshold) {
                return Optional.of(CompactUnit.fromLevelRuns(maxLevel, runs));
            }
        }
        return Optional.empty();
    }

    public void updateLastFullCompaction() {
        lastFullCompaction = currentTimeMillis();
    }

    @VisibleForTesting
    long currentTimeMillis() {
        return System.currentTimeMillis();
    }
}
