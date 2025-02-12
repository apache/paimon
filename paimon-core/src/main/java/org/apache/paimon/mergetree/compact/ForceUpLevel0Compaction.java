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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.mergetree.LevelSortedRun;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Optional;

/**
 * A {@link CompactStrategy} to force compacting level 0 files.
 *
 * <p>Note: This strategy will increase the compaction frequency drastically when updates are
 * seriously disordered for partitioned PK table.
 *
 * <p>In certain cases, users can actually accept worse data freshness for the late arrived data in
 * the historical partitions for lower compaction frequency. This can significantly reduce the
 * compaction resource usage. {@link UniversalCompaction} can be used in this case by configuring
 * CoreOptions.LATE_ARRIVAL_THRESHOLD.
 *
 * <p>Notice: Enabling CoreOptions.LATE_ARRIVAL_THRESHOLD may result in several data files in L0
 * cannot be seen for a long time. This can be fixed with offline Historical Partition Compact.
 */
public class ForceUpLevel0Compaction implements CompactStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(ForceUpLevel0Compaction.class);

    private final Duration lateArrivalThreshold;
    private final LocalDateTime currentPartitionDate;

    private final UniversalCompaction universal;

    public ForceUpLevel0Compaction(UniversalCompaction universal) {
        this(universal, null, null);
    }

    public ForceUpLevel0Compaction(
            UniversalCompaction universal,
            Duration lateArrivalThreshold,
            LocalDateTime currentPartitionDate) {
        this.universal = universal;
        this.lateArrivalThreshold = lateArrivalThreshold;
        this.currentPartitionDate = currentPartitionDate;
    }

    @Override
    public Optional<CompactUnit> pick(int numLevels, List<LevelSortedRun> runs) {
        Optional<CompactUnit> pick = universal.pick(numLevels, runs);
        if (pick.isPresent() || isLateArrival()) {
            return pick;
        }

        // collect all level 0 files
        int candidateCount = 0;
        for (int i = candidateCount; i < runs.size(); i++) {
            if (runs.get(i).level() > 0) {
                break;
            }
            candidateCount++;
        }

        return candidateCount == 0
                ? Optional.empty()
                : Optional.of(
                        universal.pickForSizeRatio(numLevels - 1, runs, candidateCount, true));
    }

    @VisibleForTesting
    public boolean isLateArrival() {
        if (lateArrivalThreshold == null) {
            return false;
        }
        LocalDateTime lateArrivalDate = LocalDateTime.now().minus(lateArrivalThreshold);
        // For lateArrivedDate=20250120, any data insert into partitions<=20250120 will be
        // considered as late arrived data.
        boolean result = !currentPartitionDate.isAfter(lateArrivalDate);
        LOG.debug(
                "Current partition Date: {}, late arrival Date: {}, late arrival result: {}.",
                currentPartitionDate.format(DateTimeFormatter.BASIC_ISO_DATE),
                lateArrivalDate.format(DateTimeFormatter.BASIC_ISO_DATE),
                result);
        return result;
    }
}
