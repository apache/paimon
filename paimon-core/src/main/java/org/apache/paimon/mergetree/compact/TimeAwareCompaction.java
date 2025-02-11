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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Optional;

/**
 * A combined {@link CompactStrategy} that uses different compact strategies for recent partitions
 * and historical partitions.
 */
public class TimeAwareCompaction implements CompactStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(TimeAwareCompaction.class);
    private final Duration lateArrivedThreshold;
    private final CompactStrategy currentCompactStrategy;
    private final CompactStrategy lateArrivedCompactStrategy;
    private final LocalDateTime currentPartitionDate;

    public TimeAwareCompaction(
            Duration lateArrivedThreshold,
            CompactStrategy currentCompactStrategy,
            CompactStrategy historicalCompactStrategy,
            LocalDateTime partitionDate) {
        this.lateArrivedThreshold = lateArrivedThreshold;
        this.currentCompactStrategy = currentCompactStrategy;
        this.lateArrivedCompactStrategy = historicalCompactStrategy;
        this.currentPartitionDate = partitionDate;
    }

    @Override
    public Optional<CompactUnit> pick(int numLevels, List<LevelSortedRun> runs) {
        if (isLateArrived()) {
            return lateArrivedCompactStrategy.pick(numLevels, runs);
        } else {
            return currentCompactStrategy.pick(numLevels, runs);
        }
    }

    private boolean isLateArrived() {
        LocalDateTime lateArrivedDate = LocalDateTime.now().minus(lateArrivedThreshold);
        // For lateArrivedDate=20250120, any data insert into partitions<=20250120 will be
        // considered as late arrived data.
        boolean result = !currentPartitionDate.isAfter(lateArrivedDate);
        LOG.debug(
                "Current partition Date: {}, late arrived Date: {}, late arrived result: {}.",
                currentPartitionDate.format(DateTimeFormatter.BASIC_ISO_DATE),
                lateArrivedDate.format(DateTimeFormatter.BASIC_ISO_DATE),
                result);
        return result;
    }
}
