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

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/** A {@link CompactStrategy} to force compacting level 0 files. */
public class ForceUpLevel0Compaction implements CompactStrategy {

    private final UniversalCompaction universal;
    @Nullable private final Integer maxCompactInterval;
    @Nullable private final AtomicInteger compactTriggerCount;

    public ForceUpLevel0Compaction(
            UniversalCompaction universal, @Nullable Integer maxCompactInterval) {
        this.universal = universal;
        this.maxCompactInterval = maxCompactInterval;
        this.compactTriggerCount = maxCompactInterval == null ? null : new AtomicInteger(0);
    }

    @Nullable
    public Integer maxCompactInterval() {
        return maxCompactInterval;
    }

    @Override
    public Optional<CompactUnit> pick(int numLevels, List<LevelSortedRun> runs) {
        Optional<CompactUnit> pick = universal.pick(numLevels, runs);
        if (pick.isPresent()) {
            return pick;
        }

        if (maxCompactInterval == null || compactTriggerCount == null) {
            return universal.forcePickL0(numLevels, runs);
        }

        compactTriggerCount.getAndIncrement();
        if (compactTriggerCount.compareAndSet(maxCompactInterval, 0)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Universal compaction due to max lookup compaction interval {}.",
                        maxCompactInterval);
            }
            return universal.forcePickL0(numLevels, runs);
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Skip universal compaction due to lookup compaction trigger count {} is less than the max interval {}.",
                        compactTriggerCount.get(),
                        maxCompactInterval);
            }
            return Optional.empty();
        }
    }
}
