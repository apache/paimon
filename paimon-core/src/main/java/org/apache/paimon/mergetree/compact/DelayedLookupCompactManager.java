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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.DeletionVectorsMaintainer;
import org.apache.paimon.mergetree.Levels;
import org.apache.paimon.operation.metrics.CompactionMetrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/** Compact manager to perform the delayed lookup compaction. */
public class DelayedLookupCompactManager extends MergeTreeCompactManager {

    private static final Logger LOG = LoggerFactory.getLogger(DelayedLookupCompactManager.class);
    private final CompactStrategy strategy;
    private final LocalDateTime currentPartitionDate;
    private final Duration delayPartitionThreshold;
    private final int l0FileTriggerThreshold;
    private final int stopTriggerThreshold;

    private final AtomicInteger triggerCount = new AtomicInteger(0);

    public DelayedLookupCompactManager(
            ExecutorService executor,
            Levels levels,
            CompactStrategy strategy,
            Comparator<InternalRow> keyComparator,
            long compactionFileSize,
            int numSortedRunStopTrigger,
            CompactRewriter rewriter,
            @Nullable CompactionMetrics.Reporter metricsReporter,
            @Nullable DeletionVectorsMaintainer dvMaintainer,
            boolean lazyGenDeletionFile,
            LocalDateTime currentPartitionDate,
            Duration delayPartitionThreshold,
            int l0FileTriggerThreshold,
            int stopTriggerThreshold) {
        super(
                executor,
                levels,
                strategy,
                keyComparator,
                compactionFileSize,
                numSortedRunStopTrigger,
                rewriter,
                metricsReporter,
                dvMaintainer,
                lazyGenDeletionFile);
        this.strategy = strategy;
        this.currentPartitionDate = currentPartitionDate;
        this.delayPartitionThreshold = delayPartitionThreshold;
        this.l0FileTriggerThreshold = l0FileTriggerThreshold;
        this.stopTriggerThreshold = stopTriggerThreshold;
    }

    @Override
    public void triggerCompaction(boolean fullCompaction) {
        if (isDelayedCompactPartition()) {
            if (shouldTriggerDelayedLookupCompact()) {
                triggerCount.set(0);
                super.triggerCompaction(fullCompaction);
            } else {
                LOG.info(
                        "Skip to trigger this lookup compaction because the compaction trigger count {} has not reached "
                                + "the stop trigger threshold {} or l0 file size {} is less than l0 file trigger threshold {}.",
                        triggerCount.get(),
                        stopTriggerThreshold,
                        level0Files(),
                        l0FileTriggerThreshold);
                triggerCount.incrementAndGet();
            }
        } else {
            super.triggerCompaction(fullCompaction);
        }
    }

    @Override
    public boolean hasDelayedCompact() {
        return strategy instanceof ForceUpLevel0Compaction && level0Files() > 0;
    }

    @VisibleForTesting
    public boolean shouldTriggerDelayedLookupCompact() {
        if (level0Files() >= l0FileTriggerThreshold) {
            LOG.info("Trigger delayed lookup compact for L0 file count: {}", level0Files());
            return true;
        } else if (triggerCount.get() >= stopTriggerThreshold) {
            LOG.info("Trigger delayed lookup compact for stopTrigger count: {}", triggerCount);
            return true;
        } else {
            return false;
        }
    }

    @VisibleForTesting
    public boolean isDelayedCompactPartition() {
        if (delayPartitionThreshold == null) {
            return false;
        }
        LocalDateTime delayedCompactPartition = LocalDateTime.now().minus(delayPartitionThreshold);
        // For delayedCompactPartitionThreshold=20250120, any data insert into partitions<=20250120
        // should be delayed compact.
        boolean result = !currentPartitionDate.isAfter(delayedCompactPartition);
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Current partition Date: {}, delayed compact partition threshold: {}, delayed compact partition result: {}.",
                    currentPartitionDate.format(DateTimeFormatter.BASIC_ISO_DATE),
                    delayedCompactPartition.format(DateTimeFormatter.BASIC_ISO_DATE),
                    result);
        }
        return result;
    }

    private int level0Files() {
        return levels().level0().size();
    }
}
