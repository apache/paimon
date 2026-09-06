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

package org.apache.paimon.operation.metrics;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.metrics.Counter;
import org.apache.paimon.metrics.MetricGroup;
import org.apache.paimon.metrics.MetricRegistry;

import java.util.EnumMap;
import java.util.Map;

/** Metrics for append-only compaction RowGroup copy fast path. */
public class CompactionFastPathMetrics {

    /** Reasons why append compaction cannot use the RowGroup copy fast path. */
    public enum MissReason {
        DV,
        SCHEMA_ID,
        CODEC,
        MESSAGE_TYPE,
        EXTRA_FILES,
        WRITE_COLS,
        BLOOM_CONFIGURED,
        FILE_INDEX,
        ENCRYPTION,
        ROW_TRACKING,
        FILE_SOURCE,
        IO_ERROR,
        OTHER
    }

    private static final String GROUP_NAME = "compactionFastPath";

    public static final String HIT_COUNT = "compactionFastPathHit";
    public static final String MISS_COUNT_PREFIX = "compactionFastPathMiss";

    private final MetricGroup metricGroup;
    private final Counter hitCounter;
    private final Map<MissReason, Counter> missCounters;

    public CompactionFastPathMetrics(MetricRegistry registry, String tableName) {
        this.metricGroup = registry.createTableMetricGroup(GROUP_NAME, tableName);
        this.hitCounter = metricGroup.counter(HIT_COUNT);
        this.missCounters = new EnumMap<>(MissReason.class);
        for (MissReason reason : MissReason.values()) {
            missCounters.put(
                    reason, metricGroup.counter(MISS_COUNT_PREFIX + capitalize(reason.name())));
        }
    }

    @VisibleForTesting
    public MetricGroup getMetricGroup() {
        return metricGroup;
    }

    public void reportHit() {
        hitCounter.inc();
    }

    public void reportMiss(MissReason reason) {
        missCounters.get(reason).inc();
    }

    private static String capitalize(String value) {
        if (value.isEmpty()) {
            return value;
        }
        return Character.toUpperCase(value.charAt(0)) + value.substring(1).toLowerCase();
    }
}
