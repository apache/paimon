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

package org.apache.paimon.flink.source.metrics;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.MetricNames;

/** Source reader metrics. */
public class FileStoreSourceReaderMetrics {

    private long latestFileCreationTime = UNDEFINED;
    private long lastSplitUpdateTime = UNDEFINED;
    private long idleStartTime = ACTIVE;

    public static final long UNDEFINED = -1L;
    private static final long ACTIVE = Long.MAX_VALUE;

    public FileStoreSourceReaderMetrics(MetricGroup sourceReaderMetricGroup) {
        sourceReaderMetricGroup.gauge(
                MetricNames.CURRENT_FETCH_EVENT_TIME_LAG, this::getFetchTimeLag);
        sourceReaderMetricGroup.gauge(MetricNames.SOURCE_IDLE_TIME, this::getIdleTime);
    }

    /** Called when consumed snapshot changes. */
    public void recordSnapshotUpdate(long fileCreationTime) {
        this.latestFileCreationTime = fileCreationTime;
        lastSplitUpdateTime = System.currentTimeMillis();
        idleStartTime = ACTIVE;
    }

    @VisibleForTesting
    long getFetchTimeLag() {
        if (latestFileCreationTime != UNDEFINED) {
            return lastSplitUpdateTime - latestFileCreationTime;
        }
        return UNDEFINED;
    }

    public long getLatestFileCreationTime() {
        return latestFileCreationTime;
    }

    @VisibleForTesting
    long getLastSplitUpdateTime() {
        return lastSplitUpdateTime;
    }

    public void idlingStarted() {
        if (!isIdling()) {
            idleStartTime = System.currentTimeMillis();
        }
    }

    boolean isIdling() {
        return idleStartTime != ACTIVE;
    }

    @VisibleForTesting
    long getIdleTime() {
        return isIdling() ? System.currentTimeMillis() - idleStartTime : 0;
    }
}
