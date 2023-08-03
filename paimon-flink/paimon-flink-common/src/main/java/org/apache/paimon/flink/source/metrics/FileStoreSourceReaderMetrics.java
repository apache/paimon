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

import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.concurrent.ConcurrentMap;


/**
 * Source reader metrics.
 */
public class FileStoreSourceReaderMetrics {
    private static final Logger LOG = LoggerFactory.getLogger(FileStoreSourceReaderMetrics.class);

    // Source reader metric group
    private final SourceReaderMetricGroup sourceReaderMetricGroup;

    // Map for tracking records lag of topic partitions
    @Nullable
    private ConcurrentMap<Integer, Long> recordsLagMetrics;

    public FileStoreSourceReaderMetrics(SourceReaderMetricGroup sourceReaderMetricGroup) {
        this.sourceReaderMetricGroup = sourceReaderMetricGroup;
        this.sourceReaderMetricGroup.setPendingRecordsGauge(
                () -> {
                    long pendingRecordsTotal = 0;
                    for (long recordsLag : this.recordsLagMetrics.values()) {
                        pendingRecordsTotal += recordsLag;
                    }
                    return pendingRecordsTotal;
                });
    }

    /**
     * Add a partition's records-lag metric to tracking list if this partition never appears before.
     *
     * <p>This method also lazily register {@link
     * org.apache.flink.runtime.metrics.MetricNames#PENDING_RECORDS} in {@link
     * SourceReaderMetricGroup}
     *
     */
    public void updateRecordsLag(int subTask, long bucketLag) {
        recordsLagMetrics.putIfAbsent(subTask, bucketLag);
    }
}
