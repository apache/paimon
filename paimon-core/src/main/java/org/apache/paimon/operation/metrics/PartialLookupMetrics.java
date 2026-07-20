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

/**
 * Request-level metrics for local partial lookup. A remote access means that at least one lookup
 * file had to be created from table storage during the request.
 */
public class PartialLookupMetrics {

    public static final String GROUP_NAME = "lookup";
    public static final String PARTIAL_LOOKUP_COUNT = "partialLookupCount";
    public static final String PARTIAL_LOOKUP_REMOTE_ACCESS_COUNT =
            "partialLookupRemoteAccessCount";

    private final MetricGroup metricGroup;
    private final Counter lookupCount;
    private final Counter remoteAccessCount;

    public PartialLookupMetrics(MetricRegistry registry, String tableName) {
        this.metricGroup = registry.createTableMetricGroup(GROUP_NAME, tableName);
        this.lookupCount = metricGroup.counter(PARTIAL_LOOKUP_COUNT);
        this.remoteAccessCount = metricGroup.counter(PARTIAL_LOOKUP_REMOTE_ACCESS_COUNT);
    }

    /** Reports one lookup invocation and whether it accessed table storage. */
    public void reportLookup(boolean remoteAccessed) {
        lookupCount.inc();
        if (remoteAccessed) {
            remoteAccessCount.inc();
        }
    }

    @VisibleForTesting
    public MetricGroup metricGroup() {
        return metricGroup;
    }

    @VisibleForTesting
    public long lookupCount() {
        return lookupCount.getCount();
    }

    @VisibleForTesting
    public long remoteAccessCount() {
        return remoteAccessCount.getCount();
    }
}
