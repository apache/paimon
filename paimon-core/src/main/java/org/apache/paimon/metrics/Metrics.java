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

package org.apache.paimon.metrics;

import java.util.ArrayList;
import java.util.List;

/** Core of Paimon metrics system. */
public class Metrics {
    private static volatile Metrics instance = new Metrics();

    /**
     * The metrics groups. All the commit & compaction & scan metric groups are collected in this
     * group container, there is no need to distinguish the groups by group name for reporters.
     */
    private final List<MetricGroup> metricGroups = new ArrayList<>();

    private Metrics() {}

    public static Metrics getInstance() {
        return instance;
    }

    /**
     * Add a metric group. Which is called by {@link org.apache.paimon.metrics.commit.CommitMetrics}
     * and other metrics instance
     */
    public synchronized void addGroup(AbstractMetricGroup group) {
        metricGroups.add(group);
    }

    /** Get metric groups. */
    public synchronized List<MetricGroup> getMetricGroups() {
        return metricGroups;
    }
}
