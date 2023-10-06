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

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/** A Table level repostory for metricGroup. */
public class MetricRepository {
    private final Queue<MetricGroup> metricGroups = new ConcurrentLinkedQueue<>();
    private final String tableName;
    private final String metricName;

    private final String repositoryName;

    public MetricRepository(String tableName, String metricName) {
        this.tableName = tableName;
        this.metricName = metricName;
        this.repositoryName = String.format("%s-%s", tableName, metricName);
        Metrics.getInstance().addMetricRepository(repositoryName, this);
    }

    public String getTableName() {
        return tableName;
    }

    public String getMetricName() {
        return metricName;
    }

    public void registerMetricGroup(MetricGroup metricGroup) {
        metricGroups.offer(metricGroup);
    }

    public Queue<MetricGroup> getMetricGroups() {
        return metricGroups;
    }

    public void close() {
        Metrics.getInstance().removeMetricRepository(repositoryName);
        metricGroups.clear();
    }
}
