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

import org.apache.paimon.data.BlobIngestObservations;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

/** Registry bridge for native Blob ingest observations. */
public final class BlobIngestMetrics implements AutoCloseable {

    public static final String GROUP_NAME = "blobIngest";

    private final String tableName;
    private final Map<String, Counter> counters = new HashMap<>();
    private final BlobIngestObservations.Observer observer = this::record;
    @Nullable private MetricGroup group;

    public BlobIngestMetrics(String tableName) {
        this.tableName = tableName;
    }

    public void withMetricRegistry(MetricRegistry registry) {
        close();
        try {
            group = registry.createTableMetricGroup(GROUP_NAME, tableName);
        } catch (RuntimeException ignored) {
            // Instrumentation is best effort.
        }
    }

    public BlobIngestObservations.Scope open() {
        return BlobIngestObservations.observe(group == null ? null : observer);
    }

    private void record(String stage, String outcome, long value) {
        String name = stage + "_" + outcome;
        counters.computeIfAbsent(name, group::counter).inc(value);
    }

    @Override
    public void close() {
        MetricGroup previous = group;
        group = null;
        counters.clear();
        if (previous != null) {
            try {
                previous.close();
            } catch (RuntimeException ignored) {
                // Instrumentation is best effort.
            }
        }
    }
}
