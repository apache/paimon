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

import org.apache.paimon.CoreOptions;

import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import java.util.Map;

/**
 * Source enumerator metrics.
 *
 * <p>This class manages metrics for the source split enumerator.
 */
public class FileStoreSourceEnumeratorMetrics {

    /**
     * Metric name for source scaling max parallelism. This metric provides a recommended upper
     * bound of parallelism for auto-scaling systems. For fixed bucket tables, this equals the
     * bucket number; for dynamic bucket tables, this equals the current parallelism. Note: This is
     * a recommendation, not a hard limit - users can configure higher parallelism manually if
     * needed.
     */
    public static final String SCALING_MAX_PARALLELISM = "sourceScalingMaxParallelism";

    private final int scalingMaxParallelism;

    /**
     * Creates enumerator metrics and registers them with the given metric group.
     *
     * @param context the split enumerator context
     * @param options the source options
     */
    public FileStoreSourceEnumeratorMetrics(
            SplitEnumeratorContext<?> context, Map<String, String> options) {
        int bucketNum = CoreOptions.fromMap(options).bucket();
        // Dynamic bucket mode uses -1.
        // In this case, scaling max parallelism equals current parallelism.
        this.scalingMaxParallelism = bucketNum < 0 ? context.currentParallelism() : bucketNum;

        context.metricGroup().gauge(SCALING_MAX_PARALLELISM, this::getScalingMaxParallelism);
    }

    public int getScalingMaxParallelism() {
        return scalingMaxParallelism;
    }
}
