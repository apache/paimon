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

package org.apache.paimon.flink.sink;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.BatchExecutionOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/** Get adaptive config from Flink. Only work for Flink 1.17+. */
public class AdaptiveParallelism {

    public static boolean isEnabled(StreamExecutionEnvironment env) {
        return env.getConfiguration().get(BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_ENABLED);
    }

    /**
     * Get default max parallelism of AdaptiveBatchScheduler of Flink. See {@link
     * org.apache.flink.runtime.scheduler.adaptivebatch.AdaptiveBatchSchedulerFactory#getDefaultMaxParallelism(Configuration,
     * ExecutionConfig)}.
     */
    public static int getDefaultMaxParallelism(
            ReadableConfig configuration, ExecutionConfig executionConfig) {
        return configuration
                .getOptional(BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_MAX_PARALLELISM)
                .orElse(
                        executionConfig.getParallelism() == ExecutionConfig.PARALLELISM_DEFAULT
                                ? BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_MAX_PARALLELISM
                                        .defaultValue()
                                : executionConfig.getParallelism());
    }
}
