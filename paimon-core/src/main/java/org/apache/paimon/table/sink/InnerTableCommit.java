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

package org.apache.paimon.table.sink;

import org.apache.paimon.metrics.MetricRegistry;

import javax.annotation.Nullable;

import java.util.Map;

/** Inner {@link TableCommit} contains overwrite setter. */
public interface InnerTableCommit extends StreamTableCommit, BatchTableCommit {

    /** Overwrite writing, same as the 'INSERT OVERWRITE T PARTITION (...)' semantics of SQL. */
    InnerTableCommit withOverwrite(@Nullable Map<String, String> staticPartition);

    /**
     * If this is set to true, when there is no new data, no snapshot will be generated. By default,
     * empty commit is ignored.
     *
     * <ul>
     *   <li>For Streaming: the default value of 'ignoreEmptyCommit' is false.
     *   <li>For Batch: the default value of 'ignoreEmptyCommit' is true.
     * </ul>
     *
     * <p>If there are no new files or compact files at the same time, no new commit will be
     * generated regardless of the configuration (No one trigger commit interface).
     */
    InnerTableCommit ignoreEmptyCommit(boolean ignoreEmptyCommit);

    InnerTableCommit withMetricRegistry(MetricRegistry registry);

    default boolean isInnerTableCommit() {
        return true;
    }

    default InnerTableCommit asInnerTableCommit() {
        return this;
    }
}
