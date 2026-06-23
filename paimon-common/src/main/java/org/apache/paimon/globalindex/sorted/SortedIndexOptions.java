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

package org.apache.paimon.globalindex.sorted;

import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;

/** Options for sorted global index build. */
public class SortedIndexOptions {

    public static final ConfigOption<Long> SORTED_INDEX_RECORDS_PER_RANGE =
            ConfigOptions.key("sorted-index.records-per-range")
                    .longType()
                    .defaultValue(10_000_000L)
                    .withFallbackKeys("btree-index.records-per-range")
                    .withDescription("The expected number of records per sorted index file.");

    public static final ConfigOption<Integer> SORTED_INDEX_BUILD_MAX_PARALLELISM =
            ConfigOptions.key("sorted-index.build.max-parallelism")
                    .intType()
                    .defaultValue(4096)
                    .withFallbackKeys("btree-index.build.max-parallelism")
                    .withDescription(
                            "The max parallelism of Flink/Spark for building sorted indexes.");
}
