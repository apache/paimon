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

package org.apache.paimon.format.sst;

import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;

/** Options for SST Format. */
public class SstOptions {

    public static final ConfigOption<Boolean> BLOOM_FILTER_ENABLED =
            ConfigOptions.key("sst.bloom-filter.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to enable the bloom filter for SST Files.");

    public static final ConfigOption<Double> BLOOM_FILTER_FPP =
            ConfigOptions.key("sst.bloom-filter.fpp")
                    .doubleType()
                    .defaultValue(0.05)
                    .withDescription(
                            "Define the default false positive probability for SST Files bloom filters.");

    public static final ConfigOption<Integer> BLOOM_FILTER_EXPECTED_ENTRY_NUM =
            ConfigOptions.key("sst.bloom-filter.expected-entry-num")
                    .intType()
                    .defaultValue(1000_000)
                    .withDescription(
                            "Defines the estimated entry num of bloom filter, user should set this"
                                    + " value slightly greater than normal file record num.");
}
