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

package org.apache.paimon.globalindex.fmindex;

import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;
import org.apache.paimon.options.MemorySize;

/** Physical build and read options for FM global index. */
public final class FMGlobalIndexOptions {

    public static final ConfigOption<MemorySize> PARTITION_SIZE =
            ConfigOptions.key("fm-index.partition-size")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(16))
                    .withDescription(
                            "Maximum encoded text bytes buffered by one independently readable FM partition.");

    public static final ConfigOption<Integer> PARTITION_ROW_COUNT =
            ConfigOptions.key("fm-index.partition-row-count")
                    .intType()
                    .defaultValue(100_000)
                    .withDescription("Maximum row count in one FM partition.");

    public static final ConfigOption<Integer> SA_SAMPLE_RATE =
            ConfigOptions.key("fm-index.sa-sample-rate")
                    .intType()
                    .defaultValue(32)
                    .withDescription(
                            "Suffix-array value sampling rate. Lower values speed locate at a larger storage cost.");

    public static final ConfigOption<String> COMPRESSION =
            ConfigOptions.key("fm-index.compression")
                    .stringType()
                    .defaultValue("lz4")
                    .withDescription("Compression for independently checksummed FM blocks.");

    public static final ConfigOption<Integer> COMPRESSION_LEVEL =
            ConfigOptions.key("fm-index.compression-level")
                    .intType()
                    .defaultValue(1)
                    .withDescription("Compression level used by codecs which support levels.");

    public static final ConfigOption<Boolean> STORE_VERIFICATION_VALUES =
            ConfigOptions.key("fm-index.store-verification-values")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to store exact values for dense-query fallback. Disabling this reduces index size; dense contains predicates then fall back to scanning source data.");

    public static final ConfigOption<MemorySize> READ_CACHE_SIZE =
            ConfigOptions.key("fm-index.read-cache-size")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(64))
                    .withDescription(
                            "Maximum decoded FM rank and sample block cache size per indexer.");

    public static final ConfigOption<MemorySize> DEMAND_PAGE_SIZE =
            ConfigOptions.key("fm-index.demand-page-size")
                    .memoryType()
                    .defaultValue(MemorySize.ofKibiBytes(512))
                    .withDescription(
                            "Target contiguous read size when demand-loading FM wavelet blocks.");

    public static final ConfigOption<Double> LOCATE_COST_RATIO =
            ConfigOptions.key("fm-index.locate-cost-ratio")
                    .doubleType()
                    .defaultValue(0.001d)
                    .withDescription(
                            "Maximum estimated SA-locate work divided by source text bytes before using exact stored values or declining index evaluation.");

    private FMGlobalIndexOptions() {}
}
