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

package org.apache.paimon.flink.action;

import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.FlinkConnectorOptions.CompactionBucketDistributionStrategy;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for compaction bucket distribution strategy resolution. */
public class CompactionBucketDistributionStrategyTest {

    @Test
    public void testDefaultStrategyIsLinear() {
        assertThat(
                        Options.fromMap(java.util.Collections.emptyMap())
                                .get(FlinkConnectorOptions.COMPACTION_BUCKET_DISTRIBUTION_STRATEGY))
                .isEqualTo(CompactionBucketDistributionStrategy.LINEAR);
    }

    @Test
    public void testStrategyOptionParsing() {
        assertThat(strategy("linear")).isEqualTo(CompactionBucketDistributionStrategy.LINEAR);
        assertThat(strategy("size-aware-batch"))
                .isEqualTo(CompactionBucketDistributionStrategy.SIZE_AWARE_BATCH);
    }

    @Test
    public void testStrategyOnlyAppliesToBatchFullCompaction() {
        Options options =
                Options.fromMap(
                        java.util.Collections.singletonMap(
                                FlinkConnectorOptions.COMPACTION_BUCKET_DISTRIBUTION_STRATEGY.key(),
                                "size-aware-batch"));

        assertThat(CompactAction.compactionBucketDistributionStrategy(options, true, false))
                .isEqualTo(CompactionBucketDistributionStrategy.SIZE_AWARE_BATCH);
        assertThat(CompactAction.compactionBucketDistributionStrategy(options, false, false))
                .isEqualTo(CompactionBucketDistributionStrategy.LINEAR);
        assertThat(CompactAction.compactionBucketDistributionStrategy(options, true, true))
                .isEqualTo(CompactionBucketDistributionStrategy.LINEAR);
    }

    private static CompactionBucketDistributionStrategy strategy(String value) {
        return Options.fromMap(
                        java.util.Collections.singletonMap(
                                FlinkConnectorOptions.COMPACTION_BUCKET_DISTRIBUTION_STRATEGY.key(),
                                value))
                .get(FlinkConnectorOptions.COMPACTION_BUCKET_DISTRIBUTION_STRATEGY);
    }
}
