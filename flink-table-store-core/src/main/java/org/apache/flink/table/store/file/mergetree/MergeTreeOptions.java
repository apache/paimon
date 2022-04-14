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

package org.apache.flink.table.store.file.mergetree;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ReadableConfig;

import java.util.HashSet;
import java.util.Set;

/** Options for merge tree. */
public class MergeTreeOptions {

    public static final ConfigOption<MemorySize> WRITE_BUFFER_SIZE =
            ConfigOptions.key("write-buffer-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("128 mb"))
                    .withDescription(
                            "Amount of data to build up in memory before converting to a sorted on-disk file.");

    public static final ConfigOption<MemorySize> PAGE_SIZE =
            ConfigOptions.key("page-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("1 mb"))
                    .withDescription("Memory page size.");

    public static final ConfigOption<MemorySize> TARGET_FILE_SIZE =
            ConfigOptions.key("target-file-size")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(128))
                    .withDescription("Target size of a file.");

    public static final ConfigOption<Integer> NUM_SORTED_RUNS_MAX =
            ConfigOptions.key("num-sorted-run.max")
                    .intType()
                    .defaultValue(5)
                    .withDescription(
                            "The max sorted run number. Includes level0 files (one file one sorted run) and "
                                    + "high-level runs (one level one sorted run).");

    public static final ConfigOption<Integer> NUM_LEVELS =
            ConfigOptions.key("num-levels")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Total level number, for example, there are 3 levels, including 0,1,2 levels.");

    public static final ConfigOption<Boolean> COMMIT_FORCE_COMPACT =
            ConfigOptions.key("commit.force-compact")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to force a compaction before commit.");

    public static final ConfigOption<Integer> COMPACTION_MAX_SIZE_AMPLIFICATION_PERCENT =
            ConfigOptions.key("compaction.max-size-amplification-percent")
                    .intType()
                    .defaultValue(200)
                    .withDescription(
                            "The size amplification is defined as the amount (in percentage) of additional storage "
                                    + "needed to store a single byte of data in the merge tree.");

    public static final ConfigOption<Integer> COMPACTION_SIZE_RATIO =
            ConfigOptions.key("compaction.size-ratio")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "Percentage flexibility while comparing sorted run size. If the candidate sorted run(s) "
                                    + "size is 1% smaller than the next sorted run's size, then include next sorted run "
                                    + "into this candidate set.");

    public final long writeBufferSize;

    public final long pageSize;

    public final long targetFileSize;

    public final int numSortedRunMax;

    public final int numLevels;

    public final boolean commitForceCompact;

    public final int maxSizeAmplificationPercent;

    public final int sizeRatio;

    public MergeTreeOptions(
            long writeBufferSize,
            long pageSize,
            long targetFileSize,
            int numSortedRunMax,
            Integer numLevels,
            boolean commitForceCompact,
            int maxSizeAmplificationPercent,
            int sizeRatio) {
        this.writeBufferSize = writeBufferSize;
        this.pageSize = pageSize;
        this.targetFileSize = targetFileSize;
        this.numSortedRunMax = numSortedRunMax;
        // By default, this ensures that the compaction does not fall to level 0, but at least to
        // level 1
        this.numLevels = numLevels == null ? numSortedRunMax + 1 : numLevels;
        this.commitForceCompact = commitForceCompact;
        this.maxSizeAmplificationPercent = maxSizeAmplificationPercent;
        this.sizeRatio = sizeRatio;
    }

    public MergeTreeOptions(ReadableConfig config) {
        this(
                config.get(WRITE_BUFFER_SIZE).getBytes(),
                config.get(PAGE_SIZE).getBytes(),
                config.get(TARGET_FILE_SIZE).getBytes(),
                config.get(NUM_SORTED_RUNS_MAX),
                config.get(NUM_LEVELS),
                config.get(COMMIT_FORCE_COMPACT),
                config.get(COMPACTION_MAX_SIZE_AMPLIFICATION_PERCENT),
                config.get(COMPACTION_SIZE_RATIO));
    }

    public static Set<ConfigOption<?>> allOptions() {
        Set<ConfigOption<?>> allOptions = new HashSet<>();
        allOptions.add(WRITE_BUFFER_SIZE);
        allOptions.add(PAGE_SIZE);
        allOptions.add(TARGET_FILE_SIZE);
        allOptions.add(NUM_SORTED_RUNS_MAX);
        allOptions.add(NUM_LEVELS);
        allOptions.add(COMMIT_FORCE_COMPACT);
        allOptions.add(COMPACTION_MAX_SIZE_AMPLIFICATION_PERCENT);
        allOptions.add(COMPACTION_SIZE_RATIO);
        return allOptions;
    }
}
