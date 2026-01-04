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

package org.apache.paimon.spark;

import org.apache.paimon.options.ConfigOption;

import static org.apache.paimon.options.ConfigOptions.key;

/** Options for spark connector. */
public class SparkConnectorOptions {

    public static final ConfigOption<Boolean> REQUIRED_SPARK_CONFS_CHECK_ENABLED =
            key("requiredSparkConfsCheck.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to verify SparkSession is initialized with required configurations.");

    public static final ConfigOption<Boolean> MERGE_SCHEMA =
            key("write.merge-schema")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "If true, merge the data schema and the table schema automatically before write data.");

    public static final ConfigOption<Boolean> EXPLICIT_CAST =
            key("write.merge-schema.explicit-cast")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "If true, allow to merge data types if the two types meet the rules for explicit casting.");

    public static final ConfigOption<Boolean> USE_V2_WRITE =
            key("write.use-v2-write")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "If true, v2 write will be used. Currently, only HASH_FIXED and BUCKET_UNAWARE bucket modes are supported. Will fall back to v1 write for other bucket modes. Currently, Spark V2 write does not support TableCapability.STREAMING_WRITE.");

    public static final ConfigOption<Integer> MAX_FILES_PER_TRIGGER =
            key("read.stream.maxFilesPerTrigger")
                    .intType()
                    .noDefaultValue()
                    .withDescription("The maximum number of files returned in a single batch.");

    public static final ConfigOption<Long> MAX_BYTES_PER_TRIGGER =
            key("read.stream.maxBytesPerTrigger")
                    .longType()
                    .noDefaultValue()
                    .withDescription("The maximum number of bytes returned in a single batch.");

    public static final ConfigOption<Long> MAX_ROWS_PER_TRIGGER =
            key("read.stream.maxRowsPerTrigger")
                    .longType()
                    .noDefaultValue()
                    .withDescription("The maximum number of rows returned in a single batch.");

    public static final ConfigOption<Long> MIN_ROWS_PER_TRIGGER =
            key("read.stream.minRowsPerTrigger")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "The minimum number of rows returned in a single batch, which used to create MinRowsReadLimit with read.stream.maxTriggerDelayMs together.");

    public static final ConfigOption<Long> MAX_DELAY_MS_PER_TRIGGER =
            key("read.stream.maxTriggerDelayMs")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "The maximum delay between two adjacent batches, which used to create MinRowsReadLimit with read.stream.minRowsPerTrigger together.");

    public static final ConfigOption<Boolean> READ_CHANGELOG =
            key("read.changelog")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to read row in the form of changelog (add rowkind column in row to represent its change type).");

    public static final ConfigOption<Boolean> READ_ALLOW_FULL_SCAN =
            key("read.allow.fullScan")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to allow full scan when reading a partitioned table.");

    public static final ConfigOption<Boolean> SOURCE_SPLIT_TARGET_SIZE_WITH_COLUMN_PRUNING =
            key("source.split.target-size-with-column-pruning")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to adjust the target split size based on pruned (projected) columns. "
                                    + "If enabled, split size estimation uses only the columns actually being read.");
}
