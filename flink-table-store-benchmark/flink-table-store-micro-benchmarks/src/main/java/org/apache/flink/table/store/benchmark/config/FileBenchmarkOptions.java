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

package org.apache.flink.table.store.benchmark.config;

import org.apache.flink.table.store.options.ConfigOption;

import static org.apache.flink.table.store.options.ConfigOptions.key;

/** Benchmark options for file. */
public class FileBenchmarkOptions {
    public static final ConfigOption<String> FILE_DATA_BASE_DIR =
            key("benchmark.file.base-data-dir")
                    .stringType()
                    .defaultValue("/tmp/flink-table-store-micro-benchmarks")
                    .withDescription("The dir to put state data.");

    public static final ConfigOption<Long> TARGET_FILE_SIZE =
            key("benchmark.file.target-size")
                    .longType()
                    .defaultValue(1024 * 1024L)
                    .withDescription("The target file size.");

    public static final ConfigOption<Integer> WRITER_BATCH_COUNT =
            key("benchmark.writer.batch-count")
                    .intType()
                    .defaultValue(50)
                    .withDescription("The number of batches to be written by writer.");

    public static final ConfigOption<Integer> WRITER_RECORD_COUNT_PER_BATCH =
            key("benchmark.writer.record-count-per-batch")
                    .intType()
                    .defaultValue(50000)
                    .withDescription("The number of records to be written in one batch by writer.");
}
