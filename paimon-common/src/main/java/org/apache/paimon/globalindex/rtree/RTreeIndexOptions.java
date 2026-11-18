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

package org.apache.paimon.globalindex.rtree;

import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;
import org.apache.paimon.options.MemorySize;

/** Options for immutable, partitioned XY R-tree indexes. */
public class RTreeIndexOptions {

    public static final ConfigOption<Integer> PAGE_SIZE =
            ConfigOptions.key("rtree-index.page-size")
                    .intType()
                    .defaultValue(256)
                    .withDescription("Maximum entries in an R-tree page, from 2 to 4096.");

    public static final ConfigOption<Integer> PARTITION_ROW_COUNT =
            ConfigOptions.key("rtree-index.partition-row-count")
                    .intType()
                    .defaultValue(65536)
                    .withDescription(
                            "Maximum source rows buffered per R-tree file, from 1 to 1048576.");

    public static final ConfigOption<Integer> READ_CACHE_PAGES =
            ConfigOptions.key("rtree-index.read-cache-pages")
                    .intType()
                    .defaultValue(16)
                    .withDescription(
                            "Legacy shared cache limit in pages of the configured page size. "
                                    + "Used only when explicitly set and read-cache-size is absent.");

    public static final ConfigOption<MemorySize> READ_CACHE_SIZE =
            ConfigOptions.key("rtree-index.read-cache-size")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(8))
                    .withDescription(
                            "Maximum cached page bytes shared by all readers from one indexer. "
                                    + "Zero disables caching.");

    public static final ConfigOption<Integer> READ_FILE_CONCURRENCY =
            ConfigOptions.key("rtree-index.read-file-concurrency")
                    .intType()
                    .defaultValue(4)
                    .withDescription("Maximum index files read concurrently per reader query.");

    public static final ConfigOption<MemorySize> READ_BATCH_SIZE =
            ConfigOptions.key("rtree-index.read-batch-size")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(1))
                    .withDescription(
                            "Maximum page bytes per read batch, from 4 KiB to 16 MiB. "
                                    + "A batch always accommodates at least one page.");
}
