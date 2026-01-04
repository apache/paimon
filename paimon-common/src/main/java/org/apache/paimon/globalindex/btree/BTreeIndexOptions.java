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

package org.apache.paimon.globalindex.btree;

import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;
import org.apache.paimon.options.MemorySize;

/** Options for BTree index. */
public class BTreeIndexOptions {

    public static final ConfigOption<String> BTREE_INDEX_COMPRESSION =
            ConfigOptions.key("btree-index.compression")
                    .stringType()
                    .defaultValue("none")
                    .withDescription("The compression algorithm to use for BTreeIndex");

    public static final ConfigOption<Integer> BTREE_INDEX_COMPRESSION_LEVEL =
            ConfigOptions.key("btree-index.compression")
                    .intType()
                    .defaultValue(1)
                    .withDescription("The compression level of the compression algorithm");

    public static final ConfigOption<MemorySize> BTREE_INDEX_BLOCK_SIZE =
            ConfigOptions.key("btree-index.block-size")
                    .memoryType()
                    .defaultValue(MemorySize.ofKibiBytes(64))
                    .withDescription("The block size to use for BTreeIndex");

    public static final ConfigOption<MemorySize> BTREE_INDEX_CACHE_SIZE =
            ConfigOptions.key("btree-index.cache-size")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(128))
                    .withDescription("The cache size to use for BTreeIndex");

    public static final ConfigOption<Double> BTREE_INDEX_HIGH_PRIORITY_POOL_RATIO =
            ConfigOptions.key("btree-index.high-priority-pool-ratio")
                    .doubleType()
                    .defaultValue(0.1)
                    .withDescription("The high priority pool ratio to use for BTreeIndex");
}
