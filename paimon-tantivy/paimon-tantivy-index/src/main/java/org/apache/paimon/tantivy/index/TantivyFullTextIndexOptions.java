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

package org.apache.paimon.tantivy.index;

import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;

/** Options for the Tantivy full-text index. */
public class TantivyFullTextIndexOptions {

    public static final ConfigOption<Integer> SEARCHER_POOL_MAX_SIZE =
            ConfigOptions.key("tantivy.searcher-pool.max-size")
                    .intType()
                    .defaultValue(32)
                    .withDescription(
                            "Maximum number of idle TantivySearcher instances kept in the pool "
                                    + "across all index shards. Each entry holds the index open in "
                                    + "Rust memory (including the FST term dictionary), so memory "
                                    + "usage scales with this value times the index size per shard. "
                                    + "Set to 0 to disable pooling.");
}
