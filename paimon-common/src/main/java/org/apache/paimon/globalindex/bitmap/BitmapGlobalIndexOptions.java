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

package org.apache.paimon.globalindex.bitmap;

import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;
import org.apache.paimon.options.MemorySize;

/** Options for bitmap global index. */
public class BitmapGlobalIndexOptions {

    public static final ConfigOption<MemorySize> BITMAP_INDEX_DICTIONARY_BLOCK_SIZE =
            ConfigOptions.key("bitmap-index.dictionary-block-size")
                    .memoryType()
                    .defaultValue(MemorySize.ofKibiBytes(16))
                    .withDescription("The target dictionary block size for bitmap global index.");

    public static final ConfigOption<String> BITMAP_INDEX_COMPRESSION =
            ConfigOptions.key("bitmap-index.compression")
                    .stringType()
                    .defaultValue("none")
                    .withDescription(
                            "The compression algorithm to use for bitmap dictionary blocks.");

    public static final ConfigOption<Integer> BITMAP_INDEX_COMPRESSION_LEVEL =
            ConfigOptions.key("bitmap-index.compression-level")
                    .intType()
                    .defaultValue(1)
                    .withDescription("The compression level of the bitmap dictionary block codec.");

    public static final ConfigOption<MemorySize> BITMAP_INDEX_FALLBACK_SCAN_MAX_SIZE =
            ConfigOptions.key("bitmap-index.fallback-scan-max-size")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(256))
                    .withDescription(
                            "The maximum total bitmap global index file size to allow fallback "
                                    + "dictionary scans for predicates that cannot use direct "
                                    + "bitmap lookup.");
}
