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

package org.apache.paimon.format.mosaic;

import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;

/** Configuration options for the Mosaic file format. */
public class MosaicOptions {

    public static final ConfigOption<Integer> NUM_COLUMN_BUCKETS =
            ConfigOptions.key("mosaic.num-column-buckets")
                    .intType()
                    .defaultValue(100)
                    .withDescription(
                            "Number of column buckets in the Mosaic format. "
                                    + "Columns are hashed into this many buckets.");

    public static final ConfigOption<Integer> DICT_MAX_TOTAL_BYTES =
            ConfigOptions.key("mosaic.dict-max-total-bytes")
                    .intType()
                    .defaultValue(32768)
                    .withDescription(
                            "Maximum cumulative bytes for variable-width dictionary "
                                    + "entries per column.");

    public static final ConfigOption<Integer> DICT_MAX_ENTRIES =
            ConfigOptions.key("mosaic.dict-max-entries")
                    .intType()
                    .defaultValue(255)
                    .withDescription(
                            "Maximum number of distinct values for dictionary encoding "
                                    + "per column. Columns exceeding this fall back to PLAIN.");
}
