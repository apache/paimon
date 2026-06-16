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

package org.apache.paimon.vector.index;

import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;

/** Options for vector index. */
public class VectorIndexOptions {

    public static final ConfigOption<Boolean> VECTOR_INDEX_BUILD_MERGE_ROW_RANGES =
            ConfigOptions.key("vector-index.build.merge-row-ranges")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to merge discontinuous row id ranges when building vector "
                                    + "indexes. This should only be enabled when row id gaps are "
                                    + "permanent and will never be filled by later writes.");
}
