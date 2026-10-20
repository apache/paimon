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

package org.apache.paimon.compact;

import org.apache.paimon.CoreOptions;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Runtime options for physical compaction and clustering rewrites. */
public final class CompactOptions {

    public static final Map<String, String> REWRITE_OPTIONS = rewriteOptions();

    private CompactOptions() {}

    private static Map<String, String> rewriteOptions() {
        Map<String, String> options = new HashMap<>();
        // Preserve stored references, including views of upstream values that are currently null.
        options.put(CoreOptions.BLOB_AS_DESCRIPTOR.key(), "true");
        options.put(CoreOptions.BLOB_VIEW_RESOLVE_ENABLED.key(), "false");
        // Keep the layout produced by the rewrite instead of applying ingestion optimizations.
        options.put(CoreOptions.PARTITION_SINK_STRATEGY.key(), "none");
        options.put(CoreOptions.CLUSTERING_INCREMENTAL_OPTIMIZE_WRITE.key(), "false");
        return Collections.unmodifiableMap(options);
    }
}
