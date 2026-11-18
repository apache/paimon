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

package org.apache.paimon.spark.globalindex;

import org.apache.paimon.options.Options;
import org.apache.paimon.spark.globalindex.sorted.SortedIndexTopoBuilder;

/** Utility class for creating {@link GlobalIndexTopologyBuilder} implementations. */
public class GlobalIndexTopologyBuilderUtils {

    public static GlobalIndexTopologyBuilder createTopoBuilder(String indexType) {
        if (SortedIndexTopoBuilder.supports(indexType)) {
            return new SortedIndexTopoBuilder();
        }
        return new DefaultGlobalIndexTopoBuilder();
    }

    public static GlobalIndexTopologyBuilder createTopoBuilder(String indexType, Options options) {
        return createTopoBuilder(indexType, options, null);
    }

    public static GlobalIndexTopologyBuilder createTopoBuilder(
            String indexType, Options options, String fieldName) {
        String key = indexType + ".shard";
        String fieldKey = "fields." + fieldName + ".shard";
        java.util.Map<String, String> dynamic = options.dynamicOptions();
        java.util.Map<String, String> stored = options.toMap();
        String shard =
                dynamic.containsKey(fieldKey)
                        ? dynamic.get(fieldKey)
                        : dynamic.containsKey(key)
                                ? dynamic.get(key)
                                : stored.getOrDefault(
                                        fieldKey, stored.getOrDefault(key, "range-based"));
        if (org.apache.paimon.globalindex.VectorIndexUtils.isVectorIndexType(indexType)) {
            if ("centroid-based".equals(shard)) {
                return new CentroidGlobalIndexTopoBuilder();
            }
            if (!"range-based".equals(shard)) {
                throw new IllegalArgumentException("Unknown vector shard mode: " + shard);
            }
        }
        if ("centroid-based".equals(shard)) {
            throw new IllegalArgumentException(
                    "Centroid sharding requires a supported vector index.");
        }
        return createTopoBuilder(indexType);
    }
}
