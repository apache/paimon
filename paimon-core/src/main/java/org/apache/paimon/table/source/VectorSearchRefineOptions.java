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

package org.apache.paimon.table.source;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Shared refine/rerank option resolution for vector search reads. */
final class VectorSearchRefineOptions {

    private VectorSearchRefineOptions() {}

    static int resolve(
            Map<String, String> queryOptions,
            Map<String, String> tableOptions,
            String vectorColumn,
            @Nullable String indexType) {
        String value = resolve(queryOptions, vectorColumn, indexType);
        if (value == null) {
            value = resolve(tableOptions, vectorColumn, indexType);
        }
        if (value == null) {
            return 0;
        }
        try {
            int factor = Integer.parseInt(value);
            if (factor <= 0) {
                throw new IllegalArgumentException(
                        "Vector refine factor must be positive, got: " + value);
            }
            return factor;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Invalid vector refine factor: " + value + ". Must be an integer.", e);
        }
    }

    static int searchLimit(int limit, int refineFactor) {
        if (refineFactor == 0) {
            return limit;
        }
        if (limit > Integer.MAX_VALUE / refineFactor) {
            throw new IllegalArgumentException(
                    String.format(
                            "Vector search limit overflow: limit=%d, refine factor=%d",
                            limit, refineFactor));
        }
        return limit * refineFactor;
    }

    @Nullable
    private static String resolve(
            Map<String, String> options, String vectorColumn, @Nullable String indexType) {
        List<String> prefixes = new ArrayList<>();
        String fieldPrefix = "fields." + vectorColumn + ".";
        addPrefixes(prefixes, fieldPrefix, indexType);
        addPrefixes(prefixes, "", indexType);

        for (String prefix : prefixes) {
            String value = option(options, prefix + "refine_factor");
            if (value == null) {
                value = option(options, prefix + "refine-factor");
            }
            if (value == null) {
                value = option(options, prefix + "rerank_factor");
            }
            if (value == null) {
                value = option(options, prefix + "rerank-factor");
            }
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    private static void addPrefixes(
            List<String> prefixes, String base, @Nullable String indexType) {
        if (indexType != null && !indexType.isEmpty()) {
            prefixes.add(base + indexType + ".");
            String normalizedIndexType = normalizeIndexType(indexType);
            if (!normalizedIndexType.equals(indexType)) {
                prefixes.add(base + normalizedIndexType + ".");
            }
            if (normalizedIndexType.startsWith("ivf")) {
                prefixes.add(base + "ivf.");
            }
        }
        prefixes.add(base);
    }

    @Nullable
    private static String option(Map<String, String> options, String key) {
        String value = options.get(key);
        return value == null ? null : value.trim();
    }

    private static String normalizeIndexType(String indexType) {
        return indexType.toLowerCase(Locale.ROOT).replace('-', '_');
    }
}
