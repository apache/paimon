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

package org.apache.paimon.options;

import org.apache.paimon.CoreOptions;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/** Vector parameter resolution shared by primary-key and Data Evolution indexes. */
public final class VectorIndexOptions {

    private VectorIndexOptions() {}

    @Nullable
    public static String indexType(Options options, String column) {
        CoreOptions core = new CoreOptions(options);
        List<String> declared =
                core.globalIndexTypes().stream()
                        .filter(CoreOptions::isVectorIndexType)
                        .filter(type -> core.globalIndexColumns(type).contains(column))
                        .collect(Collectors.toList());
        if (!declared.isEmpty()) {
            return declared.size() == 1 ? declared.get(0) : null;
        }
        for (String key :
                Arrays.asList(
                        "fields." + column + ".pk-vector.index.type",
                        "index_type",
                        "index-type",
                        "vector.index-type",
                        "fields." + column + ".index-type")) {
            String type = options.get(key);
            if (type != null) {
                return type.toLowerCase(Locale.ROOT).trim();
            }
        }
        return null;
    }

    public static Options resolve(Options options, @Nullable String type, String column) {
        return resolve(options, type, column, true);
    }

    /** Retains an unspecified metric when readers must obtain it from persisted index metadata. */
    public static Options resolve(
            Options options, @Nullable String type, String column, boolean defaultMetric) {
        if (type == null) {
            type = indexType(options, column);
            if (type == null) {
                return new Options(options.toMap(), options);
            }
        }
        String prefix = type + ".";
        String fieldPrefix = "fields." + column + ".";
        List<String> aliases =
                Arrays.asList(
                        fieldPrefix + "distance.metric",
                        fieldPrefix + "metric",
                        prefix + "distance.metric",
                        prefix + "metric");
        String metric = null;
        for (String key :
                Arrays.asList(
                        fieldPrefix + "distance.metric",
                        fieldPrefix + "metric",
                        fieldPrefix + "pk-vector.distance.metric",
                        prefix + "distance.metric",
                        prefix + "metric",
                        "distance.metric",
                        "metric")) {
            if (options.get(key) != null) {
                metric = options.get(key);
                break;
            }
        }
        Options defaults = new Options(options.toMap(), options);
        // Metric aliases are one parameter. Merge field JSON before applying that parameter,
        // so generic string conflict checks cannot reject aliases such as COSINE and cosine.
        aliases.forEach(defaults::remove);
        Options resolved = new CoreOptions(defaults).globalIndexOptions(type, column);
        for (String key : aliases) {
            if (resolved.get(key) != null) {
                metric = resolved.get(key);
                break;
            }
        }
        if (metric != null || defaultMetric) {
            String normalized = normalizeMetric(metric);
            aliases.forEach(key -> resolved.setString(key, normalized));
        }
        return resolved;
    }

    public static String metric(Options options, @Nullable String type, String column) {
        if (type == null) {
            type = indexType(options, column);
        }
        return type == null
                ? normalizeMetric(options.get("fields." + column + ".pk-vector.distance.metric"))
                : resolve(options, type, column).get(type + ".metric");
    }

    private static String normalizeMetric(@Nullable String metric) {
        return (metric == null ? "inner_product" : metric)
                .toLowerCase(Locale.ROOT)
                .replace('-', '_');
    }
}
