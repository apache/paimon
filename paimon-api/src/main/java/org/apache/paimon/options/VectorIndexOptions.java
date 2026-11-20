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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/** Vector parameter resolution shared by primary-key and Data Evolution indexes. */
public final class VectorIndexOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String LEGACY_INDEX_COLUMNS = "pk-vector.index.columns";

    @Nullable private final String type;
    private final Options options;
    private final String metric;

    private VectorIndexOptions(@Nullable String type, Options options, String metric) {
        this.type = type;
        this.options = options;
        this.metric = metric;
    }

    @Nullable
    public String type() {
        return type;
    }

    public Options options() {
        return options;
    }

    /** Returns the normalized metric, defaulting to inner product for single-vector searches. */
    public String metric() {
        return metric;
    }

    public static List<String> columns(Options options) {
        CoreOptions core = new CoreOptions(options);
        List<String> types = indexTypes(core);
        if (!types.isEmpty()) {
            return types.stream()
                    .flatMap(type -> columns(options, type).stream())
                    .collect(Collectors.toList());
        }
        return legacyColumns(options);
    }

    public static List<String> columns(Options options, String type) {
        List<String> declared = new CoreOptions(options).globalIndexColumns(type);
        return declared.isEmpty()
                ? legacyColumns(options).stream()
                        .filter(column -> type.equals(legacyIndexType(options, column)))
                        .collect(Collectors.toList())
                : declared;
    }

    private static List<String> legacyColumns(Options options) {
        String columns = options.get(LEGACY_INDEX_COLUMNS);
        return columns == null
                ? Collections.emptyList()
                : Arrays.stream(columns.split(",", -1))
                        .map(String::trim)
                        .collect(Collectors.toList());
    }

    public static String columnsOptionKey(Options options) {
        CoreOptions core = new CoreOptions(options);
        List<String> types = indexTypes(core);
        return types.isEmpty()
                ? LEGACY_INDEX_COLUMNS
                : types.stream()
                        .map(core::globalIndexColumnsOptionKey)
                        .collect(Collectors.joining(", "));
    }

    private static List<String> indexTypes(CoreOptions core) {
        return core.globalIndexTypes().stream()
                .filter(CoreOptions::isVectorIndexType)
                .collect(Collectors.toList());
    }

    @Nullable
    private static String legacyIndexType(Options options, String column) {
        String type = options.get("fields." + column + ".pk-vector.index.type");
        return type == null ? null : type.toLowerCase(Locale.ROOT).trim();
    }

    /** Resolves single-vector options, including the default metric for a known index type. */
    public static VectorIndexOptions resolve(
            Options options, @Nullable String type, String column) {
        VectorIndexOptions resolved = resolveConfigured(options, type, column);
        if (resolved.type != null && resolved.options.get(resolved.type + ".metric") == null) {
            metricAliases(resolved.type, column)
                    .forEach(key -> resolved.options.setString(key, resolved.metric));
        }
        return resolved;
    }

    /** Keeps metric aliases absent from {@link #options()} when no metric was configured. */
    public static VectorIndexOptions resolveConfigured(
            Options options, @Nullable String type, String column) {
        if (type == null) {
            CoreOptions core = new CoreOptions(options);
            List<String> declared =
                    indexTypes(core).stream()
                            .filter(
                                    candidate ->
                                            core.globalIndexColumns(candidate).contains(column))
                            .collect(Collectors.toList());
            type =
                    declared.isEmpty()
                            ? legacyIndexType(options, column)
                            : (declared.size() == 1 ? declared.get(0) : null);
            if (type == null) {
                return new VectorIndexOptions(
                        null,
                        new Options(options.toMap(), options),
                        normalizeMetric(
                                options.get("fields." + column + ".pk-vector.distance.metric")));
            }
        }
        String prefix = type + ".";
        String fieldPrefix = "fields." + column + ".";
        List<String> aliases = metricAliases(type, column);
        List<String> metricKeys =
                Arrays.asList(
                        fieldPrefix + "distance.metric",
                        fieldPrefix + "metric",
                        fieldPrefix + "pk-vector.distance.metric",
                        prefix + "distance.metric",
                        prefix + "metric",
                        "distance.metric",
                        "metric");
        Map<String, String> overrides = options.dynamicOptions();
        Map<String, String> defaults = new HashMap<>(options.toMap());
        overrides.keySet().forEach(defaults::remove);
        String fieldOptionsKey =
                options.containsKey(fieldPrefix + "index-options")
                        ? fieldPrefix + "index-options"
                        : fieldPrefix + "pk-vector.index.options";
        boolean dynamicFieldOptions = overrides.containsKey(fieldOptionsKey);
        // Expand field JSON in its own source so its keys retain the same precedence.
        // Apply metric aliases together after expansion (COSINE and cosine are equivalent).
        Options source = new Options(dynamicFieldOptions ? overrides : defaults);
        aliases.forEach(source::remove);
        Options expanded = new CoreOptions(source).globalIndexOptions(type, column);
        Options resolved =
                dynamicFieldOptions
                        ? new Options(defaults, expanded.toMap())
                        : new Options(expanded.toMap(), overrides);
        String metric = firstValue(expanded.toMap(), aliases);
        if (metric == null || !dynamicFieldOptions) {
            String override = firstValue(overrides, metricKeys);
            if (override != null) {
                metric = override;
            }
        }
        if (metric == null) {
            metric = firstValue(options.toMap(), metricKeys);
        }
        String normalized = normalizeMetric(metric);
        if (metric != null) {
            aliases.forEach(key -> resolved.setString(key, normalized));
        }
        return new VectorIndexOptions(type, resolved, normalized);
    }

    @Nullable
    private static String firstValue(Map<String, String> options, List<String> keys) {
        for (String key : keys) {
            String value = options.get(key);
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    private static List<String> metricAliases(String type, String column) {
        String fieldPrefix = "fields." + column + ".";
        return Arrays.asList(
                fieldPrefix + "distance.metric",
                fieldPrefix + "metric",
                type + ".distance.metric",
                type + ".metric");
    }

    private static String normalizeMetric(@Nullable String metric) {
        return (metric == null ? "inner_product" : metric)
                .toLowerCase(Locale.ROOT)
                .replace('-', '_');
    }
}
