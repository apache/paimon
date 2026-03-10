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

package org.apache.paimon.lumina.index;

import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;
import org.apache.paimon.options.Options;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Options for the Lumina vector index.
 *
 * <p>All option keys use the {@code lumina.} prefix so that both paimon-cpp and paimon-lumina share
 * the same table properties. For example:
 *
 * <pre>
 *   lumina.index.dimension = 1024
 *   lumina.distance.metric = inner_product
 *   lumina.encoding.type   = pq
 *   lumina.diskann.build.ef_construction = 128
 *   lumina.diskann.search.list_size = 100
 * </pre>
 *
 * <p>Use {@link #toLuminaOptions()} to obtain a {@code Map<String, String>} with the {@code
 * lumina.} prefix stripped, suitable for passing directly to the native Lumina API. This mirrors
 * paimon-cpp's {@code FetchOptionsWithPrefix("lumina.", options)}.
 */
public class LuminaVectorIndexOptions {

    /** The common prefix for all Lumina options. */
    public static final String LUMINA_PREFIX = "lumina.";

    public static final ConfigOption<Integer> DIMENSION =
            ConfigOptions.key("lumina.index.dimension")
                    .intType()
                    .defaultValue(128)
                    .withDescription("The dimension of the vector.");

    public static final ConfigOption<String> INDEX_TYPE =
            ConfigOptions.key("lumina.index.type")
                    .stringType()
                    .defaultValue("diskann")
                    .withDescription("The index type for vector search diskann.");

    public static final ConfigOption<String> DISTANCE_METRIC =
            ConfigOptions.key("lumina.distance.metric")
                    .stringType()
                    .defaultValue("l2")
                    .withDescription(
                            "The distance metric for vector search (l2, cosine, inner_product).");

    public static final ConfigOption<String> ENCODING_TYPE =
            ConfigOptions.key("lumina.encoding.type")
                    .stringType()
                    .defaultValue("pq")
                    .withDescription("The encoding type for vectors (rawf32, sq8, pq).");

    public static final ConfigOption<Double> PRETRAIN_SAMPLE_RATIO =
            ConfigOptions.key("lumina.pretrain.sample_ratio")
                    .doubleType()
                    .defaultValue(0.2)
                    .withDescription("The sample ratio for pretraining.");

    public static final ConfigOption<Integer> DISKANN_BUILD_EF_CONSTRUCTION =
            ConfigOptions.key("lumina.diskann.build.ef_construction")
                    .intType()
                    .defaultValue(1024)
                    .withDescription(
                            "Controls the size of the dynamic candidate list during graph construction.");

    public static final ConfigOption<Integer> DISKANN_BUILD_NEIGHBOR_COUNT =
            ConfigOptions.key("lumina.diskann.build.neighbor_count")
                    .intType()
                    .defaultValue(64)
                    .withDescription("Maximum number of neighbors per node in the graph.");

    public static final ConfigOption<Integer> DISKANN_BUILD_THREAD_COUNT =
            ConfigOptions.key("lumina.diskann.build.thread_count")
                    .intType()
                    .defaultValue(64)
                    .withDescription("Number of threads used for DiskANN index building.");

    public static final ConfigOption<Integer> DISKANN_SEARCH_LIST_SIZE =
            ConfigOptions.key("lumina.diskann.search.list_size")
                    .intType()
                    .defaultValue(1024)
                    .withDescription("The search list size for DiskANN search.");

    public static final ConfigOption<Integer> DISKANN_SEARCH_BEAM_WIDTH =
            ConfigOptions.key("lumina.diskann.search.beam_width")
                    .intType()
                    .defaultValue(4)
                    .withDescription("The beam width for DiskANN search.");

    public static final ConfigOption<Integer> SEARCH_PARALLEL_NUMBER =
            ConfigOptions.key("lumina.search.parallel_number")
                    .intType()
                    .defaultValue(5)
                    .withDescription("The parallel number for search.");

    private final int dimension;
    private final LuminaVectorMetric metric;
    private final Map<String, String> luminaOptions;

    public LuminaVectorIndexOptions(Options options) {
        this.dimension = validatePositive(options.get(DIMENSION), DIMENSION.key());
        this.metric = parseMetric(options.get(DISTANCE_METRIC));
        validateEncodingMetricCombination(options.get(ENCODING_TYPE), this.metric);
        this.luminaOptions = buildLuminaOptions(options);
    }

    /**
     * Returns all {@code lumina.*} options with the prefix stripped, producing native Lumina keys.
     * For example, {@code lumina.diskann.build.ef_construction} becomes {@code
     * diskann.build.ef_construction}.
     */
    public Map<String, String> toLuminaOptions() {
        return new LinkedHashMap<>(luminaOptions);
    }

    public int dimension() {
        return dimension;
    }

    public LuminaVectorMetric metric() {
        return metric;
    }

    /**
     * Converts a {@link ConfigOption} key (e.g. {@code lumina.index.dimension}) to the native
     * Lumina key (e.g. {@code index.dimension}) by stripping the {@link #LUMINA_PREFIX}.
     */
    public static String toLuminaKey(ConfigOption<?> option) {
        String key = option.key();
        if (key.startsWith(LUMINA_PREFIX)) {
            return key.substring(LUMINA_PREFIX.length());
        }
        return key;
    }

    /** All ConfigOptions with defaults that should always appear in lumina options. */
    @SuppressWarnings("rawtypes")
    private static final List<ConfigOption> ALL_OPTIONS =
            Arrays.asList(
                    DIMENSION,
                    INDEX_TYPE,
                    DISTANCE_METRIC,
                    ENCODING_TYPE,
                    PRETRAIN_SAMPLE_RATIO,
                    DISKANN_BUILD_EF_CONSTRUCTION,
                    DISKANN_BUILD_NEIGHBOR_COUNT,
                    DISKANN_BUILD_THREAD_COUNT,
                    DISKANN_SEARCH_LIST_SIZE,
                    DISKANN_SEARCH_BEAM_WIDTH,
                    SEARCH_PARALLEL_NUMBER);

    /**
     * Builds native Lumina options by first populating all known ConfigOptions (with defaults) and
     * then overlaying any user-specified {@code lumina.*} options. This ensures that required keys
     * like {@code index.type} are always present in the metadata, matching paimon-cpp behavior.
     */
    @SuppressWarnings("unchecked")
    private static Map<String, String> buildLuminaOptions(Options options) {
        Map<String, String> result = new LinkedHashMap<>();
        // Populate all known options with their resolved values (user-set or default).
        for (ConfigOption<?> opt : ALL_OPTIONS) {
            Object value = options.get(opt);
            if (value != null) {
                result.put(toLuminaKey(opt), String.valueOf(value));
            }
        }
        // Overlay any extra user-specified lumina.* options not in ALL_OPTIONS.
        for (Map.Entry<String, String> entry : options.toMap().entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(LUMINA_PREFIX)) {
                result.putIfAbsent(key.substring(LUMINA_PREFIX.length()), entry.getValue());
            }
        }
        return result;
    }

    /**
     * Parses the distance metric string, accepting both lumina native names (l2, cosine,
     * inner_product) and enum names (L2, COSINE, INNER_PRODUCT).
     */
    private static LuminaVectorMetric parseMetric(String value) {
        try {
            return LuminaVectorMetric.fromLuminaName(value);
        } catch (IllegalArgumentException e) {
            return LuminaVectorMetric.fromString(value);
        }
    }

    private static void validateEncodingMetricCombination(
            String encoding, LuminaVectorMetric metric) {
        if ("pq".equalsIgnoreCase(encoding) && metric == LuminaVectorMetric.COSINE) {
            throw new IllegalArgumentException(
                    "Lumina does not support PQ encoding with cosine metric. "
                            + "Please use 'rawf32' or 'sq8' encoding, or switch to 'l2' or 'inner_product' metric.");
        }
    }

    private static int validatePositive(int value, String key) {
        if (value <= 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid value for '%s': %d. Must be a positive integer.", key, value));
        }
        return value;
    }
}
