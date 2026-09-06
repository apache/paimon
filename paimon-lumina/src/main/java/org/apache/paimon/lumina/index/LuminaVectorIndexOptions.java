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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;
import org.apache.paimon.options.Options;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Options for the Lumina vector index. */
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
                    .defaultValue("inner_product")
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
                    .defaultValue(32)
                    .withDescription("Number of threads used for DiskANN index building.");

    public static final ConfigOption<Integer> DISKANN_SEARCH_LIST_SIZE =
            ConfigOptions.key("lumina.diskann.search.list_size")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The search list size for DiskANN search. "
                                    + "If not set, defaults to 1.5x topK.");

    public static final ConfigOption<Integer> DISKANN_SEARCH_BEAM_WIDTH =
            ConfigOptions.key("lumina.diskann.search.beam_width")
                    .intType()
                    .defaultValue(4)
                    .withDescription("The beam width for DiskANN search.");

    public static final ConfigOption<Integer> ENCODING_PQ_M =
            ConfigOptions.key("lumina.encoding.pq.m")
                    .intType()
                    .defaultValue(64)
                    .withDescription("Number of sub-quantizers for PQ encoding.");

    public static final ConfigOption<Integer> SEARCH_PARALLEL_NUMBER =
            ConfigOptions.key("lumina.search.parallel_number")
                    .intType()
                    .defaultValue(5)
                    .withDescription("The parallel number for search.");

    private final int dimension;
    private final boolean dimensionExplicitlyConfigured;
    private final LuminaVectorMetric metric;
    private final String indexType;
    private final Map<String, String> luminaOptions;

    public LuminaVectorIndexOptions(Options options) {
        this.dimension = validatePositive(options.get(DIMENSION), DIMENSION.key());
        this.dimensionExplicitlyConfigured = options.contains(DIMENSION);
        this.metric = parseMetric(options.get(DISTANCE_METRIC));
        this.indexType = options.get(INDEX_TYPE);
        validateEncodingMetricCombination(options.get(ENCODING_TYPE), this.metric);
        this.luminaOptions = buildLuminaOptions(options);
        // Persist the canonical native metric name so LuminaIndexMeta.metric() can read it
        // back, regardless of whether the user configured "L2" (enum) or "l2" (native).
        this.luminaOptions.put(toLuminaKey(DISTANCE_METRIC), metric.getLuminaName());
    }

    /**
     * Resolves per-field Lumina options for {@code fieldName} into an effective {@link Options}.
     *
     * <p>Following the convention shared with {@code paimon-vector} (PR #8239), a field-level
     * option is written {@code fields.<fieldName>.<option>} — <b>without</b> the {@code lumina.}
     * index-type prefix — and overrides the column-agnostic {@code lumina.<option>} for that field
     * only. For example {@code fields.embed.distance.metric} overrides {@code
     * lumina.distance.metric} for column {@code embed}. The {@code <option>} suffix is exactly the
     * key used after {@code lumina.} at the table level.
     *
     * <p>Only recognized Lumina options (the keys in {@code FIELD_OVERRIDABLE_KEYS}) are accepted;
     * any other {@code fields.<fieldName>.*} key (e.g. a merge/aggregation option) is left
     * untouched, mirroring how {@code paimon-vector} ignores keys it does not recognize.
     *
     * <p>Each recognized field option is flattened back to its plain {@code lumina.*} form, so the
     * rest of this class still sees only {@code lumina.*} keys and the metadata produced from these
     * options keeps the exact same native-key shape as before (no {@code fields.*} keys ever reach
     * the meta) — only the resolved value changes. This is what lets the reader consume the meta
     * unchanged.
     *
     * <p>The same resolution runs on both the write and read paths, since both build the indexer
     * through {@link LuminaVectorGlobalIndexerFactory#create}.
     */
    public static Options resolveFieldOptions(String fieldName, Options options) {
        String fieldPrefix = CoreOptions.FIELDS_PREFIX + "." + fieldName + ".";
        Map<String, String> tableOptions = options.toMap();
        Map<String, String> result = new LinkedHashMap<>();
        // Base: table-level options, including the column-agnostic lumina.* options. Drop all
        // fields.* keys; this field's recognized options are re-added below as lumina.* keys.
        for (Map.Entry<String, String> entry : tableOptions.entrySet()) {
            if (!entry.getKey().startsWith(CoreOptions.FIELDS_PREFIX + ".")) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        // Overlay this field's options. fields.<field>.<option> (no lumina. prefix) overrides the
        // column-agnostic lumina.<option>. Only recognized Lumina options are taken.
        for (Map.Entry<String, String> entry : tableOptions.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(fieldPrefix)) {
                String option = key.substring(fieldPrefix.length());
                if (FIELD_OVERRIDABLE_KEYS.contains(option)) {
                    result.put(LUMINA_PREFIX + option, entry.getValue());
                }
            }
        }
        return Options.fromMap(result);
    }

    /**
     * Returns all {@code lumina.*} options with the prefix stripped, producing native Lumina keys.
     * For example, {@code lumina.diskann.build.ef_construction} becomes {@code
     * diskann.build.ef_construction}.
     */
    public Map<String, String> toLuminaOptions() {
        return toLuminaOptions(dimension);
    }

    public Map<String, String> toLuminaOptions(int dimension) {
        Map<String, String> result = new LinkedHashMap<>(luminaOptions);
        result.put(
                toLuminaKey(DIMENSION),
                String.valueOf(validatePositive(dimension, DIMENSION.key())));
        capPqM(result, dimension);
        return result;
    }

    public int dimension() {
        return dimension;
    }

    public boolean isDimensionExplicitlyConfigured() {
        return dimensionExplicitlyConfigured;
    }

    public LuminaVectorMetric metric() {
        return metric;
    }

    public String indexType() {
        return indexType;
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
                    DISKANN_SEARCH_BEAM_WIDTH,
                    ENCODING_PQ_M,
                    SEARCH_PARALLEL_NUMBER);

    /**
     * Native Lumina keys (the {@code lumina.} prefix stripped) that may be overridden per field via
     * {@code fields.<fieldName>.<key>}. Any {@code fields.<fieldName>.*} key outside this set is
     * ignored, so unrelated per-field options do not leak into the Lumina index metadata.
     */
    private static final Set<String> FIELD_OVERRIDABLE_KEYS = buildFieldOverridableKeys();

    private static Set<String> buildFieldOverridableKeys() {
        Set<String> keys = new HashSet<>();
        for (ConfigOption<?> opt : ALL_OPTIONS) {
            keys.add(toLuminaKey(opt));
        }
        keys.add(toLuminaKey(DISKANN_SEARCH_LIST_SIZE));
        return keys;
    }

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
     * Ensures {@code encoding.pq.m} does not exceed the vector dimension. Lumina's QuantizerTrainer
     * requires numChunks (pq.m) to be &gt; 0 and &le; dimension.
     */
    private static void capPqM(Map<String, String> opts, int dimension) {
        String encoding = opts.get(toLuminaKey(ENCODING_TYPE));
        if (!"pq".equalsIgnoreCase(encoding)) {
            return;
        }
        String pqMKey = toLuminaKey(ENCODING_PQ_M);
        String pqMStr = opts.get(pqMKey);
        if (pqMStr != null) {
            int pqM = Integer.parseInt(pqMStr);
            if (pqM > dimension) {
                opts.put(pqMKey, String.valueOf(dimension));
            }
        }
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
