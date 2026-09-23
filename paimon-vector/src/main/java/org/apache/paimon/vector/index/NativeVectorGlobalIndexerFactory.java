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

import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.GlobalIndexerFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.VectorType;

import java.util.LinkedHashMap;
import java.util.Map;

/** Factory for creating vector indexes backed by paimon-vector-index-java. */
public abstract class NativeVectorGlobalIndexerFactory implements GlobalIndexerFactory {

    private static final int DEFAULT_DIMENSION = 128;
    static final String TRAIN_SAMPLE_RATIO_OPTION = "train.sample-ratio";
    static final double DEFAULT_TRAIN_SAMPLE_RATIO = 1.0;

    @Override
    public GlobalIndexer create(DataField field, Options options) {
        String identifier = identifier();
        return new NativeVectorGlobalIndexer(
                field.type(),
                nativeOptions(field.type(), options, identifier, field.name()),
                identifier,
                trainSampleRatio(options, identifier, field.name()));
    }

    static Map<String, String> nativeOptions(
            DataType fieldType, Options tableOptions, String identifier, String fieldName) {
        Map<String, String> nativeOptions = new LinkedHashMap<>();
        collectNativeOptions(nativeOptions, tableOptions.toMap(), identifier, fieldName, false);
        collectNativeOptions(
                nativeOptions, tableOptions.dynamicOptions(), identifier, fieldName, true);

        nativeOptions.put("index.type", identifier.replace('-', '_'));
        nativeOptions.put(
                "dimension", String.valueOf(dimension(fieldType, nativeOptions, identifier)));
        nativeOptions.putIfAbsent("metric", NativeVectorGlobalIndexer.DEFAULT_METRIC);
        return nativeOptions;
    }

    static double trainSampleRatio(Options tableOptions, String identifier, String fieldName) {
        Map<String, String> source = tableOptions.dynamicOptions();
        String key =
                resolveFieldOverriddenKey(source, identifier, fieldName, TRAIN_SAMPLE_RATIO_OPTION);
        if (key == null) {
            source = tableOptions.toMap();
            key =
                    resolveFieldOverriddenKey(
                            source, identifier, fieldName, TRAIN_SAMPLE_RATIO_OPTION);
        }
        if (key == null) {
            return DEFAULT_TRAIN_SAMPLE_RATIO;
        }
        String value = source.get(key);

        try {
            double parsed = Double.parseDouble(value.trim());
            if (!Double.isNaN(parsed) && !Double.isInfinite(parsed) && parsed > 0 && parsed <= 1) {
                return parsed;
            }
            throw invalidTrainSampleRatio(key, value);
        } catch (NumberFormatException e) {
            throw invalidTrainSampleRatio(key, value);
        }
    }

    private static IllegalArgumentException invalidTrainSampleRatio(String key, String value) {
        return new IllegalArgumentException(
                "Invalid value for '"
                        + key
                        + "': "
                        + value
                        + ". Must be greater than 0 and less than or equal to 1.");
    }

    /**
     * Resolves a single option key that supports index-level ({@code <index-type>.<option>}) and
     * field-level ({@code fields.<field-name>.<option>}) forms, where the field-level key overrides
     * the index-level key. Returns the winning fully-qualified key, or {@code null} if neither is
     * set.
     *
     * <p>This is the same index/field precedence applied in bulk by {@link #nativeOptions}; the
     * difference is that this helper resolves a single option so it can stay local (for example
     * {@code train.sample-ratio}) instead of being forwarded to the native writer.
     */
    private static String resolveFieldOverriddenKey(
            Map<String, String> tableOptionsMap,
            String identifier,
            String fieldName,
            String option) {
        String fieldKey = "fields." + fieldName + "." + option;
        if (tableOptionsMap.containsKey(fieldKey)) {
            return fieldKey;
        }
        String indexKey = identifier + "." + option;
        if (tableOptionsMap.containsKey(indexKey)) {
            return indexKey;
        }
        return null;
    }

    private static void collectNativeOptions(
            Map<String, String> nativeOptions,
            Map<String, String> options,
            String identifier,
            String fieldName,
            boolean validate) {
        String optionPrefix = identifier + ".";
        String fieldPrefix = "fields." + fieldName + ".";

        // Native names have the lowest precedence within each option source.
        for (Map.Entry<String, String> entry : options.entrySet()) {
            String nativeKey = nativeOptionKey(entry.getKey());
            if (entry.getKey().equals(nativeKey) && is050BuildOption(nativeKey)) {
                putNativeOption(
                        nativeOptions,
                        entry.getKey(),
                        entry.getKey(),
                        entry.getValue(),
                        identifier,
                        validate);
            }
        }
        for (Map.Entry<String, String> entry : options.entrySet()) {
            String optionKey = entry.getKey();
            if (optionKey.startsWith(optionPrefix)) {
                putNativeOption(
                        nativeOptions,
                        optionKey,
                        optionKey.substring(optionPrefix.length()),
                        entry.getValue(),
                        identifier,
                        validate);
            }
        }
        for (Map.Entry<String, String> entry : options.entrySet()) {
            String optionKey = entry.getKey();
            if (optionKey.startsWith(fieldPrefix)) {
                putNativeOption(
                        nativeOptions,
                        optionKey,
                        optionKey.substring(fieldPrefix.length()),
                        entry.getValue(),
                        identifier,
                        validate);
            }
        }
    }

    private static String nativeOptionKey(String optionKey) {
        switch (optionKey) {
            case "index.dimension":
            case "dimension":
                return "dimension";
            case "distance.metric":
            case "metric":
                return "metric";
            case "nlist":
            case "expected-vector-count":
            case "ivf.coarse-assignment":
            case "ivf.pq-encoding":
            case "ivf.train.max-points-per-centroid":
            case "pq.train.max-points-per-centroid":
            case "pq.m":
            case "pq.code-ratio":
            case "pq.bits":
            case "rq.bits":
            case "target-recall":
            case "max-bytes-per-vector":
            case "deployment-profile":
                return optionKey;
            case "pq.use-opq":
            case "use-opq":
                return "use-opq";
            case "build-preset":
            case "diskann.build-preset":
                return "diskann.build-preset";
            case "max-degree":
            case "diskann.max-degree":
                return "diskann.max-degree";
            case "build-search-list-size":
            case "diskann.build-search-list-size":
                return "diskann.build-search-list-size";
            case "alpha":
            case "diskann.alpha":
                return "diskann.alpha";
            case "seed":
            case "diskann.seed":
                return "diskann.seed";
            case "memory-budget-bytes":
            case "diskann.memory-budget-bytes":
                return "diskann.memory-budget-bytes";
            case "storage-layout":
            case "diskann.storage-layout":
                return "diskann.storage-layout";
            case "raw-vector-encoding":
            case "diskann.raw-vector-encoding":
                return "diskann.raw-vector-encoding";
            case "build-distance":
            case "diskann.build-distance":
                return "diskann.build-distance";
            default:
                return null;
        }
    }

    private static void putNativeOption(
            Map<String, String> nativeOptions,
            String optionKey,
            String optionSuffix,
            String value,
            String identifier,
            boolean validate) {
        String nativeKey = nativeOptionKey(optionSuffix);
        if (nativeKey == null) {
            return;
        }
        if (is050BuildOption(nativeKey) && !isAllowed050BuildOption(nativeKey, identifier)) {
            if (validate) {
                throw new IllegalArgumentException(
                        "Option '"
                                + optionKey
                                + "' is not supported for index type '"
                                + identifier
                                + "'.");
            }
            return;
        }
        nativeOptions.put(nativeKey, value);
    }

    private static boolean is050BuildOption(String key) {
        return "ivf.coarse-assignment".equals(key)
                || "ivf.pq-encoding".equals(key)
                || "ivf.train.max-points-per-centroid".equals(key)
                || "pq.train.max-points-per-centroid".equals(key);
    }

    private static boolean isAllowed050BuildOption(String key, String identifier) {
        if ("ivf.pq-encoding".equals(key)) {
            return IvfPqAlgorithmVectorGlobalIndexerFactory.IDENTIFIER.equals(identifier);
        }
        if ("pq.train.max-points-per-centroid".equals(key)) {
            return IvfPqAlgorithmVectorGlobalIndexerFactory.IDENTIFIER.equals(identifier)
                    || DiskAnnVectorGlobalIndexerFactory.IDENTIFIER.equals(identifier);
        }
        return !DiskAnnVectorGlobalIndexerFactory.IDENTIFIER.equals(identifier);
    }

    private static int dimension(
            DataType fieldType, Map<String, String> nativeOptions, String identifier) {
        if (fieldType instanceof VectorType) {
            return ((VectorType) fieldType).getLength();
        }
        String dimension = nativeOptions.get("dimension");
        int value = dimension == null ? DEFAULT_DIMENSION : Integer.parseInt(dimension);
        if (value <= 0) {
            throw new IllegalArgumentException(
                    "Invalid value for '"
                            + identifier
                            + ".dimension': "
                            + value
                            + ". Must be a positive integer.");
        }
        return value;
    }
}
