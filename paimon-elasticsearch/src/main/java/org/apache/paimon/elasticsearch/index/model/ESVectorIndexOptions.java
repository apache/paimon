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

package org.apache.paimon.elasticsearch.index.model;

import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;
import org.apache.paimon.options.Options;

import org.elasticsearch.vectorindex.model.VectorIndexConfig;

import java.util.LinkedHashMap;
import java.util.Map;

/** Options for the ES (DiskBBQ) vector index. */
public class ESVectorIndexOptions {

    public static final String ES_PREFIX = "es.";

    public static final ConfigOption<Integer> DIMENSION =
            ConfigOptions.key("es.index.dimension")
                    .intType()
                    .defaultValue(128)
                    .withDescription("The dimension of the vector.");

    public static final ConfigOption<String> DISTANCE_METRIC =
            ConfigOptions.key("es.distance.metric")
                    .stringType()
                    .defaultValue("l2")
                    .withDescription("Distance metric: l2, cosine, inner_product.");

    public static final ConfigOption<Integer> VECTORS_PER_CLUSTER =
            ConfigOptions.key("es.vectors_per_cluster")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Number of vectors per IVF cluster.");

    public static final ConfigOption<Integer> CENTROIDS_PER_PARENT_CLUSTER =
            ConfigOptions.key("es.centroids_per_parent_cluster")
                    .intType()
                    .defaultValue(8)
                    .withDescription(
                            "Number of centroids per parent cluster in hierarchical KMeans.");

    public static final ConfigOption<Boolean> SEARCH_PARALLEL =
            ConfigOptions.key("es.search.parallel")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Enable parallel cluster search within a single shard.");

    public static final ConfigOption<Boolean> SEARCH_BULK =
            ConfigOptions.key("es.search.bulk")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Pre-load all cluster data into memory for zero-IO search.");

    public static final ConfigOption<Integer> NPROBE =
            ConfigOptions.key("es.nprobe")
                    .intType()
                    .defaultValue(10)
                    .withDescription("Number of clusters to probe during IVF search.");

    public static final ConfigOption<Boolean> RESCORE_ENABLED =
            ConfigOptions.key("es.rescore.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Enable two-phase search: collect topK*oversample quantized "
                                    + "candidates from BBQ, then rerank using exact distances "
                                    + "computed against raw vectors. Requires a raw vector "
                                    + "source to be wired into the reader; see paimon-elasticsearch "
                                    + "ESVectorGlobalIndexReader integration.");

    public static final ConfigOption<Integer> RESCORE_OVERSAMPLE =
            ConfigOptions.key("es.rescore.oversample")
                    .intType()
                    .defaultValue(4)
                    .withDescription(
                            "Phase-1 oversample factor when rescore is enabled. "
                                    + "Phase-1 collects topK*oversample candidates; "
                                    + "phase-2 reranks back to topK. Must be >= 1; "
                                    + "1 means rerank-only without candidate enlargement.");

    private final int dimension;
    private final ESVectorMetric metric;
    private final int vectorsPerCluster;
    private final int centroidsPerParentCluster;
    private final int nprobe;
    private final boolean searchParallel;
    private final boolean searchBulk;
    private final boolean rescoreEnabled;
    private final int rescoreOversample;

    public ESVectorIndexOptions(Options options) {
        this.dimension = validatePositive(options.get(DIMENSION), DIMENSION.key());
        this.metric = ESVectorMetric.fromName(options.get(DISTANCE_METRIC));
        this.vectorsPerCluster =
                validatePositive(options.get(VECTORS_PER_CLUSTER), VECTORS_PER_CLUSTER.key());
        this.centroidsPerParentCluster =
                validatePositive(
                        options.get(CENTROIDS_PER_PARENT_CLUSTER),
                        CENTROIDS_PER_PARENT_CLUSTER.key());
        this.nprobe = validatePositive(options.get(NPROBE), NPROBE.key());
        this.searchParallel = options.get(SEARCH_PARALLEL);
        this.searchBulk = options.get(SEARCH_BULK);
        this.rescoreEnabled = options.get(RESCORE_ENABLED);
        this.rescoreOversample =
                validatePositive(options.get(RESCORE_OVERSAMPLE), RESCORE_OVERSAMPLE.key());
    }

    public int dimension() {
        return dimension;
    }

    public ESVectorMetric metric() {
        return metric;
    }

    public int vectorsPerCluster() {
        return vectorsPerCluster;
    }

    public int centroidsPerParentCluster() {
        return centroidsPerParentCluster;
    }

    public boolean searchParallel() {
        return searchParallel;
    }

    public boolean searchBulk() {
        return searchBulk;
    }

    public int nprobe() {
        return nprobe;
    }

    public boolean rescoreEnabled() {
        return rescoreEnabled;
    }

    public int rescoreOversample() {
        return rescoreOversample;
    }

    public VectorIndexConfig toVectorIndexConfig() {
        return new VectorIndexConfig(
                dimension, metric.toLuceneFunction(), vectorsPerCluster, centroidsPerParentCluster);
    }

    public VectorIndexConfig toVectorIndexConfig(String fieldName) {
        return new VectorIndexConfig(
                fieldName,
                dimension,
                metric.toLuceneFunction(),
                vectorsPerCluster,
                centroidsPerParentCluster);
    }

    public Map<String, String> toOptionsMap() {
        Map<String, String> map = new LinkedHashMap<>();
        map.put("index.dimension", String.valueOf(dimension));
        map.put("distance.metric", metric.getName());
        map.put("vectors_per_cluster", String.valueOf(vectorsPerCluster));
        map.put("centroids_per_parent_cluster", String.valueOf(centroidsPerParentCluster));
        map.put("nprobe", String.valueOf(nprobe));
        map.put("search.parallel", String.valueOf(searchParallel));
        map.put("search.bulk", String.valueOf(searchBulk));
        map.put("rescore.enabled", String.valueOf(rescoreEnabled));
        map.put("rescore.oversample", String.valueOf(rescoreOversample));
        return map;
    }

    private static int validatePositive(int value, String key) {
        if (value <= 0) {
            throw new IllegalArgumentException(
                    String.format("Invalid value for '%s': %d. Must be positive.", key, value));
        }
        return value;
    }
}
