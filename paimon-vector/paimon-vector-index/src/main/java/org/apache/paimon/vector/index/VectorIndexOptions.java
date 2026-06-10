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

import org.apache.paimon.index.ivfpq.HnswConfig;
import org.apache.paimon.index.ivfpq.IndexType;
import org.apache.paimon.index.ivfpq.VectorIndexConfig;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;
import org.apache.paimon.options.Options;

import java.util.Objects;

/** Options for the Paimon vector index backed by paimon-vector-index. */
public class VectorIndexOptions {

    public static final String IVF_FLAT_IDENTIFIER = "ivf-flat";
    public static final String IVF_PQ_IDENTIFIER = "ivf-pq";
    public static final String IVF_HNSW_FLAT_IDENTIFIER = "ivf-hnsw-flat";
    public static final String IVF_HNSW_SQ_IDENTIFIER = "ivf-hnsw-sq";

    public static final ConfigOption<Integer> DIMENSION =
            ConfigOptions.key("vector.index.dimension")
                    .intType()
                    .defaultValue(128)
                    .withDescription("The dimension of the vector.");

    public static final ConfigOption<String> DISTANCE_METRIC =
            ConfigOptions.key("vector.distance.metric")
                    .stringType()
                    .defaultValue("inner_product")
                    .withDescription(
                            "Distance metric for vector search (l2, cosine, inner_product).");

    public static final ConfigOption<Integer> NLIST =
            ConfigOptions.key("vector.nlist")
                    .intType()
                    .defaultValue(256)
                    .withDescription("Number of IVF partitions (Voronoi cells).");

    public static final ConfigOption<Integer> M =
            ConfigOptions.key("vector.pq.m")
                    .intType()
                    .defaultValue(16)
                    .withDescription(
                            "Number of PQ sub-quantizers. Must divide the vector dimension.");

    public static final ConfigOption<Boolean> USE_OPQ =
            ConfigOptions.key("vector.pq.use-opq")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to use OPQ (Optimized Product Quantization) rotation.");

    public static final ConfigOption<Integer> HNSW_M =
            ConfigOptions.key("vector.hnsw.m")
                    .intType()
                    .defaultValue(HnswConfig.DEFAULT.m())
                    .withDescription("Maximum number of HNSW neighbors per node.");

    public static final ConfigOption<Integer> HNSW_EF_CONSTRUCTION =
            ConfigOptions.key("vector.hnsw.ef-construction")
                    .intType()
                    .defaultValue(HnswConfig.DEFAULT.efConstruction())
                    .withDescription("HNSW efConstruction value used during index build.");

    public static final ConfigOption<Integer> HNSW_MAX_LEVEL =
            ConfigOptions.key("vector.hnsw.max-level")
                    .intType()
                    .defaultValue(HnswConfig.DEFAULT.maxLevel())
                    .withDescription("Maximum HNSW graph level.");

    public static final ConfigOption<Integer> NPROBE =
            ConfigOptions.key("vector.nprobe")
                    .intType()
                    .defaultValue(16)
                    .withDescription("Number of IVF partitions to probe during search.");

    public static final ConfigOption<Integer> EF_SEARCH =
            ConfigOptions.key("vector.hnsw.ef-search")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            "HNSW efSearch value used during search. 0 uses the native default.");

    public static final ConfigOption<Double> TRAIN_SAMPLE_RATIO =
            ConfigOptions.key("vector.train.sample-ratio")
                    .doubleType()
                    .defaultValue(1.0)
                    .withDescription(
                            "Ratio of vectors sampled for training (0.0-1.0]. "
                                    + "1.0 means use all vectors for training.");

    public static final ConfigOption<Integer> ADD_BATCH_SIZE =
            ConfigOptions.key("vector.add.batch-size")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("Batch size for adding vectors after training.");

    private final IndexType indexType;
    private final int dimension;
    private final VectorMetric metric;
    private final int nlist;
    private final int m;
    private final boolean useOpq;
    private final HnswConfig hnswConfig;
    private final int nprobe;
    private final int efSearch;
    private final double trainSampleRatio;
    private final int addBatchSize;

    public VectorIndexOptions(Options options, IndexType indexType) {
        this.indexType = Objects.requireNonNull(indexType, "indexType must not be null");
        this.dimension = validatePositive(options.get(DIMENSION), optionKey(DIMENSION));
        this.metric = parseMetric(options.get(DISTANCE_METRIC));
        this.nlist = validatePositive(options.get(NLIST), optionKey(NLIST));
        this.m = validatePositive(options.get(M), optionKey(M));
        this.useOpq = options.get(USE_OPQ);
        this.hnswConfig =
                new HnswConfig(
                        validatePositive(options.get(HNSW_M), optionKey(HNSW_M)),
                        validatePositive(
                                options.get(HNSW_EF_CONSTRUCTION), optionKey(HNSW_EF_CONSTRUCTION)),
                        validatePositive(options.get(HNSW_MAX_LEVEL), optionKey(HNSW_MAX_LEVEL)));
        this.nprobe = validatePositive(options.get(NPROBE), optionKey(NPROBE));
        this.efSearch = validateNonNegative(options.get(EF_SEARCH), optionKey(EF_SEARCH));
        this.trainSampleRatio = options.get(TRAIN_SAMPLE_RATIO);
        this.addBatchSize =
                validatePositive(options.get(ADD_BATCH_SIZE), optionKey(ADD_BATCH_SIZE));

        if (indexType == IndexType.IVF_PQ && dimension % m != 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "%s (%d) must divide %s (%d)",
                            optionKey(M), m, optionKey(DIMENSION), dimension));
        }
        if (trainSampleRatio <= 0 || trainSampleRatio > 1.0) {
            throw new IllegalArgumentException(
                    String.format(
                            "%s must be in (0, 1.0], but got %f",
                            optionKey(TRAIN_SAMPLE_RATIO), trainSampleRatio));
        }
    }

    public IndexType indexType() {
        return indexType;
    }

    public int dimension() {
        return dimension;
    }

    public VectorMetric metric() {
        return metric;
    }

    public int nlist() {
        return nlist;
    }

    public int m() {
        return m;
    }

    public boolean useOpq() {
        return useOpq;
    }

    public HnswConfig hnswConfig() {
        return hnswConfig;
    }

    public int nprobe() {
        return nprobe;
    }

    public int efSearch() {
        return efSearch;
    }

    public double trainSampleRatio() {
        return trainSampleRatio;
    }

    public int addBatchSize() {
        return addBatchSize;
    }

    public VectorIndexConfig toVectorIndexConfig(int effectiveNlist) {
        switch (indexType) {
            case IVF_FLAT:
                return VectorIndexConfig.ivfFlat(
                        dimension, effectiveNlist, metric.toNativeMetric());
            case IVF_PQ:
                return VectorIndexConfig.ivfPq(
                        dimension, effectiveNlist, m, metric.toNativeMetric(), useOpq);
            case IVF_HNSW_FLAT:
                return VectorIndexConfig.ivfHnswFlat(
                        dimension, effectiveNlist, metric.toNativeMetric(), hnswConfig);
            case IVF_HNSW_SQ:
                return VectorIndexConfig.ivfHnswSq(
                        dimension, effectiveNlist, metric.toNativeMetric(), hnswConfig);
            default:
                throw new IllegalArgumentException("Unsupported vector index type: " + indexType);
        }
    }

    public String logName() {
        return toIdentifier(indexType);
    }

    private static VectorMetric parseMetric(String value) {
        try {
            return VectorMetric.fromConfigName(value);
        } catch (IllegalArgumentException e) {
            return VectorMetric.fromString(value);
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

    private static int validateNonNegative(int value, String key) {
        if (value < 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid value for '%s': %d. Must be a non-negative integer.",
                            key, value));
        }
        return value;
    }

    public static IndexType parseIndexType(String value) {
        if (IVF_PQ_IDENTIFIER.equals(value)) {
            return IndexType.IVF_PQ;
        } else if (IVF_FLAT_IDENTIFIER.equals(value)) {
            return IndexType.IVF_FLAT;
        } else if (IVF_HNSW_FLAT_IDENTIFIER.equals(value)) {
            return IndexType.IVF_HNSW_FLAT;
        } else if (IVF_HNSW_SQ_IDENTIFIER.equals(value)) {
            return IndexType.IVF_HNSW_SQ;
        }
        throw new IllegalArgumentException("Unknown vector index type: " + value);
    }

    public static String toIdentifier(IndexType indexType) {
        switch (indexType) {
            case IVF_FLAT:
                return IVF_FLAT_IDENTIFIER;
            case IVF_PQ:
                return IVF_PQ_IDENTIFIER;
            case IVF_HNSW_FLAT:
                return IVF_HNSW_FLAT_IDENTIFIER;
            case IVF_HNSW_SQ:
                return IVF_HNSW_SQ_IDENTIFIER;
            default:
                throw new IllegalArgumentException("Unsupported vector index type: " + indexType);
        }
    }

    private static String optionKey(ConfigOption<?> option) {
        return option.key();
    }
}
