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

package org.apache.paimon.ivfpq.index;

import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;
import org.apache.paimon.options.Options;

/** Options for the IVF-PQ vector index. */
public class IvfpqVectorIndexOptions {

    public static final ConfigOption<Integer> DIMENSION =
            ConfigOptions.key("ivfpq.index.dimension")
                    .intType()
                    .defaultValue(128)
                    .withDescription("The dimension of the vector.");

    public static final ConfigOption<String> DISTANCE_METRIC =
            ConfigOptions.key("ivfpq.distance.metric")
                    .stringType()
                    .defaultValue("inner_product")
                    .withDescription(
                            "Distance metric for vector search (l2, cosine, inner_product).");

    public static final ConfigOption<Integer> NLIST =
            ConfigOptions.key("ivfpq.nlist")
                    .intType()
                    .defaultValue(256)
                    .withDescription("Number of IVF partitions (Voronoi cells).");

    public static final ConfigOption<Integer> M =
            ConfigOptions.key("ivfpq.m")
                    .intType()
                    .defaultValue(16)
                    .withDescription(
                            "Number of PQ sub-quantizers. Must divide the vector dimension.");

    public static final ConfigOption<Boolean> USE_OPQ =
            ConfigOptions.key("ivfpq.use_opq")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to use OPQ (Optimized Product Quantization) rotation.");

    public static final ConfigOption<Integer> NPROBE =
            ConfigOptions.key("ivfpq.nprobe")
                    .intType()
                    .defaultValue(16)
                    .withDescription("Number of IVF partitions to probe during search.");

    public static final ConfigOption<Double> TRAIN_SAMPLE_RATIO =
            ConfigOptions.key("ivfpq.train.sample_ratio")
                    .doubleType()
                    .defaultValue(1.0)
                    .withDescription(
                            "Ratio of vectors sampled for training (0.0-1.0]. "
                                    + "1.0 means use all vectors for training.");

    public static final ConfigOption<Integer> ADD_BATCH_SIZE =
            ConfigOptions.key("ivfpq.add.batch_size")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("Batch size for adding vectors after training.");

    private final int dimension;
    private final IvfpqVectorMetric metric;
    private final int nlist;
    private final int m;
    private final boolean useOpq;
    private final int nprobe;
    private final double trainSampleRatio;
    private final int addBatchSize;

    public IvfpqVectorIndexOptions(Options options) {
        this.dimension = validatePositive(options.get(DIMENSION), DIMENSION.key());
        this.metric = parseMetric(options.get(DISTANCE_METRIC));
        this.nlist = validatePositive(options.get(NLIST), NLIST.key());
        this.m = validatePositive(options.get(M), M.key());
        this.useOpq = options.get(USE_OPQ);
        this.nprobe = validatePositive(options.get(NPROBE), NPROBE.key());
        this.trainSampleRatio = options.get(TRAIN_SAMPLE_RATIO);
        this.addBatchSize = validatePositive(options.get(ADD_BATCH_SIZE), ADD_BATCH_SIZE.key());

        if (dimension % m != 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "ivfpq.m (%d) must divide ivfpq.index.dimension (%d)", m, dimension));
        }
        if (trainSampleRatio <= 0 || trainSampleRatio > 1.0) {
            throw new IllegalArgumentException(
                    String.format(
                            "ivfpq.train.sample_ratio must be in (0, 1.0], but got %f",
                            trainSampleRatio));
        }
    }

    public int dimension() {
        return dimension;
    }

    public IvfpqVectorMetric metric() {
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

    public int nprobe() {
        return nprobe;
    }

    public double trainSampleRatio() {
        return trainSampleRatio;
    }

    public int addBatchSize() {
        return addBatchSize;
    }

    private static IvfpqVectorMetric parseMetric(String value) {
        try {
            return IvfpqVectorMetric.fromConfigName(value);
        } catch (IllegalArgumentException e) {
            return IvfpqVectorMetric.fromString(value);
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
