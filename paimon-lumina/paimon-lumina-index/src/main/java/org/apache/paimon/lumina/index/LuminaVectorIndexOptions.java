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

/** Options for Lumina vector index. */
public class LuminaVectorIndexOptions {

    public static final ConfigOption<Integer> VECTOR_DIM =
            ConfigOptions.key("vector.dim")
                    .intType()
                    .defaultValue(128)
                    .withDescription("The dimension of the vector");

    public static final ConfigOption<LuminaVectorMetric> VECTOR_METRIC =
            ConfigOptions.key("vector.metric")
                    .enumType(LuminaVectorMetric.class)
                    .defaultValue(LuminaVectorMetric.L2)
                    .withDescription(
                            "The distance metric for vector search (L2, COSINE, INNER_PRODUCT)");

    public static final ConfigOption<LuminaIndexType> VECTOR_INDEX_TYPE =
            ConfigOptions.key("vector.index-type")
                    .enumType(LuminaIndexType.class)
                    .defaultValue(LuminaIndexType.DISKANN)
                    .withDescription("The type of Lumina index (DISKANN)");

    public static final ConfigOption<String> VECTOR_ENCODING_TYPE =
            ConfigOptions.key("vector.encoding-type")
                    .stringType()
                    .defaultValue("rawf32")
                    .withDescription("The encoding type for vectors (rawf32, sq8, pq)");

    public static final ConfigOption<Integer> VECTOR_SIZE_PER_INDEX =
            ConfigOptions.key("vector.size-per-index")
                    .intType()
                    .defaultValue(200_0000)
                    .withDescription("The number of vectors stored in each index file");

    public static final ConfigOption<Integer> VECTOR_TRAINING_SIZE =
            ConfigOptions.key("vector.training-size")
                    .intType()
                    .defaultValue(50_0000)
                    .withDescription(
                            "The number of vectors to use for pretraining DiskANN indices");

    public static final ConfigOption<Integer> VECTOR_SEARCH_FACTOR =
            ConfigOptions.key("vector.search-factor")
                    .intType()
                    .defaultValue(10)
                    .withDescription(
                            "The multiplier for the search limit when filtering is applied");

    public static final ConfigOption<Boolean> VECTOR_NORMALIZE =
            ConfigOptions.key("vector.normalize")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to L2 normalize vectors before indexing and searching");

    public static final ConfigOption<Integer> VECTOR_SEARCH_LIST_SIZE =
            ConfigOptions.key("vector.search-list-size")
                    .intType()
                    .defaultValue(100)
                    .withDescription("The search list size for DiskANN search (search.list_size)");

    public static final ConfigOption<Double> PRETRAIN_SAMPLE_RATIO =
            ConfigOptions.key("vector.pretrain-sample-ratio")
                    .doubleType()
                    .defaultValue(1.0)
                    .withDescription(
                            "The sample ratio for pretraining (Lumina's pretrain.sample_ratio)");

    private final int dimension;
    private final LuminaVectorMetric metric;
    private final LuminaIndexType indexType;
    private final String encodingType;
    private final int sizePerIndex;
    private final int trainingSize;
    private final int searchFactor;
    private final int searchListSize;
    private final boolean normalize;
    private final double pretrainSampleRatio;

    public LuminaVectorIndexOptions(Options options) {
        this.dimension = options.get(VECTOR_DIM);
        this.metric = options.get(VECTOR_METRIC);
        this.indexType = options.get(VECTOR_INDEX_TYPE);
        this.encodingType = options.get(VECTOR_ENCODING_TYPE);
        this.sizePerIndex =
                options.get(VECTOR_SIZE_PER_INDEX) > 0
                        ? options.get(VECTOR_SIZE_PER_INDEX)
                        : VECTOR_SIZE_PER_INDEX.defaultValue();
        this.trainingSize = options.get(VECTOR_TRAINING_SIZE);
        this.searchFactor = options.get(VECTOR_SEARCH_FACTOR);
        this.searchListSize = options.get(VECTOR_SEARCH_LIST_SIZE);
        this.normalize = options.get(VECTOR_NORMALIZE);
        this.pretrainSampleRatio = options.get(PRETRAIN_SAMPLE_RATIO);
    }

    public int dimension() {
        return dimension;
    }

    public LuminaVectorMetric metric() {
        return metric;
    }

    public LuminaIndexType indexType() {
        return indexType;
    }

    public String encodingType() {
        return encodingType;
    }

    public int sizePerIndex() {
        return sizePerIndex;
    }

    public int trainingSize() {
        return trainingSize;
    }

    public int searchFactor() {
        return searchFactor;
    }

    public int searchListSize() {
        return searchListSize;
    }

    public boolean normalize() {
        return normalize;
    }

    public double pretrainSampleRatio() {
        return pretrainSampleRatio;
    }
}
