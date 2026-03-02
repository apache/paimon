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

package org.apache.paimon.diskann.index;

import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;
import org.apache.paimon.options.Options;

/** Options for DiskANN vector index. */
public class DiskAnnVectorIndexOptions {

    public static final ConfigOption<Integer> VECTOR_DIM =
            ConfigOptions.key("vector.dim")
                    .intType()
                    .defaultValue(128)
                    .withDescription("The dimension of the vector");

    public static final ConfigOption<DiskAnnVectorMetric> VECTOR_METRIC =
            ConfigOptions.key("vector.metric")
                    .enumType(DiskAnnVectorMetric.class)
                    .defaultValue(DiskAnnVectorMetric.L2)
                    .withDescription(
                            "The similarity metric for vector search (L2, INNER_PRODUCT, COSINE), and L2 is the default");

    public static final ConfigOption<Integer> VECTOR_MAX_DEGREE =
            ConfigOptions.key("vector.diskann.max-degree")
                    .intType()
                    .defaultValue(64)
                    .withDescription("The maximum degree (R) for DiskANN graph construction");

    public static final ConfigOption<Integer> VECTOR_BUILD_LIST_SIZE =
            ConfigOptions.key("vector.diskann.build-list-size")
                    .intType()
                    .defaultValue(100)
                    .withDescription("The build list size (L) for DiskANN index construction");

    public static final ConfigOption<Integer> VECTOR_SEARCH_LIST_SIZE =
            ConfigOptions.key("vector.diskann.search-list-size")
                    .intType()
                    .defaultValue(100)
                    .withDescription("The search list size (L) for DiskANN query");

    public static final ConfigOption<Integer> VECTOR_SIZE_PER_INDEX =
            ConfigOptions.key("vector.size-per-index")
                    .intType()
                    .defaultValue(200_0000)
                    .withDescription("The size of vectors stored in each vector index file");

    public static final ConfigOption<Integer> VECTOR_SEARCH_FACTOR =
            ConfigOptions.key("vector.search-factor")
                    .intType()
                    .defaultValue(10)
                    .withDescription(
                            "The multiplier for the search limit when filtering is applied. "
                                    + "This is used to fetch more results to ensure enough records after filtering.");

    public static final ConfigOption<Integer> VECTOR_PQ_SUBSPACES =
            ConfigOptions.key("vector.diskann.pq-subspaces")
                    .intType()
                    .defaultValue(-1)
                    .withDescription(
                            "Number of subspaces (M) for Product Quantization. "
                                    + "Dimension must be divisible by M. "
                                    + "Default (-1) auto-computes as max(1, dim/4).");

    public static final ConfigOption<Integer> VECTOR_PQ_KMEANS_ITERATIONS =
            ConfigOptions.key("vector.diskann.pq-kmeans-iterations")
                    .intType()
                    .defaultValue(20)
                    .withDescription("Number of K-Means iterations for PQ codebook training.");

    public static final ConfigOption<Integer> VECTOR_PQ_SAMPLE_SIZE =
            ConfigOptions.key("vector.diskann.pq-sample-size")
                    .intType()
                    .defaultValue(100_000)
                    .withDescription("Maximum number of vectors sampled for PQ codebook training.");

    private final int dimension;
    private final DiskAnnVectorMetric metric;
    private final int maxDegree;
    private final int buildListSize;
    private final int searchListSize;
    private final int sizePerIndex;
    private final int searchFactor;
    private final int pqSubspaces;
    private final int pqKmeansIterations;
    private final int pqSampleSize;

    public DiskAnnVectorIndexOptions(Options options) {
        this.dimension = options.get(VECTOR_DIM);
        this.metric = options.get(VECTOR_METRIC);
        this.maxDegree = options.get(VECTOR_MAX_DEGREE);
        this.buildListSize = options.get(VECTOR_BUILD_LIST_SIZE);
        this.searchListSize = options.get(VECTOR_SEARCH_LIST_SIZE);
        this.sizePerIndex =
                options.get(VECTOR_SIZE_PER_INDEX) > 0
                        ? options.get(VECTOR_SIZE_PER_INDEX)
                        : VECTOR_SIZE_PER_INDEX.defaultValue();
        this.searchFactor = options.get(VECTOR_SEARCH_FACTOR);

        int rawPqSub = options.get(VECTOR_PQ_SUBSPACES);
        this.pqSubspaces = rawPqSub > 0 ? rawPqSub : defaultNumSubspaces(dimension);
        this.pqKmeansIterations = options.get(VECTOR_PQ_KMEANS_ITERATIONS);
        this.pqSampleSize = options.get(VECTOR_PQ_SAMPLE_SIZE);
    }

    public int dimension() {
        return dimension;
    }

    public DiskAnnVectorMetric metric() {
        return metric;
    }

    public int maxDegree() {
        return maxDegree;
    }

    public int buildListSize() {
        return buildListSize;
    }

    public int searchListSize() {
        return searchListSize;
    }

    public int sizePerIndex() {
        return sizePerIndex;
    }

    public int searchFactor() {
        return searchFactor;
    }

    /** Number of PQ subspaces (M). */
    public int pqSubspaces() {
        return pqSubspaces;
    }

    /** Number of K-Means iterations for PQ training. */
    public int pqKmeansIterations() {
        return pqKmeansIterations;
    }

    /** Maximum number of training samples for PQ. */
    public int pqSampleSize() {
        return pqSampleSize;
    }

    /**
     * Compute a reasonable default number of PQ subspaces for the given dimension. The result is
     * the largest divisor of {@code dim} that is {@code <= dim / 4} and at least 1.
     */
    static int defaultNumSubspaces(int dim) {
        int target = Math.max(1, dim / 4);
        while (target > 1 && dim % target != 0) {
            target--;
        }
        return target;
    }
}
