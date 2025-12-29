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

package org.apache.paimon.faiss.index;

import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;
import org.apache.paimon.options.Options;

/** Options for FAISS vector index. */
public class FaissVectorIndexOptions {

    public static final ConfigOption<Integer> VECTOR_DIM =
            ConfigOptions.key("vector.dim")
                    .intType()
                    .defaultValue(128)
                    .withDescription("The dimension of the vector");

    public static final ConfigOption<FaissVectorMetric> VECTOR_METRIC =
            ConfigOptions.key("vector.metric")
                    .enumType(FaissVectorMetric.class)
                    .defaultValue(FaissVectorMetric.L2)
                    .withDescription(
                            "The similarity metric for vector search (L2, INNER_PRODUCT), and L2 is the default");

    public static final ConfigOption<FaissIndexType> VECTOR_INDEX_TYPE =
            ConfigOptions.key("vector.index-type")
                    .enumType(FaissIndexType.class)
                    .defaultValue(FaissIndexType.HNSW)
                    .withDescription(
                            "The type of FAISS index to use (FLAT, HNSW, IVF, IVF_PQ), and HNSW is the default");

    public static final ConfigOption<Integer> VECTOR_M =
            ConfigOptions.key("vector.m")
                    .intType()
                    .defaultValue(32)
                    .withDescription(
                            "The maximum number of connections for each element in HNSW index");

    public static final ConfigOption<Integer> VECTOR_EF_CONSTRUCTION =
            ConfigOptions.key("vector.ef-construction")
                    .intType()
                    .defaultValue(40)
                    .withDescription(
                            "The size of the dynamic candidate list during HNSW index construction");

    public static final ConfigOption<Integer> VECTOR_EF_SEARCH =
            ConfigOptions.key("vector.ef-search")
                    .intType()
                    .defaultValue(16)
                    .withDescription("The size of the dynamic candidate list during HNSW search");

    public static final ConfigOption<Integer> VECTOR_NLIST =
            ConfigOptions.key("vector.nlist")
                    .intType()
                    .defaultValue(100)
                    .withDescription("The number of inverted lists (clusters) for IVF index");

    public static final ConfigOption<Integer> VECTOR_NPROBE =
            ConfigOptions.key("vector.nprobe")
                    .intType()
                    .defaultValue(10)
                    .withDescription("The number of clusters to visit during IVF search");

    public static final ConfigOption<Integer> VECTOR_PQ_M =
            ConfigOptions.key("vector.pq-m")
                    .intType()
                    .defaultValue(8)
                    .withDescription("The number of sub-quantizers for IVF-PQ index");

    public static final ConfigOption<Integer> VECTOR_PQ_NBITS =
            ConfigOptions.key("vector.pq-nbits")
                    .intType()
                    .defaultValue(8)
                    .withDescription("The number of bits per sub-quantizer for IVF-PQ index");

    public static final ConfigOption<Integer> VECTOR_SIZE_PER_INDEX =
            ConfigOptions.key("vector.size-per-index")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("The size of vectors stored in each vector index file");

    public static final ConfigOption<Integer> VECTOR_TRAINING_SIZE =
            ConfigOptions.key("vector.training-size")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("The number of vectors to use for training IVF-based indices");

    private final int dimension;
    private final FaissVectorMetric metric;
    private final FaissIndexType indexType;
    private final int m;
    private final int efConstruction;
    private final int efSearch;
    private final int nlist;
    private final int nprobe;
    private final int pqM;
    private final int pqNbits;
    private final int sizePerIndex;
    private final int trainingSize;

    public FaissVectorIndexOptions(Options options) {
        this.dimension = options.get(VECTOR_DIM);
        this.metric = options.get(VECTOR_METRIC);
        this.indexType = options.get(VECTOR_INDEX_TYPE);
        this.m = options.get(VECTOR_M);
        this.efConstruction = options.get(VECTOR_EF_CONSTRUCTION);
        this.efSearch = options.get(VECTOR_EF_SEARCH);
        this.nlist = options.get(VECTOR_NLIST);
        this.nprobe = options.get(VECTOR_NPROBE);
        this.pqM = options.get(VECTOR_PQ_M);
        this.pqNbits = options.get(VECTOR_PQ_NBITS);
        this.sizePerIndex =
                options.get(VECTOR_SIZE_PER_INDEX) > 0
                        ? options.get(VECTOR_SIZE_PER_INDEX)
                        : VECTOR_SIZE_PER_INDEX.defaultValue();
        this.trainingSize = options.get(VECTOR_TRAINING_SIZE);
    }

    public int dimension() {
        return dimension;
    }

    public FaissVectorMetric metric() {
        return metric;
    }

    public FaissIndexType indexType() {
        return indexType;
    }

    public int m() {
        return m;
    }

    public int efConstruction() {
        return efConstruction;
    }

    public int efSearch() {
        return efSearch;
    }

    public int nlist() {
        return nlist;
    }

    public int nprobe() {
        return nprobe;
    }

    public int pqM() {
        return pqM;
    }

    public int pqNbits() {
        return pqNbits;
    }

    public int sizePerIndex() {
        return sizePerIndex;
    }

    public int trainingSize() {
        return trainingSize;
    }
}
