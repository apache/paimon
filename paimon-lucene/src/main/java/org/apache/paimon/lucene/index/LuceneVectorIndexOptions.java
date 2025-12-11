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

package org.apache.paimon.lucene.index;

import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;
import org.apache.paimon.options.Options;

/** Options for Lucene knn vector index. */
public class LuceneVectorIndexOptions {

    public static final ConfigOption<Integer> VECTOR_DIM =
            ConfigOptions.key("vector.dim")
                    .intType()
                    .defaultValue(128)
                    .withDescription("The dimension of the vector");

    public static final ConfigOption<LuceneVectorMetric> VECTOR_METRIC =
            ConfigOptions.key("vector.metric")
                    .enumType(LuceneVectorMetric.class)
                    .defaultValue(LuceneVectorMetric.EUCLIDEAN)
                    .withDescription(
                            "The similarity metric for vector search (COSINE, DOT_PRODUCT, EUCLIDEAN, MAX_INNER_PRODUCT), and EUCLIDEAN is the default");

    public static final ConfigOption<Integer> VECTOR_M =
            ConfigOptions.key("vector.m")
                    .intType()
                    .defaultValue(16)
                    .withDescription(
                            "The maximum number of connections for each element during the index construction");

    public static final ConfigOption<Integer> VECTOR_EF_CONSTRUCTION =
            ConfigOptions.key("vector.ef-construction")
                    .intType()
                    .defaultValue(100)
                    .withDescription(
                            "The size of the dynamic candidate list during the index construction");

    public static final ConfigOption<Integer> VECTOR_SIZE_PER_INDEX =
            ConfigOptions.key("vector.size-per-index")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("The size of vectors stored in each vector index file");

    public static final ConfigOption<Integer> VECTOR_WRITE_BUFFER_SIZE =
            ConfigOptions.key("vector.write-buffer-size")
                    .intType()
                    .defaultValue(256)
                    .withDescription("Write buffer size in MB for vector index");

    private final int dimension;
    private final LuceneVectorMetric metric;
    private final int m;
    private final int efConstruction;
    private final int sizePerIndex;
    private final int writeBufferSize;

    public LuceneVectorIndexOptions(Options options) {
        this.dimension = options.get(VECTOR_DIM);
        this.metric = options.get(VECTOR_METRIC);
        this.m = options.get(VECTOR_M);
        this.efConstruction = options.get(VECTOR_EF_CONSTRUCTION);
        this.sizePerIndex =
                options.get(VECTOR_SIZE_PER_INDEX) > 0
                        ? options.get(VECTOR_SIZE_PER_INDEX)
                        : VECTOR_SIZE_PER_INDEX.defaultValue();
        this.writeBufferSize = options.get(VECTOR_WRITE_BUFFER_SIZE);
    }

    public int dimension() {
        return dimension;
    }

    public LuceneVectorMetric metric() {
        return metric;
    }

    public int m() {
        return m;
    }

    public int efConstruction() {
        return efConstruction;
    }

    public int sizePerIndex() {
        return sizePerIndex;
    }

    public int writeBufferSize() {
        return writeBufferSize;
    }
}
