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

package org.apache.paimon.vector;

import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;
import org.apache.paimon.options.Options;

/** Options for vector index. */
public class VectorIndexOptions {

    public static final ConfigOption<Integer> VECTOR_DIM =
            ConfigOptions.key("vector.dim")
                    .intType()
                    .defaultValue(128)
                    .withDescription("The dimension of the vector");

    public static final ConfigOption<String> VECTOR_METRIC =
            ConfigOptions.key("vector.metric")
                    .stringType()
                    .defaultValue("EUCLIDEAN")
                    .withDescription(
                            "The similarity metric for vector search (COSINE, DOT_PRODUCT, EUCLIDEAN)");

    public static final ConfigOption<Integer> VECTOR_M =
            ConfigOptions.key("vector.M")
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

    private final int dimension;
    private final String metric;
    private final int m;
    private final int efConstruction;

    public VectorIndexOptions(Options options) {
        this.dimension = options.get(VECTOR_DIM);
        this.metric = options.get(VECTOR_METRIC);
        this.m = options.get(VECTOR_M);
        this.efConstruction = options.get(VECTOR_EF_CONSTRUCTION);
    }

    public int dimension() {
        return dimension;
    }

    public String metric() {
        return metric;
    }

    public int m() {
        return m;
    }

    public int efConstruction() {
        return efConstruction;
    }
}
