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

import org.apache.lucene.index.VectorSimilarityFunction;

/** Supported vector similarity metrics for ES vector index. */
public enum ESVectorMetric {
    L2("l2", VectorSimilarityFunction.EUCLIDEAN),

    COSINE("cosine", VectorSimilarityFunction.DOT_PRODUCT),

    INNER_PRODUCT("inner_product", VectorSimilarityFunction.DOT_PRODUCT);

    private final String name;
    private final VectorSimilarityFunction luceneFunction;

    ESVectorMetric(String name, VectorSimilarityFunction luceneFunction) {
        this.name = name;
        this.luceneFunction = luceneFunction;
    }

    public String getName() {
        return name;
    }

    public VectorSimilarityFunction toLuceneFunction() {
        return luceneFunction;
    }

    public static ESVectorMetric fromName(String name) {
        for (ESVectorMetric metric : values()) {
            if (metric.name.equals(name)) {
                return metric;
            }
        }
        throw new IllegalArgumentException("Unknown ES vector metric: " + name);
    }
}
