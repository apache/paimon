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

import org.apache.lucene.index.VectorSimilarityFunction;

/** Enumeration of supported Lucene vector similarity metrics. */
public enum LuceneVectorMetric {
    /** Cosine similarity metric. */
    COSINE(VectorSimilarityFunction.COSINE),

    /** Dot product similarity metric. */
    DOT_PRODUCT(VectorSimilarityFunction.DOT_PRODUCT),

    /** Euclidean distance metric. */
    EUCLIDEAN(VectorSimilarityFunction.EUCLIDEAN),

    /** Maximum inner product metric. */
    MAX_INNER_PRODUCT(VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT);

    private final VectorSimilarityFunction vectorSimilarityFunction;

    LuceneVectorMetric(VectorSimilarityFunction vectorSimilarityFunction) {
        this.vectorSimilarityFunction = vectorSimilarityFunction;
    }

    /**
     * Get the corresponding Lucene vector similarity function.
     *
     * @return the Lucene VectorSimilarityFunction
     */
    public VectorSimilarityFunction vectorSimilarityFunction() {
        return vectorSimilarityFunction;
    }

    public static LuceneVectorMetric fromString(String name) {
        return valueOf(name.toUpperCase());
    }
}
