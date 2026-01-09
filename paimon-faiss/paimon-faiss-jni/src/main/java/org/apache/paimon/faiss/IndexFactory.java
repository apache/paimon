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

package org.apache.paimon.faiss;

/** Factory for creating Faiss indexes. */
public final class IndexFactory {

    /**
     * Create a Faiss index using the index factory.
     *
     * @param dimension the dimension of the vectors
     * @param description the index description string
     * @param metricType the metric type for similarity computation
     * @return the created index
     */
    public static Index create(int dimension, String description, MetricType metricType) {
        if (dimension <= 0) {
            throw new IllegalArgumentException("Dimension must be positive: " + dimension);
        }
        if (description == null || description.isEmpty()) {
            throw new IllegalArgumentException("Index description cannot be null or empty");
        }
        if (metricType == null) {
            throw new IllegalArgumentException("Metric type cannot be null");
        }

        long handle = FaissNative.indexFactoryCreate(dimension, description, metricType.getValue());
        return new Index(handle, dimension);
    }

    /**
     * Create a Faiss index with L2 (Euclidean) metric.
     *
     * @param dimension the dimension of the vectors
     * @param description the index description string
     * @return the created index
     */
    public static Index create(int dimension, String description) {
        return create(dimension, description, MetricType.L2);
    }
}
