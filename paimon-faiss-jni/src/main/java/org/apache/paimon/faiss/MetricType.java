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

/**
 * Metric type for similarity search.
 *
 * <p>Faiss supports two main metric types for measuring similarity between vectors:
 *
 * <ul>
 *   <li>{@link #L2} - Euclidean distance (L2 norm). Smaller values indicate more similar vectors.
 *   <li>{@link #INNER_PRODUCT} - Inner product (dot product). Larger values indicate more similar
 *       vectors.
 * </ul>
 */
public enum MetricType {
    /**
     * Euclidean distance (L2 norm).
     *
     * <p>The squared L2 distance between two vectors is computed as: {@code sum((a[i] - b[i])^2)}
     *
     * <p>Smaller distances indicate more similar vectors.
     */
    L2(0),

    /**
     * Inner product (dot product).
     *
     * <p>The inner product between two vectors is computed as: {@code sum(a[i] * b[i])}
     *
     * <p>Larger values indicate more similar vectors. For normalized vectors, this is equivalent to
     * cosine similarity.
     */
    INNER_PRODUCT(1);

    private final int value;

    MetricType(int value) {
        this.value = value;
    }

    /**
     * Get the numeric value of this metric type.
     *
     * @return the numeric value
     */
    public int getValue() {
        return value;
    }

    /**
     * Get a MetricType from its numeric value.
     *
     * @param value the numeric value
     * @return the corresponding MetricType
     * @throws IllegalArgumentException if the value is not valid
     */
    public static MetricType fromValue(int value) {
        for (MetricType type : values()) {
            if (type.value == value) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown metric type value: " + value);
    }
}
