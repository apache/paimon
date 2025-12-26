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

/** Enumeration of supported FAISS vector similarity metrics. */
public enum FaissVectorMetric {

    /**
     * L2 (Euclidean) distance metric.
     *
     * <p>This is the default metric for FAISS. Lower values indicate more similar vectors.
     */
    L2(0),

    /**
     * Inner product (dot product) metric.
     *
     * <p>Higher values indicate more similar vectors. For normalized vectors, this is equivalent to
     * cosine similarity.
     */
    INNER_PRODUCT(1);

    private final int value;

    FaissVectorMetric(int value) {
        this.value = value;
    }

    /**
     * Get the numeric value used by FAISS.
     *
     * @return the metric value
     */
    public int getValue() {
        return value;
    }

    /**
     * Convert a string to a metric.
     *
     * @param name the metric name
     * @return the metric
     */
    public static FaissVectorMetric fromString(String name) {
        return valueOf(name.toUpperCase());
    }

    /**
     * Convert a numeric value to a metric.
     *
     * @param value the metric value
     * @return the metric
     */
    public static FaissVectorMetric fromValue(int value) {
        for (FaissVectorMetric metric : values()) {
            if (metric.value == value) {
                return metric;
            }
        }
        throw new IllegalArgumentException("Unknown metric value: " + value);
    }
}

