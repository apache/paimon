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

/** Enumeration of supported Lumina vector similarity metrics. */
public enum LuminaVectorMetric {

    /** L2 (Euclidean) distance metric. Lower values indicate more similar vectors. */
    L2(0, "l2"),

    /** Cosine distance metric. Lower values indicate more similar vectors. */
    COSINE(1, "cosine"),

    /**
     * Inner product (dot product) metric. Higher values indicate more similar vectors. For
     * normalized vectors, this is equivalent to cosine similarity.
     */
    INNER_PRODUCT(2, "inner_product");

    private final int value;
    private final String luminaName;

    LuminaVectorMetric(int value, String luminaName) {
        this.value = value;
        this.luminaName = luminaName;
    }

    public int getValue() {
        return value;
    }

    /** Returns the Lumina native distance metric name (e.g. "l2", "cosine", "inner_product"). */
    public String getLuminaName() {
        return luminaName;
    }

    public static LuminaVectorMetric fromString(String name) {
        return valueOf(name.toUpperCase());
    }

    public static LuminaVectorMetric fromValue(int value) {
        for (LuminaVectorMetric metric : values()) {
            if (metric.value == value) {
                return metric;
            }
        }
        throw new IllegalArgumentException(String.format("Unknown metric value: %s", value));
    }

    /** Resolves a Lumina native metric name (e.g. "l2") to the corresponding enum constant. */
    public static LuminaVectorMetric fromLuminaName(String luminaName) {
        for (LuminaVectorMetric metric : values()) {
            if (metric.luminaName.equals(luminaName)) {
                return metric;
            }
        }
        throw new IllegalArgumentException(
                String.format("Unknown lumina metric name: %s", luminaName));
    }
}
