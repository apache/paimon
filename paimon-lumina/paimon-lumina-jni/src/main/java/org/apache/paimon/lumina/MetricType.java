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

package org.apache.paimon.lumina;

/**
 * Distance metric type for Lumina vector search.
 *
 * <p>Maps to Lumina's distance.metric option values.
 */
public enum MetricType {

    /** L2 (Euclidean) distance. Smaller values indicate more similar vectors. */
    L2("l2"),

    /** Cosine distance. Smaller values indicate more similar vectors. */
    COSINE("cosine"),

    /** Inner product. Larger values indicate more similar vectors. */
    INNER_PRODUCT("inner_product");

    private final String luminaValue;

    MetricType(String luminaValue) {
        this.luminaValue = luminaValue;
    }

    /** Get the Lumina option value string for this metric. */
    public String getLuminaValue() {
        return luminaValue;
    }

    public static MetricType fromLuminaValue(String value) {
        for (MetricType type : values()) {
            if (type.luminaValue.equals(value)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown metric type: " + value);
    }
}
