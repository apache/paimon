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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.paimon.predicate;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** A single vector-search route in a multi-vector search. */
public class MultiVectorSearchRoute implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String fieldName;
    private final float[] vector;
    private final int limit;
    private final float weight;
    private final Map<String, String> options;

    public MultiVectorSearchRoute(String fieldName, float[] vector, int limit, float weight) {
        this(fieldName, vector, limit, weight, Collections.emptyMap());
    }

    public MultiVectorSearchRoute(
            String fieldName,
            float[] vector,
            int limit,
            float weight,
            Map<String, String> options) {
        if (fieldName == null || fieldName.isEmpty()) {
            throw new IllegalArgumentException("Field name cannot be null or empty");
        }
        if (vector == null) {
            throw new IllegalArgumentException("Search vector cannot be null");
        }
        if (limit <= 0) {
            throw new IllegalArgumentException("Limit must be positive, got: " + limit);
        }
        if (weight <= 0) {
            throw new IllegalArgumentException("Weight must be positive, got: " + weight);
        }
        this.fieldName = fieldName;
        this.vector = vector;
        this.limit = limit;
        this.weight = weight;
        this.options =
                options == null
                        ? Collections.emptyMap()
                        : Collections.unmodifiableMap(new HashMap<>(options));
    }

    public String fieldName() {
        return fieldName;
    }

    public float[] vector() {
        return vector;
    }

    public int limit() {
        return limit;
    }

    public float weight() {
        return weight;
    }

    public Map<String, String> options() {
        return options;
    }

    public VectorSearch toVectorSearch() {
        return new VectorSearch(vector, limit, fieldName, options);
    }

    @Override
    public String toString() {
        return "FieldName(" + fieldName + "), Limit(" + limit + "), Weight(" + weight + ")";
    }
}
