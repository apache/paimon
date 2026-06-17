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

import org.apache.paimon.annotation.Experimental;

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

    @Experimental
    public static Builder builder() {
        return new Builder();
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

    /** Builder for {@link MultiVectorSearchRoute}. */
    @Experimental
    public static class Builder {

        private String fieldName;
        private float[] vector;
        private int limit;
        private float weight = 1.0f;
        private Map<String, String> options = new HashMap<>();

        public Builder vectorColumn(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        public Builder field(String fieldName) {
            return vectorColumn(fieldName);
        }

        public Builder queryVector(float[] vector) {
            this.vector = vector;
            return this;
        }

        public Builder vector(float[] vector) {
            return queryVector(vector);
        }

        public Builder limit(int limit) {
            this.limit = limit;
            return this;
        }

        public Builder weight(float weight) {
            this.weight = weight;
            return this;
        }

        public Builder option(String key, String value) {
            this.options.put(key, value);
            return this;
        }

        public Builder options(Map<String, String> options) {
            if (options != null) {
                this.options.putAll(options);
            }
            return this;
        }

        public MultiVectorSearchRoute build() {
            return new MultiVectorSearchRoute(fieldName, vector, limit, weight, options);
        }
    }
}
