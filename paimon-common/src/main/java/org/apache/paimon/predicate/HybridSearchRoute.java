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

package org.apache.paimon.predicate;

import org.apache.paimon.annotation.Experimental;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** A single search route in a hybrid search. */
public class HybridSearchRoute implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Search route type. */
    public enum RouteType {
        VECTOR,
        FULL_TEXT
    }

    private final RouteType routeType;
    private final String fieldName;
    private final float[] vector;
    private final FullTextQuery fullTextQuery;
    private final int limit;
    private final float weight;
    private final Map<String, String> options;

    public HybridSearchRoute(String fieldName, float[] vector, int limit, float weight) {
        this(fieldName, vector, limit, weight, Collections.emptyMap());
    }

    public HybridSearchRoute(
            String fieldName,
            float[] vector,
            int limit,
            float weight,
            Map<String, String> options) {
        this(RouteType.VECTOR, fieldName, vector, null, limit, weight, options);
    }

    public static HybridSearchRoute vector(
            String fieldName,
            float[] vector,
            int limit,
            float weight,
            Map<String, String> options) {
        return new HybridSearchRoute(fieldName, vector, limit, weight, options);
    }

    public static HybridSearchRoute fullText(
            String queryJson, int limit, float weight, Map<String, String> options) {
        FullTextQuery query = FullTextQuery.fromJson(queryJson);
        return new HybridSearchRoute(
                RouteType.FULL_TEXT, query.columns().get(0), null, query, limit, weight, options);
    }

    private HybridSearchRoute(
            RouteType routeType,
            String fieldName,
            float[] vector,
            FullTextQuery fullTextQuery,
            int limit,
            float weight,
            Map<String, String> options) {
        if (routeType == null) {
            throw new IllegalArgumentException("Route type cannot be null");
        }
        if (fieldName == null || fieldName.isEmpty()) {
            throw new IllegalArgumentException("Field name cannot be null or empty");
        }
        if (routeType == RouteType.VECTOR && vector == null) {
            throw new IllegalArgumentException("Search vector cannot be null for vector route");
        }
        if (routeType == RouteType.FULL_TEXT && fullTextQuery == null) {
            throw new IllegalArgumentException("Query cannot be null for full-text route");
        }
        if (limit <= 0) {
            throw new IllegalArgumentException("Limit must be positive, got: " + limit);
        }
        if (weight <= 0) {
            throw new IllegalArgumentException("Weight must be positive, got: " + weight);
        }
        this.routeType = routeType;
        this.fieldName = fieldName;
        this.vector = vector;
        this.fullTextQuery = fullTextQuery;
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

    public RouteType routeType() {
        return routeType;
    }

    public boolean isVector() {
        return routeType == RouteType.VECTOR;
    }

    public boolean isFullText() {
        return routeType == RouteType.FULL_TEXT;
    }

    public float[] vector() {
        return vector;
    }

    public FullTextQuery fullTextQuery() {
        return fullTextQuery;
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

    public FullTextSearch toFullTextSearch() {
        return new FullTextSearch(fullTextQuery, limit);
    }

    public Iterable<String> columns() {
        if (isFullText()) {
            return fullTextQuery.columns();
        }
        return Collections.singletonList(fieldName);
    }

    @Override
    public String toString() {
        return "Type("
                + routeType
                + "), FieldName("
                + fieldName
                + "), Limit("
                + limit
                + "), Weight("
                + weight
                + ")";
    }

    /** Builder for {@link HybridSearchRoute}. */
    @Experimental
    public static class Builder {

        private String fieldName;
        private float[] vector;
        private FullTextQuery fullTextQuery;
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

        public Builder query(String queryJson) {
            this.fullTextQuery = FullTextQuery.fromJson(queryJson);
            this.fieldName = fullTextQuery.columns().get(0);
            return this;
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

        public HybridSearchRoute build() {
            if (fullTextQuery != null) {
                return new HybridSearchRoute(
                        RouteType.FULL_TEXT,
                        fullTextQuery.columns().get(0),
                        null,
                        fullTextQuery,
                        limit,
                        weight,
                        options);
            }
            return HybridSearchRoute.vector(fieldName, vector, limit, weight, options);
        }
    }
}
