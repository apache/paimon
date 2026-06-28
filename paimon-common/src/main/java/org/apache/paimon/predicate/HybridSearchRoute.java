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
        checkFullTextOptions(options);
        FullTextQuery query = FullTextQuery.fromJson(queryJson);
        return new HybridSearchRoute(
                RouteType.FULL_TEXT, query.columns().get(0), null, query, limit, weight, options);
    }

    private static void checkFullTextOptions(Map<String, String> options) {
        if (options != null && !options.isEmpty()) {
            throw new IllegalArgumentException(
                    "Full-text hybrid route options are not supported yet.");
        }
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
        if (!Float.isFinite(weight) || weight <= 0) {
            throw new IllegalArgumentException(
                    "Weight must be finite and positive, got: " + weight);
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
        private RouteType routeType;
        private int limit;
        private float weight = 1.0f;
        private Map<String, String> options = new HashMap<>();

        public Builder vectorColumn(String fieldName) {
            setRouteType(RouteType.VECTOR);
            this.fieldName = fieldName;
            return this;
        }

        public Builder field(String fieldName) {
            return vectorColumn(fieldName);
        }

        public Builder queryVector(float[] vector) {
            setRouteType(RouteType.VECTOR);
            this.vector = vector;
            return this;
        }

        public Builder vector(float[] vector) {
            return queryVector(vector);
        }

        public Builder query(String queryJson) {
            checkRouteType(RouteType.FULL_TEXT);
            FullTextQuery fullTextQuery = FullTextQuery.fromJson(queryJson);
            this.routeType = RouteType.FULL_TEXT;
            this.fullTextQuery = fullTextQuery;
            this.fieldName = fullTextQuery.columns().get(0);
            return this;
        }

        private void setRouteType(RouteType routeType) {
            checkRouteType(routeType);
            this.routeType = routeType;
        }

        private void checkRouteType(RouteType routeType) {
            if (this.routeType != null && this.routeType != routeType) {
                throw new IllegalArgumentException(
                        "Cannot mix vector and full-text hybrid route settings");
            }
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
            if (routeType == RouteType.FULL_TEXT) {
                checkFullTextOptions(options);
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
