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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Multi-vector search over multiple vector columns. */
public class MultiVectorSearch implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String RRF_RANKER = "rrf";
    public static final String WEIGHTED_SCORE_RANKER = "weighted_score";

    private final List<MultiVectorSearchRoute> routes;
    private final int limit;
    private final String ranker;

    public MultiVectorSearch(List<MultiVectorSearchRoute> routes, int limit, String ranker) {
        if (routes == null || routes.isEmpty()) {
            throw new IllegalArgumentException("Routes cannot be null or empty");
        }
        if (limit <= 0) {
            throw new IllegalArgumentException("Limit must be positive, got: " + limit);
        }
        this.routes = Collections.unmodifiableList(new ArrayList<>(routes));
        this.limit = limit;
        this.ranker = normalizeRanker(ranker);
    }

    @Experimental
    public static Builder builder() {
        return new Builder();
    }

    public List<MultiVectorSearchRoute> routes() {
        return routes;
    }

    public int limit() {
        return limit;
    }

    public String ranker() {
        return ranker;
    }

    private static String normalizeRanker(String ranker) {
        if (ranker == null || ranker.trim().isEmpty()) {
            return RRF_RANKER;
        }
        String normalized = ranker.trim().toLowerCase();
        if (!RRF_RANKER.equals(normalized) && !WEIGHTED_SCORE_RANKER.equals(normalized)) {
            throw new IllegalArgumentException("Unsupported multi-vector ranker: " + ranker);
        }
        return normalized;
    }

    @Override
    public String toString() {
        return "Ranker(" + ranker + "), Limit(" + limit + "), Routes(" + routes + ")";
    }

    /** Builder for {@link MultiVectorSearch}. */
    @Experimental
    public static class Builder {

        private final List<MultiVectorSearchRoute> routes = new ArrayList<>();
        private int limit;
        private String ranker = RRF_RANKER;

        public Builder addRoute(MultiVectorSearchRoute route) {
            this.routes.add(route);
            return this;
        }

        public Builder addVectorSearch(VectorSearch vectorSearch) {
            return addVectorSearch(vectorSearch, 1.0f);
        }

        public Builder addVectorSearch(VectorSearch vectorSearch, float weight) {
            if (vectorSearch == null) {
                throw new IllegalArgumentException("Vector search cannot be null");
            }
            return addRoute(
                    new MultiVectorSearchRoute(
                            vectorSearch.fieldName(),
                            vectorSearch.vector(),
                            vectorSearch.limit(),
                            weight,
                            vectorSearch.options()));
        }

        public Builder routes(List<MultiVectorSearchRoute> routes) {
            this.routes.clear();
            if (routes != null) {
                this.routes.addAll(routes);
            }
            return this;
        }

        public Builder limit(int limit) {
            this.limit = limit;
            return this;
        }

        public Builder ranker(String ranker) {
            this.ranker = ranker;
            return this;
        }

        public Builder rrfRanker() {
            return ranker(RRF_RANKER);
        }

        public Builder weightedScoreRanker() {
            return ranker(WEIGHTED_SCORE_RANKER);
        }

        public MultiVectorSearch build() {
            return new MultiVectorSearch(routes, limit, ranker);
        }
    }
}
