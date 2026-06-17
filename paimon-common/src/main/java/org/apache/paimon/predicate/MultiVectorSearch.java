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

    public static final String FUSION_RRF = "rrf";
    public static final String FUSION_WEIGHTED_SCORE = "weighted_score";

    private final List<MultiVectorSearchRoute> routes;
    private final int limit;
    private final String fusion;

    public MultiVectorSearch(List<MultiVectorSearchRoute> routes, int limit, String fusion) {
        if (routes == null || routes.isEmpty()) {
            throw new IllegalArgumentException("Routes cannot be null or empty");
        }
        if (limit <= 0) {
            throw new IllegalArgumentException("Limit must be positive, got: " + limit);
        }
        this.routes = Collections.unmodifiableList(new ArrayList<>(routes));
        this.limit = limit;
        this.fusion = normalizeFusion(fusion);
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

    public String fusion() {
        return fusion;
    }

    private static String normalizeFusion(String fusion) {
        if (fusion == null || fusion.trim().isEmpty()) {
            return FUSION_RRF;
        }
        String normalized = fusion.trim().toLowerCase();
        if (!FUSION_RRF.equals(normalized) && !FUSION_WEIGHTED_SCORE.equals(normalized)) {
            throw new IllegalArgumentException(
                    "Unsupported multi-vector fusion strategy: " + fusion);
        }
        return normalized;
    }

    @Override
    public String toString() {
        return "Fusion(" + fusion + "), Limit(" + limit + "), Routes(" + routes + ")";
    }

    /** Builder for {@link MultiVectorSearch}. */
    @Experimental
    public static class Builder {

        private final List<MultiVectorSearchRoute> routes = new ArrayList<>();
        private int limit;
        private String fusion = FUSION_RRF;

        public Builder addRoute(MultiVectorSearchRoute route) {
            this.routes.add(route);
            return this;
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

        public Builder fusion(String fusion) {
            this.fusion = fusion;
            return this;
        }

        public Builder fusionRrf() {
            return fusion(FUSION_RRF);
        }

        public Builder fusionWeightedScore() {
            return fusion(FUSION_WEIGHTED_SCORE);
        }

        public MultiVectorSearch build() {
            return new MultiVectorSearch(routes, limit, fusion);
        }
    }
}
