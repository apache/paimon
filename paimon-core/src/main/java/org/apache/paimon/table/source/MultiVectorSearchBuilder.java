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

package org.apache.paimon.table.source;

import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.MultiVectorSearchRoute;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.VectorSearch;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Builder to build multi-vector search. */
public interface MultiVectorSearchBuilder extends Serializable {

    /** Push partition filters. */
    MultiVectorSearchBuilder withPartitionFilter(PartitionPredicate partitionPredicate);

    /** Push pre-filter for vector search. */
    MultiVectorSearchBuilder withFilter(Predicate predicate);

    /** Add a vector-search route. */
    MultiVectorSearchBuilder addRoute(MultiVectorSearchRoute route);

    /** Add a vector-search route. */
    default MultiVectorSearchBuilder addRoute(String vectorColumn, float[] vector, int limit) {
        return addRoute(vectorColumn, vector, limit, 1.0f);
    }

    /** Add a vector-search route. */
    default MultiVectorSearchBuilder addRoute(
            String vectorColumn, float[] vector, int limit, float weight) {
        return addRoute(new MultiVectorSearchRoute(vectorColumn, vector, limit, weight));
    }

    /** Add a vector-search route. */
    default MultiVectorSearchBuilder addRoute(
            String vectorColumn,
            float[] vector,
            int limit,
            float weight,
            Map<String, String> options) {
        return addRoute(new MultiVectorSearchRoute(vectorColumn, vector, limit, weight, options));
    }

    /** Add an existing vector search as a route. */
    default MultiVectorSearchBuilder addVectorSearch(VectorSearch vectorSearch) {
        return addVectorSearch(vectorSearch, 1.0f);
    }

    /** Add an existing vector search as a route. */
    default MultiVectorSearchBuilder addVectorSearch(VectorSearch vectorSearch, float weight) {
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

    /** The final top k ranked results to return. */
    MultiVectorSearchBuilder withLimit(int limit);

    /** Ranker for combining route results. */
    MultiVectorSearchBuilder withRanker(String ranker);

    /** Use reciprocal rank fusion to combine route results. */
    MultiVectorSearchBuilder withRrfRanker();

    /** Use weighted score to combine route results. */
    MultiVectorSearchBuilder withWeightedScoreRanker();

    /** Create one vector-search builder for each route so engines can dispatch route work. */
    List<Route> routeBuilders();

    /** Convert a vector-search result into a weighted route result. */
    RouteResult toRouteResult(Route route, GlobalIndexResult result);

    /** Rank route results. */
    ScoredGlobalIndexResult rank(List<RouteResult> routeResults);

    /** Execute multi-vector index search in local. */
    default ScoredGlobalIndexResult executeLocal() {
        List<Route> routes = routeBuilders();
        List<RouteResult> routeResults = new ArrayList<>(routes.size());
        for (Route route : routes) {
            routeResults.add(toRouteResult(route, route.vectorSearchBuilder().executeLocal()));
        }
        return rank(routeResults);
    }

    /** A route and its configured vector-search builder. */
    class Route implements Serializable {

        private static final long serialVersionUID = 1L;

        private final MultiVectorSearchRoute route;
        private final VectorSearchBuilder vectorSearchBuilder;

        public Route(MultiVectorSearchRoute route, VectorSearchBuilder vectorSearchBuilder) {
            this.route = route;
            this.vectorSearchBuilder = vectorSearchBuilder;
        }

        public MultiVectorSearchRoute route() {
            return route;
        }

        public VectorSearchBuilder vectorSearchBuilder() {
            return vectorSearchBuilder;
        }
    }

    /** A scored result produced by one route. */
    class RouteResult implements Serializable {

        private static final long serialVersionUID = 1L;

        private final MultiVectorSearchRoute route;
        private final ScoredGlobalIndexResult result;

        public RouteResult(MultiVectorSearchRoute route, ScoredGlobalIndexResult result) {
            this.route = route;
            this.result = result;
        }

        public MultiVectorSearchRoute route() {
            return route;
        }

        public ScoredGlobalIndexResult result() {
            return result;
        }
    }
}
