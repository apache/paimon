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
import org.apache.paimon.predicate.HybridSearchRoute;
import org.apache.paimon.predicate.Predicate;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Builder to build hybrid search. */
public interface HybridSearchBuilder extends Serializable {

    /** Push partition filters. */
    HybridSearchBuilder withPartitionFilter(PartitionPredicate partitionPredicate);

    /** Push pre-filter for route search. */
    HybridSearchBuilder withFilter(Predicate predicate);

    /** Add a hybrid-search route. */
    HybridSearchBuilder addRoute(HybridSearchRoute route);

    /** Add a vector-search route. */
    default HybridSearchBuilder addVectorRoute(String vectorColumn, float[] vector, int limit) {
        return addVectorRoute(vectorColumn, vector, limit, 1.0f);
    }

    /** Add a vector-search route. */
    default HybridSearchBuilder addVectorRoute(
            String vectorColumn, float[] vector, int limit, float weight) {
        return addRoute(new HybridSearchRoute(vectorColumn, vector, limit, weight));
    }

    /** Add a vector-search route. */
    default HybridSearchBuilder addVectorRoute(
            String vectorColumn,
            float[] vector,
            int limit,
            float weight,
            Map<String, String> options) {
        return addRoute(new HybridSearchRoute(vectorColumn, vector, limit, weight, options));
    }

    /** Add a full-text-search route. */
    default HybridSearchBuilder addFullTextRoute(
            String textColumn, String queryText, int limit, float weight) {
        return addFullTextRoute(textColumn, queryText, "or", limit, weight, null);
    }

    /** Add a full-text-search route. */
    default HybridSearchBuilder addFullTextRoute(
            String textColumn,
            String queryText,
            String queryOperator,
            int limit,
            float weight,
            Map<String, String> options) {
        return addRoute(
                HybridSearchRoute.fullText(
                        textColumn, queryText, queryOperator, limit, weight, options));
    }

    /** The final top k ranked results to return. */
    HybridSearchBuilder withLimit(int limit);

    /** Ranker for combining route results. */
    HybridSearchBuilder withRanker(String ranker);

    /** Use reciprocal rank fusion to combine route results. */
    HybridSearchBuilder withRrfRanker();

    /** Use weighted score to combine route results. */
    HybridSearchBuilder withWeightedScoreRanker();

    /** Create one search builder for each route so engines can dispatch route work. */
    List<Route> routeBuilders();

    /** Convert a search result into a weighted route result. */
    RouteResult toRouteResult(Route route, GlobalIndexResult result);

    /** Rank route results. */
    ScoredGlobalIndexResult rank(List<RouteResult> routeResults);

    /** Execute hybrid index search in local. */
    default ScoredGlobalIndexResult executeLocal() {
        List<Route> routes = routeBuilders();
        List<RouteResult> routeResults = new ArrayList<>(routes.size());
        for (Route route : routes) {
            routeResults.add(toRouteResult(route, route.executeLocal()));
        }
        return rank(routeResults);
    }

    /** A route and its configured search builder. */
    class Route implements Serializable {

        private static final long serialVersionUID = 1L;

        private final HybridSearchRoute route;
        private final VectorSearchBuilder vectorSearchBuilder;
        private final FullTextSearchBuilder fullTextSearchBuilder;

        public Route(HybridSearchRoute route, VectorSearchBuilder vectorSearchBuilder) {
            this.route = route;
            this.vectorSearchBuilder = vectorSearchBuilder;
            this.fullTextSearchBuilder = null;
        }

        public Route(HybridSearchRoute route, FullTextSearchBuilder fullTextSearchBuilder) {
            this.route = route;
            this.vectorSearchBuilder = null;
            this.fullTextSearchBuilder = fullTextSearchBuilder;
        }

        public HybridSearchRoute route() {
            return route;
        }

        public VectorSearchBuilder vectorSearchBuilder() {
            return vectorSearchBuilder;
        }

        public FullTextSearchBuilder fullTextSearchBuilder() {
            return fullTextSearchBuilder;
        }

        public GlobalIndexResult executeLocal() {
            if (route.isVector()) {
                return vectorSearchBuilder.executeLocal();
            }
            return fullTextSearchBuilder.executeLocal();
        }
    }

    /** A scored result produced by one route. */
    class RouteResult implements Serializable {

        private static final long serialVersionUID = 1L;

        private final HybridSearchRoute route;
        private final ScoredGlobalIndexResult result;

        public RouteResult(HybridSearchRoute route, ScoredGlobalIndexResult result) {
            this.route = route;
            this.result = result;
        }

        public HybridSearchRoute route() {
            return route;
        }

        public ScoredGlobalIndexResult result() {
            return result;
        }
    }
}
