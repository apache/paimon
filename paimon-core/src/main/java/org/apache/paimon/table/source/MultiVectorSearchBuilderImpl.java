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
import org.apache.paimon.globalindex.MultiVectorSearchRanker;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.MultiVectorSearchRoute;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.InnerTable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Implementation for {@link MultiVectorSearchBuilder}. */
public class MultiVectorSearchBuilderImpl implements MultiVectorSearchBuilder {

    private static final long serialVersionUID = 1L;

    protected final InnerTable table;

    protected final List<MultiVectorSearchRoute> routes = new ArrayList<>();
    protected int limit;
    protected String ranker = MultiVectorSearchRanker.RRF_RANKER;
    protected PartitionPredicate partitionFilter;
    protected Predicate filter;

    public MultiVectorSearchBuilderImpl(InnerTable table) {
        this.table = table;
    }

    @Override
    public MultiVectorSearchBuilder withPartitionFilter(PartitionPredicate partitionFilter) {
        this.partitionFilter = partitionFilter;
        return this;
    }

    @Override
    public MultiVectorSearchBuilder withFilter(Predicate predicate) {
        if (this.filter == null) {
            this.filter = predicate;
        } else {
            this.filter = PredicateBuilder.and(this.filter, predicate);
        }
        return this;
    }

    @Override
    public MultiVectorSearchBuilder addRoute(MultiVectorSearchRoute route) {
        this.routes.add(Objects.requireNonNull(route, "Route cannot be null"));
        return this;
    }

    @Override
    public MultiVectorSearchBuilder withLimit(int limit) {
        this.limit = limit;
        return this;
    }

    @Override
    public MultiVectorSearchBuilder withRanker(String ranker) {
        this.ranker = MultiVectorSearchRanker.normalizeRanker(ranker);
        return this;
    }

    @Override
    public MultiVectorSearchBuilder withRrfRanker() {
        return withRanker(MultiVectorSearchRanker.RRF_RANKER);
    }

    @Override
    public MultiVectorSearchBuilder withWeightedScoreRanker() {
        return withRanker(MultiVectorSearchRanker.WEIGHTED_SCORE_RANKER);
    }

    @Override
    public List<Route> routeBuilders() {
        validateSearch();

        List<Route> routeBuilders = new ArrayList<>(routes.size());
        for (MultiVectorSearchRoute route : routes) {
            VectorSearchBuilder vectorSearchBuilder = newVectorSearchBuilder(route);
            routeBuilders.add(new Route(route, vectorSearchBuilder));
        }
        return routeBuilders;
    }

    private void validateSearch() {
        if (routes.isEmpty()) {
            throw new IllegalArgumentException("Routes cannot be empty");
        }
        if (limit <= 0) {
            throw new IllegalArgumentException("Limit must be positive, got: " + limit);
        }
    }

    @Override
    public ScoredGlobalIndexResult rank(List<RouteResult> routeResults) {
        validateSearch();

        List<MultiVectorSearchRanker.WeightedResult> weightedResults =
                new ArrayList<>(routeResults.size());
        for (RouteResult routeResult : routeResults) {
            if (!routeResult.result().results().isEmpty()) {
                weightedResults.add(
                        new MultiVectorSearchRanker.WeightedResult(
                                routeResult.result(), routeResult.route().weight()));
            }
        }
        return MultiVectorSearchRanker.rank(ranker, weightedResults, limit);
    }

    @Override
    public RouteResult toRouteResult(Route route, GlobalIndexResult result) {
        if (result instanceof ScoredGlobalIndexResult) {
            return new RouteResult(route.route(), (ScoredGlobalIndexResult) result);
        } else if (result.results().isEmpty()) {
            return new RouteResult(route.route(), ScoredGlobalIndexResult.createEmpty());
        } else {
            throw new UnsupportedOperationException(
                    "Multi-vector search requires scored vector index results, but got: "
                            + result.getClass().getName());
        }
    }

    protected VectorSearchBuilder newVectorSearchBuilder(MultiVectorSearchRoute route) {
        VectorSearchBuilder vectorSearchBuilder =
                table.newVectorSearchBuilder()
                        .withVector(route.vector())
                        .withVectorColumn(route.fieldName())
                        .withLimit(route.limit())
                        .withOptions(route.options());
        if (partitionFilter != null) {
            vectorSearchBuilder.withPartitionFilter(partitionFilter);
        }
        if (filter != null) {
            vectorSearchBuilder.withFilter(filter);
        }
        return vectorSearchBuilder;
    }
}
