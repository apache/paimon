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
import org.apache.paimon.globalindex.HybridSearchRanker;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.HybridSearchRoute;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.InnerTable;
import org.apache.paimon.utils.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Implementation for {@link HybridSearchBuilder}. */
public class HybridSearchBuilderImpl implements HybridSearchBuilder {

    private static final long serialVersionUID = 1L;

    protected final InnerTable table;

    protected final List<HybridSearchRoute> routes = new ArrayList<>();
    protected int limit;
    protected String ranker = HybridSearchRanker.RRF_RANKER;
    protected PartitionPredicate partitionFilter;
    protected Predicate filter;

    public HybridSearchBuilderImpl(InnerTable table) {
        this.table = table;
    }

    @Override
    public HybridSearchBuilder withPartitionFilter(PartitionPredicate partitionFilter) {
        addPartitionFilter(partitionFilter);
        return this;
    }

    @Override
    public HybridSearchBuilder withFilter(Predicate predicate) {
        Pair<Optional<PartitionPredicate>, List<Predicate>> pair =
                PartitionPredicate.splitPartitionPredicatesAndDataPredicates(
                        predicate, table.rowType(), table.partitionKeys());
        if (pair.getLeft().isPresent()) {
            addPartitionFilter(pair.getLeft().get());
        }
        if (!pair.getRight().isEmpty()) {
            Predicate dataFilter = PredicateBuilder.and(pair.getRight());
            if (this.filter == null) {
                this.filter = dataFilter;
            } else {
                this.filter = PredicateBuilder.and(this.filter, dataFilter);
            }
        }
        return this;
    }

    private void addPartitionFilter(PartitionPredicate partitionFilter) {
        if (partitionFilter == null) {
            return;
        }
        if (this.partitionFilter == null) {
            this.partitionFilter = partitionFilter;
        } else {
            this.partitionFilter =
                    PartitionPredicate.and(Arrays.asList(this.partitionFilter, partitionFilter));
        }
    }

    @Override
    public HybridSearchBuilder addRoute(HybridSearchRoute route) {
        this.routes.add(Objects.requireNonNull(route, "Route cannot be null"));
        return this;
    }

    @Override
    public HybridSearchBuilder withLimit(int limit) {
        this.limit = limit;
        return this;
    }

    @Override
    public HybridSearchBuilder withRanker(String ranker) {
        this.ranker = HybridSearchRanker.normalizeRanker(ranker);
        return this;
    }

    @Override
    public HybridSearchBuilder withRrfRanker() {
        return withRanker(HybridSearchRanker.RRF_RANKER);
    }

    @Override
    public HybridSearchBuilder withWeightedScoreRanker() {
        return withRanker(HybridSearchRanker.WEIGHTED_SCORE_RANKER);
    }

    @Override
    public List<Route> routeBuilders() {
        validateSearch();

        List<Route> routeBuilders = new ArrayList<>(routes.size());
        for (HybridSearchRoute route : routes) {
            if (route.isVector()) {
                routeBuilders.add(new Route(route, newVectorSearchBuilder(route)));
            } else {
                routeBuilders.add(new Route(route, newFullTextSearchBuilder(route)));
            }
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
        if (filter != null) {
            for (HybridSearchRoute route : routes) {
                if (!route.isVector()) {
                    throw new UnsupportedOperationException(
                            "Hybrid search with full-text routes does not support non-partition "
                                    + "filters because full-text indexes cannot apply row-id "
                                    + "pre-filters before top-k ranking.");
                }
            }
        }
    }

    @Override
    public ScoredGlobalIndexResult rank(List<RouteResult> routeResults) {
        validateSearch();

        List<HybridSearchRanker.WeightedResult> weightedResults =
                new ArrayList<>(routeResults.size());
        for (RouteResult routeResult : routeResults) {
            if (!routeResult.result().results().isEmpty()) {
                weightedResults.add(
                        new HybridSearchRanker.WeightedResult(
                                routeResult.result(), routeResult.route().weight()));
            }
        }
        return HybridSearchRanker.rank(ranker, weightedResults, limit);
    }

    @Override
    public RouteResult toRouteResult(Route route, GlobalIndexResult result) {
        if (result instanceof ScoredGlobalIndexResult) {
            return new RouteResult(route.route(), (ScoredGlobalIndexResult) result);
        } else if (result.results().isEmpty()) {
            return new RouteResult(route.route(), ScoredGlobalIndexResult.createEmpty());
        } else {
            throw new UnsupportedOperationException(
                    "Hybrid search requires scored index results, but got: "
                            + result.getClass().getName());
        }
    }

    protected VectorSearchBuilder newVectorSearchBuilder(HybridSearchRoute route) {
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

    protected FullTextSearchBuilder newFullTextSearchBuilder(HybridSearchRoute route) {
        FullTextSearchBuilder fullTextSearchBuilder =
                table.newFullTextSearchBuilder()
                        .withQuery(route.fieldName(), route.fullTextQuery())
                        .withLimit(route.limit());
        if (fullTextSearchBuilder.newFullTextScan() instanceof PrimaryKeyFullTextScan) {
            throw new UnsupportedOperationException(
                    "Hybrid search does not support primary-key full-text indexes because their "
                            + "results use physical file positions instead of global row ids.");
        }
        if (partitionFilter != null) {
            fullTextSearchBuilder.withPartitionFilter(partitionFilter);
        }
        return fullTextSearchBuilder;
    }
}
