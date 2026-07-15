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

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.HybridSearchRanker;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.HybridSearchRoute;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.InnerTable;
import org.apache.paimon.table.source.snapshot.TimeTravelUtil;
import org.apache.paimon.utils.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.apache.paimon.utils.Preconditions.checkArgument;

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

        Snapshot snapshot = null;
        if (table instanceof FileStoreTable) {
            FileStoreTable fileStoreTable = (FileStoreTable) table;
            if (!TimeTravelUtil.tryTravelToSnapshot(fileStoreTable).isPresent()) {
                snapshot = fileStoreTable.latestSnapshot().orElse(null);
            }
        }
        List<Route> routeBuilders = new ArrayList<>(routes.size());
        for (HybridSearchRoute route : routes) {
            if (route.isVector()) {
                VectorSearchBuilder builder = newVectorSearchBuilder(route);
                if (snapshot != null && builder instanceof VectorSearchBuilderImpl) {
                    ((VectorSearchBuilderImpl) builder).withSnapshot(snapshot);
                }
                routeBuilders.add(new Route(route, builder));
            } else {
                FullTextSearchBuilder builder = newFullTextSearchBuilder(route);
                if (snapshot != null && builder instanceof FullTextSearchBuilderImpl) {
                    ((FullTextSearchBuilderImpl) builder).withSnapshot(snapshot);
                }
                routeBuilders.add(new Route(route, builder));
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

        boolean hasPhysical = false;
        boolean hasGlobal = false;
        for (RouteResult routeResult : routeResults) {
            if (routeResult.result() instanceof PrimaryKeyScoredResult) {
                hasPhysical = true;
            } else {
                hasGlobal = true;
            }
        }
        if (hasPhysical && hasGlobal) {
            throw new UnsupportedOperationException(
                    "Hybrid search cannot combine physical primary-key positions and global row-id address spaces.");
        }
        if (hasPhysical) {
            return rankPhysical(routeResults);
        }

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

    private PrimaryKeyScoredResult rankPhysical(List<RouteResult> routeResults) {
        Long snapshotId = null;
        List<PrimaryKeySearchRanker.Ranking> rankings = new ArrayList<>(routeResults.size());
        List<PrimaryKeyScoredResult> physicalResults = new ArrayList<>(routeResults.size());
        for (RouteResult routeResult : routeResults) {
            PrimaryKeyScoredResult result = (PrimaryKeyScoredResult) routeResult.result();
            if (snapshotId == null) {
                snapshotId = result.snapshotId();
            } else {
                checkArgument(
                        snapshotId == result.snapshotId(),
                        "Primary-key hybrid routes must use the same snapshot, but found %s and %s.",
                        snapshotId,
                        result.snapshotId());
            }
            physicalResults.add(result);
            if (!result.positions().isEmpty()) {
                rankings.add(
                        new PrimaryKeySearchRanker.Ranking(
                                result.positions(), routeResult.route().weight()));
            }
        }

        List<PrimaryKeySearchPosition> positions;
        if (HybridSearchRanker.WEIGHTED_SCORE_RANKER.equals(ranker)) {
            positions = PrimaryKeySearchRanker.weightedScore(rankings, limit);
        } else if (HybridSearchRanker.MRR_RANKER.equals(ranker)) {
            positions = PrimaryKeySearchRanker.weightedMrr(rankings, limit);
        } else {
            positions = PrimaryKeySearchRanker.weightedRrf(rankings, limit);
        }
        return new PrimaryKeyScoredResult(
                snapshotId, physicalSources(physicalResults, positions), positions);
    }

    private static List<DataSplit> physicalSources(
            List<PrimaryKeyScoredResult> results, List<PrimaryKeySearchPosition> positions) {
        Map<PhysicalFileKey, DataSplit> available = new LinkedHashMap<>();
        for (PrimaryKeyScoredResult result : results) {
            for (IndexedSplit indexedSplit : result.splits()) {
                DataSplit source = indexedSplit.dataSplit();
                checkArgument(
                        source.dataFiles().size() == 1,
                        "Primary-key scored source split must contain exactly one data file.");
                PhysicalFileKey key =
                        new PhysicalFileKey(
                                source.partition(),
                                source.bucket(),
                                source.dataFiles().get(0).fileName());
                DataSplit previous = available.putIfAbsent(key, source);
                if (previous != null) {
                    checkArgument(
                            previous.snapshotId() == source.snapshotId()
                                    && previous.bucketPath().equals(source.bucketPath())
                                    && Objects.equals(
                                            previous.totalBuckets(), source.totalBuckets())
                                    && previous.dataFiles().get(0).fileSize()
                                            == source.dataFiles().get(0).fileSize()
                                    && previous.dataFiles().get(0).rowCount()
                                            == source.dataFiles().get(0).rowCount()
                                    && Objects.equals(deletionFile(previous), deletionFile(source)),
                            "Primary-key hybrid routes contain inconsistent metadata for data file %s.",
                            key.dataFileName);
                }
            }
        }

        Map<PhysicalFileKey, DataSplit> selected = new LinkedHashMap<>();
        for (PrimaryKeySearchPosition position : positions) {
            PhysicalFileKey key = PhysicalFileKey.from(position);
            DataSplit source = available.get(key);
            checkArgument(
                    source != null,
                    "Primary-key hybrid result references unknown data file %s.",
                    position.dataFileName());
            selected.putIfAbsent(key, source);
        }
        return Collections.unmodifiableList(new ArrayList<>(selected.values()));
    }

    private static DeletionFile deletionFile(DataSplit split) {
        if (!split.deletionFiles().isPresent()) {
            return null;
        }
        checkArgument(
                split.deletionFiles().get().size() == 1,
                "Primary-key scored source split must contain exactly one deletion-file entry.");
        return split.deletionFiles().get().get(0);
    }

    private static class PhysicalFileKey {

        private final BinaryRow partition;
        private final int bucket;
        private final String dataFileName;

        private PhysicalFileKey(BinaryRow partition, int bucket, String dataFileName) {
            this.partition = partition.copy();
            this.bucket = bucket;
            this.dataFileName = dataFileName;
        }

        private static PhysicalFileKey from(PrimaryKeySearchPosition position) {
            return new PhysicalFileKey(
                    position.partition(), position.bucket(), position.dataFileName());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof PhysicalFileKey)) {
                return false;
            }
            PhysicalFileKey that = (PhysicalFileKey) o;
            return bucket == that.bucket
                    && partition.equals(that.partition)
                    && dataFileName.equals(that.dataFileName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(partition, bucket, dataFileName);
        }
    }

    @Override
    public RouteResult toRouteResult(Route route, GlobalIndexResult result) {
        if (result instanceof PrimaryKeyVectorResult) {
            return new RouteResult(route.route(), ((PrimaryKeyVectorResult) result).scoredResult());
        } else if (result instanceof ScoredGlobalIndexResult) {
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
        if (partitionFilter != null) {
            fullTextSearchBuilder.withPartitionFilter(partitionFilter);
        }
        return fullTextSearchBuilder;
    }
}
