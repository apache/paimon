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

package org.apache.paimon.vector.index;

import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.globalindex.AbstractVectorGlobalIndexReader;
import org.apache.paimon.globalindex.CentroidIndexFile;
import org.apache.paimon.globalindex.CentroidShardFilterReader;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.ScoreGetter;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.index.vector.CentroidRouter;
import org.apache.paimon.predicate.BatchVectorSearch;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.IOFunction;
import org.apache.paimon.utils.RoaringNavigableMap64;
import org.apache.paimon.vector.index.NativeVectorGlobalIndexReader.SearchScope;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Routes queries to shard files using row IDs relative to their shared range. */
final class NativeCentroidRoutedIndexReader extends AbstractVectorGlobalIndexReader
        implements CentroidShardFilterReader {
    private static final int JOINT_OUTER_PROBES = 4;
    private static final int[] NO_CENTERS = new int[0];

    private final GlobalIndexFileReader files;
    private final CentroidIndexFile routing;
    private final String metric;
    private final Map<Integer, GlobalIndexIOMeta> shards = new HashMap<>();
    private final Set<Integer> jointLayout = new HashSet<>();
    private final DataType fieldType;
    private final ExecutorService executor;
    private final long vectorCount;
    private final String indexType;
    private CentroidRouter model;
    private IOFunction<Integer, RoaringNavigableMap64> shardFilter;
    private volatile boolean closed;

    NativeCentroidRoutedIndexReader(
            GlobalIndexFileReader files,
            List<GlobalIndexIOMeta> entries,
            DataType fieldType,
            ExecutorService executor,
            String indexType) {
        this.files = files;
        this.fieldType = fieldType;
        this.executor = executor;
        this.indexType = indexType;
        CentroidIndexFile modelMeta = null;
        long count = 0;
        checkArgument(
                !entries.isEmpty() && entries.get(0) instanceof CentroidIndexFile,
                "Missing centroid index metadata.");
        for (GlobalIndexIOMeta file : entries) {
            checkArgument(
                    file instanceof CentroidIndexFile, "Mixed centroid and range index files.");
            CentroidIndexFile meta = (CentroidIndexFile) file;
            if (meta.scalarIndex()) {
                continue;
            }
            if (meta.routingModel()) {
                checkArgument(modelMeta == null, "Multiple routing models.");
                modelMeta = meta;
            } else {
                checkArgument(file.rowCount() > 0, "Centroid shards must contain vectors.");
                checkArgument(
                        shards.put(meta.centroid(), file) == null,
                        "Duplicate centroid shard %s.",
                        meta.centroid());
                count = Math.addExact(count, file.rowCount());
            }
        }
        checkArgument(modelMeta != null, "Missing routing model.");
        this.routing = modelMeta;
        this.metric = NativeCentroidVectorGlobalIndexer.routingMetric(modelMeta);
        this.vectorCount = count;
        int nlist = routing.nlist();
        boolean storesLocalNlist = storesLocalNlist(indexType, metric);
        for (Map.Entry<Integer, GlobalIndexIOMeta> shard : shards.entrySet()) {
            int centroid = shard.getKey();
            checkArgument(
                    centroid >= 0 && centroid < nlist, "Invalid shard centroid %s.", centroid);
            byte[] local = shard.getValue().metadata();
            if (storesLocalNlist) {
                checkArgument(
                        local != null && local.length == Integer.BYTES,
                        "Invalid centroid shard metadata.");
                int localNlist = ByteBuffer.wrap(local).getInt();
                checkArgument(localNlist > 0, "Invalid centroid shard nlist.");
                if (localNlist == 64) {
                    jointLayout.add(centroid);
                }
            } else {
                checkArgument(local == null, "Unexpected centroid shard metadata.");
            }
        }
    }

    static boolean storesLocalNlist(String indexType, String metric) {
        return "l2".equals(metric) && ("ivf-flat".equals(indexType) || "ivf-pq".equals(indexType));
    }

    @Override
    public void withShardFilter(IOFunction<Integer, RoaringNavigableMap64> filter) {
        this.shardFilter = filter;
    }

    private synchronized CentroidRouter model() {
        checkArgument(!closed, "Centroid reader is closed.");
        if (model == null) {
            try (SeekableInputStream in = files.getInputStream(routing)) {
                byte[] payload = VectorTrainingModelIO.read(in, routing.fileSize());
                CentroidRouter loaded = CentroidRouter.deserialize(payload);
                if (loaded.nlist() != routing.nlist()) {
                    loaded.close();
                    throw new IOException("Routing nlist differs from manifest metadata.");
                }
                model = loaded;
            } catch (IOException e) {
                throw new RuntimeException("Cannot read centroid routing model.", e);
            }
        }
        return model;
    }

    private NativeVectorGlobalIndexReader openShard(GlobalIndexIOMeta shard) {
        return new NativeVectorGlobalIndexReader(
                files, Collections.singletonList(shard), fieldType, executor, indexType);
    }

    private int initialWidth(VectorSearch query) {
        String width = query.options().get("centroid.nprobe");
        if ("diskann".equals(indexType) && query.options().containsKey("ivf.nprobe")) {
            throw new IllegalArgumentException(
                    "Use centroid.nprobe for DiskANN outer routing and diskann.l_search for graph search");
        }
        int nlist = routing.nlist();
        if (width != null) {
            int probes = Integer.parseInt(width);
            checkArgument(probes > 0, "centroid.nprobe must be greater than 0");
            return Math.min(probes, nlist);
        }
        // The coarse router orders Euclidean distances. A distant, high-norm shard can
        // contain every maximum inner product, even when nearer shards fill top-K.
        if ("inner_product".equals(metric)) {
            return nlist;
        }
        return automaticWidth(
                nlist,
                vectorCount,
                query.limit(),
                query.includeRowIds() == null ? -1 : query.includeRowIds().getLongCardinality());
    }

    static int automaticWidth(int nlist, long vectorCount, int topK, long matchingCount) {
        checkArgument(vectorCount > 0, "Centroid routing requires a positive vector count.");
        int floor = 8;
        if (nlist == 4) {
            floor = 3;
        } else if (nlist == 8) {
            floor = 4;
        }
        // Probe at least 1/16 of the shards and budget four candidates per requested result.
        long rowsPerShard = divideRoundingUp(vectorCount, nlist);
        long candidateShards = divideRoundingUp(4L * topK, rowsPerShard);
        long minimumShards = Math.max(floor, divideRoundingUp(nlist, 16));
        long width = Math.min(nlist, Math.max(minimumShards, candidateShards));
        if (matchingCount == 0) {
            return 1;
        }
        if (matchingCount < 0 || matchingCount >= vectorCount || width == nlist) {
            return (int) width;
        }
        // Scale by inverse selectivity. The product may exceed a long for large row-ID ranges.
        return BigDecimal.valueOf(vectorCount)
                .multiply(BigDecimal.valueOf(width))
                .divide(BigDecimal.valueOf(matchingCount), 0, RoundingMode.CEILING)
                .min(BigDecimal.valueOf(nlist))
                .intValueExact();
    }

    private static long divideRoundingUp(long count, long divisor) {
        return 1 + (count - 1) / divisor;
    }

    private CompletableFuture<List<int[]>> routes(BatchVectorSearch batch) {
        VectorSearch query = batch.forIndex(0);
        float[][] vectors = batch.vectors();
        // Submitting to the bounded I/O executor acquires a permit. Dispatch subsequent
        // stages asynchronously so they never submit child work while holding that permit.
        return CompletableFuture.supplyAsync(
                () -> {
                    checkArgument(!closed, "Centroid reader is closed.");
                    int width = initialWidth(query);
                    if (automatic(query)
                            && width == routing.nlist()
                            && "inner_product".equals(metric)) {
                        int[] all = new int[width];
                        for (int i = 0; i < width; i++) {
                            all[i] = i;
                        }
                        return Collections.nCopies(vectors.length, all);
                    }
                    if (width == routing.nlist()) {
                        CentroidRouter router = model();
                        int[] all = router.findNearestCentroids(vectors[0], width);
                        // Validate each query against the coarse model before opening any shard.
                        // All queries visit the same files, so only one complete probe order is
                        // needed.
                        for (int i = 1; i < vectors.length; i++) {
                            router.findNearestCentroids(vectors[i], 1);
                        }
                        return Collections.nCopies(vectors.length, all);
                    }
                    List<int[]> centers = new ArrayList<>(vectors.length);
                    for (float[] vector : vectors) {
                        centers.add(model().findNearestCentroids(vector, width));
                    }
                    return centers;
                },
                executor);
    }

    private CompletableFuture<List<Optional<ScoredGlobalIndexResult>>> search(
            List<int[]> centers, BatchVectorSearch batch) {
        VectorSearch query = batch.forIndex(0);
        SearchScope scope = SearchScope.of(batch.includeRowIds(), batch.limit());
        boolean considerJoint =
                shardFilter == null
                        && centers.get(0).length == 8
                        && routing.nlist() == 64
                        && query.limit() >= 10
                        && query.limit() <= 100
                        && query.includeRowIds() == null
                        && automatic(query)
                        && !query.options().containsKey("ivf.nprobe")
                        && !query.options().containsKey("diskann.l_search")
                        && !query.options().containsKey("ivf.max_initial_filter_expansion_factor")
                        && storesLocalNlist(indexType, metric);
        List<Route> plans = new ArrayList<>(centers.size());
        for (int[] nearest : centers) {
            boolean joint = considerJoint;
            for (int i = 0; joint && i < JOINT_OUTER_PROBES; i++) {
                int centroid = nearest[i];
                GlobalIndexIOMeta shard = shards.get(centroid);
                joint =
                        shard != null
                                && shard.rowCount()
                                        >= (long) NativeVectorGlobalIndexReader.JOINT_INNER_PROBES
                                                * query.limit()
                                && jointLayout.contains(centroid);
            }
            plans.add(
                    new Route(
                            joint ? Arrays.copyOf(nearest, JOINT_OUTER_PROBES) : nearest,
                            joint,
                            scope.effectiveK));
        }
        return searchRound(batch, plans, scope);
    }

    private static final class Route {
        int[] centers;
        boolean joint;
        final Set<Integer> visited = new HashSet<>();
        final ShardTopK matches;

        Route(int[] centers, boolean joint, int limit) {
            this.centers = centers;
            this.joint = joint;
            this.matches = new ShardTopK(limit);
        }
    }

    /** Each row belongs to one centroid and each shard is visited once per route. */
    static final class ShardTopK {
        private final int limit;
        private final PriorityQueue<Hit> hits = new PriorityQueue<>();

        ShardTopK(int limit) {
            this.limit = limit;
        }

        synchronized void add(ScoredGlobalIndexResult result) {
            ScoreGetter scores = result.scoreGetter();
            for (long rowId : result.results()) {
                float score = scores.score(rowId);
                if (hits.size() == limit) {
                    if (hits.element().compareTo(score, rowId) >= 0) {
                        continue;
                    }
                    hits.remove();
                }
                hits.add(new Hit(score, rowId));
            }
        }

        int size() {
            return hits.size();
        }

        Optional<ScoredGlobalIndexResult> finish() {
            if (hits.isEmpty()) {
                return Optional.empty();
            }
            RoaringNavigableMap64 rows = new RoaringNavigableMap64();
            Map<Long, Float> scores = new HashMap<>();
            for (Hit hit : hits) {
                rows.add(hit.rowId);
                scores.put(hit.rowId, hit.score);
            }
            hits.clear();
            return Optional.of(ScoredGlobalIndexResult.create(rows, scores::get));
        }
    }

    /** Weakest score first, breaking ties by evicting the largest row ID. */
    private static final class Hit implements Comparable<Hit> {
        final float score;
        final long rowId;

        Hit(float score, long rowId) {
            this.score = score;
            this.rowId = rowId;
        }

        @Override
        public int compareTo(Hit other) {
            return compareTo(other.score, other.rowId);
        }

        int compareTo(float otherScore, long otherRowId) {
            int cmp = Float.compare(score, otherScore);
            return cmp != 0 ? cmp : Long.compare(otherRowId, rowId);
        }
    }

    private boolean automatic(VectorSearch query) {
        return !query.options().containsKey("centroid.nprobe");
    }

    @Override
    public CompletableFuture<Optional<ScoredGlobalIndexResult>> visitVectorSearch(
            VectorSearch query) {
        BatchVectorSearch batch =
                new BatchVectorSearch(
                                new float[][] {query.vector()},
                                query.limit(),
                                query.fieldName(),
                                query.options())
                        .withIncludeRowIds(query.includeRowIds());
        return visitBatchVectorSearch(batch).thenApply(results -> results.get(0));
    }

    @Override
    public CompletableFuture<List<Optional<ScoredGlobalIndexResult>>> visitBatchVectorSearch(
            BatchVectorSearch batch) {
        int count = batch.vectorCount();
        checkArgument(count > 0, "Batch vector count must be positive.");
        if (vectorCount == 0
                || (batch.includeRowIds() != null && batch.includeRowIds().isEmpty())) {
            return CompletableFuture.completedFuture(Collections.nCopies(count, Optional.empty()));
        }
        return routes(batch).thenComposeAsync(centers -> search(centers, batch));
    }

    private CompletableFuture<List<Optional<ScoredGlobalIndexResult>>> searchRound(
            BatchVectorSearch batch, List<Route> plans, SearchScope scope) {
        Map<GlobalIndexIOMeta, Map<Boolean, List<Integer>>> groups = shardsForRound(plans);
        List<CompletableFuture<Void>> pending = new ArrayList<>(groups.size());
        groups.forEach(
                (file, policies) ->
                        pending.add(
                                CompletableFuture.runAsync(
                                        () -> searchShard(file, policies, batch, plans, scope),
                                        executor)));
        return CompletableFuture.allOf(pending.toArray(new CompletableFuture[0]))
                .thenComposeAsync(done -> finishRound(batch, plans, scope));
    }

    private void searchShard(
            GlobalIndexIOMeta file,
            Map<Boolean, List<Integer>> policies,
            BatchVectorSearch batch,
            List<Route> plans,
            SearchScope scope) {
        try {
            if (shardFilter != null) {
                RoaringNavigableMap64 rows =
                        shardFilter.apply(((CentroidIndexFile) file).centroid());
                if (batch.includeRowIds() != null) {
                    rows.and(batch.includeRowIds());
                }
                if (rows.isEmpty()) {
                    return;
                }
                scope = SearchScope.of(rows, batch.limit());
            }
            try (NativeVectorGlobalIndexReader reader = openShard(file)) {
                // A mixed batch may select this shard with both probe policies.
                for (Map.Entry<Boolean, List<Integer>> policy : policies.entrySet()) {
                    searchSubset(
                            reader,
                            batch,
                            scope,
                            policy.getValue(),
                            policy.getKey(),
                            (index, result) -> plans.get(index).matches.add(result));
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Map<GlobalIndexIOMeta, Map<Boolean, List<Integer>>> shardsForRound(List<Route> plans) {
        Map<GlobalIndexIOMeta, Map<Boolean, List<Integer>>> groups = new LinkedHashMap<>();
        if (plans.stream()
                .allMatch(
                        route ->
                                route.centers.length == routing.nlist()
                                        && route.visited.isEmpty())) {
            // Share the full batch across all shards instead of building a query/shard cross
            // product.
            List<Integer> indices = new ArrayList<>(plans.size());
            for (int i = 0; i < plans.size(); i++) {
                indices.add(i);
            }
            Map<Boolean, List<Integer>> policy = Collections.singletonMap(false, indices);
            for (int centroid : plans.get(0).centers) {
                GlobalIndexIOMeta file = shards.get(centroid);
                if (file != null) {
                    groups.put(file, policy);
                }
            }
            for (Route plan : plans) {
                plan.centers = NO_CENTERS;
            }
            return groups;
        }
        for (int i = 0; i < plans.size(); i++) {
            Route route = plans.get(i);
            for (int centroid : route.centers) {
                if (route.visited.add(centroid)) {
                    GlobalIndexIOMeta file = shards.get(centroid);
                    if (file != null) {
                        groups.computeIfAbsent(file, ignored -> new LinkedHashMap<>())
                                .computeIfAbsent(route.joint, ignored -> new ArrayList<>())
                                .add(i);
                    }
                }
            }
        }
        return groups;
    }

    private static void searchSubset(
            NativeVectorGlobalIndexReader reader,
            BatchVectorSearch batch,
            SearchScope scope,
            List<Integer> indices,
            boolean joint,
            BiConsumer<Integer, ScoredGlobalIndexResult> consumer) {
        if (indices.isEmpty()) {
            return;
        }
        BatchVectorSearch subset = batch;
        if (indices.size() != batch.vectorCount()) {
            float[][] queries = new float[indices.size()][];
            for (int i = 0; i < queries.length; i++) {
                queries[i] = batch.vectors()[indices.get(i)];
            }
            subset =
                    new BatchVectorSearch(
                            queries, batch.limit(), batch.fieldName(), batch.options());
        }
        List<Optional<ScoredGlobalIndexResult>> found =
                reader.searchShardBatch(subset, joint, scope);
        for (int i = 0; i < found.size(); i++) {
            Optional<ScoredGlobalIndexResult> result = found.get(i);
            if (result.isPresent()) {
                consumer.accept(indices.get(i), result.get());
            }
        }
    }

    private CompletableFuture<List<Optional<ScoredGlobalIndexResult>>> finishRound(
            BatchVectorSearch batch, List<Route> plans, SearchScope scope) {
        long required = Math.min(vectorCount, scope.effectiveK);
        boolean widen = false;
        for (int i = 0; i < plans.size(); i++) {
            Route route = plans.get(i);
            if (route.centers.length > 0) {
                int visited = route.visited.size();
                if (!batch.options().containsKey("centroid.nprobe")
                        && visited < routing.nlist()
                        && route.matches.size() < required) {
                    int width = (int) Math.min(routing.nlist(), (long) visited * 2);
                    route.centers = model().findNearestCentroids(batch.vectors()[i], width);
                    route.joint = false;
                    widen = true;
                } else {
                    route.centers = NO_CENTERS;
                }
            }
        }
        if (widen) {
            return searchRound(batch, plans, scope);
        }
        List<Optional<ScoredGlobalIndexResult>> output = new ArrayList<>(plans.size());
        for (Route route : plans) {
            output.add(route.matches.finish());
        }
        return CompletableFuture.completedFuture(output);
    }

    @Override
    public synchronized void close() {
        closed = true;
        if (model != null) {
            model.close();
            model = null;
        }
    }
}
