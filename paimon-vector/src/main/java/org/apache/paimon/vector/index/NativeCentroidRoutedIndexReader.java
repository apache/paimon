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
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.index.vector.CentroidRouter;
import org.apache.paimon.predicate.BatchVectorSearch;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.IOFunction;
import org.apache.paimon.vector.index.NativeVectorGlobalIndexReader.SearchScope;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Routes queries to shard files using row IDs relative to their shared range. */
final class NativeCentroidRoutedIndexReader extends AbstractVectorGlobalIndexReader {
    private static final int JOINT_OUTER_PROBES = 4;

    private final GlobalIndexFileReader files;
    private final GlobalIndexIOMeta routing;
    private String metric;
    private final VectorIndexShardMeta metadata;
    private final Map<Integer, GlobalIndexIOMeta> shards = new HashMap<>();
    // Populated for shard descriptors that record their native geometry.
    private final Map<Integer, Boolean> jointLayout = new HashMap<>();
    private final DataType fieldType;
    private final ExecutorService executor;
    private final long vectorCount;
    private final String indexType;
    private CentroidRouter model;
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
        GlobalIndexIOMeta routing = null;
        VectorIndexShardMeta modelMeta = null;
        long count = 0;
        for (GlobalIndexIOMeta file : entries) {
            VectorIndexShardMeta meta = VectorIndexShardMeta.deserialize(file.metadata());
            if (meta.routingModel()) {
                modelMeta = meta;
                checkArgument(routing == null, "Multiple routing models.");
                routing = file;
            } else {
                checkArgument(
                        file.rowCount() >= 0, "Centroid shards must record their vector count.");
                Integer localNlist = meta.localNlist();
                if (localNlist != null) {
                    jointLayout.put(
                            meta.centroid(), localNlist == 64 && "l2".equals(meta.metric()));
                }
                shards.put(
                        meta.centroid(),
                        new GlobalIndexIOMeta(
                                file.filePath(),
                                file.fileSize(),
                                file.rowCount(),
                                meta.payloadMeta()));
                count = Math.addExact(count, file.rowCount());
            }
        }
        checkArgument(routing != null, "Missing routing model.");
        this.routing = routing;
        this.metadata = modelMeta;
        this.metric = modelMeta.metric();
        this.vectorCount = count;
    }

    private synchronized CentroidRouter model() {
        checkArgument(!closed, "Centroid reader is closed.");
        if (model == null) {
            try (SeekableInputStream in = files.getInputStream(routing)) {
                byte[] payload =
                        VectorTrainingModelIO.read(
                                in,
                                routing.fileSize(),
                                metadata.modelId(),
                                CentroidRouter.maxSerializedBytes());
                if (metric == null) {
                    metric = VectorTrainingModelIO.legacyMetric(payload);
                }
                CentroidRouter loaded = CentroidRouter.deserialize(payload);
                if (loaded.nlist() != metadata.nlist()) {
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

    private GlobalIndexIOMeta shard(int centroid) {
        return checkNotNull(shards.get(centroid), "Missing routed centroid shard %s.", centroid);
    }

    private <T> CompletableFuture<T> readShard(
            GlobalIndexIOMeta shard, IOFunction<NativeVectorGlobalIndexReader, T> operation) {
        return CompletableFuture.supplyAsync(
                () -> {
                    try (NativeVectorGlobalIndexReader reader =
                            new NativeVectorGlobalIndexReader(
                                    files,
                                    Collections.singletonList(shard),
                                    fieldType,
                                    executor,
                                    indexType)) {
                        return operation.apply(reader);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                },
                executor);
    }

    private synchronized String metric() {
        if (metric == null) {
            model();
        }
        return metric;
    }

    private int initialWidth(VectorSearch query) {
        String width = query.options().get("centroid.nprobe");
        if ("diskann".equals(indexType) && query.options().containsKey("ivf.nprobe")) {
            throw new IllegalArgumentException(
                    "Use centroid.nprobe for DiskANN outer routing and diskann.l_search for graph search");
        }
        int nlist = metadata.nlist();
        if (width != null) {
            int probes = Integer.parseInt(width);
            checkArgument(probes > 0, "centroid.nprobe must be greater than 0");
            return Math.min(probes, nlist);
        }
        // The coarse router orders Euclidean distances. A distant, high-norm shard can
        // contain every maximum inner product, even when nearer shards fill top-K.
        if ("inner_product".equals(metric())) {
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
                            && width == metadata.nlist()
                            && "inner_product".equals(metric())) {
                        int[] all = new int[width];
                        for (int i = 0; i < width; i++) {
                            all[i] = i;
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
                centers.get(0).length == 8
                        && metadata.nlist() == 64
                        && query.limit() >= 10
                        && query.limit() <= 100
                        && query.includeRowIds() == null
                        && automatic(query)
                        && "l2".equals(metric())
                        && !query.options().containsKey("ivf.nprobe")
                        && !query.options().containsKey("diskann.l_search")
                        && !query.options().containsKey("ivf.max_initial_filter_expansion_factor")
                        && ("ivf-flat".equals(indexType) || "ivf-pq".equals(indexType));
        List<Route> plans = new ArrayList<>(centers.size());
        Map<Integer, CompletableFuture<LegacyProbe>> legacy = new HashMap<>();
        for (int[] nearest : centers) {
            boolean joint = considerJoint;
            for (int i = 0; joint && i < JOINT_OUTER_PROBES; i++) {
                int centroid = nearest[i];
                joint =
                        shard(centroid).rowCount()
                                        >= (long) NativeVectorGlobalIndexReader.JOINT_INNER_PROBES
                                                * query.limit()
                                && !Boolean.FALSE.equals(jointLayout.get(centroid));
            }
            plans.add(new Route(nearest, joint));
            if (joint) {
                for (int i = 0; i < JOINT_OUTER_PROBES; i++) {
                    if (!jointLayout.containsKey(nearest[i])) {
                        legacy.put(nearest[i], null);
                    }
                }
            }
        }
        if (legacy.isEmpty()) {
            for (Route plan : plans) {
                if (plan.joint) {
                    plan.centers = Arrays.copyOf(plan.centers, JOINT_OUTER_PROBES);
                }
            }
            return searchRound(batch, plans, scope);
        }
        legacy.replaceAll(
                (centroid, ignored) ->
                        readShard(
                                shard(centroid),
                                reader -> probeLegacyShard(reader, centroid, batch, plans, scope)));
        return CompletableFuture.allOf(legacy.values().toArray(new CompletableFuture[0]))
                .thenComposeAsync(
                        done -> {
                            for (int i = 0; i < plans.size(); i++) {
                                Route route = plans.get(i);
                                for (int j = 0; route.joint && j < JOINT_OUTER_PROBES; j++) {
                                    CompletableFuture<LegacyProbe> probe =
                                            legacy.get(route.centers[j]);
                                    route.joint = probe == null || probe.join().compatible;
                                }
                                if (route.joint) {
                                    route.centers =
                                            Arrays.copyOf(route.centers, JOINT_OUTER_PROBES);
                                }
                                for (int c : route.centers) {
                                    CompletableFuture<LegacyProbe> probe = legacy.get(c);
                                    if (probe != null) {
                                        route.visited.add(c);
                                        (route.joint ? probe.join().joint : probe.join().ordinary)
                                                .get(i)
                                                .ifPresent(route.matches::add);
                                    }
                                }
                            }
                            return searchRound(batch, plans, scope);
                        });
    }

    // Old shard descriptors lack local layout information. The other three files determine
    // which width this query needs. Keep candidate results for both possible plans while this
    // shard is open, then release the reader before waiting for any other shard.
    private static LegacyProbe probeLegacyShard(
            NativeVectorGlobalIndexReader reader,
            int centroid,
            BatchVectorSearch batch,
            List<Route> plans,
            SearchScope scope)
            throws IOException {
        LegacyProbe probe = new LegacyProbe(reader.hasJointLayout());
        List<Integer> ordinary = new ArrayList<>();
        List<Integer> joint = new ArrayList<>();
        for (int i = 0; i < plans.size(); i++) {
            Route route = plans.get(i);
            for (int j = 0; j < route.centers.length; j++) {
                if (route.centers[j] == centroid) {
                    ordinary.add(i);
                    if (probe.compatible && route.joint && j < JOINT_OUTER_PROBES) {
                        joint.add(i);
                    }
                    break;
                }
            }
        }
        searchSubset(reader, batch, scope, ordinary, false, probe.ordinary::put);
        searchSubset(reader, batch, scope, joint, true, probe.joint::put);
        return probe;
    }

    private static final class LegacyProbe {
        final boolean compatible;
        final Map<Integer, Optional<ScoredGlobalIndexResult>> ordinary = new HashMap<>();
        final Map<Integer, Optional<ScoredGlobalIndexResult>> joint = new HashMap<>();

        LegacyProbe(boolean compatible) {
            this.compatible = compatible;
        }
    }

    private static final class Route {
        int[] centers;
        boolean joint;
        final Set<Integer> visited = new HashSet<>();
        final List<ScoredGlobalIndexResult> matches = new ArrayList<>();

        Route(int[] centers, boolean joint) {
            this.centers = centers;
            this.joint = joint;
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
        Map<GlobalIndexIOMeta, Map<Boolean, List<Integer>>> groups = new LinkedHashMap<>();
        for (int i = 0; i < plans.size(); i++) {
            Route route = plans.get(i);
            for (int centroid : route.centers) {
                if (route.visited.add(centroid)) {
                    GlobalIndexIOMeta file = shard(centroid);
                    if (file.rowCount() > 0) {
                        groups.computeIfAbsent(file, ignored -> new LinkedHashMap<>())
                                .computeIfAbsent(route.joint, ignored -> new ArrayList<>())
                                .add(i);
                    }
                }
            }
        }
        List<CompletableFuture<Void>> pending = new ArrayList<>(groups.size());
        groups.forEach(
                (file, policies) ->
                        pending.add(
                                readShard(
                                        file,
                                        reader -> {
                                            // A mixed batch may select this shard with both probe
                                            // policies.
                                            for (Map.Entry<Boolean, List<Integer>> policy :
                                                    policies.entrySet()) {
                                                List<Integer> indices = policy.getValue();
                                                searchSubset(
                                                        reader,
                                                        batch,
                                                        scope,
                                                        indices,
                                                        policy.getKey(),
                                                        (index, result) -> {
                                                            List<ScoredGlobalIndexResult> matches =
                                                                    plans.get(index).matches;
                                                            synchronized (matches) {
                                                                result.ifPresent(matches::add);
                                                            }
                                                        });
                                            }
                                            return null;
                                        })));
        return CompletableFuture.allOf(pending.toArray(new CompletableFuture[0]))
                .thenComposeAsync(done -> finishRound(batch, plans, scope));
    }

    private static void searchSubset(
            NativeVectorGlobalIndexReader reader,
            BatchVectorSearch batch,
            SearchScope scope,
            List<Integer> indices,
            boolean joint,
            BiConsumer<Integer, Optional<ScoredGlobalIndexResult>> consumer) {
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
            consumer.accept(indices.get(i), found.get(i));
        }
    }

    private CompletableFuture<List<Optional<ScoredGlobalIndexResult>>> finishRound(
            BatchVectorSearch batch, List<Route> plans, SearchScope scope) {
        long required = Math.min(vectorCount, scope.effectiveK);
        boolean widen = false;
        List<Optional<ScoredGlobalIndexResult>> output = new ArrayList<>(plans.size());
        for (int i = 0; i < plans.size(); i++) {
            Route route = plans.get(i);
            if (route.centers.length > 0) {
                ScoredGlobalIndexResult merged =
                        ScoredGlobalIndexResult.merge(route.matches).topK(batch.limit());
                route.matches.clear();
                if (!merged.results().isEmpty()) {
                    route.matches.add(merged);
                }
                int visited = route.visited.size();
                if (!batch.options().containsKey("centroid.nprobe")
                        && visited < metadata.nlist()
                        && merged.results().getLongCardinality() < required) {
                    int width = (int) Math.min(metadata.nlist(), (long) visited * 2);
                    route.centers = model().findNearestCentroids(batch.vectors()[i], width);
                    route.joint = false;
                    widen = true;
                } else {
                    route.centers = new int[0];
                }
            }
            output.add(
                    route.matches.isEmpty() ? Optional.empty() : Optional.of(route.matches.get(0)));
        }
        return widen ? searchRound(batch, plans, scope) : CompletableFuture.completedFuture(output);
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
