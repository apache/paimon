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
import org.apache.paimon.utils.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Routes queries to shard files using row IDs relative to their shared range. */
final class NativeCentroidRoutedIndexReader extends AbstractVectorGlobalIndexReader {
    private static final int JOINT_OUTER_PROBES = 4;

    private final GlobalIndexFileReader files;
    private final GlobalIndexIOMeta routing;
    private final VectorIndexShardMeta metadata;
    private final Map<Integer, GlobalIndexIOMeta> shards = new HashMap<>();
    private final Map<Integer, NativeVectorGlobalIndexReader> readers = new HashMap<>();
    private final DataType fieldType;
    private final ExecutorService executor;
    private final long vectorCount;
    private final String indexType;
    private CentroidRouter model;
    private boolean closed;

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
        this.vectorCount = count;
    }

    private synchronized CentroidRouter model() {
        checkArgument(!closed, "Centroid reader is closed.");
        if (model == null) {
            try (SeekableInputStream in = files.getInputStream(routing)) {
                CentroidRouter loaded =
                        CentroidRouter.deserialize(
                                VectorTrainingModelIO.read(
                                        in,
                                        routing.fileSize(),
                                        metadata.modelId(),
                                        CentroidRouter.maxSerializedBytes()));
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

    private synchronized NativeVectorGlobalIndexReader reader(int centroid) {
        checkArgument(!closed, "Centroid reader is closed.");
        return readers.computeIfAbsent(
                centroid,
                c ->
                        new NativeVectorGlobalIndexReader(
                                files,
                                Collections.singletonList(
                                        checkNotNull(
                                                shards.get(c),
                                                "Missing routed centroid shard %s.",
                                                c)),
                                fieldType,
                                executor,
                                indexType));
    }

    private int initialWidth(VectorSearch query) {
        String width = query.options().get("centroid.nprobe");
        if ("diskann".equals(indexType) && query.options().containsKey("ivf.nprobe")) {
            throw new IllegalArgumentException(
                    "Use centroid.nprobe for DiskANN outer routing and diskann.l_search for graph search");
        }
        org.apache.paimon.index.vector.VectorSearchParams params =
                width == null
                        ? org.apache.paimon.index.vector.VectorSearchParams.automatic(query.limit())
                        : org.apache.paimon.index.vector.VectorSearchParams.ivf(
                                query.limit(), Integer.parseInt(width));
        return model().resolveNprobe(
                        params,
                        vectorCount,
                        query.includeRowIds() == null
                                ? -1
                                : query.includeRowIds().getLongCardinality());
    }

    private CompletableFuture<List<Route>> routes(float[][] vectors, VectorSearch query) {
        return CompletableFuture.supplyAsync(
                        () -> {
                            int width = initialWidth(query);
                            List<int[]> centers = new ArrayList<>(vectors.length);
                            for (float[] vector : vectors) {
                                centers.add(model().findNearestCentroids(vector, width));
                            }
                            return centers;
                        },
                        executor)
                .thenCompose(centers -> planShardSearches(centers, query));
    }

    private CompletableFuture<List<Route>> planShardSearches(
            List<int[]> centers, VectorSearch query) {
        // For unfiltered 64-by-64 L2 indexes, searching 16 lists in four nearby shards
        // improved recall at comparable I/O on SIFT and GloVe for K=10..100. Verify the local
        // geometry on files needed by either plan; queries in a batch share their header reads.
        Map<Integer, CompletableFuture<Boolean>> compatible = new HashMap<>();
        if (centers.get(0).length == 8
                && metadata.nlist() == 64
                && query.limit() >= 10
                && query.limit() <= 100
                && query.includeRowIds() == null
                && automatic(query)
                && !query.options().containsKey("ivf.nprobe")
                && !query.options().containsKey("diskann.l_search")
                && !query.options().containsKey("ivf.max_initial_filter_expansion_factor")
                && ("ivf-flat".equals(indexType) || "ivf-pq".equals(indexType))) {
            for (int[] nearest : centers) {
                for (int i = 0; i < JOINT_OUTER_PROBES; i++) {
                    compatible.computeIfAbsent(
                            nearest[i],
                            c ->
                                    CompletableFuture.supplyAsync(
                                            () -> reader(c).supportsJointProbes(query.limit()),
                                            executor));
                }
            }
        }
        return CompletableFuture.allOf(compatible.values().toArray(new CompletableFuture[0]))
                .thenApply(
                        ignored -> {
                            List<Route> plans = new ArrayList<>(centers.size());
                            for (int[] nearest : centers) {
                                boolean joint = !compatible.isEmpty();
                                for (int i = 0; joint && i < JOINT_OUTER_PROBES; i++) {
                                    joint = compatible.get(nearest[i]).join();
                                }
                                plans.add(
                                        new Route(
                                                joint
                                                        ? Arrays.copyOf(nearest, JOINT_OUTER_PROBES)
                                                        : nearest,
                                                joint));
                            }
                            return plans;
                        });
    }

    private static final class Route {
        final int[] centers;
        final boolean joint;

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
        if (query.includeRowIds() != null && query.includeRowIds().isEmpty()) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        return routes(new float[][] {query.vector()}, query)
                .thenCompose(
                        plans ->
                                search(
                                        query,
                                        plans.get(0),
                                        Collections.emptyList(),
                                        Collections.emptyList()));
    }

    private CompletableFuture<Optional<ScoredGlobalIndexResult>> search(
            VectorSearch query,
            Route route,
            List<Integer> visited,
            List<ScoredGlobalIndexResult> previous) {
        int width = route.centers.length;
        List<Integer> nextVisited = new ArrayList<>(visited);
        List<CompletableFuture<Optional<ScoredGlobalIndexResult>>> pending = new ArrayList<>();
        for (int centroid : route.centers) {
            if (!visited.contains(centroid)) {
                nextVisited.add(centroid);
                pending.add(reader(centroid).visitVectorSearch(query, route.joint));
            }
        }
        return CompletableFuture.allOf(pending.toArray(new CompletableFuture[0]))
                .thenCompose(
                        ignored -> {
                            List<ScoredGlobalIndexResult> results = new ArrayList<>(previous);
                            for (CompletableFuture<Optional<ScoredGlobalIndexResult>> future :
                                    pending) {
                                future.join().ifPresent(results::add);
                            }
                            ScoredGlobalIndexResult merged =
                                    ScoredGlobalIndexResult.merge(results).topK(query.limit());
                            if (automatic(query)
                                    && merged.results().getLongCardinality() < query.limit()
                                    && width < metadata.nlist()) {
                                int nextWidth = (int) Math.min(metadata.nlist(), (long) width * 2);
                                int[] nearest =
                                        model().findNearestCentroids(query.vector(), nextWidth);
                                return search(
                                        query, new Route(nearest, false), nextVisited, results);
                            }
                            return CompletableFuture.completedFuture(
                                    merged.results().isEmpty()
                                            ? Optional.empty()
                                            : Optional.of(merged));
                        });
    }

    @Override
    public CompletableFuture<List<Optional<ScoredGlobalIndexResult>>> visitBatchVectorSearch(
            BatchVectorSearch batch) {
        int count = batch.vectorCount();
        checkArgument(count > 0, "Batch vector count must be positive.");
        if (batch.includeRowIds() != null && batch.includeRowIds().isEmpty()) {
            return CompletableFuture.completedFuture(Collections.nCopies(count, Optional.empty()));
        }
        return routes(batch.vectors(), batch.forIndex(0))
                .thenApply(
                        plans -> {
                            Map<Pair<Integer, Boolean>, List<Integer>> groups =
                                    new LinkedHashMap<>();
                            for (int i = 0; i < plans.size(); i++) {
                                Route route = plans.get(i);
                                for (int centroid : route.centers) {
                                    groups.computeIfAbsent(
                                                    Pair.of(centroid, route.joint),
                                                    c -> new ArrayList<>())
                                            .add(i);
                                }
                            }
                            return groups;
                        })
                .thenCompose(
                        groups -> {
                            List<List<ScoredGlobalIndexResult>> results = new ArrayList<>();
                            for (int i = 0; i < count; i++) {
                                results.add(new ArrayList<>());
                            }
                            List<CompletableFuture<Void>> pending = new ArrayList<>();
                            for (Map.Entry<Pair<Integer, Boolean>, List<Integer>> group :
                                    groups.entrySet()) {
                                float[][] queries = new float[group.getValue().size()][];
                                for (int i = 0; i < queries.length; i++) {
                                    queries[i] = batch.vectors()[group.getValue().get(i)];
                                }
                                BatchVectorSearch subset =
                                        new BatchVectorSearch(
                                                        queries,
                                                        batch.limit(),
                                                        batch.fieldName(),
                                                        batch.options())
                                                .withIncludeRowIds(batch.includeRowIds());
                                pending.add(
                                        reader(group.getKey().getLeft())
                                                .visitBatchVectorSearch(
                                                        subset, group.getKey().getRight())
                                                .thenAccept(
                                                        found -> {
                                                            synchronized (results) {
                                                                for (int i = 0;
                                                                        i < found.size();
                                                                        i++) {
                                                                    final int inputIndex =
                                                                            group.getValue().get(i);
                                                                    found.get(i)
                                                                            .ifPresent(
                                                                                    value ->
                                                                                            results.get(
                                                                                                            inputIndex)
                                                                                                    .add(
                                                                                                            value));
                                                                }
                                                            }
                                                        }));
                            }
                            return CompletableFuture.allOf(
                                            pending.toArray(new CompletableFuture[0]))
                                    .thenCompose(
                                            ignored -> {
                                                List<
                                                                CompletableFuture<
                                                                        Optional<
                                                                                ScoredGlobalIndexResult>>>
                                                        merged = new ArrayList<>();
                                                for (int i = 0; i < count; i++) {
                                                    VectorSearch query = batch.forIndex(i);
                                                    ScoredGlobalIndexResult result =
                                                            ScoredGlobalIndexResult.merge(
                                                                            results.get(i))
                                                                    .topK(batch.limit());
                                                    if (automatic(query)
                                                            && result.results().getLongCardinality()
                                                                    < batch.limit()) {
                                                        merged.add(visitVectorSearch(query));
                                                    } else {
                                                        merged.add(
                                                                CompletableFuture.completedFuture(
                                                                        result.results().isEmpty()
                                                                                ? Optional.empty()
                                                                                : Optional.of(
                                                                                        result)));
                                                    }
                                                }
                                                return CompletableFuture.allOf(
                                                                merged.toArray(
                                                                        new CompletableFuture[0]))
                                                        .thenApply(
                                                                done -> {
                                                                    List<
                                                                                    Optional<
                                                                                            ScoredGlobalIndexResult>>
                                                                            output =
                                                                                    new ArrayList<>();
                                                                    for (CompletableFuture<
                                                                                    Optional<
                                                                                            ScoredGlobalIndexResult>>
                                                                            result : merged) {
                                                                        output.add(result.join());
                                                                    }
                                                                    return output;
                                                                });
                                            });
                        });
    }

    @Override
    public synchronized void close() throws IOException {
        closed = true;
        IOException failure = null;
        for (NativeVectorGlobalIndexReader reader : readers.values()) {
            try {
                reader.close();
            } catch (IOException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (model != null) {
            model.close();
            model = null;
        }
        readers.clear();
        if (failure != null) {
            throw failure;
        }
    }
}
