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

import java.io.IOException;
import java.util.ArrayList;
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

/** Routes before opening shard files; all filters and results use absolute Paimon row IDs. */
final class NativeCentroidRoutedIndexReader extends AbstractVectorGlobalIndexReader {
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

    private boolean automatic(VectorSearch query) {
        return !query.options().containsKey("centroid.nprobe");
    }

    @Override
    public CompletableFuture<Optional<ScoredGlobalIndexResult>> visitVectorSearch(
            VectorSearch query) {
        if (query.includeRowIds() != null && query.includeRowIds().isEmpty()) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        return CompletableFuture.supplyAsync(() -> initialWidth(query), executor)
                .thenCompose(
                        width ->
                                search(
                                        query,
                                        width,
                                        Collections.emptyList(),
                                        Collections.emptyList()));
    }

    private CompletableFuture<Optional<ScoredGlobalIndexResult>> search(
            VectorSearch query,
            int width,
            List<Integer> visited,
            List<ScoredGlobalIndexResult> previous) {
        int[] selected = model().findNearestCentroids(query.vector(), width);
        List<Integer> nextVisited = new ArrayList<>(visited);
        List<CompletableFuture<Optional<ScoredGlobalIndexResult>>> pending = new ArrayList<>();
        for (int centroid : selected) {
            if (!visited.contains(centroid)) {
                nextVisited.add(centroid);
                pending.add(reader(centroid).visitVectorSearch(query));
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
                                return search(
                                        query,
                                        (int) Math.min(metadata.nlist(), (long) width * 2),
                                        nextVisited,
                                        results);
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
        return CompletableFuture.supplyAsync(
                        () -> {
                            Map<Integer, List<Integer>> groups = new LinkedHashMap<>();
                            for (int i = 0; i < count; i++) {
                                VectorSearch query = batch.forIndex(i);
                                for (int centroid :
                                        model().findNearestCentroids(
                                                        query.vector(), initialWidth(query))) {
                                    groups.computeIfAbsent(centroid, c -> new ArrayList<>()).add(i);
                                }
                            }
                            return groups;
                        },
                        executor)
                .thenCompose(
                        groups -> {
                            List<List<ScoredGlobalIndexResult>> results = new ArrayList<>();
                            for (int i = 0; i < count; i++) {
                                results.add(new ArrayList<>());
                            }
                            List<CompletableFuture<Void>> pending = new ArrayList<>();
                            for (Map.Entry<Integer, List<Integer>> group : groups.entrySet()) {
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
                                        reader(group.getKey())
                                                .visitBatchVectorSearch(subset)
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
