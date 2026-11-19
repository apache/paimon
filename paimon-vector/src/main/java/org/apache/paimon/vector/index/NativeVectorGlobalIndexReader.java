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

import org.apache.paimon.fs.FileRange;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.VectoredReadUtils;
import org.apache.paimon.fs.VectoredReadable;
import org.apache.paimon.globalindex.AbstractVectorGlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.index.vector.VectorIndexInput;
import org.apache.paimon.index.vector.VectorIndexMetadata;
import org.apache.paimon.index.vector.VectorIndexReader;
import org.apache.paimon.index.vector.VectorSearchBatchResult;
import org.apache.paimon.index.vector.VectorSearchParams;
import org.apache.paimon.index.vector.VectorSearchResult;
import org.apache.paimon.predicate.BatchVectorSearch;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.VectorType;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.RoaringNavigableMap64;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Vector global index reader using paimon-vector-index-java.
 *
 * <p>Each shard has exactly one vector index file. The reader lazily opens the index and performs
 * vector similarity search.
 */
public class NativeVectorGlobalIndexReader extends AbstractVectorGlobalIndexReader {

    static final int JOINT_INNER_PROBES = 16;
    private static final String NPROBE_PARAMETER = "ivf.nprobe";
    private static final String L_SEARCH_PARAMETER = "diskann.l_search";
    private static final String MAX_INITIAL_FILTER_EXPANSION_FACTOR_PARAMETER =
            "ivf.max_initial_filter_expansion_factor";
    private static final String IVF_PQ_BATCH_TABLE_REUSE_PARAMETER = "ivf_pq.batch_table_reuse";
    private static final String IVF_PQ_BATCH_TABLE_REUSE_MAX_BYTES_PARAMETER =
            "ivf_pq.batch_table_reuse.max_bytes";
    private static final int VECTOR_INDEX_MIN_SEEK_FOR_VECTOR_READS = 16 * 1024;
    private static final int VECTOR_INDEX_PARALLELISM_FOR_VECTOR_READS = 32;

    private final GlobalIndexIOMeta ioMeta;
    private final GlobalIndexFileReader fileReader;
    private final DataType fieldType;
    private final ExecutorService executor;
    private final String expectedIndexType;

    private volatile VectorIndexMetadata nativeMeta;
    private volatile VectorIndexReader vectorReader;
    private SeekableInputStream openStream;

    public NativeVectorGlobalIndexReader(
            GlobalIndexFileReader fileReader,
            List<GlobalIndexIOMeta> ioMetas,
            DataType fieldType,
            ExecutorService executor) {
        this(fileReader, ioMetas, fieldType, executor, null);
    }

    NativeVectorGlobalIndexReader(
            GlobalIndexFileReader fileReader,
            List<GlobalIndexIOMeta> ioMetas,
            DataType fieldType,
            ExecutorService executor,
            String expectedIndexType) {
        checkArgument(ioMetas.size() == 1, "Expected exactly one index file per shard");
        this.executor = executor;
        this.fileReader = fileReader;
        this.ioMeta = ioMetas.get(0);
        this.fieldType = fieldType;
        this.expectedIndexType = expectedIndexType;
    }

    boolean hasJointLayout() throws IOException {
        ensureLoaded();
        return nativeMeta.nlist() == 64 && "l2".equals(nativeMeta.metric());
    }

    @Override
    public CompletableFuture<Optional<ScoredGlobalIndexResult>> visitVectorSearch(
            VectorSearch vectorSearch) {
        if (ioMeta.rowCount() == 0) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        return CompletableFuture.supplyAsync(() -> searchShard(vectorSearch), executor);
    }

    private Optional<ScoredGlobalIndexResult> searchShard(VectorSearch query) {
        try {
            ensureLoaded();
            return Optional.ofNullable(
                    search(query, false, SearchScope.of(query.includeRowIds(), query.limit())));
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format(
                            "Failed vector index search: field=%s, limit=%d",
                            query.fieldName(), query.limit()),
                    e);
        }
    }

    @Override
    public CompletableFuture<List<Optional<ScoredGlobalIndexResult>>> visitBatchVectorSearch(
            BatchVectorSearch batch) {
        if (ioMeta.rowCount() == 0) {
            return CompletableFuture.completedFuture(
                    Collections.nCopies(batch.vectorCount(), Optional.empty()));
        }
        return CompletableFuture.supplyAsync(
                () ->
                        searchShardBatch(
                                batch, false, SearchScope.of(batch.includeRowIds(), batch.limit())),
                executor);
    }

    List<Optional<ScoredGlobalIndexResult>> searchShardBatch(
            BatchVectorSearch batch, boolean joint, SearchScope scope) {
        if (ioMeta.rowCount() == 0) {
            return Collections.nCopies(batch.vectorCount(), Optional.empty());
        }
        try {
            ensureLoaded();
            return searchBatch(batch, joint, scope);
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format(
                            "Failed batch vector index search: field=%s, limit=%d, vectorCount=%d",
                            batch.fieldName(), batch.limit(), batch.vectorCount()),
                    e);
        }
    }

    private List<Optional<ScoredGlobalIndexResult>> searchBatch(
            BatchVectorSearch batchVectorSearch, boolean joint, SearchScope scope)
            throws IOException {
        int n = batchVectorSearch.vectorCount();
        // Single vector: reuse the scalar path; no batching benefit.
        if (n == 1) {
            List<Optional<ScoredGlobalIndexResult>> results = new ArrayList<>(1);
            results.add(Optional.ofNullable(search(batchVectorSearch.forIndex(0), joint, scope)));
            return results;
        }

        float[][] vectors = batchVectorSearch.vectors();
        for (float[] vector : vectors) {
            validateSearchVector(vector);
        }
        int dim = nativeMeta.dimension();
        String metric = nativeMeta.metric();

        if (scope.effectiveK == 0) {
            return emptyResults(n);
        }
        VectorSearchParams searchParams =
                batchSearchParams(
                        shardOptions(batchVectorSearch.options(), joint), scope.effectiveK);

        // Flatten query vectors into one contiguous array for a single native call.
        float[] queries = new float[n * dim];
        for (int i = 0; i < n; i++) {
            System.arraycopy(vectors[i], 0, queries, i * dim, dim);
        }

        VectorSearchBatchResult batchResult =
                scope.filterBytes != null
                        ? vectorReader.searchBatch(queries, n, searchParams, scope.filterBytes)
                        : vectorReader.searchBatch(queries, n, searchParams);

        // result i corresponds to vectors[i], matching input order.
        List<Optional<ScoredGlobalIndexResult>> results = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            Optional<ScoredGlobalIndexResult> found =
                    buildScoredResult(
                            batchResult.idsForQuery(i), batchResult.distancesForQuery(i), metric);
            if (joint && incomplete(found, batchVectorSearch.limit())) {
                found = Optional.ofNullable(search(batchVectorSearch.forIndex(i), false, scope));
            }
            results.add(found);
        }
        return results;
    }

    private ScoredGlobalIndexResult search(
            VectorSearch vectorSearch, boolean joint, SearchScope scope) throws IOException {
        validateSearchVector(vectorSearch.vector());
        float[] queryVector = vectorSearch.vector().clone();
        int limit = vectorSearch.limit();
        String metric = nativeMeta.metric();

        if (scope.effectiveK == 0) {
            return null;
        }
        VectorSearchResult result =
                scope.filterBytes != null
                        ? vectorReader.search(
                                queryVector,
                                searchParams(vectorSearch.options(), scope.effectiveK),
                                scope.filterBytes)
                        : vectorReader.search(
                                queryVector,
                                searchParams(
                                        shardOptions(vectorSearch.options(), joint),
                                        scope.effectiveK));

        Optional<ScoredGlobalIndexResult> found =
                buildScoredResult(result.ids(), result.distances(), metric);
        return joint && incomplete(found, limit)
                ? search(vectorSearch, false, scope)
                : found.orElse(null);
    }

    private static boolean incomplete(Optional<ScoredGlobalIndexResult> found, int limit) {
        return !found.isPresent() || found.get().results().getLongCardinality() < limit;
    }

    private static Map<String, String> shardOptions(Map<String, String> options, boolean joint) {
        if (!joint) {
            return options;
        }
        Map<String, String> widened = new HashMap<>(options);
        widened.put(NPROBE_PARAMETER, String.valueOf(JOINT_INNER_PROBES));
        return widened;
    }

    static Optional<ScoredGlobalIndexResult> buildScoredResult(
            long[] ids, float[] distances, String metric) {
        if (ids.length == 0) {
            return Optional.empty();
        }

        RoaringNavigableMap64 resultBitmap = new RoaringNavigableMap64();
        HashMap<Long, Float> id2scores = new HashMap<>(ids.length);

        for (int i = 0; i < ids.length; i++) {
            long rowId = ids[i];
            if (rowId < 0) {
                continue;
            }
            float score = convertDistanceToScore(distances[i], metric);
            resultBitmap.add(rowId);
            id2scores.put(rowId, score);
        }

        if (resultBitmap.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(
                ScoredGlobalIndexResult.create(
                        resultBitmap,
                        rowId -> {
                            Float score = id2scores.get(rowId);
                            if (score == null) {
                                throw new IllegalArgumentException(
                                        "No score found for rowId: "
                                                + rowId
                                                + ". Only rowIds present in results() are valid.");
                            }
                            return score;
                        }));
    }

    private static List<Optional<ScoredGlobalIndexResult>> emptyResults(int n) {
        List<Optional<ScoredGlobalIndexResult>> results = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            results.add(Optional.empty());
        }
        return results;
    }

    /** The serialized filter and effective top-K for one search, shared by its shard reads. */
    static final class SearchScope {
        private final byte[] filterBytes;
        final int effectiveK;

        private SearchScope(byte[] filterBytes, int effectiveK) {
            this.filterBytes = filterBytes;
            this.effectiveK = effectiveK;
        }

        static SearchScope of(RoaringNavigableMap64 includeRowIds, int limit) {
            if (includeRowIds == null) {
                return new SearchScope(null, limit);
            }
            long cardinality = includeRowIds.getLongCardinality();
            if (cardinality == 0) {
                return new SearchScope(null, 0);
            }
            try {
                return new SearchScope(
                        includeRowIds.serialize(), (int) Math.min(limit, cardinality));
            } catch (IOException e) {
                throw new UncheckedIOException("Cannot serialize the vector row-ID filter.", e);
            }
        }
    }

    private static float convertDistanceToScore(float distance, String metric) {
        if ("l2".equals(metric)) {
            return 1.0f / (1.0f + distance);
        } else if ("cosine".equals(metric)) {
            return 1.0f - distance;
        } else if ("inner_product".equals(metric)) {
            return -distance;
        }
        throw new IllegalArgumentException("Unknown metric: " + metric);
    }

    static VectorSearchParams searchParams(Map<String, String> parameters, int topK) {
        Integer nprobe = intParameter(parameters, NPROBE_PARAMETER);
        Integer lSearch = intParameter(parameters, L_SEARCH_PARAMETER);
        Integer maxInitialFilterExpansionFactor =
                intParameter(parameters, MAX_INITIAL_FILTER_EXPANSION_FACTOR_PARAMETER);
        if (nprobe != null && lSearch != null) {
            throw new IllegalArgumentException(
                    "Cannot set both '" + NPROBE_PARAMETER + "' and '" + L_SEARCH_PARAMETER + "'.");
        }
        VectorSearchParams searchParams;
        if (nprobe != null) {
            searchParams = VectorSearchParams.ivf(topK, nprobe);
        } else if (lSearch != null) {
            searchParams = VectorSearchParams.diskAnn(topK, lSearch);
        } else {
            searchParams = VectorSearchParams.automatic(topK);
        }
        return maxInitialFilterExpansionFactor == null
                ? searchParams
                : searchParams.withMaxInitialFilterExpansionFactor(maxInitialFilterExpansionFactor);
    }

    static VectorSearchParams batchSearchParams(Map<String, String> parameters, int topK) {
        VectorSearchParams searchParams = searchParams(parameters, topK);
        String reuseMode = parameters.get(IVF_PQ_BATCH_TABLE_REUSE_PARAMETER);
        if (reuseMode != null) {
            searchParams = searchParams.withIvfPqBatchTableReuse(reuseMode);
        }
        Long reuseMaxBytes =
                longParameter(parameters, IVF_PQ_BATCH_TABLE_REUSE_MAX_BYTES_PARAMETER);
        return reuseMaxBytes == null
                ? searchParams
                : searchParams.withIvfPqBatchTableReuseMaxBytes(reuseMaxBytes);
    }

    private static Integer intParameter(Map<String, String> parameters, String key) {
        String value = parameters.get(key);
        if (value == null) {
            return null;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Invalid value for '" + key + "': " + value + ". Must be an integer.", e);
        }
    }

    private static Long longParameter(Map<String, String> parameters, String key) {
        String value = parameters.get(key);
        if (value == null) {
            return null;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Invalid value for '" + key + "': " + value + ". Must be a long integer.", e);
        }
    }

    private void validateSearchVector(Object vector) {
        if (!(vector instanceof float[])) {
            throw new IllegalArgumentException(
                    "Expected float[] vector but got: " + vector.getClass());
        }
        boolean validFieldType = false;
        if (fieldType instanceof VectorType) {
            validFieldType = ((VectorType) fieldType).getElementType() instanceof FloatType;
        } else if (fieldType instanceof ArrayType) {
            validFieldType = ((ArrayType) fieldType).getElementType() instanceof FloatType;
        }
        if (!validFieldType) {
            throw new IllegalArgumentException(
                    "Vector index requires VectorType<FLOAT> or ArrayType<FLOAT>, but field type is: "
                            + fieldType);
        }
        int queryDim = ((float[]) vector).length;
        if (queryDim != nativeMeta.dimension()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Query vector dimension mismatch: index expects %d, but got %d",
                            nativeMeta.dimension(), queryDim));
        }
    }

    private void ensureLoaded() throws IOException {
        if (vectorReader == null) {
            synchronized (this) {
                if (vectorReader == null) {
                    SeekableInputStream in = fileReader.getInputStream(ioMeta);
                    VectorIndexReader reader = null;
                    try {
                        VectorIndexInput input = new SeekableStreamVectorIndexInput(in);
                        reader = new VectorIndexReader(input);
                        nativeMeta = reader.metadata();
                        if (expectedIndexType != null
                                && !expectedIndexType.equals(
                                        nativeMeta.indexType().replace('_', '-'))) {
                            throw new IOException(
                                    "Index file type differs from manifest index type: "
                                            + ioMeta.filePath());
                        }
                        reader.optimizeForSearch();
                        vectorReader = reader;
                        openStream = in;
                    } catch (Exception e) {
                        if (reader != null) {
                            try {
                                reader.close();
                            } catch (Exception closeException) {
                                e.addSuppressed(closeException);
                            }
                        }
                        IOUtils.closeQuietly(in);
                        throw e;
                    }
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        Throwable firstException = null;

        if (vectorReader != null) {
            try {
                vectorReader.close();
            } catch (Throwable t) {
                firstException = t;
            }
            vectorReader = null;
        }

        if (openStream != null) {
            try {
                openStream.close();
            } catch (Throwable t) {
                if (firstException == null) {
                    firstException = t;
                } else {
                    firstException.addSuppressed(t);
                }
            }
            openStream = null;
        }

        if (firstException != null) {
            if (firstException instanceof IOException) {
                throw (IOException) firstException;
            } else if (firstException instanceof RuntimeException) {
                throw (RuntimeException) firstException;
            } else {
                throw new RuntimeException(
                        "Failed to close vector global index reader", firstException);
            }
        }
    }

    static class SeekableStreamVectorIndexInput implements VectorIndexInput {

        private final SeekableInputStream input;

        SeekableStreamVectorIndexInput(SeekableInputStream input) {
            this.input = input;
        }

        @Override
        public void pread(long[] positions, byte[][] buffers) {
            if (positions.length != buffers.length) {
                throw new IllegalArgumentException(
                        "positions length "
                                + positions.length
                                + " != buffers length "
                                + buffers.length);
            }
            try {
                if (input instanceof VectoredReadable
                        && areRangesNonOverlapping(positions, buffers)) {
                    preadVectored((VectoredReadable) input, positions, buffers);
                } else {
                    synchronized (this) {
                        preadSequential(positions, buffers);
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to read vector index", e);
            }
        }

        private void preadVectored(VectoredReadable readable, long[] positions, byte[][] buffers)
                throws IOException {
            List<FileRange> ranges = new ArrayList<>(positions.length);
            for (int i = 0; i < positions.length; i++) {
                ranges.add(FileRange.createFileRange(positions[i], buffers[i]));
            }

            VectoredReadUtils.ReadOptions options =
                    VectoredReadUtils.ReadOptions.from(readable)
                            .withMinSeekForVectorReads(VECTOR_INDEX_MIN_SEEK_FOR_VECTOR_READS)
                            .withParallelismForVectorReads(
                                    VECTOR_INDEX_PARALLELISM_FOR_VECTOR_READS)
                            .withSequentialReadFallback(false);
            VectoredReadUtils.readVectored(readable, ranges, options);

            for (FileRange range : ranges) {
                range.getData().join();
            }
        }

        private void preadSequential(long[] positions, byte[][] buffers) throws IOException {
            for (int i = 0; i < positions.length; i++) {
                input.seek(positions[i]);
                readFully(input, buffers[i]);
            }
        }

        private static void readFully(SeekableInputStream input, byte[] buffer) throws IOException {
            int offset = 0;
            while (offset < buffer.length) {
                int read = input.read(buffer, offset, buffer.length - offset);
                if (read < 0) {
                    throw new IOException("Unexpected end of vector index file");
                }
                offset += read;
            }
        }

        private static boolean areRangesNonOverlapping(long[] positions, byte[][] buffers) {
            if (positions.length < 2) {
                return true;
            }

            List<Integer> indexes = new ArrayList<>(positions.length);
            for (int i = 0; i < positions.length; i++) {
                indexes.add(i);
            }
            indexes.sort(Comparator.comparingLong(index -> positions[index]));

            boolean hasPrevious = false;
            long previousEnd = 0;
            for (int index : indexes) {
                long offset = positions[index];
                long end = offset + buffers[index].length;
                if (end < offset || (hasPrevious && offset < previousEnd)) {
                    return false;
                }
                previousEnd = end;
                hasPrevious = true;
            }
            return true;
        }
    }
}
