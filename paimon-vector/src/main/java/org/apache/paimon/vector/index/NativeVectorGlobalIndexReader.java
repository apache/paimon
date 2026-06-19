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
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.index.vector.VectorIndexInput;
import org.apache.paimon.index.vector.VectorIndexMetadata;
import org.apache.paimon.index.vector.VectorIndexReader;
import org.apache.paimon.index.vector.VectorSearchBatchResult;
import org.apache.paimon.index.vector.VectorSearchResult;
import org.apache.paimon.predicate.BatchVectorSearch;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.VectorType;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.RoaringNavigableMap64;

import java.io.IOException;
import java.util.ArrayList;
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
public class NativeVectorGlobalIndexReader implements GlobalIndexReader {

    private static final String NPROBE_PARAMETER = "ivf.nprobe";
    private static final String EF_SEARCH_PARAMETER = "hnsw.ef_search";
    private static final int DEFAULT_NPROBE = 16;
    private static final int DEFAULT_EF_SEARCH = 0;
    private static final int VECTOR_INDEX_MIN_SEEK_FOR_VECTOR_READS = 16 * 1024;
    private static final int VECTOR_INDEX_PARALLELISM_FOR_VECTOR_READS = 32;

    private final GlobalIndexIOMeta ioMeta;
    private final GlobalIndexFileReader fileReader;
    private final DataType fieldType;
    private final ExecutorService executor;

    private volatile VectorIndexMetadata nativeMeta;
    private volatile VectorIndexReader vectorReader;
    private SeekableInputStream openStream;

    public NativeVectorGlobalIndexReader(
            GlobalIndexFileReader fileReader,
            List<GlobalIndexIOMeta> ioMetas,
            DataType fieldType,
            ExecutorService executor) {
        checkArgument(ioMetas.size() == 1, "Expected exactly one index file per shard");
        this.executor = executor;
        this.fileReader = fileReader;
        this.ioMeta = ioMetas.get(0);
        this.fieldType = fieldType;
    }

    @Override
    public CompletableFuture<Optional<ScoredGlobalIndexResult>> visitVectorSearch(
            VectorSearch vectorSearch) {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        ensureLoaded();
                        return Optional.ofNullable(search(vectorSearch));
                    } catch (IOException e) {
                        throw new RuntimeException(
                                String.format(
                                        "Failed vector index search: field=%s, limit=%d",
                                        vectorSearch.fieldName(), vectorSearch.limit()),
                                e);
                    }
                },
                executor);
    }

    @Override
    public CompletableFuture<List<Optional<ScoredGlobalIndexResult>>> visitBatchVectorSearch(
            BatchVectorSearch batchVectorSearch) {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        ensureLoaded();
                        return searchBatch(batchVectorSearch);
                    } catch (IOException e) {
                        throw new RuntimeException(
                                String.format(
                                        "Failed batch vector index search: field=%s, limit=%d, vectorCount=%d",
                                        batchVectorSearch.fieldName(),
                                        batchVectorSearch.limit(),
                                        batchVectorSearch.vectorCount()),
                                e);
                    }
                },
                executor);
    }

    private List<Optional<ScoredGlobalIndexResult>> searchBatch(BatchVectorSearch batchVectorSearch)
            throws IOException {
        int n = batchVectorSearch.vectorCount();
        // Single vector: reuse the scalar path; no batching benefit.
        if (n == 1) {
            List<Optional<ScoredGlobalIndexResult>> results = new ArrayList<>(1);
            results.add(Optional.ofNullable(search(batchVectorSearch.forIndex(0))));
            return results;
        }

        float[][] vectors = batchVectorSearch.vectors();
        for (float[] vector : vectors) {
            validateSearchVector(vector);
        }
        int dim = nativeMeta.dimension();
        int nprobe = nprobe(batchVectorSearch.options());
        int efSearch = efSearch(batchVectorSearch.options());
        String metric = nativeMeta.metric();

        SearchScope scope =
                resolveScope(batchVectorSearch.includeRowIds(), batchVectorSearch.limit());
        if (scope == null) {
            return emptyResults(n);
        }

        // Flatten query vectors into one contiguous array for a single native call.
        float[] queries = new float[n * dim];
        for (int i = 0; i < n; i++) {
            System.arraycopy(vectors[i], 0, queries, i * dim, dim);
        }

        VectorSearchBatchResult batchResult =
                scope.filterBytes != null
                        ? vectorReader.searchBatch(
                                queries, n, scope.effectiveK, nprobe, efSearch, scope.filterBytes)
                        : vectorReader.searchBatch(queries, n, scope.effectiveK, nprobe, efSearch);

        // result i corresponds to vectors[i], matching input order.
        List<Optional<ScoredGlobalIndexResult>> results = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            results.add(
                    buildScoredResult(
                            batchResult.idsForQuery(i), batchResult.distancesForQuery(i), metric));
        }
        return results;
    }

    private ScoredGlobalIndexResult search(VectorSearch vectorSearch) throws IOException {
        validateSearchVector(vectorSearch.vector());
        float[] queryVector = vectorSearch.vector().clone();
        int limit = vectorSearch.limit();
        int nprobe = nprobe(vectorSearch.options());
        int efSearch = efSearch(vectorSearch.options());
        String metric = nativeMeta.metric();

        SearchScope scope = resolveScope(vectorSearch.includeRowIds(), limit);
        if (scope == null) {
            return null;
        }
        VectorSearchResult result =
                scope.filterBytes != null
                        ? vectorReader.search(
                                queryVector, scope.effectiveK, nprobe, efSearch, scope.filterBytes)
                        : vectorReader.search(queryVector, scope.effectiveK, nprobe, efSearch);

        return buildScoredResult(result.ids(), result.distances(), metric).orElse(null);
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

    /** Resolves filter bytes and effective top-K; returns null when the filter selects no rows. */
    private static SearchScope resolveScope(RoaringNavigableMap64 includeRowIds, int limit)
            throws IOException {
        if (includeRowIds == null) {
            return new SearchScope(null, limit);
        }
        long cardinality = includeRowIds.getLongCardinality();
        if (cardinality == 0) {
            return null;
        }
        return new SearchScope(includeRowIds.serialize(), (int) Math.min(limit, cardinality));
    }

    /** Resolved filter state for a query. {@code filterBytes} is null when no filter is set. */
    private static final class SearchScope {
        private final byte[] filterBytes;
        private final int effectiveK;

        private SearchScope(byte[] filterBytes, int effectiveK) {
            this.filterBytes = filterBytes;
            this.effectiveK = effectiveK;
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

    static int nprobe(Map<String, String> parameters) {
        return intParameter(parameters, NPROBE_PARAMETER, DEFAULT_NPROBE);
    }

    static int efSearch(Map<String, String> parameters) {
        return intParameter(parameters, EF_SEARCH_PARAMETER, DEFAULT_EF_SEARCH);
    }

    private static int intParameter(Map<String, String> parameters, String key, int defaultValue) {
        String value = parameters.get(key);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Invalid value for '" + key + "': " + value + ". Must be an integer.", e);
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
                    try {
                        NativeVectorIndexLoader.loadJni();
                        VectorIndexReader reader =
                                new VectorIndexReader(new SeekableStreamVectorIndexInput(in));
                        nativeMeta = reader.metadata();
                        vectorReader = reader;
                        openStream = in;
                    } catch (Exception e) {
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
                ranges.add(FileRange.createFileRange(positions[i], buffers[i].length));
            }

            VectoredReadUtils.ReadOptions options =
                    VectoredReadUtils.ReadOptions.from(readable)
                            .withMinSeekForVectorReads(VECTOR_INDEX_MIN_SEEK_FOR_VECTOR_READS)
                            .withParallelismForVectorReads(
                                    VECTOR_INDEX_PARALLELISM_FOR_VECTOR_READS)
                            .withSequentialReadFallback(false);
            VectoredReadUtils.readVectored(readable, ranges, options);

            for (int i = 0; i < ranges.size(); i++) {
                byte[] bytes = ranges.get(i).getData().join();
                System.arraycopy(bytes, 0, buffers[i], 0, bytes.length);
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

    // =================== unsupported =====================

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitIsNotNull(FieldRef fieldRef) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitIsNull(FieldRef fieldRef) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitStartsWith(
            FieldRef fieldRef, Object literal) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitEndsWith(
            FieldRef fieldRef, Object literal) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitContains(
            FieldRef fieldRef, Object literal) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitLike(
            FieldRef fieldRef, Object literal) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitLessThan(
            FieldRef fieldRef, Object literal) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitGreaterOrEqual(
            FieldRef fieldRef, Object literal) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitNotEqual(
            FieldRef fieldRef, Object literal) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitLessOrEqual(
            FieldRef fieldRef, Object literal) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitEqual(
            FieldRef fieldRef, Object literal) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitGreaterThan(
            FieldRef fieldRef, Object literal) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitIn(
            FieldRef fieldRef, List<Object> literals) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitNotIn(
            FieldRef fieldRef, List<Object> literals) {
        return CompletableFuture.completedFuture(Optional.empty());
    }
}
