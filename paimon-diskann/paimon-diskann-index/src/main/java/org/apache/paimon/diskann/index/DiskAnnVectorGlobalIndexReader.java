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

package org.apache.paimon.diskann.index;

import org.apache.paimon.diskann.IndexSearcher;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.RoaringNavigableMap64;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.PriorityQueue;

/**
 * Vector global index reader using DiskANN.
 *
 * <p>This implementation uses DiskANN for efficient approximate nearest neighbor search. Both the
 * Vamana graph and full-precision vectors are read on demand from Paimon FileIO-backed storage
 * (local, HDFS, S3, OSS, etc.) via {@link SeekableInputStream}, ensuring that neither is loaded
 * into Java memory in full.
 */
public class DiskAnnVectorGlobalIndexReader implements GlobalIndexReader {

    /**
     * Loaded search handles. Each entry wraps a DiskANN {@link IndexSearcher} (Rust native beam
     * search with both graph and vectors read on-demand from FileIO-backed storage via {@link
     * FileIOGraphReader} and {@link FileIOVectorReader}).
     */
    private final List<SearchHandle> handles;

    private final List<DiskAnnIndexMeta> indexMetas;
    private final List<GlobalIndexIOMeta> ioMetas;
    private final GlobalIndexFileReader fileReader;
    private final DataType fieldType;
    private final DiskAnnVectorIndexOptions options;
    private volatile boolean metasLoaded = false;
    private volatile boolean indicesLoaded = false;

    /**
     * Number of vectors to cache per searcher in the LRU cache inside {@link FileIOVectorReader}.
     */
    private static final int VECTOR_CACHE_SIZE = 4096;

    public DiskAnnVectorGlobalIndexReader(
            GlobalIndexFileReader fileReader,
            List<GlobalIndexIOMeta> ioMetas,
            DataType fieldType,
            DiskAnnVectorIndexOptions options) {
        this.fileReader = fileReader;
        this.ioMetas = ioMetas;
        this.fieldType = fieldType;
        this.options = options;
        this.handles = new ArrayList<>();
        this.indexMetas = new ArrayList<>();
    }

    /** Wrapper around a search implementation for lifecycle management. */
    private interface SearchHandle extends AutoCloseable {
        void search(
                long n,
                float[] queryVectors,
                int k,
                int searchListSize,
                float[] distances,
                long[] labels);

        @Override
        void close() throws IOException;
    }

    /**
     * Uses DiskANN's native Rust beam search via {@link IndexSearcher}. Both graph and vectors are
     * read on demand from Paimon FileIO-backed storage through {@link FileIOGraphReader} and {@link
     * FileIOVectorReader} JNI callbacks.
     */
    private static class DiskAnnSearchHandle implements SearchHandle {
        private final IndexSearcher searcher;

        DiskAnnSearchHandle(IndexSearcher searcher) {
            this.searcher = searcher;
        }

        @Override
        public void search(
                long n,
                float[] queryVectors,
                int k,
                int searchListSize,
                float[] distances,
                long[] labels) {
            searcher.search(n, queryVectors, k, searchListSize, distances, labels);
        }

        @Override
        public void close() {
            searcher.close();
        }
    }

    @Override
    public Optional<GlobalIndexResult> visitVectorSearch(VectorSearch vectorSearch) {
        try {
            ensureLoadMetas();

            RoaringNavigableMap64 includeRowIds = vectorSearch.includeRowIds();
            if (includeRowIds != null) {
                List<Integer> matchingIndices = new ArrayList<>();
                for (int i = 0; i < indexMetas.size(); i++) {
                    DiskAnnIndexMeta meta = indexMetas.get(i);
                    if (hasOverlap(meta.minId(), meta.maxId(), includeRowIds)) {
                        matchingIndices.add(i);
                    }
                }
                if (matchingIndices.isEmpty()) {
                    return Optional.empty();
                }
                ensureLoadIndices(matchingIndices);
            } else {
                ensureLoadAllIndices();
            }

            return Optional.ofNullable(search(vectorSearch));
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format(
                            "Failed to search DiskANN vector index with fieldName=%s, limit=%d",
                            vectorSearch.fieldName(), vectorSearch.limit()),
                    e);
        }
    }

    private boolean hasOverlap(long minId, long maxId, RoaringNavigableMap64 includeRowIds) {
        for (Long id : includeRowIds) {
            if (id >= minId && id <= maxId) {
                return true;
            }
            if (id > maxId) {
                break;
            }
        }
        return false;
    }

    private GlobalIndexResult search(VectorSearch vectorSearch) throws IOException {
        validateVectorType(vectorSearch.vector());
        float[] queryVector = ((float[]) vectorSearch.vector()).clone();
        int limit = vectorSearch.limit();

        PriorityQueue<ScoredRow> result =
                new PriorityQueue<>(Comparator.comparingDouble(sr -> sr.score));

        RoaringNavigableMap64 includeRowIds = vectorSearch.includeRowIds();
        int searchK = limit;
        if (includeRowIds != null) {
            searchK =
                    Math.max(
                            limit * options.searchFactor(),
                            (int) includeRowIds.getLongCardinality());
        }

        for (SearchHandle handle : handles) {
            if (handle == null) {
                continue;
            }
            int effectiveK = searchK;
            if (effectiveK <= 0) {
                continue;
            }

            float[] distances = new float[effectiveK];
            long[] labels = new long[effectiveK];

            // Dynamic search list sizing: use max of configured value and effectiveK
            // This follows Milvus best practice: search_list should be >= topk
            int dynamicSearchListSize = Math.max(options.searchListSize(), effectiveK);
            handle.search(1, queryVector, effectiveK, dynamicSearchListSize, distances, labels);

            for (int i = 0; i < effectiveK; i++) {
                long rowId = labels[i];
                if (rowId < 0) {
                    continue;
                }
                if (includeRowIds != null && !includeRowIds.contains(rowId)) {
                    continue;
                }
                float score = convertDistanceToScore(distances[i]);

                if (result.size() < limit) {
                    result.offer(new ScoredRow(rowId, score));
                } else {
                    if (result.peek() != null && score > result.peek().score) {
                        result.poll();
                        result.offer(new ScoredRow(rowId, score));
                    }
                }
            }
        }

        RoaringNavigableMap64 roaringBitmap64 = new RoaringNavigableMap64();
        HashMap<Long, Float> id2scores = new HashMap<>(result.size());
        for (ScoredRow scoredRow : result) {
            id2scores.put(scoredRow.rowId, scoredRow.score);
            roaringBitmap64.add(scoredRow.rowId);
        }
        return new DiskAnnScoredGlobalIndexResult(roaringBitmap64, id2scores);
    }

    private float convertDistanceToScore(float distance) {
        if (options.metric() == DiskAnnVectorMetric.L2
                || options.metric() == DiskAnnVectorMetric.COSINE) {
            return 1.0f / (1.0f + distance);
        } else {
            return distance;
        }
    }

    private void validateVectorType(Object vector) {
        if (!(vector instanceof float[])) {
            throw new IllegalArgumentException(
                    "Expected float[] vector but got: " + vector.getClass());
        }
        if (!(fieldType instanceof ArrayType)
                || !(((ArrayType) fieldType).getElementType() instanceof FloatType)) {
            throw new IllegalArgumentException(
                    "DiskANN currently only supports float arrays, but field type is: "
                            + fieldType);
        }
    }

    private void ensureLoadMetas() throws IOException {
        if (!metasLoaded) {
            synchronized (this) {
                if (!metasLoaded) {
                    for (GlobalIndexIOMeta ioMeta : ioMetas) {
                        byte[] metaBytes = ioMeta.metadata();
                        DiskAnnIndexMeta meta = DiskAnnIndexMeta.deserialize(metaBytes);
                        indexMetas.add(meta);
                    }
                    metasLoaded = true;
                }
            }
        }
    }

    private void ensureLoadAllIndices() throws IOException {
        if (!indicesLoaded) {
            synchronized (this) {
                if (!indicesLoaded) {
                    for (int i = 0; i < ioMetas.size(); i++) {
                        loadIndexAt(i);
                    }
                    indicesLoaded = true;
                }
            }
        }
    }

    private void ensureLoadIndices(List<Integer> positions) throws IOException {
        synchronized (this) {
            while (handles.size() < ioMetas.size()) {
                handles.add(null);
            }
            for (int pos : positions) {
                if (handles.get(pos) == null) {
                    loadIndexAt(pos);
                }
            }
        }
    }

    /**
     * Load an index at the given position.
     *
     * <p>The index file (graph) and the data file (vectors) are accessed on demand via {@link
     * SeekableInputStream}s — neither is loaded into Java memory in full. The PQ pivots and
     * compressed codes are loaded into memory as the "memory thumbnail" for approximate distance
     * computation during native beam search.
     */
    private void loadIndexAt(int position) throws IOException {
        GlobalIndexIOMeta ioMeta = ioMetas.get(position);
        DiskAnnIndexMeta meta = indexMetas.get(position);
        SearchHandle handle = null;
        try {
            // 1. Open index file (graph only, no header) as a SeekableInputStream.
            //    FileIOGraphReader scans the graph section + builds offset index; graph neighbors
            //    are read on demand during beam search.
            //    numNodes = user vectors + 1 start point.
            int numNodes = (int) meta.numVectors() + 1;
            SeekableInputStream graphStream = fileReader.getInputStream(ioMeta);
            FileIOGraphReader graphReader =
                    new FileIOGraphReader(
                            graphStream,
                            meta.dim(),
                            meta.metricValue(),
                            meta.maxDegree(),
                            meta.buildListSize(),
                            numNodes,
                            meta.startId(),
                            VECTOR_CACHE_SIZE);

            // 2. Open data file stream for on-demand full-vector reads.
            Path dataPath = new Path(ioMeta.filePath().getParent(), meta.dataFileName());
            GlobalIndexIOMeta dataIOMeta = new GlobalIndexIOMeta(dataPath, 0L, new byte[0]);
            SeekableInputStream vectorStream = fileReader.getInputStream(dataIOMeta);
            FileIOVectorReader vectorReader =
                    new FileIOVectorReader(vectorStream, meta.dim(), VECTOR_CACHE_SIZE);

            // 3. Create DiskANN native searcher with on-demand graph + vector access.
            handle =
                    new DiskAnnSearchHandle(
                            IndexSearcher.createFromReaders(
                                    graphReader, vectorReader, meta.dim(), meta.minId()));

            if (handles.size() <= position) {
                while (handles.size() < position) {
                    handles.add(null);
                }
                handles.add(handle);
            } else {
                handles.set(position, handle);
            }
        } catch (Exception e) {
            IOUtils.closeQuietly(handle);
            throw e instanceof IOException ? (IOException) e : new IOException(e);
        }
    }

    @Override
    public void close() throws IOException {
        Throwable firstException = null;

        // Close all search handles (also closes their FileIOVectorReader streams).
        for (SearchHandle handle : handles) {
            if (handle == null) {
                continue;
            }
            try {
                handle.close();
            } catch (Throwable t) {
                if (firstException == null) {
                    firstException = t;
                } else {
                    firstException.addSuppressed(t);
                }
            }
        }
        handles.clear();

        if (firstException != null) {
            if (firstException instanceof IOException) {
                throw (IOException) firstException;
            } else if (firstException instanceof RuntimeException) {
                throw (RuntimeException) firstException;
            } else {
                throw new RuntimeException(
                        "Failed to close DiskANN vector global index reader", firstException);
            }
        }
    }

    private static class ScoredRow {
        final long rowId;
        final float score;

        ScoredRow(long rowId, float score) {
            this.rowId = rowId;
            this.score = score;
        }
    }

    // =================== unsupported =====================

    @Override
    public Optional<GlobalIndexResult> visitIsNotNull(FieldRef fieldRef) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitIsNull(FieldRef fieldRef) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitStartsWith(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitEndsWith(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitContains(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitLike(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitLessThan(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitGreaterOrEqual(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitNotEqual(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitLessOrEqual(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitEqual(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitGreaterThan(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitIn(FieldRef fieldRef, List<Object> literals) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitNotIn(FieldRef fieldRef, List<Object> literals) {
        return Optional.empty();
    }
}
