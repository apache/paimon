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

package org.apache.paimon.lumina.index;

import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.VectorType;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.RoaringNavigableMap64;

import org.aliyun.lumina.LuminaFileInput;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Vector global index reader using Lumina.
 *
 * <p>Each shard has exactly one Lumina index file. This reader loads the single index and performs
 * vector similarity search.
 */
public class LuminaVectorGlobalIndexReader implements GlobalIndexReader {

    /**
     * Sets {@code diskann.search.list_size} when not explicitly configured. The list size is set to
     * at least {@code MIN_SEARCH_LIST_SIZE} to ensure sufficient recall for DiskANN, even when topK
     * is very small.
     */
    private static final int MIN_SEARCH_LIST_SIZE = 16;

    private final GlobalIndexIOMeta ioMeta;
    private final GlobalIndexFileReader fileReader;
    private final DataType fieldType;
    private final LuminaVectorIndexOptions options;

    private volatile LuminaIndexMeta indexMeta;
    private volatile LuminaIndex index;
    private SeekableInputStream openStream;
    private InputStreamFileInput inputStreamFileInput;

    public LuminaVectorGlobalIndexReader(
            GlobalIndexFileReader fileReader,
            List<GlobalIndexIOMeta> ioMetas,
            DataType fieldType,
            LuminaVectorIndexOptions options) {
        checkArgument(ioMetas.size() == 1, "Expected exactly one index file per shard");
        this.fileReader = fileReader;
        this.ioMeta = ioMetas.get(0);
        this.fieldType = fieldType;
        this.options = options;
    }

    @Override
    public Optional<ScoredGlobalIndexResult> visitVectorSearch(VectorSearch vectorSearch) {
        try {
            ensureLoaded();
            return Optional.ofNullable(search(vectorSearch));
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format(
                            "Failed to search Lumina vector index with fieldName=%s, limit=%d",
                            vectorSearch.fieldName(), vectorSearch.limit()),
                    e);
        }
    }

    private ScoredGlobalIndexResult search(VectorSearch vectorSearch) throws IOException {
        validateSearchVector(vectorSearch.vector());
        float[] queryVector = vectorSearch.vector().clone();
        int limit = vectorSearch.limit();
        LuminaVectorMetric indexMetric = indexMeta.metric();

        int effectiveK = (int) Math.min(limit, index.size());
        if (effectiveK <= 0) {
            return null;
        }

        RoaringNavigableMap64 includeRowIds = vectorSearch.includeRowIds();
        float[] distances;
        long[] labels;

        if (includeRowIds != null) {
            long cardinality = includeRowIds.getLongCardinality();
            if (cardinality > Integer.MAX_VALUE) {
                throw new IllegalArgumentException(
                        "includeRowIds cardinality ("
                                + cardinality
                                + ") exceeds Integer.MAX_VALUE");
            }
            long[] scopedIds = new long[(int) cardinality];
            Iterator<Long> iter = includeRowIds.iterator();
            for (int i = 0; i < scopedIds.length; i++) {
                scopedIds[i] = iter.next();
            }
            if (scopedIds.length == 0) {
                return null;
            }
            effectiveK = Math.min(effectiveK, scopedIds.length);
            distances = new float[effectiveK];
            labels = new long[effectiveK];
            Map<String, String> searchOptions = options.toLuminaOptions();
            searchOptions.putAll(indexMeta.options());
            searchOptions.put("search.thread_safe_filter", "true");
            ensureSearchListSize(searchOptions, effectiveK);
            index.searchWithFilter(
                    queryVector, 1, effectiveK, distances, labels, scopedIds, searchOptions);
        } else {
            distances = new float[effectiveK];
            labels = new long[effectiveK];
            Map<String, String> searchOptions = options.toLuminaOptions();
            searchOptions.putAll(indexMeta.options());
            ensureSearchListSize(searchOptions, effectiveK);
            index.search(queryVector, 1, effectiveK, distances, labels, searchOptions);
        }

        // Min-heap: smallest score at head, so we can evict the weakest candidate efficiently.
        PriorityQueue<ScoredRow> topK =
                new PriorityQueue<>(effectiveK + 1, Comparator.comparingDouble(s -> s.score));
        collectResults(distances, labels, effectiveK, effectiveK, topK, indexMetric);

        RoaringNavigableMap64 roaringBitmap64 = new RoaringNavigableMap64();
        HashMap<Long, Float> id2scores = new HashMap<>(topK.size());
        for (ScoredRow row : topK) {
            roaringBitmap64.add(row.rowId);
            id2scores.put(row.rowId, row.score);
        }
        return new LuminaScoredGlobalIndexResult(roaringBitmap64, id2scores);
    }

    private static void ensureSearchListSize(Map<String, String> searchOptions, int topK) {
        if (!searchOptions.containsKey("diskann.search.list_size")) {
            int listSize = Math.max((int) (topK * 1.5), MIN_SEARCH_LIST_SIZE);
            searchOptions.put("diskann.search.list_size", String.valueOf(listSize));
        }
    }

    private static void collectResults(
            float[] distances,
            long[] labels,
            int count,
            int limit,
            PriorityQueue<ScoredRow> topK,
            LuminaVectorMetric metric) {
        for (int i = 0; i < count; i++) {
            long rowId = labels[i];
            if (rowId < 0) {
                continue;
            }
            float score = convertDistanceToScore(distances[i], metric);
            if (topK.size() < limit) {
                topK.offer(new ScoredRow(rowId, score));
            } else if (score > topK.peek().score) {
                topK.poll();
                topK.offer(new ScoredRow(rowId, score));
            }
        }
    }

    private static float convertDistanceToScore(float distance, LuminaVectorMetric metric) {
        if (metric == LuminaVectorMetric.L2) {
            return 1.0f / (1.0f + distance);
        } else if (metric == LuminaVectorMetric.COSINE) {
            return 1.0f - distance;
        } else {
            // Inner product is already a similarity
            return distance;
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
                    "Lumina requires VectorType<FLOAT> or ArrayType<FLOAT>, but field type is: "
                            + fieldType);
        }
        int queryDim = ((float[]) vector).length;
        if (queryDim != indexMeta.dim()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Query vector dimension mismatch: index expects %d, but got %d",
                            indexMeta.dim(), queryDim));
        }
    }

    private void ensureLoaded() throws IOException {
        if (index == null) {
            synchronized (this) {
                if (index == null) {
                    indexMeta = LuminaIndexMeta.deserialize(ioMeta.metadata());
                    SeekableInputStream in = fileReader.getInputStream(ioMeta);
                    try {
                        InputStreamFileInput fileInput = new InputStreamFileInput(in);
                        Map<String, String> searcherOptions = options.toLuminaOptions();
                        searcherOptions.putAll(indexMeta.options());
                        index =
                                LuminaIndex.fromStream(
                                        indexMeta.indexType(),
                                        fileInput,
                                        ioMeta.fileSize(),
                                        indexMeta.dim(),
                                        indexMeta.metric(),
                                        searcherOptions);
                        fileInput.markOpenPhaseDone();
                        openStream = in;
                        inputStreamFileInput = fileInput;
                    } catch (Exception e) {
                        IOUtils.closeQuietly(in);
                        throw e;
                    }
                }
            }
        }
    }

    /** Returns the total bytes read by the underlying {@link InputStreamFileInput}, or 0. */
    public long getTotalBytesRead() {
        return inputStreamFileInput != null ? inputStreamFileInput.getTotalBytesRead() : 0;
    }

    // =================== open-phase I/O stats =====================

    public long getOpenBytesRead() {
        return inputStreamFileInput != null ? inputStreamFileInput.getOpenBytesRead() : 0;
    }

    public long getOpenSeekCount() {
        return inputStreamFileInput != null ? inputStreamFileInput.getOpenSeekCount() : 0;
    }

    public long getOpenReadCount() {
        return inputStreamFileInput != null ? inputStreamFileInput.getOpenReadCount() : 0;
    }

    public long getOpenReadTimeNanos() {
        return inputStreamFileInput != null ? inputStreamFileInput.getOpenReadTimeNanos() : 0;
    }

    public long getOpenSeekTimeNanos() {
        return inputStreamFileInput != null ? inputStreamFileInput.getOpenSeekTimeNanos() : 0;
    }

    // =================== search-phase I/O stats =====================

    public long getSearchBytesRead() {
        return inputStreamFileInput != null ? inputStreamFileInput.getSearchBytesRead() : 0;
    }

    public long getSearchSeekCount() {
        return inputStreamFileInput != null ? inputStreamFileInput.getSearchSeekCount() : 0;
    }

    public long getSearchReadCount() {
        return inputStreamFileInput != null ? inputStreamFileInput.getSearchReadCount() : 0;
    }

    public long getSearchReadTimeNanos() {
        return inputStreamFileInput != null ? inputStreamFileInput.getSearchReadTimeNanos() : 0;
    }

    public long getSearchSeekTimeNanos() {
        return inputStreamFileInput != null ? inputStreamFileInput.getSearchSeekTimeNanos() : 0;
    }

    @Override
    public void close() throws IOException {
        Throwable firstException = null;

        if (index != null) {
            try {
                index.close();
            } catch (Throwable t) {
                firstException = t;
            }
            index = null;
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
                        "Failed to close Lumina vector global index reader", firstException);
            }
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

    /**
     * Adapts a {@link SeekableInputStream} to the {@link LuminaFileInput} JNI callback API.
     *
     * <p>This mirrors the C++ {@code LuminaFileReader} adapter that bridges Paimon's {@code
     * InputStream} to Lumina's {@code FileReader} interface. The stream lifecycle is managed by the
     * enclosing reader, not by this adapter.
     */
    static class InputStreamFileInput implements LuminaFileInput {
        private final SeekableInputStream in;
        private long totalBytesRead;
        private long totalSeekCount;
        private long totalReadCount;
        private long totalReadTimeNanos;
        private long totalSeekTimeNanos;

        // Snapshot captured after open() phase.
        private long openBytesRead;
        private long openSeekCount;
        private long openReadCount;
        private long openReadTimeNanos;
        private long openSeekTimeNanos;

        InputStreamFileInput(SeekableInputStream in) {
            this.in = in;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            long start = System.nanoTime();
            int bytesRead = in.read(b, off, len);
            totalReadTimeNanos += System.nanoTime() - start;
            totalReadCount++;
            if (bytesRead > 0) {
                totalBytesRead += bytesRead;
            }
            return bytesRead;
        }

        @Override
        public void seek(long position) throws IOException {
            long start = System.nanoTime();
            in.seek(position);
            totalSeekTimeNanos += System.nanoTime() - start;
            totalSeekCount++;
        }

        @Override
        public long getPos() throws IOException {
            return in.getPos();
        }

        /** Snapshot current counters as the open-phase baseline. */
        void markOpenPhaseDone() {
            openBytesRead = totalBytesRead;
            openSeekCount = totalSeekCount;
            openReadCount = totalReadCount;
            openReadTimeNanos = totalReadTimeNanos;
            openSeekTimeNanos = totalSeekTimeNanos;
        }

        long getTotalBytesRead() {
            return totalBytesRead;
        }

        long getOpenBytesRead() {
            return openBytesRead;
        }

        long getOpenSeekCount() {
            return openSeekCount;
        }

        long getOpenReadCount() {
            return openReadCount;
        }

        long getOpenReadTimeNanos() {
            return openReadTimeNanos;
        }

        long getOpenSeekTimeNanos() {
            return openSeekTimeNanos;
        }

        long getSearchBytesRead() {
            return totalBytesRead - openBytesRead;
        }

        long getSearchSeekCount() {
            return totalSeekCount - openSeekCount;
        }

        long getSearchReadCount() {
            return totalReadCount - openReadCount;
        }

        long getSearchReadTimeNanos() {
            return totalReadTimeNanos - openReadTimeNanos;
        }

        long getSearchSeekTimeNanos() {
            return totalSeekTimeNanos - openSeekTimeNanos;
        }

        @Override
        public void close() {
            // Stream lifecycle is managed by the enclosing Reader.
        }
    }

    /** A row ID paired with its similarity score, used in the top-k min-heap. */
    private static class ScoredRow {
        final long rowId;
        final float score;

        ScoredRow(long rowId, float score) {
            this.rowId = rowId;
            this.score = score;
        }
    }
}
