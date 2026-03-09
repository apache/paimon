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
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.RoaringNavigableMap64;

import org.aliyun.lumina.LuminaFileInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;

/**
 * Vector global index reader using Lumina.
 *
 * <p>This reader loads Lumina indices from global index files and performs vector similarity
 * search.
 */
public class LuminaVectorGlobalIndexReader implements GlobalIndexReader {

    private final LuminaIndex[] indices;
    private final LuminaIndexMeta[] indexMetas;
    private final List<SeekableInputStream> openStreams;
    private final List<GlobalIndexIOMeta> ioMetas;
    private final GlobalIndexFileReader fileReader;
    private final DataType fieldType;
    private final LuminaVectorIndexOptions options;
    private volatile boolean metasLoaded = false;
    private volatile boolean indicesLoaded = false;

    public LuminaVectorGlobalIndexReader(
            GlobalIndexFileReader fileReader,
            List<GlobalIndexIOMeta> ioMetas,
            DataType fieldType,
            LuminaVectorIndexOptions options) {
        this.fileReader = fileReader;
        this.ioMetas = ioMetas;
        this.fieldType = fieldType;
        this.options = options;
        this.indices = new LuminaIndex[ioMetas.size()];
        this.indexMetas = new LuminaIndexMeta[ioMetas.size()];
        this.openStreams = Collections.synchronizedList(new ArrayList<>());
    }

    @Override
    public Optional<GlobalIndexResult> visitVectorSearch(VectorSearch vectorSearch) {
        try {
            ensureLoadMetas();

            RoaringNavigableMap64 includeRowIds = vectorSearch.includeRowIds();

            if (includeRowIds != null) {
                List<Integer> matchingIndices = new ArrayList<>();
                for (int i = 0; i < indexMetas.length; i++) {
                    LuminaIndexMeta meta = indexMetas[i];
                    if (includeRowIds.containsRange(meta.minId(), meta.maxId())) {
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
                            "Failed to search Lumina vector index with fieldName=%s, limit=%d",
                            vectorSearch.fieldName(), vectorSearch.limit()),
                    e);
        }
    }

    private GlobalIndexResult search(VectorSearch vectorSearch) throws IOException {
        validateVectorType(vectorSearch.vector());
        float[] queryVector = ((float[]) vectorSearch.vector()).clone();
        if (options.normalize()) {
            LuminaVectorUtils.normalizeL2(queryVector);
        }
        int limit = vectorSearch.limit();

        PriorityQueue<ScoredRow> result =
                new PriorityQueue<>(Comparator.comparingDouble(sr -> sr.score));

        RoaringNavigableMap64 includeRowIds = vectorSearch.includeRowIds();

        // Extract sorted filter IDs once; per-index scoping happens inside the loop.
        long[] allFilterIds = null;
        if (includeRowIds != null) {
            long cardinality = includeRowIds.getLongCardinality();
            if (cardinality <= 0) {
                return new LuminaScoredGlobalIndexResult(
                        new RoaringNavigableMap64(), new HashMap<>());
            }
            if (cardinality > Integer.MAX_VALUE) {
                throw new RuntimeException(
                        "Filter bitmap cardinality ("
                                + cardinality
                                + ") exceeds maximum supported size for native pre-filtering");
            }
            allFilterIds = new long[(int) cardinality];
            int idx = 0;
            for (long id : includeRowIds) {
                allFilterIds[idx++] = id;
            }
        }

        Map<String, String> filterSearchOptions = null;
        Map<String, String> plainSearchOptions = null;
        if (allFilterIds != null) {
            filterSearchOptions = new LinkedHashMap<>();
            int listSize = Math.max(limit * options.searchFactor(), options.searchListSize());
            filterSearchOptions.put("diskann.search.list_size", String.valueOf(listSize));
            filterSearchOptions.put("search.thread_safe_filter", "true");
        } else {
            plainSearchOptions = new LinkedHashMap<>();
            int listSize = Math.max(limit, options.searchListSize());
            plainSearchOptions.put("diskann.search.list_size", String.valueOf(listSize));
        }

        for (int i = 0; i < indices.length; i++) {
            LuminaIndex index = indices[i];
            if (index == null) {
                continue;
            }

            int effectiveK = (int) Math.min(limit, index.size());
            if (effectiveK <= 0) {
                continue;
            }

            if (allFilterIds != null) {
                LuminaIndexMeta meta = indexMetas[i];
                long[] scopedIds = scopeFilterIds(allFilterIds, meta.minId(), meta.maxId());
                if (scopedIds.length == 0) {
                    continue;
                }
                effectiveK = (int) Math.min(effectiveK, scopedIds.length);

                float[] distances = new float[effectiveK];
                long[] labels = new long[effectiveK];
                index.searchWithFilter(
                        queryVector, 1, effectiveK, distances, labels, scopedIds,
                        filterSearchOptions);
                collectResults(distances, labels, effectiveK, limit, result);
            } else {
                float[] distances = new float[effectiveK];
                long[] labels = new long[effectiveK];
                index.search(queryVector, 1, effectiveK, distances, labels, plainSearchOptions);
                collectResults(distances, labels, effectiveK, limit, result);
            }
        }

        RoaringNavigableMap64 roaringBitmap64 = new RoaringNavigableMap64();
        HashMap<Long, Float> id2scores = new HashMap<>(result.size());
        for (ScoredRow scoredRow : result) {
            id2scores.put(scoredRow.rowId, scoredRow.score);
            roaringBitmap64.add(scoredRow.rowId);
        }
        return new LuminaScoredGlobalIndexResult(roaringBitmap64, id2scores);
    }

    private void collectResults(
            float[] distances, long[] labels, int count, int limit,
            PriorityQueue<ScoredRow> result) {
        for (int i = 0; i < count; i++) {
            long rowId = labels[i];
            if (rowId < 0) {
                continue;
            }
            float score = convertDistanceToScore(distances[i]);
            if (result.size() < limit) {
                result.offer(new ScoredRow(rowId, score));
            } else if (result.peek() != null && score > result.peek().score) {
                result.poll();
                result.offer(new ScoredRow(rowId, score));
            }
        }
    }

    /**
     * Extract the subset of {@code sortedIds} that falls within [{@code minId}, {@code maxId}]
     * using binary search. The input array must be sorted in ascending order (guaranteed by roaring
     * bitmap iteration order).
     */
    private static long[] scopeFilterIds(long[] sortedIds, long minId, long maxId) {
        int from = lowerBound(sortedIds, minId);
        int to = upperBound(sortedIds, maxId);
        if (from >= to) {
            return new long[0];
        }
        if (from == 0 && to == sortedIds.length) {
            return sortedIds;
        }
        return Arrays.copyOfRange(sortedIds, from, to);
    }

    /** Return the index of the first element >= target. */
    private static int lowerBound(long[] arr, long target) {
        int lo = 0;
        int hi = arr.length;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (arr[mid] < target) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    /** Return the index of the first element > target. */
    private static int upperBound(long[] arr, long target) {
        int lo = 0;
        int hi = arr.length;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (arr[mid] <= target) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    private float convertDistanceToScore(float distance) {
        if (options.metric() == LuminaVectorMetric.L2) {
            return 1.0f / (1.0f + distance);
        } else if (options.metric() == LuminaVectorMetric.COSINE) {
            // Cosine distance is in [0, 2]; convert to similarity in [-1, 1]
            return 1.0f - distance;
        } else {
            // Inner product is already a similarity
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
                    "Lumina currently only supports float arrays, but field type is: " + fieldType);
        }
    }

    private void ensureLoadMetas() throws IOException {
        if (!metasLoaded) {
            synchronized (this) {
                if (!metasLoaded) {
                    for (int i = 0; i < ioMetas.size(); i++) {
                        byte[] metaBytes = ioMetas.get(i).metadata();
                        indexMetas[i] = LuminaIndexMeta.deserialize(metaBytes);
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
                        if (indices[i] == null) {
                            loadIndexAt(i);
                        }
                    }
                    indicesLoaded = true;
                }
            }
        }
    }

    private void ensureLoadIndices(List<Integer> positions) throws IOException {
        synchronized (this) {
            for (int pos : positions) {
                if (indices[pos] == null) {
                    loadIndexAt(pos);
                }
            }
            // Check if all indices are now loaded.
            if (!indicesLoaded) {
                boolean allLoaded = true;
                for (LuminaIndex idx : indices) {
                    if (idx == null) {
                        allLoaded = false;
                        break;
                    }
                }
                if (allLoaded) {
                    indicesLoaded = true;
                }
            }
        }
    }

    private void loadIndexAt(int position) throws IOException {
        GlobalIndexIOMeta ioMeta = ioMetas.get(position);
        SeekableInputStream in = fileReader.getInputStream(ioMeta);
        LuminaIndex index = null;
        try {
            index = loadIndex(in, position, ioMeta.fileSize());
            openStreams.add(in);
            indices[position] = index;
        } catch (Exception e) {
            IOUtils.closeQuietly(index);
            IOUtils.closeQuietly(in);
            throw e;
        }
    }

    private LuminaIndex loadIndex(SeekableInputStream in, int position, long fileSize)
            throws IOException {
        LuminaIndexMeta meta = indexMetas[position];
        LuminaVectorMetric metric = LuminaVectorMetric.fromValue(meta.metricValue());
        LuminaIndexType indexType = meta.indexType();

        LuminaFileInput fileInput =
                new LuminaFileInput() {
                    @Override
                    public int read(byte[] b, int off, int len) throws IOException {
                        return in.read(b, off, len);
                    }

                    @Override
                    public void seek(long position) throws IOException {
                        in.seek(position);
                    }

                    @Override
                    public long getPos() throws IOException {
                        return in.getPos();
                    }

                    @Override
                    public void close() throws IOException {
                        // Do not close: stream lifecycle is managed by the Reader.
                    }
                };

        Map<String, String> extraOptions = new LinkedHashMap<>();
        return LuminaIndex.fromStream(
                fileInput, fileSize, meta.dim(), metric, indexType, extraOptions);
    }

    @Override
    public void close() throws IOException {
        Throwable firstException = null;

        // Close all indices first (releases native FileReader → JNI global refs).
        for (int i = 0; i < indices.length; i++) {
            LuminaIndex index = indices[i];
            if (index == null) {
                continue;
            }
            try {
                index.close();
            } catch (Throwable t) {
                if (firstException == null) {
                    firstException = t;
                } else {
                    firstException.addSuppressed(t);
                }
            }
            indices[i] = null;
        }

        // Then close underlying streams.
        for (SeekableInputStream stream : openStreams) {
            try {
                if (stream != null) {
                    stream.close();
                }
            } catch (Throwable t) {
                if (firstException == null) {
                    firstException = t;
                } else {
                    firstException.addSuppressed(t);
                }
            }
        }
        openStreams.clear();

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
