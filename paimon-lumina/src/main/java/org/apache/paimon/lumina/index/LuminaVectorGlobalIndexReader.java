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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.UUID;

/**
 * Vector global index reader using Lumina.
 *
 * <p>This reader loads Lumina indices from global index files and performs vector similarity
 * search.
 */
public class LuminaVectorGlobalIndexReader implements GlobalIndexReader {

    private final LuminaIndex[] indices;
    private final LuminaIndexMeta[] indexMetas;
    private final List<File> localIndexFiles;
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
        this.localIndexFiles = Collections.synchronizedList(new ArrayList<>());
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

        // Extract filter IDs once for native pre-filtering
        long[] filterIds = null;
        if (includeRowIds != null) {
            long cardinality = includeRowIds.getLongCardinality();
            if (cardinality <= 0) {
                // Empty filter — no rows can match.
                return new LuminaScoredGlobalIndexResult(
                        new RoaringNavigableMap64(), new HashMap<>());
            }
            if (cardinality > Integer.MAX_VALUE) {
                throw new RuntimeException(
                        "Filter bitmap cardinality ("
                                + cardinality
                                + ") exceeds maximum supported size for native pre-filtering");
            }
            filterIds = new long[(int) cardinality];
            int idx = 0;
            for (long id : includeRowIds) {
                filterIds[idx++] = id;
            }
        }

        Map<String, String> searchOptions = new LinkedHashMap<>();
        if (filterIds != null) {
            // With native pre-filtering, increase list_size to improve recall under
            // selective filters. DiskANN's graph traversal skips filtered-out nodes,
            // so a larger list_size helps find enough qualifying candidates.
            int listSize = Math.max(limit * options.searchFactor(), options.searchListSize());
            searchOptions.put("diskann.search.list_size", String.valueOf(listSize));
            searchOptions.put("search.thread_safe_filter", "true");
        } else {
            int listSize = Math.max(limit, options.searchListSize());
            searchOptions.put("diskann.search.list_size", String.valueOf(listSize));
        }

        for (LuminaIndex index : indices) {
            if (index == null) {
                continue;
            }

            int effectiveK = (int) Math.min(limit, index.size());
            if (effectiveK <= 0) {
                continue;
            }

            float[] distances = new float[effectiveK];
            long[] labels = new long[effectiveK];

            if (filterIds != null) {
                index.searchWithFilter(
                        queryVector, 1, effectiveK, distances, labels, filterIds, searchOptions);
            } else {
                index.search(queryVector, 1, effectiveK, distances, labels, searchOptions);
            }

            for (int i = 0; i < effectiveK; i++) {
                long rowId = labels[i];
                if (rowId < 0) {
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
        return new LuminaScoredGlobalIndexResult(roaringBitmap64, id2scores);
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
        LuminaIndex index = null;
        try (SeekableInputStream in = fileReader.getInputStream(ioMeta)) {
            index = loadIndex(in, position);
            indices[position] = index;
        } catch (Exception e) {
            IOUtils.closeQuietly(index);
            throw e;
        }
    }

    private LuminaIndex loadIndex(SeekableInputStream in, int position) throws IOException {
        File rawIndexFile =
                Files.createTempFile("paimon-lumina-" + UUID.randomUUID(), ".lmi").toFile();
        localIndexFiles.add(rawIndexFile);

        try (FileOutputStream fos = new FileOutputStream(rawIndexFile)) {
            byte[] buffer = new byte[32768];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                fos.write(buffer, 0, bytesRead);
            }
        }

        LuminaIndexMeta meta = indexMetas[position];
        LuminaVectorMetric metric = LuminaVectorMetric.fromValue(meta.metricValue());
        LuminaIndexType indexType = meta.indexType();

        // Searcher only accepts index.dimension and index.type per Lumina API.
        // Do not pass builder-only options like encoding.type.
        Map<String, String> extraOptions = new LinkedHashMap<>();

        return LuminaIndex.fromFile(rawIndexFile, meta.dim(), metric, indexType, extraOptions);
    }

    @Override
    public void close() throws IOException {
        Throwable firstException = null;

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

        for (File localFile : localIndexFiles) {
            try {
                if (localFile != null && localFile.exists()) {
                    localFile.delete();
                }
            } catch (Throwable t) {
                if (firstException == null) {
                    firstException = t;
                } else {
                    firstException.addSuppressed(t);
                }
            }
        }
        localIndexFiles.clear();

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
