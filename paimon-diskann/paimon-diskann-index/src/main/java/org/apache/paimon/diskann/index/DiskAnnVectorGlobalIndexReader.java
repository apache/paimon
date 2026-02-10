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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.UUID;

/**
 * Vector global index reader using DiskANN.
 *
 * <p>This implementation uses DiskANN for efficient approximate nearest neighbor search. It
 * supports lazy loading of indices and optional memory-mapped file loading for better memory
 * efficiency with large indices.
 */
public class DiskAnnVectorGlobalIndexReader implements GlobalIndexReader {

    private final List<DiskAnnIndex> indices;
    private final List<DiskAnnIndexMeta> indexMetas;
    private final List<File> localIndexFiles;
    private final List<GlobalIndexIOMeta> ioMetas;
    private final GlobalIndexFileReader fileReader;
    private final DataType fieldType;
    private final DiskAnnVectorIndexOptions options;
    private volatile boolean metasLoaded = false;
    private volatile boolean indicesLoaded = false;

    public DiskAnnVectorGlobalIndexReader(
            GlobalIndexFileReader fileReader,
            List<GlobalIndexIOMeta> ioMetas,
            DataType fieldType,
            DiskAnnVectorIndexOptions options) {
        this.fileReader = fileReader;
        this.ioMetas = ioMetas;
        this.fieldType = fieldType;
        this.options = options;
        this.indices = new ArrayList<>();
        this.indexMetas = new ArrayList<>();
        this.localIndexFiles = new ArrayList<>();
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
        if (options.normalize()) {
            normalizeL2(queryVector);
        }
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

        for (DiskAnnIndex index : indices) {
            if (index == null) {
                continue;
            }
            int effectiveK = (int) Math.min(searchK, index.size());
            if (effectiveK <= 0) {
                continue;
            }

            float[] distances = new float[effectiveK];
            long[] labels = new long[effectiveK];

            // Dynamic search list sizing: use max of configured value and effectiveK
            // This follows Milvus best practice: search_list should be >= topk
            int dynamicSearchListSize = Math.max(options.searchListSize(), effectiveK);
            index.search(queryVector, 1, effectiveK, dynamicSearchListSize, distances, labels);

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
            while (indices.size() < ioMetas.size()) {
                indices.add(null);
            }
            for (int pos : positions) {
                if (indices.get(pos) == null) {
                    loadIndexAt(pos);
                }
            }
        }
    }

    private void loadIndexAt(int position) throws IOException {
        GlobalIndexIOMeta ioMeta = ioMetas.get(position);
        DiskAnnIndex index = null;
        try (SeekableInputStream in = fileReader.getInputStream(ioMeta)) {
            index = loadIndex(in);
            if (indices.size() <= position) {
                while (indices.size() < position) {
                    indices.add(null);
                }
                indices.add(index);
            } else {
                indices.set(position, index);
            }
        } catch (Exception e) {
            IOUtils.closeQuietly(index);
            throw e;
        }
    }

    private DiskAnnIndex loadIndex(SeekableInputStream in) throws IOException {
        // For better memory efficiency, write to a temporary file
        // This allows the OS to manage memory more efficiently for large indices
        File tempIndexFile =
                Files.createTempFile("paimon-diskann-" + UUID.randomUUID(), ".index").toFile();
        localIndexFiles.add(tempIndexFile);

        // Copy index data to temp file
        try (FileOutputStream fos = new FileOutputStream(tempIndexFile)) {
            byte[] buffer = new byte[32768];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                fos.write(buffer, 0, bytesRead);
            }
        }

        // Load from file for potential mmap benefits
        // Note: Current implementation still deserializes to memory
        // Future enhancement: Add native file-based loading if supported
        byte[] data = Files.readAllBytes(tempIndexFile.toPath());
        return DiskAnnIndex.deserialize(data, options.metric());
    }

    private void normalizeL2(float[] vector) {
        float norm = 0.0f;
        for (float v : vector) {
            norm += v * v;
        }
        norm = (float) Math.sqrt(norm);
        if (norm > 0) {
            for (int i = 0; i < vector.length; i++) {
                vector[i] /= norm;
            }
        }
    }

    @Override
    public void close() throws IOException {
        Throwable firstException = null;

        // Close all DiskANN indices
        for (DiskAnnIndex index : indices) {
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
        }
        indices.clear();

        // Delete temporary files
        for (File tempFile : localIndexFiles) {
            try {
                if (tempFile != null && tempFile.exists()) {
                    tempFile.delete();
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
