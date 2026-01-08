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

package org.apache.paimon.faiss.index;

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

import java.io.DataInputStream;
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
 * Vector global index reader using FAISS.
 *
 * <p>This implementation uses FAISS for efficient approximate nearest neighbor search.
 */
public class FaissVectorGlobalIndexReader implements GlobalIndexReader {

    private static final int VERSION = 1;

    private final List<FaissIndex> indices;
    private final List<File> localIndexFiles;
    private final List<GlobalIndexIOMeta> ioMetas;
    private final GlobalIndexFileReader fileReader;
    private final DataType fieldType;
    private final FaissVectorIndexOptions options;
    private volatile boolean indicesLoaded = false;

    public FaissVectorGlobalIndexReader(
            GlobalIndexFileReader fileReader,
            List<GlobalIndexIOMeta> ioMetas,
            DataType fieldType,
            FaissVectorIndexOptions options) {
        this.fileReader = fileReader;
        this.ioMetas = ioMetas;
        this.fieldType = fieldType;
        this.options = options;
        this.indices = new ArrayList<>();
        this.localIndexFiles = new ArrayList<>();
    }

    @Override
    public Optional<GlobalIndexResult> visitVectorSearch(VectorSearch vectorSearch) {
        try {
            ensureLoadIndices();
            return Optional.ofNullable(search(vectorSearch));
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format(
                            "Failed to search FAISS vector index with fieldName=%s, limit=%d",
                            vectorSearch.fieldName(), vectorSearch.limit()),
                    e);
        }
    }

    private GlobalIndexResult search(VectorSearch vectorSearch) throws IOException {
        validateVectorType(vectorSearch.vector());
        float[] queryVector = ((float[]) vectorSearch.vector()).clone();
        // L2 normalize the query vector if enabled
        if (options.normalize()) {
            normalizeL2(queryVector);
        }
        int limit = vectorSearch.limit();

        // Collect results from all indices using a min-heap
        PriorityQueue<ScoredRow> result =
                new PriorityQueue<>(Comparator.comparingDouble(sr -> sr.score));

        RoaringNavigableMap64 includeRowIds = vectorSearch.includeRowIds();

        // When filtering is enabled, we need to fetch more results to ensure
        // we have enough after filtering. Use a multiplier based on index size.
        int searchK = limit;
        if (includeRowIds != null) {
            // Fetch more results when filtering - up to searchFactor * limit or all filtered IDs
            searchK =
                    Math.max(
                            limit * options.searchFactor(),
                            (int) includeRowIds.getLongCardinality());
        }

        for (FaissIndex index : indices) {
            // Configure search parameters based on index type
            configureSearchParams(index);

            // Limit searchK to the index size
            int effectiveK = (int) Math.min(searchK, index.size());
            if (effectiveK <= 0) {
                continue;
            }

            // Allocate result arrays
            float[] distances = new float[effectiveK];
            long[] labels = new long[effectiveK];

            // Perform search
            index.search(queryVector, 1, effectiveK, distances, labels);

            for (int i = 0; i < effectiveK; i++) {
                long rowId = labels[i];
                if (rowId < 0) {
                    // Invalid result (not enough neighbors)
                    continue;
                }

                // Filter by include row IDs if specified
                if (includeRowIds != null && !includeRowIds.contains(rowId)) {
                    continue;
                }

                // Convert distance to score (higher is better for similarity)
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
        return new FaissVectorSearchGlobalIndexResult(roaringBitmap64, id2scores);
    }

    private void configureSearchParams(FaissIndex index) {
        switch (index.indexType()) {
            case HNSW:
                index.setHnswEfSearch(options.efSearch());
                break;
            case IVF:
            case IVF_PQ:
            case IVF_SQ8:
                // For small indices, use higher nprobe to ensure enough results
                // Use at least nprobe or 10% of index size, whichever is larger
                int effectiveNprobe =
                        Math.max(options.nprobe(), (int) Math.max(1, index.size() / 10));
                index.setIvfNprobe(effectiveNprobe);
                break;
            default:
                // No special configuration needed
                break;
        }
    }

    private float convertDistanceToScore(float distance) {
        // For L2 distance, smaller is better, so we invert it
        // For inner product, larger is better (already a similarity)
        if (options.metric() == FaissVectorMetric.L2) {
            // Convert L2 distance to similarity score
            return 1.0f / (1.0f + distance);
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
                    "FAISS currently only supports float arrays, but field type is: " + fieldType);
        }
    }

    private void ensureLoadIndices() throws IOException {
        if (!indicesLoaded) {
            synchronized (this) {
                if (!indicesLoaded) {
                    for (GlobalIndexIOMeta meta : ioMetas) {
                        FaissIndex index = null;
                        try (SeekableInputStream in = fileReader.getInputStream(meta.fileName())) {
                            index = loadIndex(in);
                            indices.add(index);
                        } catch (Exception e) {
                            IOUtils.closeQuietly(index);
                            throw e;
                        }
                    }
                    indicesLoaded = true;
                }
            }
        }
    }

    private FaissIndex loadIndex(SeekableInputStream in) throws IOException {
        DataInputStream dataIn = new DataInputStream(in);
        int version = dataIn.readInt();
        if (version != VERSION) {
            throw new IOException("Unsupported FAISS index version: " + version);
        }

        // Read header fields (required for file format compatibility)
        dataIn.readInt(); // dim
        dataIn.readInt(); // metricValue
        dataIn.readInt(); // indexTypeOrdinal
        dataIn.readLong(); // numVectors
        long indexDataLength = dataIn.readLong();

        // Create a temp file for the raw FAISS index data (for mmap loading)
        File rawIndexFile =
                Files.createTempFile("paimon-faiss-raw-" + UUID.randomUUID(), ".faiss").toFile();
        localIndexFiles.add(rawIndexFile);

        // Write raw FAISS index data to the temp file
        try (FileOutputStream fos = new FileOutputStream(rawIndexFile)) {
            byte[] buffer = new byte[32768];
            long remaining = indexDataLength;
            while (remaining > 0) {
                int toRead = (int) Math.min(buffer.length, remaining);
                dataIn.readFully(buffer, 0, toRead);
                fos.write(buffer, 0, toRead);
                remaining -= toRead;
            }
        }
        // Load via mmap from the raw index file
        return FaissIndex.fromFile(rawIndexFile);
    }

    /**
     * L2 normalize the vector in place.
     *
     * @param vector the vector to normalize
     */
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

        // Close all FAISS indices
        for (FaissIndex index : indices) {
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

        // Delete local temporary files
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
                        "Failed to close FAISS vector global index reader", firstException);
            }
        }
    }

    /** Helper class to store row ID with its score. */
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
