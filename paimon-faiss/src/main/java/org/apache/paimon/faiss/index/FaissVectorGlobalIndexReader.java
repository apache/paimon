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
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Vector global index reader using FAISS.
 *
 * <p>This implementation uses FAISS for efficient approximate nearest neighbor search.
 */
public class FaissVectorGlobalIndexReader implements GlobalIndexReader {

    private static final int VERSION = 1;

    private final List<FaissIndex> indices;
    private final List<GlobalIndexIOMeta> ioMetas;
    private final GlobalIndexFileReader fileReader;
    private final GlobalIndexResult defaultResult;
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
        this.defaultResult = GlobalIndexResult.fromRange(new Range(0, ioMetas.get(0).rangeEnd()));
    }

    @Override
    public GlobalIndexResult visitVectorSearch(VectorSearch vectorSearch) {
        try {
            ensureLoadIndices();
            return search(vectorSearch);
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
        float[] queryVector = (float[]) vectorSearch.vector();
        int limit = vectorSearch.limit();

        // Collect results from all indices using a min-heap
        PriorityQueue<ScoredRow> result =
                new PriorityQueue<>(Comparator.comparingDouble(sr -> sr.score));

        RoaringNavigableMap64 includeRowIds = vectorSearch.includeRowIds();

        // When filtering is enabled, we need to fetch more results to ensure
        // we have enough after filtering. Use a multiplier based on index size.
        int searchK = limit;
        if (includeRowIds != null) {
            // Fetch more results when filtering - up to 10x the limit or all filtered IDs
            searchK = Math.max(limit * 10, (int) includeRowIds.getLongCardinality());
        }

        for (FaissIndex index : indices) {
            // Configure search parameters based on index type
            configureSearchParams(index);

            // Limit searchK to the index size
            int effectiveK = (int) Math.min(searchK, index.size());
            if (effectiveK <= 0) {
                continue;
            }

            FaissIndex.SearchResult searchResult = index.search(queryVector, effectiveK);
            float[] distances = searchResult.getDistancesForQuery(0);
            long[] labels = searchResult.getLabelsForQuery(0);

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
                index.setIvfNprobe(options.nprobe());
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

        int dim = dataIn.readInt();
        int metricValue = dataIn.readInt();
        int indexTypeOrdinal = dataIn.readInt();
        long numVectors = dataIn.readLong();
        int indexDataLength = dataIn.readInt();

        byte[] indexData = new byte[indexDataLength];
        dataIn.readFully(indexData);

        return FaissIndex.fromBytes(indexData);
    }

    @Override
    public void close() throws IOException {
        Throwable firstException = null;

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
    public GlobalIndexResult visitIsNotNull(FieldRef fieldRef) {
        return defaultResult;
    }

    @Override
    public GlobalIndexResult visitIsNull(FieldRef fieldRef) {
        return defaultResult;
    }

    @Override
    public GlobalIndexResult visitStartsWith(FieldRef fieldRef, Object literal) {
        return defaultResult;
    }

    @Override
    public GlobalIndexResult visitEndsWith(FieldRef fieldRef, Object literal) {
        return defaultResult;
    }

    @Override
    public GlobalIndexResult visitContains(FieldRef fieldRef, Object literal) {
        return defaultResult;
    }

    @Override
    public GlobalIndexResult visitLike(FieldRef fieldRef, Object literal) {
        return defaultResult;
    }

    @Override
    public GlobalIndexResult visitLessThan(FieldRef fieldRef, Object literal) {
        return defaultResult;
    }

    @Override
    public GlobalIndexResult visitGreaterOrEqual(FieldRef fieldRef, Object literal) {
        return defaultResult;
    }

    @Override
    public GlobalIndexResult visitNotEqual(FieldRef fieldRef, Object literal) {
        return defaultResult;
    }

    @Override
    public GlobalIndexResult visitLessOrEqual(FieldRef fieldRef, Object literal) {
        return defaultResult;
    }

    @Override
    public GlobalIndexResult visitEqual(FieldRef fieldRef, Object literal) {
        return defaultResult;
    }

    @Override
    public GlobalIndexResult visitGreaterThan(FieldRef fieldRef, Object literal) {
        return defaultResult;
    }

    @Override
    public GlobalIndexResult visitIn(FieldRef fieldRef, List<Object> literals) {
        return defaultResult;
    }

    @Override
    public GlobalIndexResult visitNotIn(FieldRef fieldRef, List<Object> literals) {
        return defaultResult;
    }
}
