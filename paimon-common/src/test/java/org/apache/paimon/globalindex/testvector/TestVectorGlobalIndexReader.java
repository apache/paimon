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

package org.apache.paimon.globalindex.testvector;

import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.utils.RoaringNavigableMap64;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;

/**
 * Test vector index reader that performs brute-force linear scan for similarity search. Loads all
 * vectors into memory and computes distances against every stored vector.
 *
 * <p>Supported distance metrics:
 *
 * <ul>
 *   <li>{@code l2} - score = 1 / (1 + L2_distance)
 *   <li>{@code cosine} - score = cosine_similarity (i.e. 1 - cosine_distance)
 *   <li>{@code inner_product} - score = dot_product
 * </ul>
 */
public class TestVectorGlobalIndexReader implements GlobalIndexReader {

    private final GlobalIndexFileReader fileReader;
    private final GlobalIndexIOMeta ioMeta;
    private final String metric;

    private float[][] vectors;
    private int dimension;
    private int count;

    public TestVectorGlobalIndexReader(
            GlobalIndexFileReader fileReader, GlobalIndexIOMeta ioMeta, String metric) {
        this.fileReader = fileReader;
        this.ioMeta = ioMeta;
        this.metric = metric;
    }

    @Override
    public Optional<ScoredGlobalIndexResult> visitVectorSearch(VectorSearch vectorSearch) {
        try {
            ensureLoaded();
        } catch (IOException e) {
            throw new RuntimeException("Failed to load test vector index", e);
        }

        float[] queryVector = vectorSearch.vector();
        if (queryVector.length != dimension) {
            throw new IllegalArgumentException(
                    String.format(
                            "Query vector dimension mismatch: index expects %d, but got %d",
                            dimension, queryVector.length));
        }

        int limit = vectorSearch.limit();
        int effectiveK = Math.min(limit, count);
        if (effectiveK <= 0) {
            return Optional.empty();
        }

        RoaringNavigableMap64 includeRowIds = vectorSearch.includeRowIds();

        // Min-heap: smallest score at head, so we evict the weakest candidate.
        PriorityQueue<ScoredRow> topK =
                new PriorityQueue<>(effectiveK + 1, Comparator.comparingDouble(s -> s.score));

        for (int i = 0; i < count; i++) {
            if (includeRowIds != null && !includeRowIds.contains(i)) {
                continue;
            }
            float score = computeScore(queryVector, vectors[i]);
            if (topK.size() < effectiveK) {
                topK.offer(new ScoredRow(i, score));
            } else if (score > topK.peek().score) {
                topK.poll();
                topK.offer(new ScoredRow(i, score));
            }
        }

        RoaringNavigableMap64 resultBitmap = new RoaringNavigableMap64();
        Map<Long, Float> scoreMap = new HashMap<>(topK.size());
        for (ScoredRow row : topK) {
            resultBitmap.add(row.rowId);
            scoreMap.put(row.rowId, row.score);
        }

        return Optional.of(ScoredGlobalIndexResult.create(() -> resultBitmap, scoreMap::get));
    }

    private float computeScore(float[] query, float[] stored) {
        switch (metric) {
            case "l2":
                return computeL2Score(query, stored);
            case "cosine":
                return computeCosineScore(query, stored);
            case "inner_product":
                return computeInnerProductScore(query, stored);
            default:
                throw new IllegalArgumentException("Unknown metric: " + metric);
        }
    }

    private static float computeL2Score(float[] a, float[] b) {
        float sumSq = 0;
        for (int i = 0; i < a.length; i++) {
            float diff = a[i] - b[i];
            sumSq += diff * diff;
        }
        return 1.0f / (1.0f + sumSq);
    }

    private static float computeCosineScore(float[] a, float[] b) {
        float dot = 0, normA = 0, normB = 0;
        for (int i = 0; i < a.length; i++) {
            dot += a[i] * b[i];
            normA += a[i] * a[i];
            normB += b[i] * b[i];
        }
        float denominator = (float) (Math.sqrt(normA) * Math.sqrt(normB));
        if (denominator == 0) {
            return 0;
        }
        return dot / denominator;
    }

    private static float computeInnerProductScore(float[] a, float[] b) {
        float dot = 0;
        for (int i = 0; i < a.length; i++) {
            dot += a[i] * b[i];
        }
        return dot;
    }

    private void ensureLoaded() throws IOException {
        if (vectors != null) {
            return;
        }

        try (SeekableInputStream in = fileReader.getInputStream(ioMeta)) {
            // Read header: dimension (4 bytes) + count (4 bytes)
            byte[] headerBytes = new byte[8];
            readFully(in, headerBytes);
            ByteBuffer header = ByteBuffer.wrap(headerBytes);
            header.order(ByteOrder.LITTLE_ENDIAN);
            dimension = header.getInt();
            count = header.getInt();

            // Read vectors
            vectors = new float[count][dimension];
            byte[] vectorBytes = new byte[dimension * Float.BYTES];
            for (int i = 0; i < count; i++) {
                readFully(in, vectorBytes);
                ByteBuffer vectorBuf = ByteBuffer.wrap(vectorBytes);
                vectorBuf.order(ByteOrder.LITTLE_ENDIAN);
                for (int j = 0; j < dimension; j++) {
                    vectors[i][j] = vectorBuf.getFloat();
                }
            }
        }
    }

    private static void readFully(SeekableInputStream in, byte[] buf) throws IOException {
        int offset = 0;
        while (offset < buf.length) {
            int bytesRead = in.read(buf, offset, buf.length - offset);
            if (bytesRead < 0) {
                throw new IOException(
                        "Unexpected end of stream: read "
                                + offset
                                + " bytes but expected "
                                + buf.length);
            }
            offset += bytesRead;
        }
    }

    @Override
    public void close() throws IOException {
        vectors = null;
    }

    // =================== unsupported predicate operations =====================

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
