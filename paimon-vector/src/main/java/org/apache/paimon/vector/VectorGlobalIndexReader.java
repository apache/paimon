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

package org.apache.paimon.vector;

import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.types.DataType;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Vector global index reader.
 *
 * <p>This is a framework implementation that provides the structure for vector search. In
 * production, integrate with JVector or other vector search engines.
 */
public class VectorGlobalIndexReader implements GlobalIndexReader {

    private final List<IndexShard> shards;

    public VectorGlobalIndexReader(
            GlobalIndexFileReader fileReader,
            List<GlobalIndexIOMeta> files,
            DataType fieldType,
            Options options)
            throws IOException {
        this.shards = new ArrayList<>();
        loadShards(fileReader, files);
    }

    private void loadShards(GlobalIndexFileReader fileReader, List<GlobalIndexIOMeta> files)
            throws IOException {
        for (GlobalIndexIOMeta meta : files) {
            try (SeekableInputStream in = fileReader.getInputStream(meta.fileName())) {
                byte[] indexBytes = new byte[(int) meta.fileSize()];
                int totalRead = 0;
                while (totalRead < indexBytes.length) {
                    int read = in.read(indexBytes, totalRead, indexBytes.length - totalRead);
                    if (read == -1) {
                        throw new IOException("Unexpected end of stream");
                    }
                    totalRead += read;
                }

                IndexShard shard = deserializeIndex(indexBytes, meta);
                shards.add(shard);
            }
        }
    }

    private IndexShard deserializeIndex(byte[] indexBytes, GlobalIndexIOMeta meta)
            throws IOException {
        ByteArrayInputStream byteIn = new ByteArrayInputStream(indexBytes);
        try (DataInputStream dataIn = new DataInputStream(byteIn)) {
            // Read version
            int version = dataIn.readInt();
            if (version != 1) {
                throw new IOException("Unsupported index version: " + version);
            }

            // Read row IDs
            int size = dataIn.readInt();
            long[] rowIds = new long[size];
            for (int i = 0; i < size; i++) {
                rowIds[i] = dataIn.readLong();
            }

            // Read vectors
            List<float[]> vectors = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                int dim = dataIn.readInt();
                float[] vector = new float[dim];
                for (int j = 0; j < dim; j++) {
                    vector[j] = dataIn.readFloat();
                }
                vectors.add(vector);
            }

            // Parse metadata
            byte[] metaBytes = meta.metadata();
            VectorIndexMetadata metadata = deserializeMetadata(metaBytes);

            return new IndexShard(rowIds, vectors, metadata, meta.rowIdRange().from);
        }
    }

    private VectorIndexMetadata deserializeMetadata(byte[] metaBytes) throws IOException {
        if (metaBytes == null || metaBytes.length == 0) {
            return new VectorIndexMetadata(128, "COSINE", 16, 100);
        }

        ByteArrayInputStream byteIn = new ByteArrayInputStream(metaBytes);
        try (DataInputStream dataIn = new DataInputStream(byteIn)) {
            int dimension = dataIn.readInt();
            String metricName = dataIn.readUTF();
            int m = dataIn.readInt();
            int efConstruction = dataIn.readInt();
            return new VectorIndexMetadata(dimension, metricName, m, efConstruction);
        }
    }

    /**
     * Search for similar vectors.
     *
     * @param query query vector
     * @param k number of results
     * @return global index result containing row IDs
     */
    public GlobalIndexResult search(float[] query, int k) {
        Set<Long> resultIds = new HashSet<>();

        // TODO: Implement vector similarity search with JVector or other engine
        // For now, return empty results as placeholder
        for (IndexShard shard : shards) {
            // Brute force search as placeholder
            List<ScoredResult> scored = new ArrayList<>();
            for (int i = 0; i < shard.vectors.size() && i < shard.rowIds.length; i++) {
                float[] vec = shard.vectors.get(i);
                float score = cosineSimilarity(query, vec);
                scored.add(new ScoredResult(shard.rowIds[i], score));
            }

            // Sort and take top k
            scored.sort((a, b) -> Float.compare(b.score, a.score));
            for (int i = 0; i < Math.min(k, scored.size()); i++) {
                resultIds.add(scored.get(i).rowId);
            }
        }

        return GlobalIndexResult.wrap(resultIds);
    }

    private float cosineSimilarity(float[] a, float[] b) {
        if (a.length != b.length) {
            return 0.0f;
        }
        float dotProduct = 0.0f;
        float normA = 0.0f;
        float normB = 0.0f;
        for (int i = 0; i < a.length; i++) {
            dotProduct += a[i] * b[i];
            normA += a[i] * a[i];
            normB += b[i] * b[i];
        }
        return dotProduct / (float) (Math.sqrt(normA) * Math.sqrt(normB));
    }

    private static class ScoredResult {
        final long rowId;
        final float score;

        ScoredResult(long rowId, float score) {
            this.rowId = rowId;
            this.score = score;
        }
    }

    @Override
    public void close() throws IOException {
        shards.clear();
    }

    // Implementation of FunctionVisitor methods
    @Override
    public Optional<GlobalIndexResult> visitIsNotNull(FieldRef fieldRef) {
        throw new UnsupportedOperationException(
                "Vector index does not support isNotNull predicate");
    }

    @Override
    public Optional<GlobalIndexResult> visitIsNull(FieldRef fieldRef) {
        throw new UnsupportedOperationException("Vector index does not support isNull predicate");
    }

    @Override
    public Optional<GlobalIndexResult> visitStartsWith(FieldRef fieldRef, Object literal) {
        throw new UnsupportedOperationException(
                "Vector index does not support startsWith predicate");
    }

    @Override
    public Optional<GlobalIndexResult> visitEndsWith(FieldRef fieldRef, Object literal) {
        throw new UnsupportedOperationException("Vector index does not support endsWith predicate");
    }

    @Override
    public Optional<GlobalIndexResult> visitContains(FieldRef fieldRef, Object literal) {
        throw new UnsupportedOperationException("Vector index does not support contains predicate");
    }

    @Override
    public Optional<GlobalIndexResult> visitLessThan(FieldRef fieldRef, Object literal) {
        throw new UnsupportedOperationException("Vector index does not support lessThan predicate");
    }

    @Override
    public Optional<GlobalIndexResult> visitGreaterOrEqual(FieldRef fieldRef, Object literal) {
        throw new UnsupportedOperationException(
                "Vector index does not support greaterOrEqual predicate");
    }

    @Override
    public Optional<GlobalIndexResult> visitNotEqual(FieldRef fieldRef, Object literal) {
        throw new UnsupportedOperationException("Vector index does not support notEqual predicate");
    }

    @Override
    public Optional<GlobalIndexResult> visitLessOrEqual(FieldRef fieldRef, Object literal) {
        throw new UnsupportedOperationException(
                "Vector index does not support lessOrEqual predicate");
    }

    @Override
    public Optional<GlobalIndexResult> visitEqual(FieldRef fieldRef, Object literal) {
        throw new UnsupportedOperationException("Vector index does not support equal predicate");
    }

    @Override
    public Optional<GlobalIndexResult> visitGreaterThan(FieldRef fieldRef, Object literal) {
        throw new UnsupportedOperationException(
                "Vector index does not support greaterThan predicate");
    }

    @Override
    public Optional<GlobalIndexResult> visitIn(FieldRef fieldRef, List<Object> literals) {
        throw new UnsupportedOperationException("Vector index does not support in predicate");
    }

    @Override
    public Optional<GlobalIndexResult> visitNotIn(FieldRef fieldRef, List<Object> literals) {
        throw new UnsupportedOperationException("Vector index does not support notIn predicate");
    }

    private static class IndexShard {
        final long[] rowIds;
        final List<float[]> vectors;
        final VectorIndexMetadata metadata;
        final long rowRangeStart;

        IndexShard(
                long[] rowIds,
                List<float[]> vectors,
                VectorIndexMetadata metadata,
                long rowRangeStart) {
            this.rowIds = rowIds;
            this.vectors = vectors;
            this.metadata = metadata;
            this.rowRangeStart = rowRangeStart;
        }
    }

    private static class VectorIndexMetadata {
        final int dimension;
        final String similarityFunction;
        final int m;
        final int efConstruction;

        VectorIndexMetadata(int dimension, String similarityFunction, int m, int efConstruction) {
            this.dimension = dimension;
            this.similarityFunction = similarityFunction;
            this.m = m;
            this.efConstruction = efConstruction;
        }
    }
}
