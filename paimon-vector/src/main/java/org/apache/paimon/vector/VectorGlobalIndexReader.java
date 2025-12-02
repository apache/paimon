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

import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Vector global index reader using JVector.
 *
 * <p>This implementation uses JVector's HNSW graph for efficient approximate nearest neighbor
 * search.
 */
public class VectorGlobalIndexReader implements GlobalIndexReader {

    private final List<IndexShard> shards;
    private final String similarityFunction;
    private final int dimension;
    private final int m;
    private final int efConstruction;

    public VectorGlobalIndexReader(
            GlobalIndexFileReader fileReader,
            List<GlobalIndexIOMeta> files,
            DataType fieldType,
            Options options)
            throws IOException {
        this.shards = new ArrayList<>();
        VectorIndexOptions vectorOptions = new VectorIndexOptions(options);
        this.similarityFunction = vectorOptions.metric();
        this.dimension = vectorOptions.dimension();
        this.m = vectorOptions.m();
        this.efConstruction = vectorOptions.efConstruction();
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
            VectorIndexMetadata metadata = VectorIndexMetadata.deserializeMetadata(metaBytes);

            if (dataIn.available() > 0) {
                var vectorTypeSupport = VectorizationProvider.getInstance().getVectorTypeSupport();
                List<VectorFloat<?>> vectorFloats = new ArrayList<>();
                for (float[] vec : vectors) {
                    vectorFloats.add(vectorTypeSupport.createFloatVector(vec));
                }

                // Create RandomAccessVectorValues
                RandomAccessVectorValues vectorValues =
                        new PaimonRandomAccessVectorValues(
                                vectorFloats.size(), metadata.dimension(), vectorFloats, false);

                // Rebuild graph using stored structure or rebuild from scratch
                VectorSimilarityFunction similarityFunction =
                        VectorSimilarityFunction.valueOf(metadata.similarityFunction());
                try (GraphIndexBuilder builder =
                        new GraphIndexBuilder(
                                vectorValues,
                                similarityFunction,
                                metadata.m(),
                                metadata.efConstruction(),
                                1.0f, // todo: need conf
                                1.0f)) { // todo: need conf
                    return new IndexShard(
                            rowIds,
                            vectors,
                            metadata,
                            meta.rowIdRange().from,
                            builder.build(vectorValues));
                }
            }
            return new IndexShard(rowIds, vectors, metadata, meta.rowIdRange().from, null);
        }
    }

    /**
     * Search for similar vectors using JVector HNSW.
     *
     * @param query query vector
     * @param k number of results
     * @return global index result containing row IDs
     */
    public GlobalIndexResult search(float[] query, int k) {
        Set<Long> resultIds = new HashSet<>();
        var vectorTypeSupport = VectorizationProvider.getInstance().getVectorTypeSupport();
        VectorFloat<?> queryVector = vectorTypeSupport.createFloatVector(query);

        for (IndexShard shard : shards) {
            try {
                if (shard.graphIndex == null) {
                    // Fall back to brute force if no graph index
                    continue;
                }

                // Use JVector's GraphSearcher for efficient ANN search
                VectorSimilarityFunction similarityFunction =
                        VectorSimilarityFunction.valueOf(shard.metadata.similarityFunction());

                // Create vector values for search context
                var vectorTypeSupportForIndex =
                        VectorizationProvider.getInstance().getVectorTypeSupport();
                List<VectorFloat<?>> vectorFloats = new ArrayList<>();
                for (float[] vec : shard.vectors) {
                    vectorFloats.add(vectorTypeSupportForIndex.createFloatVector(vec));
                }
                RandomAccessVectorValues vectorValues =
                        new PaimonRandomAccessVectorValues(
                                vectorFloats.size(),
                                shard.metadata.dimension(),
                                vectorFloats,
                                false);
                // Search using static method
                SearchResult result =
                        GraphSearcher.search(
                                queryVector,
                                k,
                                vectorValues,
                                similarityFunction,
                                shard.graphIndex,
                                Bits.ALL);

                // Collect row IDs from results
                var nodes = result.getNodes();
                for (int i = 0; i < nodes.length && i < k; i++) {
                    // todo: could get score here: nodeScore.score
                    SearchResult.NodeScore nodeScore = nodes[i];
                    int nodeId = nodeScore.node;
                    if (nodeId >= 0 && nodeId < shard.rowIds.length) {
                        resultIds.add(shard.rowIds[nodeId]);
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to search vector index", e);
            }
        }

        return GlobalIndexResult.wrap(resultIds);
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
}
