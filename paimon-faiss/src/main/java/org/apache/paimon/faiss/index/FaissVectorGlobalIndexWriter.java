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

import org.apache.paimon.globalindex.GlobalIndexSingletonWriter;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.FloatType;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Vector global index writer using FAISS.
 *
 * <p>This implementation uses FAISS for efficient approximate nearest neighbor search with support
 * for multiple index types including Flat, HNSW, IVF, and IVF-PQ.
 */
public class FaissVectorGlobalIndexWriter implements GlobalIndexSingletonWriter {

    private static final int VERSION = 1;

    private final GlobalIndexFileWriter fileWriter;
    private final FaissVectorIndexOptions options;
    private final int sizePerIndex;
    private final DataType fieldType;

    private long count = 0;
    private final List<VectorEntry> vectorEntries;
    private final List<ResultEntry> results;

    public FaissVectorGlobalIndexWriter(
            GlobalIndexFileWriter fileWriter, DataType fieldType, FaissVectorIndexOptions options) {
        this.fileWriter = fileWriter;
        this.fieldType = fieldType;
        this.options = options;
        this.sizePerIndex = options.sizePerIndex();
        this.vectorEntries = new ArrayList<>();
        this.results = new ArrayList<>();

        validateFieldType(fieldType);
    }

    private void validateFieldType(DataType dataType) {
        if (!(dataType instanceof ArrayType)) {
            throw new IllegalArgumentException(
                    "FAISS vector index requires ArrayType, but got: " + dataType);
        }
        DataType elementType = ((ArrayType) dataType).getElementType();
        if (!(elementType instanceof FloatType)) {
            throw new IllegalArgumentException(
                    "FAISS vector index requires float array, but got: " + elementType);
        }
    }

    @Override
    public void write(Object key) {
        float[] vector = (float[]) key;
        checkDimension(vector);
        vectorEntries.add(new VectorEntry(count, vector));

        if (vectorEntries.size() >= sizePerIndex) {
            try {
                flush();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        count++;
    }

    @Override
    public List<ResultEntry> finish() {
        try {
            if (!vectorEntries.isEmpty()) {
                flush();
            }
            return results;
        } catch (IOException e) {
            throw new RuntimeException("Failed to write FAISS vector global index", e);
        }
    }

    private void flush() throws IOException {
        String fileName = fileWriter.newFileName(FaissVectorGlobalIndexerFactory.IDENTIFIER);
        try (OutputStream out = new BufferedOutputStream(fileWriter.newOutputStream(fileName))) {
            buildIndex(vectorEntries, out);
        }
        results.add(new ResultEntry(fileName, count, null));
        vectorEntries.clear();
    }

    private void buildIndex(List<VectorEntry> entries, OutputStream out) throws IOException {
        int n = entries.size();
        int dim = options.dimension();

        // Create the FAISS index based on configuration
        FaissIndex index = createIndex();

        try {
            // Prepare vectors and IDs
            float[][] vectors = new float[n][dim];
            long[] ids = new long[n];
            for (int i = 0; i < n; i++) {
                vectors[i] = entries.get(i).vector;
                ids[i] = entries.get(i).id;
            }

            // Train if necessary (for IVF-based indices)
            if (!index.isTrained()) {
                int trainingSize = Math.min(n, options.trainingSize());
                float[][] trainingVectors = new float[trainingSize][dim];
                System.arraycopy(vectors, 0, trainingVectors, 0, trainingSize);
                index.train(trainingVectors);
            }

            // Add vectors with IDs
            index.addWithIds(vectors, ids);

            // Serialize the index
            byte[] indexData = index.toBytes();

            // Write to output stream with metadata
            DataOutputStream dataOut = new DataOutputStream(out);
            dataOut.writeInt(VERSION);
            dataOut.writeInt(dim);
            dataOut.writeInt(options.metric().getValue());
            dataOut.writeInt(options.indexType().ordinal());
            dataOut.writeLong(n);
            dataOut.writeInt(indexData.length);
            dataOut.write(indexData);
            dataOut.flush();
        } finally {
            index.close();
        }
    }

    private FaissIndex createIndex() {
        int dim = options.dimension();
        FaissVectorMetric metric = options.metric();

        switch (options.indexType()) {
            case FLAT:
                return FaissIndex.createFlatIndex(dim, metric);
            case HNSW:
                return FaissIndex.createHnswIndex(
                        dim, options.m(), options.efConstruction(), metric);
            case IVF:
                return FaissIndex.createIvfIndex(dim, options.nlist(), metric);
            case IVF_PQ:
                return FaissIndex.createIvfPqIndex(
                        dim, options.nlist(), options.pqM(), options.pqNbits(), metric);
            default:
                throw new IllegalArgumentException(
                        "Unsupported index type: " + options.indexType());
        }
    }

    private void checkDimension(float[] vector) {
        if (vector.length != options.dimension()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Vector dimension mismatch: expected %d, but got %d",
                            options.dimension(), vector.length));
        }
    }

    /** Entry holding a vector and its row ID. */
    private static class VectorEntry {
        final long id;
        final float[] vector;

        VectorEntry(long id, float[] vector) {
            this.id = id;
            this.vector = vector;
        }
    }
}
