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

import org.apache.paimon.data.InternalArray;
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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.LongBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Vector global index writer using FAISS.
 *
 * <p>This implementation uses FAISS for efficient approximate nearest neighbor search with support
 * for multiple index types including Flat, HNSW, IVF, and IVF-PQ.
 *
 * <p>Vectors are added to the index in batches. When the current index reaches {@code sizePerIndex}
 * vectors, it is serialized to a file and a new index is created.
 */
public class FaissVectorGlobalIndexWriter implements GlobalIndexSingletonWriter {

    private static final int VERSION = 1;
    private static final int DEFAULT_BATCH_SIZE = 10000;

    private final GlobalIndexFileWriter fileWriter;
    private final FaissVectorIndexOptions options;
    private final int sizePerIndex;
    private final int batchSize;
    private final int dim;
    private final DataType fieldType;

    private long count = 0;
    private long currentIndexCount = 0;
    private final List<VectorEntry> pendingBatch;
    private final List<ResultEntry> results;
    private FaissIndex currentIndex;
    private boolean trained = false;

    public FaissVectorGlobalIndexWriter(
            GlobalIndexFileWriter fileWriter, DataType fieldType, FaissVectorIndexOptions options) {
        this.fileWriter = fileWriter;
        this.fieldType = fieldType;
        this.options = options;
        this.sizePerIndex = options.sizePerIndex();
        this.batchSize = Math.min(DEFAULT_BATCH_SIZE, sizePerIndex);
        this.dim = options.dimension();
        this.pendingBatch = new ArrayList<>(batchSize);
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
    public void write(Object fieldData) {
        float[] vector;
        if (fieldData instanceof float[]) {
            vector = (float[]) fieldData;
        } else if (fieldData instanceof InternalArray) {
            vector = ((InternalArray) fieldData).toFloatArray();
        } else {
            throw new RuntimeException(
                    "Unsupported vector type: " + fieldData.getClass().getName());
        }
        checkDimension(vector);
        // L2 normalize the vector if enabled
        if (options.normalize()) {
            normalizeL2(vector);
        }
        pendingBatch.add(new VectorEntry(count, vector));
        count++;

        try {
            // When batch is full, add to current index
            if (pendingBatch.size() >= batchSize) {
                addBatchToIndex();
            }
            // When current index reaches sizePerIndex, flush to file
            if (currentIndexCount >= sizePerIndex) {
                flushCurrentIndex();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<ResultEntry> finish() {
        try {
            // Add any remaining pending batch to current index
            if (!pendingBatch.isEmpty()) {
                addBatchToIndex();
            }
            // Flush current index if it has data
            if (currentIndex != null && currentIndexCount > 0) {
                flushCurrentIndex();
            }
            return results;
        } catch (IOException e) {
            throw new RuntimeException("Failed to write FAISS vector global index", e);
        }
    }

    private void addBatchToIndex() throws IOException {
        if (pendingBatch.isEmpty()) {
            return;
        }

        // Create index if not exists
        if (currentIndex == null) {
            currentIndex = createIndex();
            trained = false;
        }

        int n = pendingBatch.size();

        // Train if necessary (for IVF-based indices) - only once per index
        if (!trained && !currentIndex.isTrained()) {
            int trainingSize = Math.min(n, options.trainingSize());
            ByteBuffer trainingBuffer = FaissIndex.allocateVectorBuffer(trainingSize, dim);
            FloatBuffer trainingFloatView = trainingBuffer.asFloatBuffer();
            for (int i = 0; i < trainingSize; i++) {
                float[] vector = pendingBatch.get(i).vector;
                for (int j = 0; j < dim; j++) {
                    trainingFloatView.put(i * dim + j, vector[j]);
                }
            }
            currentIndex.train(trainingBuffer, trainingSize);
            trained = true;
        }

        // Allocate buffers for this batch only
        ByteBuffer vectorBuffer = FaissIndex.allocateVectorBuffer(n, dim);
        ByteBuffer idBuffer = FaissIndex.allocateIdBuffer(n);
        FloatBuffer floatView = vectorBuffer.asFloatBuffer();
        LongBuffer longView = idBuffer.asLongBuffer();

        // Fill buffers
        for (int i = 0; i < n; i++) {
            VectorEntry entry = pendingBatch.get(i);
            float[] vector = entry.vector;
            for (int j = 0; j < dim; j++) {
                floatView.put(i * dim + j, vector[j]);
            }
            longView.put(i, entry.id);
        }

        // Add to index
        currentIndex.addWithIds(vectorBuffer, idBuffer, n);
        currentIndexCount += n;
        pendingBatch.clear();
    }

    private void flushCurrentIndex() throws IOException {
        if (currentIndex == null || currentIndexCount == 0) {
            return;
        }

        String fileName = fileWriter.newFileName(FaissVectorGlobalIndexerFactory.IDENTIFIER);
        try (OutputStream out = new BufferedOutputStream(fileWriter.newOutputStream(fileName))) {
            // Serialize the index
            long serializeSize = currentIndex.serializeSize();
            ByteBuffer serializeBuffer =
                    ByteBuffer.allocateDirect((int) serializeSize).order(ByteOrder.nativeOrder());
            long bytesWritten = currentIndex.serialize(serializeBuffer);

            // Write to output stream with metadata
            DataOutputStream dataOut = new DataOutputStream(out);
            dataOut.writeInt(VERSION);
            dataOut.writeInt(dim);
            dataOut.writeInt(options.metric().getValue());
            dataOut.writeInt(options.indexType().ordinal());
            dataOut.writeLong(currentIndexCount);
            dataOut.writeLong(bytesWritten);

            // Write serialized index data
            byte[] indexData = new byte[(int) bytesWritten];
            serializeBuffer.rewind();
            serializeBuffer.get(indexData);
            dataOut.write(indexData);
            dataOut.flush();
        }
        results.add(new ResultEntry(fileName, count, null));

        // Close and reset
        currentIndex.close();
        currentIndex = null;
        currentIndexCount = 0;
        trained = false;
    }

    private FaissIndex createIndex() {
        int dim = options.dimension();
        FaissVectorMetric metric = options.metric();
        // Auto-calculate nlist: min(configured nlist, sqrt(sizePerIndex))
        // This ensures nlist is appropriate for the expected data size
        int effectiveNlist = Math.max(1, Math.min(options.nlist(), (int) Math.sqrt(sizePerIndex)));

        switch (options.indexType()) {
            case FLAT:
                return FaissIndex.createFlatIndex(dim, metric);
            case HNSW:
                return FaissIndex.createHnswIndex(
                        dim, options.m(), options.efConstruction(), metric);
            case IVF:
                return FaissIndex.createIvfIndex(dim, effectiveNlist, metric);
            case IVF_PQ:
                return FaissIndex.createIvfPqIndex(
                        dim, effectiveNlist, options.pqM(), options.pqNbits(), metric);
            case IVF_SQ8:
                return FaissIndex.createIvfSq8Index(dim, effectiveNlist, metric);
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
