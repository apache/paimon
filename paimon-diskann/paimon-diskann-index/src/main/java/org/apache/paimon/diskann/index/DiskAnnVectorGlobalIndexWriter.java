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

import org.apache.paimon.data.InternalArray;
import org.apache.paimon.globalindex.GlobalIndexSingletonWriter;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.FloatType;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.LongBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Vector global index writer using DiskANN.
 *
 * <p>Vectors are added to the index in batches. When the current index reaches {@code sizePerIndex}
 * vectors, it is built and serialized to a file and a new index is created.
 */
public class DiskAnnVectorGlobalIndexWriter implements GlobalIndexSingletonWriter {

    private static final int DEFAULT_BATCH_SIZE = 10000;

    private final GlobalIndexFileWriter fileWriter;
    private final DiskAnnVectorIndexOptions options;
    private final int sizePerIndex;
    private final int batchSize;
    private final int dim;
    private final DataType fieldType;

    private long count = 0;
    private long currentIndexCount = 0;
    private long currentIndexMinId = Long.MAX_VALUE;
    private long currentIndexMaxId = Long.MIN_VALUE;
    private final List<VectorEntry> pendingBatch;
    private final List<ResultEntry> results;
    private DiskAnnIndex currentIndex;
    private boolean built = false;

    public DiskAnnVectorGlobalIndexWriter(
            GlobalIndexFileWriter fileWriter,
            DataType fieldType,
            DiskAnnVectorIndexOptions options) {
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
                    "DiskANN vector index requires ArrayType, but got: " + dataType);
        }
        DataType elementType = ((ArrayType) dataType).getElementType();
        if (!(elementType instanceof FloatType)) {
            throw new IllegalArgumentException(
                    "DiskANN vector index requires float array, but got: " + elementType);
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
        if (options.normalize()) {
            normalizeL2(vector);
        }
        currentIndexMinId = Math.min(currentIndexMinId, count);
        currentIndexMaxId = Math.max(currentIndexMaxId, count);
        pendingBatch.add(new VectorEntry(count, vector));
        count++;

        try {
            if (pendingBatch.size() >= batchSize) {
                addBatchToIndex();
            }
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
            if (!pendingBatch.isEmpty()) {
                addBatchToIndex();
            }
            if (currentIndex != null && currentIndexCount > 0) {
                flushCurrentIndex();
            }
            return results;
        } catch (IOException e) {
            throw new RuntimeException("Failed to write DiskANN vector global index", e);
        }
    }

    private void addBatchToIndex() throws IOException {
        if (pendingBatch.isEmpty()) {
            return;
        }

        if (currentIndex == null) {
            currentIndex = createIndex();
            built = false;
        }

        int n = pendingBatch.size();
        ByteBuffer vectorBuffer = DiskAnnIndex.allocateVectorBuffer(n, dim);
        ByteBuffer idBuffer = DiskAnnIndex.allocateIdBuffer(n);
        FloatBuffer floatView = vectorBuffer.asFloatBuffer();
        LongBuffer longView = idBuffer.asLongBuffer();

        for (int i = 0; i < n; i++) {
            VectorEntry entry = pendingBatch.get(i);
            float[] vector = entry.vector;
            for (int j = 0; j < dim; j++) {
                floatView.put(i * dim + j, vector[j]);
            }
            longView.put(i, entry.id);
        }

        currentIndex.addWithIds(vectorBuffer, idBuffer, n);
        currentIndexCount += n;
        pendingBatch.clear();
        built = false;
    }

    private void flushCurrentIndex() throws IOException {
        if (currentIndex == null || currentIndexCount == 0) {
            return;
        }

        if (!built) {
            currentIndex.build(options.buildListSize());
            built = true;
        }

        String fileName = fileWriter.newFileName(DiskAnnVectorGlobalIndexerFactory.IDENTIFIER);
        try (OutputStream out = new BufferedOutputStream(fileWriter.newOutputStream(fileName))) {
            long serializeSize = currentIndex.serializeSize();
            if (serializeSize > Integer.MAX_VALUE) {
                throw new IOException(
                        "Index too large to serialize: "
                                + serializeSize
                                + " bytes exceeds maximum buffer size");
            }
            ByteBuffer serializeBuffer =
                    ByteBuffer.allocateDirect((int) serializeSize).order(ByteOrder.nativeOrder());
            long bytesWritten = currentIndex.serialize(serializeBuffer);

            byte[] indexData = new byte[(int) bytesWritten];
            serializeBuffer.rewind();
            serializeBuffer.get(indexData);
            out.write(indexData);
            out.flush();
        }

        DiskAnnIndexMeta meta =
                new DiskAnnIndexMeta(
                        dim,
                        options.metric().toMetricType().value(),
                        options.indexType().value(),
                        currentIndexCount,
                        currentIndexMinId,
                        currentIndexMaxId);
        results.add(new ResultEntry(fileName, currentIndexCount, meta.serialize()));

        currentIndex.close();
        currentIndex = null;
        currentIndexCount = 0;
        currentIndexMinId = Long.MAX_VALUE;
        currentIndexMaxId = Long.MIN_VALUE;
        built = false;
    }

    private DiskAnnIndex createIndex() {
        return DiskAnnIndex.create(
                options.dimension(),
                options.metric(),
                options.indexType(),
                options.maxDegree(),
                options.buildListSize());
    }

    private void checkDimension(float[] vector) {
        if (vector.length != options.dimension()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Vector dimension mismatch: expected %d, but got %d",
                            options.dimension(), vector.length));
        }
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
