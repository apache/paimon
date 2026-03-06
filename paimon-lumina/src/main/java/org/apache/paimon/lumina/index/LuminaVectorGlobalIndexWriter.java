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

import org.apache.paimon.data.InternalArray;
import org.apache.paimon.globalindex.GlobalIndexSingletonWriter;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.FloatType;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.FloatBuffer;
import java.nio.LongBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Vector global index writer using Lumina.
 *
 * <p>Vectors are collected until the current index reaches {@code sizePerIndex} vectors, then
 * pretrained, inserted in a single batch, and dumped to a file. DiskANN requires exactly one
 * pretrain and one insertBatch call per index.
 */
public class LuminaVectorGlobalIndexWriter implements GlobalIndexSingletonWriter {

    private final GlobalIndexFileWriter fileWriter;
    private final LuminaVectorIndexOptions options;
    private final int sizePerIndex;
    private final int dim;
    private final DataType fieldType;

    private long count = 0;
    private long currentIndexMinId = Long.MAX_VALUE;
    private long currentIndexMaxId = Long.MIN_VALUE;
    private final List<VectorEntry> pendingBatch;
    private final List<ResultEntry> results;

    public LuminaVectorGlobalIndexWriter(
            GlobalIndexFileWriter fileWriter,
            DataType fieldType,
            LuminaVectorIndexOptions options) {
        this.fileWriter = fileWriter;
        this.fieldType = fieldType;
        this.options = options;
        this.sizePerIndex = options.sizePerIndex();
        this.dim = options.dimension();
        this.pendingBatch = new ArrayList<>();
        this.results = new ArrayList<>();

        validateFieldType(fieldType);
    }

    private void validateFieldType(DataType dataType) {
        if (!(dataType instanceof ArrayType)) {
            throw new IllegalArgumentException(
                    "Lumina vector index requires ArrayType, but got: " + dataType);
        }
        DataType elementType = ((ArrayType) dataType).getElementType();
        if (!(elementType instanceof FloatType)) {
            throw new IllegalArgumentException(
                    "Lumina vector index requires float array, but got: " + elementType);
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
            if (pendingBatch.size() >= sizePerIndex) {
                buildAndFlushIndex();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<ResultEntry> finish() {
        try {
            if (!pendingBatch.isEmpty()) {
                buildAndFlushIndex();
            }
            return results;
        } catch (IOException e) {
            throw new RuntimeException("Failed to write Lumina vector global index", e);
        }
    }

    /**
     * Build a complete DiskANN index from the current pending batch: create index, pretrain, insert
     * all vectors in a single batch, dump, and close.
     */
    private void buildAndFlushIndex() throws IOException {
        if (pendingBatch.isEmpty()) {
            return;
        }

        int n = pendingBatch.size();
        LuminaIndex index = createIndex();

        try {
            int trainingSize = Math.min(n, options.trainingSize());
            ByteBuffer trainingBuffer = LuminaIndex.allocateVectorBuffer(trainingSize, dim);
            FloatBuffer trainingFloatView = trainingBuffer.asFloatBuffer();
            for (int i = 0; i < trainingSize; i++) {
                float[] vector = pendingBatch.get(i).vector;
                for (int j = 0; j < dim; j++) {
                    trainingFloatView.put(i * dim + j, vector[j]);
                }
            }
            index.pretrain(trainingBuffer, trainingSize);

            ByteBuffer vectorBuffer = LuminaIndex.allocateVectorBuffer(n, dim);
            ByteBuffer idBuffer = LuminaIndex.allocateIdBuffer(n);
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

            index.insertBatch(vectorBuffer, idBuffer, n);

            File tempFile =
                    Files.createTempFile("paimon-lumina-build-" + UUID.randomUUID(), ".lmi")
                            .toFile();
            try {
                index.dump(tempFile.getAbsolutePath());

                String fileName =
                        fileWriter.newFileName(LuminaVectorGlobalIndexerFactory.IDENTIFIER);
                try (OutputStream out = fileWriter.newOutputStream(fileName)) {
                    byte[] indexData = Files.readAllBytes(tempFile.toPath());
                    out.write(indexData);
                    out.flush();
                }

                LuminaIndexMeta meta =
                        new LuminaIndexMeta(
                                dim,
                                options.metric().getValue(),
                                options.indexType().name(),
                                n,
                                currentIndexMinId,
                                currentIndexMaxId);
                results.add(new ResultEntry(fileName, n, meta.serialize()));
            } finally {
                tempFile.delete();
            }
        } finally {
            index.close();
        }

        pendingBatch.clear();
        currentIndexMinId = Long.MAX_VALUE;
        currentIndexMaxId = Long.MIN_VALUE;
    }

    private LuminaIndex createIndex() {
        Map<String, String> extraOptions = new LinkedHashMap<>();
        extraOptions.put("encoding.type", options.encodingType());

        if (options.pretrainSampleRatio() != 1.0) {
            extraOptions.put(
                    "pretrain.sample_ratio", String.valueOf(options.pretrainSampleRatio()));
        }

        return LuminaIndex.createForBuild(dim, options.metric(), options.indexType(), extraOptions);
    }

    private void checkDimension(float[] vector) {
        if (vector.length != dim) {
            throw new IllegalArgumentException(
                    String.format(
                            "Vector dimension mismatch: expected %d, but got %d",
                            dim, vector.length));
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

    private static class VectorEntry {
        final long id;
        final float[] vector;

        VectorEntry(long id, float[] vector) {
            this.id = id;
            this.vector = vector;
        }
    }
}
