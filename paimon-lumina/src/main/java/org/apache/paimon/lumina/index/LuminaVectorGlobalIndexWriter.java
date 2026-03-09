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
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.globalindex.GlobalIndexSingletonWriter;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.FloatType;

import org.aliyun.lumina.LuminaFileOutput;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.FloatBuffer;
import java.nio.LongBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Vector global index writer using Lumina.
 *
 * <p>Vectors are collected until the current index reaches {@code sizePerIndex} vectors, then
 * pretrained, inserted in a single batch, and dumped to a file. DiskANN requires exactly one
 * pretrain and one insertBatch call per index.
 *
 * <p>Each written vector is assigned a monotonically increasing 64-bit row ID ({@code count}) that
 * spans across all produced index files. The second index file's IDs therefore start from {@code
 * sizePerIndex}, not from 0. The min/max IDs stored in {@link LuminaIndexMeta} reflect this global
 * range, enabling the reader to skip index files that have no overlap with a given filter set.
 */
public class LuminaVectorGlobalIndexWriter implements GlobalIndexSingletonWriter, Closeable {

    private final GlobalIndexFileWriter fileWriter;
    private final LuminaVectorIndexOptions options;
    private final int sizePerIndex;
    private final int dim;

    private long count = 0; // monotonically increasing global row ID across all index files
    private long currentIndexMinId = Long.MAX_VALUE;
    private long currentIndexMaxId = Long.MIN_VALUE;
    private ByteBuffer pendingVectors;
    private ByteBuffer pendingIds;
    private FloatBuffer pendingFloatView;
    private LongBuffer pendingLongView;
    private int pendingCount = 0;
    private final List<ResultEntry> results;

    public LuminaVectorGlobalIndexWriter(
            GlobalIndexFileWriter fileWriter,
            DataType fieldType,
            LuminaVectorIndexOptions options) {
        this.fileWriter = fileWriter;
        this.options = options;
        this.dim = options.dimension();
        int configuredSize = options.sizePerIndex();
        long buildMemoryLimit = options.buildMemoryLimit();
        int maxByDim =
                (int) Math.min(configuredSize, buildMemoryLimit / ((long) dim * Float.BYTES));
        this.sizePerIndex = Math.max(maxByDim, 1);
        this.pendingVectors = LuminaIndex.allocateVectorBuffer(sizePerIndex, dim);
        this.pendingIds = LuminaIndex.allocateIdBuffer(sizePerIndex);
        this.pendingFloatView = pendingVectors.asFloatBuffer();
        this.pendingLongView = pendingIds.asLongBuffer();
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
            LuminaVectorUtils.normalizeL2(vector);
        }
        currentIndexMinId = Math.min(currentIndexMinId, count);
        currentIndexMaxId = Math.max(currentIndexMaxId, count);
        int offset = pendingCount * dim;
        for (int i = 0; i < dim; i++) {
            pendingFloatView.put(offset + i, vector[i]);
        }
        pendingLongView.put(pendingCount, count);
        pendingCount++;
        count++;

        try {
            if (pendingCount >= sizePerIndex) {
                buildAndFlushIndex();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<ResultEntry> finish() {
        try {
            if (pendingCount > 0) {
                buildAndFlushIndex();
            }
            return results;
        } catch (IOException e) {
            throw new RuntimeException("Failed to write Lumina vector global index", e);
        }
    }

    /**
     * Build a complete DiskANN index from the current pending batch: create index, pretrain, insert
     * all vectors in a single batch, dump directly to the output stream, and close.
     */
    private void buildAndFlushIndex() throws IOException {
        if (pendingCount == 0) {
            return;
        }

        int n = pendingCount;
        LuminaIndex index = createIndex();

        try {
            int trainingSize = Math.min(n, options.trainingSize());
            int[] sampleIndices = reservoirSample(n, trainingSize);
            ByteBuffer trainingBuffer = LuminaIndex.allocateVectorBuffer(trainingSize, dim);
            FloatBuffer trainingFloatView = trainingBuffer.asFloatBuffer();
            for (int i = 0; i < trainingSize; i++) {
                int srcOffset = sampleIndices[i] * dim;
                for (int j = 0; j < dim; j++) {
                    trainingFloatView.put(i * dim + j, pendingFloatView.get(srcOffset + j));
                }
            }
            index.pretrain(trainingBuffer, trainingSize);
            trainingBuffer = null;
            trainingFloatView = null;

            index.insertBatch(pendingVectors, pendingIds, n);

            String fileName = fileWriter.newFileName(LuminaVectorGlobalIndexerFactory.IDENTIFIER);
            try (PositionOutputStream out = fileWriter.newOutputStream(fileName)) {
                index.dump(new OutputStreamFileOutput(out));
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
            index.close();
        }

        pendingCount = 0;
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

        if (options.diskannEfConstruction() != null) {
            extraOptions.put(
                    "diskann.build.ef_construction",
                    String.valueOf(options.diskannEfConstruction()));
        }
        if (options.diskannNeighborCount() != null) {
            extraOptions.put(
                    "diskann.build.neighbor_count", String.valueOf(options.diskannNeighborCount()));
        }
        if (options.diskannBuildThreadCount() != null) {
            extraOptions.put(
                    "diskann.build.thread_count",
                    String.valueOf(options.diskannBuildThreadCount()));
        }

        return LuminaIndex.createForBuild(dim, options.metric(), options.indexType(), extraOptions);
    }

    /**
     * Selects {@code k} indices from [0, n) using reservoir sampling (Algorithm R).
     *
     * <p>When {@code k >= n} all indices are returned in order. Otherwise a random representative
     * subset is chosen, ensuring training data covers the full vector distribution instead of being
     * biased toward the first {@code k} inserted vectors.
     */
    private static int[] reservoirSample(int n, int k) {
        int[] reservoir = new int[k];
        for (int i = 0; i < k; i++) {
            reservoir[i] = i;
        }
        if (k < n) {
            Random random = new Random(42);
            for (int i = k; i < n; i++) {
                int j = random.nextInt(i + 1);
                if (j < k) {
                    reservoir[j] = i;
                }
            }
        }
        return reservoir;
    }

    private void checkDimension(float[] vector) {
        if (vector.length != dim) {
            throw new IllegalArgumentException(
                    String.format(
                            "Vector dimension mismatch: expected %d, but got %d",
                            dim, vector.length));
        }
    }

    @Override
    public void close() {
        pendingVectors = null;
        pendingIds = null;
        pendingFloatView = null;
        pendingLongView = null;
        pendingCount = 0;
    }

    /** Adapts a {@link PositionOutputStream} to the {@link LuminaFileOutput} JNI callback API. */
    private static class OutputStreamFileOutput implements LuminaFileOutput {
        private final OutputStream out;

        OutputStreamFileOutput(OutputStream out) {
            this.out = out;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            out.write(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            out.flush();
        }

        @Override
        public long getPos() {
            return -1;
        }

        @Override
        public void close() {
            // Lifecycle managed by the caller's try-with-resources.
        }
    }
}
