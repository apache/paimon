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
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Vector global index writer using DiskANN.
 *
 * <p>The build pipeline follows two phases:
 *
 * <ol>
 *   <li><b>Vamana Graph Construction</b> — vectors are added in batches and the Vamana graph (with
 *       alpha-pruning) is built via the native DiskANN library.
 *   <li><b>PQ Compression</b> — after the graph is built, a Product Quantization codebook is
 *       trained via the native DiskANN library and all vectors are compressed to compact PQ codes.
 * </ol>
 *
 * <p>For each index flush, four files are produced:
 *
 * <ul>
 *   <li>{@code .index} — Vamana graph (adjacency lists only, no header)
 *   <li>{@code .data} — raw vectors stored sequentially (position = ID)
 *   <li>{@code .pq_pivots} — PQ codebook (centroids)
 *   <li>{@code .pq_compressed} — PQ compressed codes (memory thumbnail)
 * </ul>
 *
 * <p>The PQ compressed data acts as a "memory thumbnail" — during search it stays resident in
 * memory and allows fast approximate distance computation, reducing disk I/O for full vectors.
 *
 * <p>This class implements {@link Closeable} so that the native DiskANN index is released even if
 * {@link #finish()} is never called or throws an exception.
 */
public class DiskAnnVectorGlobalIndexWriter implements GlobalIndexSingletonWriter, Closeable {

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
        FloatBuffer floatView = vectorBuffer.asFloatBuffer();

        for (int i = 0; i < n; i++) {
            VectorEntry entry = pendingBatch.get(i);
            float[] vector = entry.vector;
            for (int j = 0; j < dim; j++) {
                floatView.put(i * dim + j, vector[j]);
            }
        }

        currentIndex.add(vectorBuffer, n);
        currentIndexCount += n;
        pendingBatch.clear();
        built = false;
    }

    private void flushCurrentIndex() throws IOException {
        if (currentIndex == null || currentIndexCount == 0) {
            return;
        }

        // ---- Phase 2: Vamana graph construction ----
        if (!built) {
            currentIndex.build();
            built = true;
        }

        // Serialize the graph + vectors into one buffer (no header — metadata goes to
        // DiskAnnIndexMeta).
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

        byte[] fullData = new byte[(int) bytesWritten];
        serializeBuffer.rewind();
        serializeBuffer.get(fullData);

        // Compute split point: data section = user vectors only (no start point).
        int dataSectionSize = (int) (currentIndexCount * dim * Float.BYTES);
        int graphSectionSize = fullData.length - dataSectionSize;

        // Generate file names — all share the same base name.
        String indexFileName = fileWriter.newFileName(DiskAnnVectorGlobalIndexerFactory.IDENTIFIER);
        String baseName = indexFileName.replaceAll("\\.index$", "");
        String dataFileName = baseName + ".data";
        String pqPivotsFileName = baseName + ".pq_pivots";
        String pqCompressedFileName = baseName + ".pq_compressed";

        // Write index file: graph section only (no header).
        try (OutputStream out =
                new BufferedOutputStream(fileWriter.newOutputStream(indexFileName))) {
            out.write(fullData, 0, graphSectionSize);
            out.flush();
        }

        // Write data file: raw vectors in sequential order (position = ID).
        try (OutputStream out =
                new BufferedOutputStream(fileWriter.newOutputStream(dataFileName))) {
            out.write(fullData, graphSectionSize, dataSectionSize);
            out.flush();
        }

        // ---- Phase 1: PQ Compression & Training (native) ----
        // Train PQ codebook on the vectors stored in the native index and encode all vectors.
        byte[][] pqResult =
                currentIndex.pqTrainAndEncode(
                        options.pqSubspaces(),
                        options.pqSampleSize(),
                        options.pqKmeansIterations());

        // Write PQ pivots file (codebook).
        try (OutputStream out =
                new BufferedOutputStream(fileWriter.newOutputStream(pqPivotsFileName))) {
            out.write(pqResult[0]);
            out.flush();
        }

        // Write PQ compressed file (memory thumbnail).
        try (OutputStream out =
                new BufferedOutputStream(fileWriter.newOutputStream(pqCompressedFileName))) {
            out.write(pqResult[1]);
            out.flush();
        }

        // Build metadata with all companion file names and graph parameters.
        DiskAnnIndexMeta meta =
                new DiskAnnIndexMeta(
                        dim,
                        options.metric().toMetricType().value(),
                        0,
                        currentIndexCount,
                        currentIndexMinId,
                        currentIndexMaxId,
                        options.maxDegree(),
                        options.buildListSize(),
                        0, // startId is always 0 (START_POINT_ID)
                        dataFileName,
                        pqPivotsFileName,
                        pqCompressedFileName);
        results.add(new ResultEntry(indexFileName, currentIndexCount, meta.serialize()));

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

    /**
     * Release native resources held by the current in-progress index.
     *
     * <p>This is a safety net: under normal operation the index is closed by {@link
     * #flushCurrentIndex()}, but if an error occurs before flushing this method ensures the native
     * handle is freed.
     */
    @Override
    public void close() {
        if (currentIndex != null) {
            currentIndex.close();
            currentIndex = null;
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
