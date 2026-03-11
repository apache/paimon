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

import org.aliyun.lumina.LuminaDataset;
import org.aliyun.lumina.LuminaFileOutput;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Vector global index writer using Lumina. Builds a single index file per shard.
 *
 * <p>Vectors are accumulated in chunked float buffers and then streamed to the Lumina builder via
 * the {@link LuminaDataset} callback API. The chunked design avoids the 2 GB single Java array
 * limit and supports up to 1 billion+ vectors regardless of dimension.
 */
public class LuminaVectorGlobalIndexWriter implements GlobalIndexSingletonWriter, Closeable {

    private static final String FILE_NAME_PREFIX = "lumina";

    /** Buffer size for wrapping PositionOutputStream when dumping index (8 MB). */
    private static final int DUMP_BUFFER_SIZE = 8 * 1024 * 1024;

    /** Target chunk size in floats (~64 MB of float data). */
    private static final int TARGET_CHUNK_FLOATS = 16 * 1024 * 1024;

    private final GlobalIndexFileWriter fileWriter;
    private final LuminaVectorIndexOptions options;
    private final int dim;

    /** Number of vectors each chunk can hold. */
    private final int chunkVectors;

    /** Chunked float buffer to break the 2 GB single-array limit. */
    private List<float[]> chunks;

    private int count;
    private boolean finished;
    private boolean closed;

    public LuminaVectorGlobalIndexWriter(
            GlobalIndexFileWriter fileWriter,
            DataType fieldType,
            LuminaVectorIndexOptions options) {
        this.fileWriter = fileWriter;
        this.options = options;
        this.dim = options.dimension();
        this.chunkVectors = Math.max(1, TARGET_CHUNK_FLOATS / dim);
        this.chunks = new ArrayList<>();
        this.count = 0;
        this.finished = false;
        this.closed = false;

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
        if (fieldData == null) {
            throw new IllegalArgumentException("Field data must not be null");
        }
        if (fieldData instanceof float[]) {
            vector = (float[]) fieldData;
        } else if (fieldData instanceof InternalArray) {
            vector = ((InternalArray) fieldData).toFloatArray();
        } else {
            throw new RuntimeException(
                    "Unsupported vector type: " + fieldData.getClass().getName());
        }
        checkDimension(vector);
        int chunkIndex = count / chunkVectors;
        int offsetInChunk = count % chunkVectors;
        if (chunkIndex >= chunks.size()) {
            chunks.add(new float[chunkVectors * dim]);
        }
        System.arraycopy(vector, 0, chunks.get(chunkIndex), offsetInChunk * dim, dim);
        count++;
    }

    @Override
    public List<ResultEntry> finish() {
        if (finished) {
            throw new IllegalStateException("finish() has already been called");
        }
        finished = true;
        try {
            if (count == 0) {
                return Collections.emptyList();
            }
            return Collections.singletonList(buildIndex());
        } catch (IOException e) {
            throw new RuntimeException("Failed to write Lumina vector global index", e);
        }
    }

    private ResultEntry buildIndex() throws IOException {
        Map<String, String> luminaOptions = options.toLuminaOptions();
        boolean needsPretrain = !"rawf32".equalsIgnoreCase(luminaOptions.get("encoding.type"));

        try (LuminaIndex index = LuminaIndex.createForBuild(dim, options.metric(), luminaOptions)) {

            if (needsPretrain) {
                index.pretrainFrom(new ChunkedDataset(chunks, dim, chunkVectors, count));
            }
            index.insertFrom(new ChunkedDataset(chunks, dim, chunkVectors, count));

            // Free vector memory after insertion
            chunks = null;

            String fileName = fileWriter.newFileName(FILE_NAME_PREFIX);
            try (PositionOutputStream out = fileWriter.newOutputStream(fileName)) {
                BufferedFileOutput bufferedOut = new BufferedFileOutput(out, DUMP_BUFFER_SIZE);
                index.dump(bufferedOut);
                bufferedOut.flush();
                out.flush();
            }

            LuminaIndexMeta meta = new LuminaIndexMeta(luminaOptions);
            return new ResultEntry(fileName, count, meta.serialize());
        }
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
        if (!closed) {
            closed = true;
            chunks = null;
        }
    }

    /**
     * A {@link LuminaDataset} backed by chunked float arrays. Supports vector counts beyond the 2
     * GB single-array limit by splitting data across multiple chunks.
     */
    static class ChunkedDataset implements LuminaDataset {
        private final List<float[]> chunks;
        private final int dim;
        private final int chunkVectors;
        private final int totalCount;
        private int cursor;

        ChunkedDataset(List<float[]> chunks, int dim, int chunkVectors, int totalCount) {
            this.chunks = chunks;
            this.dim = dim;
            this.chunkVectors = chunkVectors;
            this.totalCount = totalCount;
            this.cursor = 0;
        }

        @Override
        public int dim() {
            return dim;
        }

        @Override
        public long totalSize() {
            return totalCount;
        }

        @Override
        public long getNextBatch(float[] vectorBuf, long[] idBuf) {
            if (cursor >= totalCount) {
                return 0;
            }
            int remaining = totalCount - cursor;
            int batchSize = Math.min(idBuf.length, remaining);
            int destOffset = 0;
            int copied = 0;
            while (copied < batchSize) {
                int chunkIndex = (cursor + copied) / chunkVectors;
                int offsetInChunk = (cursor + copied) % chunkVectors;
                int availableInChunk = chunkVectors - offsetInChunk;
                int toCopy = Math.min(batchSize - copied, availableInChunk);
                System.arraycopy(
                        chunks.get(chunkIndex),
                        offsetInChunk * dim,
                        vectorBuf,
                        destOffset,
                        toCopy * dim);
                destOffset += toCopy * dim;
                copied += toCopy;
            }
            for (int i = 0; i < batchSize; i++) {
                idBuf[i] = cursor + i;
            }
            cursor += batchSize;
            return batchSize;
        }
    }

    /**
     * Adapts a {@link PositionOutputStream} to the {@link LuminaFileOutput} JNI callback API with
     * buffering for better I/O throughput (especially important for OSS writes).
     */
    static class BufferedFileOutput implements LuminaFileOutput {
        private final PositionOutputStream posOut;
        private final BufferedOutputStream bufOut;

        BufferedFileOutput(PositionOutputStream posOut, int bufferSize) {
            this.posOut = posOut;
            this.bufOut = new BufferedOutputStream(posOut, bufferSize);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            bufOut.write(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            bufOut.flush();
        }

        @Override
        public long getPos() throws IOException {
            bufOut.flush();
            return posOut.getPos();
        }

        @Override
        public void close() throws IOException {
            // Flush buffered data before the caller closes the underlying stream.
            bufOut.flush();
        }
    }
}
