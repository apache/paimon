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
import java.nio.ByteBuffer;
import java.nio.FloatBuffer;
import java.nio.LongBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Vector global index writer using Lumina. Builds a single index file per shard. */
public class LuminaVectorGlobalIndexWriter implements GlobalIndexSingletonWriter, Closeable {

    private static final String FILE_NAME_PREFIX = "lumina";

    private final GlobalIndexFileWriter fileWriter;
    private final LuminaVectorIndexOptions options;
    private final int dim;

    private List<float[]> pendingVectors;

    public LuminaVectorGlobalIndexWriter(
            GlobalIndexFileWriter fileWriter,
            DataType fieldType,
            LuminaVectorIndexOptions options) {
        this.fileWriter = fileWriter;
        this.options = options;
        this.dim = options.dimension();
        this.pendingVectors = new ArrayList<>();

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
            vector = ((float[]) fieldData).clone();
        } else if (fieldData instanceof InternalArray) {
            vector = ((InternalArray) fieldData).toFloatArray();
        } else {
            throw new RuntimeException(
                    "Unsupported vector type: " + fieldData.getClass().getName());
        }
        checkDimension(vector);
        pendingVectors.add(vector);
    }

    @Override
    public List<ResultEntry> finish() {
        try {
            if (pendingVectors.isEmpty()) {
                return Collections.emptyList();
            }
            return Collections.singletonList(buildIndex());
        } catch (IOException e) {
            throw new RuntimeException("Failed to write Lumina vector global index", e);
        }
    }

    private ResultEntry buildIndex() throws IOException {
        int n = pendingVectors.size();

        try (LuminaIndex index =
                LuminaIndex.createForBuild(dim, options.metric(), options.toLuminaOptions())) {
            ByteBuffer vectorBuffer = buildVectorBuffer(pendingVectors);
            ByteBuffer idBuffer = buildIdBuffer(n);

            index.pretrain(vectorBuffer, n);
            index.insertBatch(vectorBuffer, idBuffer, n);

            String fileName = fileWriter.newFileName(FILE_NAME_PREFIX);
            try (PositionOutputStream out = fileWriter.newOutputStream(fileName)) {
                index.dump(new OutputStreamFileOutput(out));
                out.flush();
            }

            LuminaIndexMeta meta = new LuminaIndexMeta(options.toLuminaOptions());
            return new ResultEntry(fileName, n, meta.serialize());
        }
    }

    private ByteBuffer buildVectorBuffer(List<float[]> vectors) {
        ByteBuffer buffer = LuminaIndex.allocateVectorBuffer(vectors.size(), dim);
        FloatBuffer floatView = buffer.asFloatBuffer();
        for (float[] vec : vectors) {
            floatView.put(vec);
        }
        return buffer;
    }

    private static ByteBuffer buildIdBuffer(int count) {
        ByteBuffer buffer = LuminaIndex.allocateIdBuffer(count);
        LongBuffer longView = buffer.asLongBuffer();
        for (int i = 0; i < count; i++) {
            longView.put(i, i);
        }
        return buffer;
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
        if (pendingVectors != null) {
            pendingVectors.clear();
            pendingVectors = null;
        }
    }

    /** Adapts a {@link PositionOutputStream} to the {@link LuminaFileOutput} JNI callback API. */
    static class OutputStreamFileOutput implements LuminaFileOutput {
        private final PositionOutputStream out;

        OutputStreamFileOutput(PositionOutputStream out) {
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
        public long getPos() throws IOException {
            return out.getPos();
        }

        @Override
        public void close() {
            // Lifecycle managed by the caller's try-with-resources.
        }
    }
}
