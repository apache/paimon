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

package org.apache.paimon.globalindex.testvector;

import org.apache.paimon.data.InternalArray;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.globalindex.GlobalIndexSingletonWriter;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Test vector index writer that stores all vectors in memory and writes them to a single binary
 * file on {@link #finish()}.
 *
 * <p>Binary format (all values little-endian):
 *
 * <pre>
 *   [4 bytes] dimension (int)
 *   [4 bytes] count (int)
 *   [count * dim * 4 bytes] float vectors (row-major order)
 * </pre>
 */
public class TestVectorGlobalIndexWriter implements GlobalIndexSingletonWriter {

    private static final String FILE_NAME_PREFIX = "test-vector";

    private final GlobalIndexFileWriter fileWriter;
    private final int dimension;
    private final List<float[]> vectors;

    public TestVectorGlobalIndexWriter(GlobalIndexFileWriter fileWriter, int dimension) {
        this.fileWriter = fileWriter;
        this.dimension = dimension;
        this.vectors = new ArrayList<>();
    }

    @Override
    public void write(Object fieldData) {
        if (fieldData == null) {
            throw new IllegalArgumentException("Vector field data must not be null");
        }

        float[] vector;
        if (fieldData instanceof float[]) {
            vector = ((float[]) fieldData).clone();
        } else if (fieldData instanceof InternalArray) {
            InternalArray array = (InternalArray) fieldData;
            vector = new float[array.size()];
            for (int i = 0; i < array.size(); i++) {
                if (array.isNullAt(i)) {
                    throw new IllegalArgumentException("Vector element at index " + i + " is null");
                }
                vector[i] = array.getFloat(i);
            }
        } else {
            throw new IllegalArgumentException(
                    "Unsupported vector type: " + fieldData.getClass().getName());
        }

        int expectedDim =
                dimension > 0
                        ? dimension
                        : (vectors.isEmpty() ? vector.length : vectors.get(0).length);
        if (vector.length != expectedDim) {
            throw new IllegalArgumentException(
                    String.format(
                            "Vector dimension mismatch: expected %d, but got %d",
                            expectedDim, vector.length));
        }
        vectors.add(vector);
    }

    @Override
    public List<ResultEntry> finish() {
        if (vectors.isEmpty()) {
            return Collections.emptyList();
        }

        int dim = dimension > 0 ? dimension : vectors.get(0).length;
        int count = vectors.size();

        try {
            String fileName = fileWriter.newFileName(FILE_NAME_PREFIX);
            try (PositionOutputStream out = fileWriter.newOutputStream(fileName)) {
                // Header: dimension + count
                ByteBuffer header = ByteBuffer.allocate(8);
                header.order(ByteOrder.LITTLE_ENDIAN);
                header.putInt(dim);
                header.putInt(count);
                out.write(header.array());

                // Vector data
                ByteBuffer vectorBuf = ByteBuffer.allocate(dim * Float.BYTES);
                vectorBuf.order(ByteOrder.LITTLE_ENDIAN);
                for (float[] vec : vectors) {
                    vectorBuf.clear();
                    for (int i = 0; i < dim; i++) {
                        vectorBuf.putFloat(vec[i]);
                    }
                    out.write(vectorBuf.array());
                }
                out.flush();
            }

            return Collections.singletonList(new ResultEntry(fileName, count, null));
        } catch (IOException e) {
            throw new RuntimeException("Failed to write test vector index", e);
        }
    }
}
