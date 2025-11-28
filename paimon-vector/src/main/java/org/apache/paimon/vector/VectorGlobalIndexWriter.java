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

import org.apache.paimon.globalindex.GlobalIndexWriter;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.Range;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Vector global index writer.
 *
 * <p>This is a framework implementation that provides the structure for vector indexing. In
 * production, integrate with JVector or other vector search engines.
 */
public class VectorGlobalIndexWriter implements GlobalIndexWriter {

    private static final String OPTION_VECTOR_DIM = "vector.dim";
    private static final String OPTION_VECTOR_METRIC = "vector.metric";
    private static final String OPTION_VECTOR_M = "vector.M";
    private static final String OPTION_VECTOR_EF_CONSTRUCTION = "vector.ef-construction";

    private final GlobalIndexFileWriter fileWriter;
    private final DataType fieldType;
    private final int dimension;
    private final String similarityFunction;
    private final int m;
    private final int efConstruction;

    private final List<VectorWithRowId> vectors;
    private long minRowId = Long.MAX_VALUE;
    private long maxRowId = Long.MIN_VALUE;

    public VectorGlobalIndexWriter(
            GlobalIndexFileWriter fileWriter, DataType fieldType, Options options) {
        this.fileWriter = fileWriter;
        this.fieldType = fieldType;
        this.vectors = new ArrayList<>();

        // Parse options
        this.dimension = options.getInteger(OPTION_VECTOR_DIM, 128);
        String metric = options.getString(OPTION_VECTOR_METRIC, "COSINE");
        this.similarityFunction = parseMetric(metric);
        this.m = options.getInteger(OPTION_VECTOR_M, 16);
        this.efConstruction = options.getInteger(OPTION_VECTOR_EF_CONSTRUCTION, 100);

        // Validate field type
        validateFieldType(fieldType);
    }

    private void validateFieldType(DataType type) {
        checkArgument(
                type instanceof ArrayType, "Vector field type must be ARRAY, but was: " + type);
    }

    private String parseMetric(String metric) {
        switch (metric.toUpperCase()) {
            case "COSINE":
            case "DOT_PRODUCT":
            case "EUCLIDEAN":
                return metric.toUpperCase();
            default:
                throw new IllegalArgumentException("Unsupported metric: " + metric);
        }
    }

    @Override
    public void write(Object key) {
        Preconditions.checkArgument(
                key instanceof VectorKey, "Key must be VectorKey, but was: " + key.getClass());

        VectorKey vectorKey = (VectorKey) key;
        long rowId = vectorKey.getRowId();
        float[] vector = vectorKey.getVector();

        checkArgument(
                vector.length == dimension,
                "Vector dimension mismatch: expected " + dimension + ", but got " + vector.length);

        vectors.add(new VectorWithRowId(rowId, vector));
        minRowId = Math.min(minRowId, rowId);
        maxRowId = Math.max(maxRowId, rowId);
    }

    @Override
    public List<ResultEntry> finish() {
        try {
            if (vectors.isEmpty()) {
                return new ArrayList<>();
            }

            // Build vector index
            // TODO: Integrate with JVector or other vector search engine
            // For now, serialize the raw vectors with metadata
            byte[] indexBytes = serializeVectors();
            byte[] metaBytes = serializeMetadata();

            // Write to file
            String fileName = fileWriter.newFileName(VectorGlobalIndexerFactory.IDENTIFIER);
            try (OutputStream out = fileWriter.newOutputStream(fileName)) {
                out.write(indexBytes);
            }

            List<ResultEntry> results = new ArrayList<>();
            results.add(ResultEntry.of(fileName, metaBytes, new Range(minRowId, maxRowId + 1)));
            return results;
        } catch (IOException e) {
            throw new RuntimeException("Failed to write vector global index", e);
        }
    }

    private byte[] serializeVectors() throws IOException {
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        try (DataOutputStream dataOut = new DataOutputStream(byteOut)) {
            // Write version
            dataOut.writeInt(1);

            // Write row IDs
            dataOut.writeInt(vectors.size());
            for (VectorWithRowId vector : vectors) {
                dataOut.writeLong(vector.rowId);
            }

            // Write vectors
            for (VectorWithRowId vector : vectors) {
                dataOut.writeInt(vector.vector.length);
                for (float v : vector.vector) {
                    dataOut.writeFloat(v);
                }
            }
        }
        return byteOut.toByteArray();
    }

    private byte[] serializeMetadata() throws IOException {
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        try (DataOutputStream dataOut = new DataOutputStream(byteOut)) {
            dataOut.writeInt(dimension);
            dataOut.writeUTF(similarityFunction);
            dataOut.writeInt(m);
            dataOut.writeInt(efConstruction);
        }
        return byteOut.toByteArray();
    }

    /** Key type for vector indexing. */
    public static class VectorKey {
        private final long rowId;
        private final float[] vector;

        public VectorKey(long rowId, float[] vector) {
            this.rowId = rowId;
            this.vector = vector;
        }

        public long getRowId() {
            return rowId;
        }

        public float[] getVector() {
            return vector;
        }
    }

    private static class VectorWithRowId {
        final long rowId;
        final float[] vector;

        VectorWithRowId(long rowId, float[] vector) {
            this.rowId = rowId;
            this.vector = vector;
        }
    }
}
