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

import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.OnHeapGraphIndex;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Vector global index writer using JVector.
 *
 * <p>This implementation uses JVector's HNSW (Hierarchical Navigable Small World) graph algorithm
 * for efficient approximate nearest neighbor search.
 */
public class VectorGlobalIndexWriter implements GlobalIndexWriter {

    private final GlobalIndexFileWriter fileWriter;
    private final DataType fieldType;
    private final VectorIndexOptions vectorOptions;
    private final VectorSimilarityFunction similarityFunction;

    private final List<VectorWithRowId> vectors;
    private long minRowId = Long.MAX_VALUE;
    private long maxRowId = Long.MIN_VALUE;

    public VectorGlobalIndexWriter(
            GlobalIndexFileWriter fileWriter, DataType fieldType, Options options) {
        this.fileWriter = fileWriter;
        this.fieldType = fieldType;
        this.vectors = new ArrayList<>();

        // Parse options
        this.vectorOptions = new VectorIndexOptions(options);
        this.similarityFunction = parseMetricToJVector(vectorOptions.metric());

        // Validate field type
        validateFieldType(fieldType);
    }

    private void validateFieldType(DataType type) {
        checkArgument(
                type instanceof ArrayType, "Vector field type must be ARRAY, but was: " + type);
    }

    private VectorSimilarityFunction parseMetricToJVector(String metric) {
        switch (metric.toUpperCase()) {
            case "COSINE":
                return VectorSimilarityFunction.COSINE;
            case "DOT_PRODUCT":
                return VectorSimilarityFunction.DOT_PRODUCT;
            case "EUCLIDEAN":
                return VectorSimilarityFunction.EUCLIDEAN;
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
                vector.length == vectorOptions.dimension(),
                "Vector dimension mismatch: expected "
                        + vectorOptions.dimension()
                        + ", but got "
                        + vector.length);

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

            // Build JVector HNSW index
            OnHeapGraphIndex graphIndex = buildJVectorIndex();

            // Serialize index and metadata
            byte[] indexBytes = serializeIndexWithJVector(graphIndex);
            VectorIndexMetadata metadata =
                    new VectorIndexMetadata(
                            vectorOptions.dimension(),
                            vectorOptions.metric(),
                            vectorOptions.m(),
                            vectorOptions.efConstruction());
            byte[] metaBytes = VectorIndexMetadata.serializeMetadata(metadata);

            // Write to file.
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

    private OnHeapGraphIndex buildJVectorIndex() {
        // Get the vectorization provider
        var vectorTypeSupport = VectorizationProvider.getInstance().getVectorTypeSupport();

        // Create list of VectorFloat for JVector
        List<VectorFloat<?>> vectorFloats = new ArrayList<>();
        for (VectorWithRowId vec : vectors) {
            vectorFloats.add(vectorTypeSupport.createFloatVector(vec.vector));
        }

        // Create vector values adapter for JVector
        RandomAccessVectorValues vectorValues =
                new PaimonRandomAccessVectorValues(
                        vectorFloats.size(), vectorOptions.dimension(), vectorFloats, false);

        // Build HNSW graph index using JVector
        GraphIndexBuilder builder =
                new GraphIndexBuilder(
                        vectorValues,
                        similarityFunction,
                        vectorOptions.m(),
                        vectorOptions.efConstruction(),
                        1.0f, // alpha (diversityWeight)
                        1.0f // neighborOverflow
                        );

        return builder.build(vectorValues);
    }

    private byte[] serializeIndexWithJVector(OnHeapGraphIndex graphIndex) throws IOException {
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        try (DataOutputStream dataOut = new DataOutputStream(byteOut)) {
            // Write version
            dataOut.writeInt(1); // Version 1 with JVector index

            // Write row IDs
            dataOut.writeInt(vectors.size());
            for (VectorWithRowId vector : vectors) {
                dataOut.writeLong(vector.rowId);
            }

            // Write vectors (still needed for graph index reconstruction)
            for (VectorWithRowId vector : vectors) {
                dataOut.writeInt(vector.vector.length);
                for (float v : vector.vector) {
                    dataOut.writeFloat(v);
                }
            }

            // Write JVector graph structure
            dataOut.writeInt(graphIndex.size());
            dataOut.writeInt(graphIndex.maxDegree());

            // Serialize graph neighbors for each node
            for (int i = 0; i < graphIndex.size(); i++) {
                var view = graphIndex.getView();
                var neighbors = view.getNeighborsIterator(i);

                // Count neighbors
                List<Integer> neighborList = new ArrayList<>();
                while (neighbors.hasNext()) {
                    neighborList.add(neighbors.nextInt());
                }

                dataOut.writeInt(neighborList.size());
                for (int neighbor : neighborList) {
                    dataOut.writeInt(neighbor);
                }
            }
        }
        return byteOut.toByteArray();
    }

    private byte[] serializeMetadata() throws IOException {
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        try (DataOutputStream dataOut = new DataOutputStream(byteOut)) {
            dataOut.writeInt(vectorOptions.dimension());
            dataOut.writeUTF(similarityFunction.name());
            dataOut.writeInt(vectorOptions.m());
            dataOut.writeInt(vectorOptions.efConstruction());
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
