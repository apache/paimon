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

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99Codec;
import org.apache.lucene.codecs.lucene99.Lucene99HnswScalarQuantizedVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Vector global index writer using Apache Lucene 9.x.
 *
 * <p>This implementation uses Lucene's native KnnFloatVectorField with HNSW algorithm for efficient
 * approximate nearest neighbor search.
 */
public class VectorGlobalIndexWriter implements GlobalIndexWriter {

    private static final String VECTOR_FIELD = "vector";
    private static final String ROW_ID_FIELD = "rowId";

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
        this.similarityFunction = parseMetricToLucene(vectorOptions.metric());

        // Validate field type
        validateFieldType(fieldType);
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

            // Build Lucene index in memory
            byte[] indexBytes =
                    buildLuceneIndex(this.vectorOptions.m(), this.vectorOptions.efConstruction());

            // Create metadata
            VectorIndexMetadata metadata =
                    new VectorIndexMetadata(
                            vectorOptions.dimension(),
                            vectorOptions.metric(),
                            vectorOptions.m(),
                            vectorOptions.efConstruction());
            byte[] metaBytes = VectorIndexMetadata.serializeMetadata(metadata);

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

    private void validateFieldType(DataType type) {
        checkArgument(
                type instanceof ArrayType, "Vector field type must be ARRAY, but was: " + type);
    }

    private VectorSimilarityFunction parseMetricToLucene(String metric) {
        switch (metric.toUpperCase()) {
            case "COSINE":
                return VectorSimilarityFunction.COSINE;
            case "DOT_PRODUCT":
                return VectorSimilarityFunction.DOT_PRODUCT;
            case "EUCLIDEAN":
                return VectorSimilarityFunction.EUCLIDEAN;
            case "MAX_INNER_PRODUCT":
                return VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;
            default:
                throw new IllegalArgumentException("Unsupported metric: " + metric);
        }
    }

    private byte[] buildLuceneIndex(int m, int efConstruction) throws IOException {
        // Create temporary directory for MMap
        java.nio.file.Path tempDir =
                java.nio.file.Files.createTempDirectory(
                        fileWriter.newFileName("paimon-vector-index"));
        Directory directory = new MMapDirectory(tempDir);

        try {
            // Configure index writer
            IndexWriterConfig config = getIndexWriterConfig(m, efConstruction);

            try (IndexWriter writer = new IndexWriter(directory, config)) {
                // Add each vector as a document
                for (VectorWithRowId vectorWithRowId : vectors) {
                    Document doc = new Document();

                    // Add KNN vector field
                    doc.add(
                            new KnnFloatVectorField(
                                    VECTOR_FIELD, vectorWithRowId.vector, similarityFunction));

                    // Store row ID
                    doc.add(new StoredField(ROW_ID_FIELD, vectorWithRowId.rowId));

                    writer.addDocument(doc);
                }

                // Commit changes
                writer.commit();
            }

            // Serialize directory to byte array
            return serializeDirectory(directory);
        } finally {
            // Clean up
            directory.close();
            deleteDirectory(tempDir);
        }
    }

    private static IndexWriterConfig getIndexWriterConfig(int m, int efConstruction) {
        IndexWriterConfig config = new IndexWriterConfig();
        config.setRAMBufferSizeMB(256); // Increase buffer for better performance
        config.setCodec(
                new Lucene99Codec(Lucene99Codec.Mode.BEST_SPEED) {
                    @Override
                    public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                        return new Lucene99HnswScalarQuantizedVectorsFormat(m, efConstruction);
                    }
                });
        return config;
    }

    private void deleteDirectory(java.nio.file.Path path) throws IOException {
        if (java.nio.file.Files.exists(path)) {
            java.nio.file.Files.walk(path)
                    .sorted(java.util.Comparator.reverseOrder())
                    .forEach(
                            p -> {
                                try {
                                    java.nio.file.Files.delete(p);
                                } catch (IOException e) {
                                    // Ignore cleanup errors
                                }
                            });
        }
    }

    private byte[] serializeDirectory(Directory directory) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        // Write all files from the directory
        String[] files = directory.listAll();

        // Write number of files
        baos.write(intToBytes(files.length));

        for (String fileName : files) {
            // Write file name length and name
            byte[] nameBytes = fileName.getBytes(StandardCharsets.UTF_8);
            baos.write(intToBytes(nameBytes.length));
            baos.write(nameBytes);

            // Write file content length and content
            long fileLength = directory.fileLength(fileName);
            baos.write(longToBytes(fileLength));

            try (org.apache.lucene.store.IndexInput input = directory.openInput(fileName, null)) {
                byte[] buffer = new byte[8192];
                long remaining = fileLength;

                while (remaining > 0) {
                    int toRead = (int) Math.min(buffer.length, remaining);
                    input.readBytes(buffer, 0, toRead);
                    baos.write(buffer, 0, toRead);
                    remaining -= toRead;
                }
            }
        }

        return baos.toByteArray();
    }

    private byte[] intToBytes(int value) {
        return new byte[] {
            (byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value
        };
    }

    private byte[] longToBytes(long value) {
        return new byte[] {
            (byte) (value >>> 56),
            (byte) (value >>> 48),
            (byte) (value >>> 40),
            (byte) (value >>> 32),
            (byte) (value >>> 24),
            (byte) (value >>> 16),
            (byte) (value >>> 8),
            (byte) value
        };
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
