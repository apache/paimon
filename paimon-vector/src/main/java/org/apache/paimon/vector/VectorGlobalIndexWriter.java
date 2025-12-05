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
import org.apache.paimon.utils.Range;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene912.Lucene912Codec;
import org.apache.lucene.codecs.lucene99.Lucene99HnswScalarQuantizedVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;

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

    private final GlobalIndexFileWriter fileWriter;
    private final DataType fieldType;
    private final VectorIndexOptions vectorOptions;
    private final VectorSimilarityFunction similarityFunction;
    private final int sizePerIndex;

    private final List<VectorIndex> vectors;

    public VectorGlobalIndexWriter(
            GlobalIndexFileWriter fileWriter, DataType fieldType, Options options) {
        checkArgument(
                fieldType instanceof ArrayType,
                "Vector field type must be ARRAY, but was: " + fieldType);
        this.fileWriter = fileWriter;
        this.fieldType = fieldType;
        this.vectors = new ArrayList<>();
        this.vectorOptions = new VectorIndexOptions(options);
        this.similarityFunction = parseMetricToLucene(vectorOptions.metric());
        this.sizePerIndex = vectorOptions.sizePerIndex();
    }

    @Override
    public void write(Object key) {
        if (key instanceof FloatVectorIndex) {
            FloatVectorIndex vectorKey = (FloatVectorIndex) key;
            float[] vector = vectorKey.vector();

            checkArgument(
                    vector.length == vectorOptions.dimension(),
                    "Vector dimension mismatch: expected "
                            + vectorOptions.dimension()
                            + ", but got "
                            + vector.length);

            vectors.add(vectorKey);
        } else if (key instanceof ByteVectorIndex) {
            ByteVectorIndex vectorKey = (ByteVectorIndex) key;
            byte[] byteVector = vectorKey.vector();

            checkArgument(
                    byteVector.length == vectorOptions.dimension(),
                    "Vector dimension mismatch: expected "
                            + vectorOptions.dimension()
                            + ", but got "
                            + byteVector.length);

            vectors.add(vectorKey);
        } else {
            throw new IllegalArgumentException(
                    "Unsupported index type: " + key.getClass().getName());
        }
    }

    @Override
    public List<ResultEntry> finish() {
        try {
            if (vectors.isEmpty()) {
                return new ArrayList<>();
            }

            List<ResultEntry> results = new ArrayList<>();

            // Split vectors into batches if size exceeds sizePerIndex
            int totalVectors = vectors.size();
            int numBatches = (int) Math.ceil((double) totalVectors / sizePerIndex);

            for (int batchIndex = 0; batchIndex < numBatches; batchIndex++) {
                int startIdx = batchIndex * sizePerIndex;
                int endIdx = Math.min(startIdx + sizePerIndex, totalVectors);
                List<VectorIndex> batchVectors = vectors.subList(startIdx, endIdx);

                // Build index
                byte[] indexBytes =
                        buildIndex(
                                batchVectors,
                                this.vectorOptions.m(),
                                this.vectorOptions.efConstruction(),
                                this.vectorOptions.writeBufferSize());

                // Write to file
                String fileName = fileWriter.newFileName(VectorGlobalIndexerFactory.IDENTIFIER);
                try (OutputStream out = fileWriter.newOutputStream(fileName)) {
                    out.write(indexBytes);
                }
                long minRowIdInBatch = batchVectors.get(0).rowId();
                long maxRowIdInBatch = batchVectors.get(batchVectors.size() - 1).rowId();
                results.add(
                        ResultEntry.of(
                                fileName, null, new Range(minRowIdInBatch, maxRowIdInBatch)));
            }

            return results;
        } catch (IOException e) {
            throw new RuntimeException("Failed to write vector global index", e);
        }
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

    private byte[] buildIndex(
            List<VectorIndex> batchVectors, int m, int efConstruction, int writeBufferSize)
            throws IOException {

        try (IndexMMapDirectory indexMMapDirectory = new IndexMMapDirectory()) {
            // Configure index writer
            IndexWriterConfig config = getIndexWriterConfig(m, efConstruction, writeBufferSize);

            try (IndexWriter writer = new IndexWriter(indexMMapDirectory.directory(), config)) {
                // Add each vector as a document
                for (VectorIndex vectorIndex : batchVectors) {
                    Document doc = new Document();

                    // Add KNN vector field
                    doc.add(vectorIndex.indexableField(similarityFunction));

                    // Store row ID
                    doc.add(vectorIndex.rowIdStoredField());

                    writer.addDocument(doc);
                }

                // Commit changes
                writer.commit();
            }

            // Serialize directory to byte array
            return serializeDirectory(indexMMapDirectory.directory());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static IndexWriterConfig getIndexWriterConfig(
            int m, int efConstruction, int writeBufferSize) {
        IndexWriterConfig config = new IndexWriterConfig();
        config.setRAMBufferSizeMB(
                writeBufferSize); // Configure RAM buffer size based on user settings
        config.setCodec(
                new Lucene912Codec(Lucene912Codec.Mode.BEST_SPEED) {
                    @Override
                    public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                        return new Lucene99HnswScalarQuantizedVectorsFormat(m, efConstruction);
                    }
                });
        return config;
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
}
