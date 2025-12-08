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

package org.apache.paimon.vector.index;

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
import org.apache.lucene.store.IOContext;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
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
    private final VectorIndexOptions vectorOptions;
    private final VectorSimilarityFunction similarityFunction;
    private final int sizePerIndex;

    private final List<VectorIndex> vectorIndices;
    private final List<ResultEntry> results;

    public VectorGlobalIndexWriter(
            GlobalIndexFileWriter fileWriter, DataType fieldType, Options options) {
        checkArgument(
                fieldType instanceof ArrayType,
                "Vector field type must be ARRAY, but was: " + fieldType);
        this.fileWriter = fileWriter;
        this.vectorIndices = new ArrayList<>();
        this.results = new ArrayList<>();
        this.vectorOptions = new VectorIndexOptions(options);
        this.similarityFunction = vectorOptions.metric().vectorSimilarityFunction();
        this.sizePerIndex = vectorOptions.sizePerIndex();
    }

    @Override
    public void write(Object key) {
        VectorIndex index;
        if (key instanceof FloatVectorIndex) {
            index = (FloatVectorIndex) key;
        } else if (key instanceof ByteVectorIndex) {
            index = (ByteVectorIndex) key;
        } else {
            throw new IllegalArgumentException(
                    "Unsupported index type: " + key.getClass().getName());
        }
        index.checkDimension(vectorOptions.dimension());
        vectorIndices.add(index);
        if (vectorIndices.size() >= sizePerIndex) {
            try {
                flush();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public List<ResultEntry> finish() {
        try {
            if (!vectorIndices.isEmpty()) {
                flush();
            }

            return results;
        } catch (IOException e) {
            throw new RuntimeException("Failed to write vector global index", e);
        }
    }

    private void flush() throws IOException {
        String fileName = fileWriter.newFileName(VectorGlobalIndexerFactory.IDENTIFIER);
        try (OutputStream out = new BufferedOutputStream(fileWriter.newOutputStream(fileName))) {
            buildIndex(
                    vectorIndices,
                    this.vectorOptions.m(),
                    this.vectorOptions.efConstruction(),
                    this.vectorOptions.writeBufferSize(),
                    out);
        }
        long minRowIdInBatch = vectorIndices.get(0).rowId();
        long maxRowIdInBatch = vectorIndices.get(vectorIndices.size() - 1).rowId();
        results.add(ResultEntry.of(fileName, null, new Range(minRowIdInBatch, maxRowIdInBatch)));
        vectorIndices.clear();
    }

    private void buildIndex(
            List<VectorIndex> batchVectors,
            int m,
            int efConstruction,
            int writeBufferSize,
            OutputStream out)
            throws IOException {

        IndexWriterConfig config = getIndexWriterConfig(m, efConstruction, writeBufferSize);
        try (IndexMMapDirectory indexMMapDirectory = new IndexMMapDirectory()) {
            try (IndexWriter writer = new IndexWriter(indexMMapDirectory.directory(), config)) {
                for (VectorIndex vectorIndex : batchVectors) {
                    Document doc = new Document();
                    doc.add(vectorIndex.indexableField(similarityFunction));
                    doc.add(vectorIndex.rowIdStoredField());
                    writer.addDocument(doc);
                }
                writer.commit();
            }

            serializeDirectory(indexMMapDirectory.directory(), out);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static IndexWriterConfig getIndexWriterConfig(
            int m, int efConstruction, int writeBufferSize) {
        IndexWriterConfig config = new IndexWriterConfig();
        config.setRAMBufferSizeMB(writeBufferSize);
        config.setCodec(
                new Lucene912Codec(Lucene912Codec.Mode.BEST_SPEED) {
                    @Override
                    public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                        return new Lucene99HnswScalarQuantizedVectorsFormat(m, efConstruction);
                    }
                });
        return config;
    }

    private void serializeDirectory(Directory directory, OutputStream out) throws IOException {
        String[] files = directory.listAll();
        out.write(intToBytes(files.length));

        for (String fileName : files) {
            byte[] nameBytes = fileName.getBytes(StandardCharsets.UTF_8);
            out.write(intToBytes(nameBytes.length));
            out.write(nameBytes);
            long fileLength = directory.fileLength(fileName);
            out.write(ByteBuffer.allocate(8).putLong(fileLength).array());

            try (org.apache.lucene.store.IndexInput input =
                    directory.openInput(fileName, IOContext.DEFAULT)) {
                byte[] buffer = new byte[32 * 1024];
                long remaining = fileLength;

                while (remaining > 0) {
                    int toRead = (int) Math.min(buffer.length, remaining);
                    input.readBytes(buffer, 0, toRead);
                    out.write(buffer, 0, toRead);
                    remaining -= toRead;
                }
            }
        }
    }

    private byte[] intToBytes(int value) {
        return ByteBuffer.allocate(4).putInt(value).array();
    }
}
