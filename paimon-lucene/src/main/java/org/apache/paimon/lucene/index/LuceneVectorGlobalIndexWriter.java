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

package org.apache.paimon.lucene.index;

import org.apache.paimon.globalindex.GlobalIndexSingletonWriter;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.types.DataType;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene912.Lucene912Codec;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorSimilarityFunction;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Vector global index writer using Apache Lucene.
 *
 * <p>This implementation uses Lucene's native KnnFloatVectorField with HNSW algorithm for efficient
 * approximate nearest neighbor search.
 */
public class LuceneVectorGlobalIndexWriter implements GlobalIndexSingletonWriter {

    private final GlobalIndexFileWriter fileWriter;
    private final LuceneVectorIndexOptions vectorIndexOptions;
    private final VectorSimilarityFunction similarityFunction;
    private final int sizePerIndex;
    private final LuceneVectorIndexFactory vectorIndexFactory;

    private long count = 0;
    private final List<LuceneVectorIndex<?>> vectorIndices;
    private final List<ResultEntry> results;

    public LuceneVectorGlobalIndexWriter(
            GlobalIndexFileWriter fileWriter,
            DataType fieldType,
            LuceneVectorIndexOptions options) {
        this.vectorIndexFactory = LuceneVectorIndexFactory.init(fieldType);
        this.fileWriter = fileWriter;
        this.vectorIndices = new ArrayList<>();
        this.results = new ArrayList<>();
        this.vectorIndexOptions = options;
        this.similarityFunction = vectorIndexOptions.metric().vectorSimilarityFunction();
        this.sizePerIndex = vectorIndexOptions.sizePerIndex();
    }

    @Override
    public void write(Object key) {
        LuceneVectorIndex<?> index = vectorIndexFactory.create(count, key);
        index.checkDimension(vectorIndexOptions.dimension());
        vectorIndices.add(index);
        count++;
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
        String fileName = fileWriter.newFileName(LuceneVectorGlobalIndexerFactory.IDENTIFIER);
        try (OutputStream out = new BufferedOutputStream(fileWriter.newOutputStream(fileName))) {
            buildIndex(
                    vectorIndices,
                    this.vectorIndexOptions.m(),
                    this.vectorIndexOptions.efConstruction(),
                    this.vectorIndexOptions.writeBufferSize(),
                    out);
        }
        results.add(new ResultEntry(fileName, count, null));
        vectorIndices.clear();
    }

    private void buildIndex(
            List<LuceneVectorIndex<?>> batchVectors,
            int m,
            int efConstruction,
            int writeBufferSize,
            OutputStream out) {

        IndexWriterConfig config = getIndexWriterConfig(m, efConstruction, writeBufferSize);
        try (LuceneIndexMMapDirectory luceneIndexMMapDirectory = new LuceneIndexMMapDirectory()) {
            try (IndexWriter writer =
                    new IndexWriter(luceneIndexMMapDirectory.directory(), config)) {
                for (LuceneVectorIndex<?> luceneVectorIndex : batchVectors) {
                    Document doc = new Document();
                    doc.add(luceneVectorIndex.indexableField(similarityFunction));
                    doc.add(luceneVectorIndex.rowIdLongPoint());
                    doc.add(luceneVectorIndex.rowIdStoredField());
                    writer.addDocument(doc);
                }
                writer.commit();
            }
            luceneIndexMMapDirectory.serialize(out);
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
                        return new Lucene99HnswVectorsFormat(m, efConstruction);
                    }
                });
        return config;
    }
}
