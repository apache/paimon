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

import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.predicate.FieldRef;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Vector global index reader using Apache Lucene 9.x.
 *
 * <p>This implementation uses Lucene's native KnnFloatVectorQuery with HNSW graph for efficient
 * approximate nearest neighbor search.
 */
public class VectorGlobalIndexReader implements GlobalIndexReader {

    private static final String VECTOR_FIELD = "vector";
    private static final String ROW_ID_FIELD = "rowId";

    private final List<IndexSearcher> searchers;
    private final List<Directory> directories;
    private final List<java.nio.file.Path> tempDirs;

    public VectorGlobalIndexReader(GlobalIndexFileReader fileReader, List<GlobalIndexIOMeta> files)
            throws IOException {
        this.searchers = new ArrayList<>();
        this.directories = new ArrayList<>();
        this.tempDirs = new ArrayList<>();
        loadIndices(fileReader, files);
    }

    /**
     * Search for similar vectors using Lucene KNN search.
     *
     * @param query query vector
     * @param k number of results
     * @return global index result containing row IDs
     */
    public GlobalIndexResult search(float[] query, int k) {
        Set<Long> resultIds = new HashSet<>();

        for (IndexSearcher searcher : searchers) {
            try {
                // Create KNN query
                KnnFloatVectorQuery knnQuery = new KnnFloatVectorQuery(VECTOR_FIELD, query, k);

                // Execute search
                TopDocs topDocs = searcher.search(knnQuery, k);
                StoredFields storedFields = searcher.storedFields();
                Set<String> fieldsToLoad = Set.of(ROW_ID_FIELD);
                // Collect row IDs from results
                for (org.apache.lucene.search.ScoreDoc scoreDoc : topDocs.scoreDocs) {
                    float rawScore = scoreDoc.score;
                    Document doc = storedFields.document(scoreDoc.doc, fieldsToLoad);
                    long rowId = doc.getField(ROW_ID_FIELD).numericValue().longValue();
                    resultIds.add(rowId);
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to search vector index", e);
            }
        }

        return GlobalIndexResult.wrap(resultIds);
    }

    @Override
    public void close() throws IOException {
        // Close readers
        for (IndexSearcher searcher : searchers) {
            searcher.getIndexReader().close();
        }
        searchers.clear();

        // Close directories
        for (Directory directory : directories) {
            directory.close();
        }
        directories.clear();

        // Clean up temp directories
        for (java.nio.file.Path tempDir : tempDirs) {
            deleteDirectory(tempDir);
        }
        tempDirs.clear();
    }

    private void loadIndices(GlobalIndexFileReader fileReader, List<GlobalIndexIOMeta> files)
            throws IOException {
        for (GlobalIndexIOMeta meta : files) {
            try (SeekableInputStream in = fileReader.getInputStream(meta.fileName())) {
                byte[] indexBytes = new byte[(int) meta.fileSize()];
                int totalRead = 0;
                while (totalRead < indexBytes.length) {
                    int read = in.read(indexBytes, totalRead, indexBytes.length - totalRead);
                    if (read == -1) {
                        throw new IOException("Unexpected end of stream");
                    }
                    totalRead += read;
                }

                Directory directory = deserializeDirectory(indexBytes);
                directories.add(directory);

                IndexReader reader = DirectoryReader.open(directory);
                IndexSearcher searcher = new IndexSearcher(reader);
                searchers.add(searcher);
            }
        }
    }

    private Directory deserializeDirectory(byte[] data) throws IOException {
        // Create temporary directory for MMap
        Path tempDir = Files.createTempDirectory("paimon-vector-read" + UUID.randomUUID());
        tempDirs.add(tempDir);
        Directory directory = new MMapDirectory(tempDir);

        ByteBuffer buffer = ByteBuffer.wrap(data);

        // Read number of files
        int numFiles = buffer.getInt();

        for (int i = 0; i < numFiles; i++) {
            // Read file name
            int nameLength = buffer.getInt();
            byte[] nameBytes = new byte[nameLength];
            buffer.get(nameBytes);
            String fileName = new String(nameBytes, StandardCharsets.UTF_8);

            // Read file content
            long fileLength = buffer.getLong();
            byte[] fileContent = new byte[(int) fileLength];
            buffer.get(fileContent);

            // Write to directory
            try (IndexOutput output = directory.createOutput(fileName, null)) {
                output.writeBytes(fileContent, 0, fileContent.length);
            }
        }

        return directory;
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

    // Implementation of FunctionVisitor methods
    @Override
    public Optional<GlobalIndexResult> visitIsNotNull(FieldRef fieldRef) {
        throw new UnsupportedOperationException(
                "Vector index does not support isNotNull predicate");
    }

    @Override
    public Optional<GlobalIndexResult> visitIsNull(FieldRef fieldRef) {
        throw new UnsupportedOperationException("Vector index does not support isNull predicate");
    }

    @Override
    public Optional<GlobalIndexResult> visitStartsWith(FieldRef fieldRef, Object literal) {
        throw new UnsupportedOperationException(
                "Vector index does not support startsWith predicate");
    }

    @Override
    public Optional<GlobalIndexResult> visitEndsWith(FieldRef fieldRef, Object literal) {
        throw new UnsupportedOperationException("Vector index does not support endsWith predicate");
    }

    @Override
    public Optional<GlobalIndexResult> visitContains(FieldRef fieldRef, Object literal) {
        throw new UnsupportedOperationException("Vector index does not support contains predicate");
    }

    @Override
    public Optional<GlobalIndexResult> visitLessThan(FieldRef fieldRef, Object literal) {
        throw new UnsupportedOperationException("Vector index does not support lessThan predicate");
    }

    @Override
    public Optional<GlobalIndexResult> visitGreaterOrEqual(FieldRef fieldRef, Object literal) {
        throw new UnsupportedOperationException(
                "Vector index does not support greaterOrEqual predicate");
    }

    @Override
    public Optional<GlobalIndexResult> visitNotEqual(FieldRef fieldRef, Object literal) {
        throw new UnsupportedOperationException("Vector index does not support notEqual predicate");
    }

    @Override
    public Optional<GlobalIndexResult> visitLessOrEqual(FieldRef fieldRef, Object literal) {
        throw new UnsupportedOperationException(
                "Vector index does not support lessOrEqual predicate");
    }

    @Override
    public Optional<GlobalIndexResult> visitEqual(FieldRef fieldRef, Object literal) {
        throw new UnsupportedOperationException("Vector index does not support equal predicate");
    }

    @Override
    public Optional<GlobalIndexResult> visitGreaterThan(FieldRef fieldRef, Object literal) {
        throw new UnsupportedOperationException(
                "Vector index does not support greaterThan predicate");
    }

    @Override
    public Optional<GlobalIndexResult> visitIn(FieldRef fieldRef, List<Object> literals) {
        throw new UnsupportedOperationException("Vector index does not support in predicate");
    }

    @Override
    public Optional<GlobalIndexResult> visitNotIn(FieldRef fieldRef, List<Object> literals) {
        throw new UnsupportedOperationException("Vector index does not support notIn predicate");
    }
}
