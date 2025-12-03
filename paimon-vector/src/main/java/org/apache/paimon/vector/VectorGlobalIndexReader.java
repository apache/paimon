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
import org.apache.paimon.utils.RoaringNavigableMap64;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnByteVectorQuery;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Vector global index reader using Apache Lucene 9.x.
 *
 * <p>This implementation uses Lucene's native KnnFloatVectorQuery with HNSW graph for efficient
 * approximate nearest neighbor search.
 */
public class VectorGlobalIndexReader implements GlobalIndexReader {

    private static final int BUFFER_SIZE = 8192; // 8KB buffer for streaming

    private final List<IndexSearcher> searchers;
    private final List<IndexMMapDirectory> directories;

    public VectorGlobalIndexReader(GlobalIndexFileReader fileReader, List<GlobalIndexIOMeta> files)
            throws IOException {
        this.searchers = new ArrayList<>();
        this.directories = new ArrayList<>();
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
        KnnFloatVectorQuery knnQuery = new KnnFloatVectorQuery(VectorIndex.VECTOR_FIELD, query, k);
        return search(knnQuery, k);
    }

    public GlobalIndexResult search(byte[] query, int k) {
        KnnByteVectorQuery knnQuery = new KnnByteVectorQuery(VectorIndex.VECTOR_FIELD, query, k);
        return search(knnQuery, k);
    }

    @Override
    public void close() throws IOException {
        // Close readers
        for (IndexSearcher searcher : searchers) {
            searcher.getIndexReader().close();
        }
        searchers.clear();

        // Close directories
        for (IndexMMapDirectory directory : directories) {
            try {
                directory.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        directories.clear();
    }

    private GlobalIndexResult search(Query query, int k) {
        RoaringNavigableMap64 roaringBitmap64 = new RoaringNavigableMap64();
        for (IndexSearcher searcher : searchers) {
            try {
                // Execute search
                TopDocs topDocs = searcher.search(query, k);
                StoredFields storedFields = searcher.storedFields();
                Set<String> fieldsToLoad = Set.of(VectorIndex.ROW_ID_FIELD);
                // Collect row IDs from results
                for (org.apache.lucene.search.ScoreDoc scoreDoc : topDocs.scoreDocs) {
                    float rawScore = scoreDoc.score;
                    Document doc = storedFields.document(scoreDoc.doc, fieldsToLoad);
                    long rowId = doc.getField(VectorIndex.ROW_ID_FIELD).numericValue().longValue();
                    roaringBitmap64.add(rowId);
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to search vector index", e);
            }
        }

        return GlobalIndexResult.create(() -> roaringBitmap64);
    }

    private void loadIndices(GlobalIndexFileReader fileReader, List<GlobalIndexIOMeta> files)
            throws IOException {
        for (GlobalIndexIOMeta meta : files) {
            try (SeekableInputStream in = fileReader.getInputStream(meta.fileName())) {
                IndexMMapDirectory directory = deserializeDirectory(in);
                directories.add(directory);
                IndexReader reader = DirectoryReader.open(directory.directory());
                IndexSearcher searcher = new IndexSearcher(reader);
                searchers.add(searcher);
            }
        }
    }

    private IndexMMapDirectory deserializeDirectory(SeekableInputStream in) throws IOException {
        IndexMMapDirectory indexMMapDirectory = new IndexMMapDirectory();

        // Read number of files
        int numFiles = readInt(in);

        // Reusable buffer for streaming
        byte[] buffer = new byte[BUFFER_SIZE];

        for (int i = 0; i < numFiles; i++) {
            // Read file name
            int nameLength = readInt(in);
            byte[] nameBytes = new byte[nameLength];
            readFully(in, nameBytes);
            String fileName = new String(nameBytes, StandardCharsets.UTF_8);

            // Read file content length
            long fileLength = readLong(in);

            // Stream file content directly to directory
            try (IndexOutput output = indexMMapDirectory.directory().createOutput(fileName, null)) {
                long remaining = fileLength;
                while (remaining > 0) {
                    int toRead = (int) Math.min(buffer.length, remaining);
                    readFully(in, buffer, 0, toRead);
                    output.writeBytes(buffer, 0, toRead);
                    remaining -= toRead;
                }
            }
        }

        return indexMMapDirectory;
    }

    private int readInt(SeekableInputStream in) throws IOException {
        byte[] bytes = new byte[4];
        readFully(in, bytes);
        return ByteBuffer.wrap(bytes).getInt();
    }

    private long readLong(SeekableInputStream in) throws IOException {
        byte[] bytes = new byte[8];
        readFully(in, bytes);
        return ByteBuffer.wrap(bytes).getLong();
    }

    private void readFully(SeekableInputStream in, byte[] buffer) throws IOException {
        readFully(in, buffer, 0, buffer.length);
    }

    private void readFully(SeekableInputStream in, byte[] buffer, int offset, int length)
            throws IOException {
        int totalRead = 0;
        while (totalRead < length) {
            int read = in.read(buffer, offset + totalRead, length - totalRead);
            if (read == -1) {
                throw new IOException("Unexpected end of stream");
            }
            totalRead += read;
        }
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
    public GlobalIndexResult visitIsNotNull(FieldRef fieldRef) {
        throw new UnsupportedOperationException(
                "Vector index does not support isNotNull predicate");
    }

    @Override
    public GlobalIndexResult visitIsNull(FieldRef fieldRef) {
        throw new UnsupportedOperationException("Vector index does not support isNull predicate");
    }

    @Override
    public GlobalIndexResult visitStartsWith(FieldRef fieldRef, Object literal) {
        throw new UnsupportedOperationException(
                "Vector index does not support startsWith predicate");
    }

    @Override
    public GlobalIndexResult visitEndsWith(FieldRef fieldRef, Object literal) {
        throw new UnsupportedOperationException("Vector index does not support endsWith predicate");
    }

    @Override
    public GlobalIndexResult visitContains(FieldRef fieldRef, Object literal) {
        throw new UnsupportedOperationException("Vector index does not support contains predicate");
    }

    @Override
    public GlobalIndexResult visitLessThan(FieldRef fieldRef, Object literal) {
        throw new UnsupportedOperationException("Vector index does not support lessThan predicate");
    }

    @Override
    public GlobalIndexResult visitGreaterOrEqual(FieldRef fieldRef, Object literal) {
        throw new UnsupportedOperationException(
                "Vector index does not support greaterOrEqual predicate");
    }

    @Override
    public GlobalIndexResult visitNotEqual(FieldRef fieldRef, Object literal) {
        throw new UnsupportedOperationException("Vector index does not support notEqual predicate");
    }

    @Override
    public GlobalIndexResult visitLessOrEqual(FieldRef fieldRef, Object literal) {
        throw new UnsupportedOperationException(
                "Vector index does not support lessOrEqual predicate");
    }

    @Override
    public GlobalIndexResult visitEqual(FieldRef fieldRef, Object literal) {
        throw new UnsupportedOperationException("Vector index does not support equal predicate");
    }

    @Override
    public GlobalIndexResult visitGreaterThan(FieldRef fieldRef, Object literal) {
        throw new UnsupportedOperationException(
                "Vector index does not support greaterThan predicate");
    }

    @Override
    public GlobalIndexResult visitIn(FieldRef fieldRef, List<Object> literals) {
        throw new UnsupportedOperationException("Vector index does not support in predicate");
    }

    @Override
    public GlobalIndexResult visitNotIn(FieldRef fieldRef, List<Object> literals) {
        throw new UnsupportedOperationException("Vector index does not support notIn predicate");
    }
}
