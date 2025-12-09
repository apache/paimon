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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * Vector global index reader using Apache Lucene.
 *
 * <p>This implementation uses Lucene's native KnnFloatVectorQuery with HNSW graph for efficient
 * approximate nearest neighbor search.
 */
public class VectorGlobalIndexReader implements GlobalIndexReader {

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
        Throwable firstException = null;

        // Close readers
        for (IndexSearcher searcher : searchers) {
            try {
                searcher.getIndexReader().close();
            } catch (Throwable t) {
                if (firstException == null) {
                    firstException = t;
                } else {
                    firstException.addSuppressed(t);
                }
            }
        }
        searchers.clear();

        // Close directories
        for (IndexMMapDirectory directory : directories) {
            try {
                directory.close();
            } catch (Throwable t) {
                if (firstException == null) {
                    firstException = t;
                } else {
                    firstException.addSuppressed(t);
                }
            }
        }
        directories.clear();

        if (firstException != null) {
            if (firstException instanceof IOException) {
                throw (IOException) firstException;
            } else if (firstException instanceof RuntimeException) {
                throw (RuntimeException) firstException;
            } else {
                throw new RuntimeException(
                        "Failed to close vector global index reader", firstException);
            }
        }
    }

    private GlobalIndexResult search(Query query, int k) {
        PriorityQueue<ScoredRow> topK =
                new PriorityQueue<>(Comparator.comparingDouble(sr -> sr.score));
        for (IndexSearcher searcher : searchers) {
            try {
                TopDocs topDocs = searcher.search(query, k);
                StoredFields storedFields = searcher.storedFields();
                Set<String> fieldsToLoad = Set.of(VectorIndex.ROW_ID_FIELD);
                for (org.apache.lucene.search.ScoreDoc scoreDoc : topDocs.scoreDocs) {
                    Document doc = storedFields.document(scoreDoc.doc, fieldsToLoad);
                    long rowId = doc.getField(VectorIndex.ROW_ID_FIELD).numericValue().longValue();
                    if (topK.size() < k) {
                        topK.offer(new ScoredRow(rowId, scoreDoc.score));
                    } else {
                        if (topK.peek() != null && scoreDoc.score > topK.peek().score) {
                            topK.poll();
                            topK.offer(new ScoredRow(rowId, scoreDoc.score));
                        }
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to search vector index", e);
            }
        }
        RoaringNavigableMap64 roaringBitmap64 = new RoaringNavigableMap64();
        for (ScoredRow scoredRow : topK) {
            roaringBitmap64.add(scoredRow.rowId);
        }
        return GlobalIndexResult.create(() -> roaringBitmap64);
    }

    /** Helper class to store row ID with its score. */
    private static class ScoredRow {
        final long rowId;
        final float score;

        ScoredRow(long rowId, float score) {
            this.rowId = rowId;
            this.score = score;
        }
    }

    private void loadIndices(GlobalIndexFileReader fileReader, List<GlobalIndexIOMeta> files)
            throws IOException {
        for (GlobalIndexIOMeta meta : files) {
            try (SeekableInputStream in = fileReader.getInputStream(meta.fileName())) {
                IndexMMapDirectory directory = null;
                IndexReader reader = null;
                boolean success = false;
                try {
                    directory = IndexMMapDirectory.deserialize(in);
                    reader = DirectoryReader.open(directory.directory());
                    IndexSearcher searcher = new IndexSearcher(reader);
                    directories.add(directory);
                    searchers.add(searcher);
                    success = true;
                } finally {
                    if (!success) {
                        if (reader != null) {
                            try {
                                reader.close();
                            } catch (IOException e) {
                            }
                        }
                        if (directory != null) {
                            try {
                                directory.close();
                            } catch (Exception e) {
                                throw new IOException("Failed to close directory", e);
                            }
                        }
                    }
                }
            }
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
    public GlobalIndexResult visitLike(FieldRef fieldRef, Object literal) {
        throw new UnsupportedOperationException("Vector index does not support like predicate");
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
