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
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.RoaringNavigableMap64;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;

import static org.apache.paimon.lucene.index.LuceneVectorIndex.ROW_ID_FIELD;

/**
 * Vector global index reader using Apache Lucene.
 *
 * <p>This implementation uses Lucene's native KnnFloatVectorQuery with HNSW graph for efficient
 * approximate nearest neighbor search.
 */
public class LuceneVectorGlobalIndexReader implements GlobalIndexReader {

    private final List<IndexSearcher> searchers;
    private final List<LuceneIndexMMapDirectory> directories;
    private final List<GlobalIndexIOMeta> ioMetas;
    private final GlobalIndexFileReader fileReader;
    private volatile boolean indicesLoaded = false;
    private final DataType fieldType;

    public LuceneVectorGlobalIndexReader(
            GlobalIndexFileReader fileReader, List<GlobalIndexIOMeta> ioMetas, DataType fieldType) {
        this.fileReader = fileReader;
        this.ioMetas = ioMetas;
        this.fieldType = fieldType;
        this.searchers = new ArrayList<>();
        this.directories = new ArrayList<>();
    }

    @Override
    public Optional<GlobalIndexResult> visitVectorSearch(VectorSearch vectorSearch) {
        try {
            ensureLoadIndices(fileReader, ioMetas);
            Query query = query(vectorSearch, fieldType);
            return search(query, vectorSearch.limit());
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format(
                            "Failed to search vector index with fieldName=%s, limit=%d",
                            vectorSearch.fieldName(), vectorSearch.limit()),
                    e);
        }
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
        for (LuceneIndexMMapDirectory directory : directories) {
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

    private Query query(VectorSearch vectorSearch, DataType dataType) {
        Query idFilterQuery = null;
        RoaringNavigableMap64 includeRowIds = vectorSearch.includeRowIds();
        if (includeRowIds != null) {
            long[] targetIds = new long[includeRowIds.getIntCardinality()];
            Iterator<Long> iterator = includeRowIds.iterator();
            for (int i = 0; i < targetIds.length; i++) {
                targetIds[i] = iterator.next();
            }
            idFilterQuery = LongPoint.newSetQuery(ROW_ID_FIELD, targetIds);
        }
        if (dataType instanceof ArrayType
                && ((ArrayType) dataType).getElementType() instanceof FloatType) {
            if (!(vectorSearch.vector() instanceof float[])) {
                throw new IllegalArgumentException(
                        "Expected float[] vector but got: " + vectorSearch.vector().getClass());
            }
            return new KnnFloatVectorQuery(
                    LuceneVectorIndex.VECTOR_FIELD,
                    (float[]) vectorSearch.vector(),
                    vectorSearch.limit(),
                    idFilterQuery);
        } else if (dataType instanceof ArrayType
                && ((ArrayType) dataType).getElementType() instanceof TinyIntType) {
            if (!(vectorSearch.vector() instanceof byte[])) {
                throw new IllegalArgumentException(
                        "Expected byte[] vector but got: " + vectorSearch.vector().getClass());
            }
            return new KnnByteVectorQuery(
                    LuceneVectorIndex.VECTOR_FIELD,
                    (byte[]) vectorSearch.vector(),
                    vectorSearch.limit(),
                    idFilterQuery);
        } else {
            throw new IllegalArgumentException("Unsupported data type: " + dataType);
        }
    }

    private Optional<GlobalIndexResult> search(Query query, int limit) throws IOException {
        PriorityQueue<ScoredRow> result =
                new PriorityQueue<>(Comparator.comparingDouble(sr -> sr.score));
        for (IndexSearcher searcher : searchers) {
            try {
                TopDocs topDocs = searcher.search(query, limit);
                StoredFields storedFields = searcher.storedFields();
                Set<String> fieldsToLoad = Set.of(ROW_ID_FIELD);
                for (org.apache.lucene.search.ScoreDoc scoreDoc : topDocs.scoreDocs) {
                    Document doc = storedFields.document(scoreDoc.doc, fieldsToLoad);
                    long rowId = doc.getField(ROW_ID_FIELD).numericValue().longValue();
                    if (result.size() < limit) {
                        result.offer(new ScoredRow(rowId, scoreDoc.score));
                    } else {
                        if (result.peek() != null && scoreDoc.score > result.peek().score) {
                            result.poll();
                            result.offer(new ScoredRow(rowId, scoreDoc.score));
                        }
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to search vector index", e);
            }
        }
        RoaringNavigableMap64 roaringBitmap64 = new RoaringNavigableMap64();
        HashMap<Long, Float> id2scores = new HashMap<>(result.size());
        for (ScoredRow scoredRow : result) {
            long rowId = scoredRow.rowId;
            id2scores.put(rowId, scoredRow.score);
            roaringBitmap64.add(rowId);
        }
        return Optional.of(new LuceneVectorSearchGlobalIndexResult(roaringBitmap64, id2scores));
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

    private void ensureLoadIndices(GlobalIndexFileReader fileReader, List<GlobalIndexIOMeta> files)
            throws IOException {
        if (!indicesLoaded) {
            synchronized (this) {
                if (!indicesLoaded) {
                    for (GlobalIndexIOMeta meta : files) {
                        try (SeekableInputStream in = fileReader.getInputStream(meta.fileName())) {
                            LuceneIndexMMapDirectory directory = null;
                            IndexReader reader = null;
                            try {
                                directory = LuceneIndexMMapDirectory.deserialize(in);
                                reader = DirectoryReader.open(directory.directory());
                                IndexSearcher searcher = new IndexSearcher(reader);
                                directories.add(directory);
                                searchers.add(searcher);
                                indicesLoaded = true;
                            } finally {
                                if (!indicesLoaded) {
                                    IOUtils.closeQuietly(reader);
                                    IOUtils.closeQuietly(directory);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // =================== unsupported =====================

    @Override
    public Optional<GlobalIndexResult> visitIsNotNull(FieldRef fieldRef) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitIsNull(FieldRef fieldRef) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitStartsWith(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitEndsWith(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitContains(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitLike(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitLessThan(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitGreaterOrEqual(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitNotEqual(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitLessOrEqual(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitEqual(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitGreaterThan(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitIn(FieldRef fieldRef, List<Object> literals) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitNotIn(FieldRef fieldRef, List<Object> literals) {
        return Optional.empty();
    }
}
