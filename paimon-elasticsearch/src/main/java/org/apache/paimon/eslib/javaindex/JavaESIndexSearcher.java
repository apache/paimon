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

package org.apache.paimon.eslib.javaindex;

import org.apache.paimon.globalindex.EsSearchQuery;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.eslib.api.ArchiveDataProvider;
import org.elasticsearch.eslib.api.ESIndexSearcher;
import org.elasticsearch.eslib.api.model.FieldIndexConfig;
import org.elasticsearch.eslib.api.model.FullTextParams;
import org.elasticsearch.eslib.api.model.FullTextQuerySpec;
import org.elasticsearch.eslib.api.model.IndexFilter;
import org.elasticsearch.eslib.api.model.ScalarPredicate;
import org.elasticsearch.eslib.api.model.SearchResult;
import org.elasticsearch.eslib.builder.DefaultESIndexBuilder;
import org.elasticsearch.eslib.io.ArchiveDirectory;
import org.elasticsearch.eslib.searcher.DefaultESIndexSearcher;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/** Java archive reader with an independently owned, lazily opened ES DSL reader. */
public final class JavaESIndexSearcher implements ESIndexSearcher {
    private ArchiveDataProvider provider;
    private Map<String, long[]> offsets;
    private Map<String, FieldIndexConfig> fields;
    private DefaultESIndexSearcher legacy;
    private ArchiveDirectory directory;
    private DirectoryReader reader;
    private IndexSearcher searcher;
    private boolean closed;

    @Override
    public void load(
            ArchiveDataProvider provider,
            Map<String, long[]> offsets,
            Map<String, FieldIndexConfig> fields)
            throws IOException {
        load(provider, offsets, fields, null);
    }

    @Override
    public synchronized void load(
            ArchiveDataProvider provider,
            Map<String, long[]> offsets,
            Map<String, FieldIndexConfig> fields,
            ExecutorService executor)
            throws IOException {
        if (closed || this.provider != null) {
            throw new IllegalStateException("Searcher already loaded or closed");
        }
        this.provider = java.util.Objects.requireNonNull(provider, "provider");
        this.offsets = new LinkedHashMap<>();
        offsets.forEach((name, range) -> this.offsets.put(name, range.clone()));
        this.fields = new LinkedHashMap<>(fields);
        // Preserve the existing load failure/retry contract: validation happens before the
        // outer reader marks itself loaded, and a failed attempt retains no open streams.
        legacy = new DefaultESIndexSearcher();
        try {
            legacy.load(provider.fork(), this.offsets, this.fields, executor);
        } catch (IOException | RuntimeException | Error e) {
            try {
                close();
            } catch (Throwable failure) {
                e.addSuppressed(failure);
            }
            throw e;
        }
    }

    private void requireLoaded() {
        if (closed || provider == null) {
            throw new IllegalStateException("Searcher not loaded or closed");
        }
    }

    private synchronized DefaultESIndexSearcher legacy() throws IOException {
        requireLoaded();
        return legacy;
    }

    private synchronized IndexSearcher dsl() throws IOException {
        requireLoaded();
        if (searcher == null) {
            ArchiveDirectory candidate = new ArchiveDirectory(provider.fork(), offsets);
            DirectoryReader candidateReader = null;
            try {
                candidateReader = DirectoryReader.open(candidate);
                IndexSearcher candidateSearcher = new IndexSearcher(candidateReader);
                reader = candidateReader;
                directory = candidate;
                searcher = candidateSearcher;
            } catch (IOException | RuntimeException | Error e) {
                if (candidateReader != null) {
                    try {
                        candidateReader.close();
                    } catch (Throwable failure) {
                        e.addSuppressed(failure);
                    }
                }
                try {
                    candidate.close();
                } catch (Throwable failure) {
                    e.addSuppressed(failure);
                }
                throw e;
            }
        }
        return searcher;
    }

    public SearchResult esSearch(EsSearchQuery query) throws IOException {
        IndexSearcher current = dsl();
        JavaEsQueryParser parser = new JavaEsQueryParser(fields);
        JsonNode knn = EsSearchQuery.parse(query.queryJson()).get("knn");
        int candidateLimit = knn == null ? query.limit() : knn.get("num_candidates").intValue();
        TopDocs top = current.search(parser.parse(query), candidateLimit);
        if (knn != null) {
            String name = knn.get("field").textValue();
            float[] vector = parser.vector(knn);
            // Quantized candidates are reranked against the stored float vectors before
            // truncating the shard window; scores are comparable across index shards.
            Arrays.sort(top.scoreDocs, Comparator.comparingInt(hit -> hit.doc));
            int previousLeaf = -1;
            FloatVectorValues vectors = null;
            LeafReader leafReader = null;
            for (ScoreDoc hit : top.scoreDocs) {
                int leaf = ReaderUtil.subIndex(hit.doc, reader.leaves());
                if (leaf != previousLeaf) {
                    leafReader = reader.leaves().get(leaf).reader();
                    vectors = leafReader.getFloatVectorValues(name);
                    previousLeaf = leaf;
                }
                int doc = hit.doc - reader.leaves().get(leaf).docBase;
                if (vectors == null || vectors.advance(doc) != doc) {
                    throw new IOException("KNN candidate is missing its float vector");
                }
                hit.score =
                        leafReader
                                .getFieldInfos()
                                .fieldInfo(name)
                                .getVectorSimilarityFunction()
                                .compare(vector, vectors.vectorValue());
            }
            Arrays.sort(
                    top.scoreDocs,
                    Comparator.<ScoreDoc>comparingDouble(hit -> -hit.score)
                            .thenComparingInt(hit -> hit.doc));
            top.scoreDocs =
                    Arrays.copyOf(top.scoreDocs, Math.min(query.limit(), top.scoreDocs.length));
        }
        long[] ids = new long[top.scoreDocs.length];
        float[] scores = new float[ids.length];
        Map<Integer, SortedNumericDocValues> values = new HashMap<>();
        for (int i = 0; i < ids.length; i++) {
            ScoreDoc hit = top.scoreDocs[i];
            int leaf = ReaderUtil.subIndex(hit.doc, reader.leaves());
            SortedNumericDocValues rows = values.get(leaf);
            // Score order is not doc order. Obtain a fresh iterator before moving backwards.
            int doc = hit.doc - reader.leaves().get(leaf).docBase;
            if (rows == null || rows.docID() > doc) {
                rows =
                        reader.leaves()
                                .get(leaf)
                                .reader()
                                .getSortedNumericDocValues(DefaultESIndexBuilder.ROW_ID_FIELD);
                values.put(leaf, rows);
            }
            if (rows == null || !rows.advanceExact(doc) || rows.docValueCount() != 1) {
                throw new IOException("Java ES search returned a document without one row ID");
            }
            ids[i] = rows.nextValue();
            scores[i] = hit.score;
        }
        return new SearchResult(ids, scores, ids.length);
    }

    @Override
    public SearchResult vectorSearch(String field, float[] vector, int topK, long[] candidates)
            throws IOException {
        return legacy().vectorSearch(field, vector, topK, candidates);
    }

    @Override
    public SearchResult fullTextSearch(String field, String text, int topK) throws IOException {
        return legacy().fullTextSearch(field, text, topK);
    }

    @Override
    public SearchResult fullTextSearch(String field, String text, int topK, FullTextParams params)
            throws IOException {
        return legacy().fullTextSearch(field, text, topK, params);
    }

    @Override
    public SearchResult fullTextSearch(FullTextQuerySpec spec, int topK) throws IOException {
        return legacy().fullTextSearch(spec, topK);
    }

    @Override
    public long[] scalarFilter(String field, ScalarPredicate predicate) throws IOException {
        return legacy().scalarFilter(field, predicate);
    }

    @Override
    public long[] filter(String field, IndexFilter filter) throws IOException {
        return legacy().filter(field, filter);
    }

    @Override
    public long[] allRowIds() throws IOException {
        return legacy().allRowIds();
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        IOException failure = null;
        for (Closeable resource : new Closeable[] {reader, directory, legacy, provider}) {
            if (resource == null) {
                continue;
            }
            try {
                resource.close();
            } catch (Exception e) {
                IOException wrapped =
                        e instanceof IOException ? (IOException) e : new IOException(e);
                if (failure == null) {
                    failure = wrapped;
                } else {
                    failure.addSuppressed(wrapped);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }
}
