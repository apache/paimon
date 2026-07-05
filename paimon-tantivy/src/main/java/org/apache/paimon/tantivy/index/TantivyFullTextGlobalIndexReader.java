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

package org.apache.paimon.tantivy.index;

import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.index.fulltext.FullTextIndexInput;
import org.apache.paimon.index.fulltext.FullTextIndexReader;
import org.apache.paimon.index.fulltext.FullTextSearchResult;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.FullTextQuery;
import org.apache.paimon.predicate.FullTextSearch;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.RoaringNavigableMap64;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Full-text global index reader using paimon-full-text.
 *
 * <p>The standalone native reader receives positional read callbacks backed by Paimon's {@link
 * SeekableInputStream}. No temp files are created.
 */
public class TantivyFullTextGlobalIndexReader implements GlobalIndexReader {

    private static final String NATIVE_TEXT_FIELD = "text";

    private final GlobalIndexIOMeta ioMeta;
    private final GlobalIndexFileReader fileReader;
    private final ExecutorService executor;

    private volatile FullTextIndexReader reader;
    private volatile SeekableInputStream inputStream;

    public TantivyFullTextGlobalIndexReader(
            GlobalIndexFileReader fileReader,
            List<GlobalIndexIOMeta> ioMetas,
            ExecutorService executor) {
        checkArgument(ioMetas.size() == 1, "Expected exactly one index file per shard");
        this.executor = executor;
        this.fileReader = fileReader;
        this.ioMeta = ioMetas.get(0);
    }

    @Override
    public CompletableFuture<Optional<ScoredGlobalIndexResult>> visitFullTextSearch(
            FullTextSearch fullTextSearch) {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        ensureLoaded();
                        RoaringNavigableMap64 includeRowIds = fullTextSearch.includeRowIds();
                        if (includeRowIds != null && includeRowIds.isEmpty()) {
                            return Optional.of(ScoredGlobalIndexResult.createEmpty());
                        }
                        checkArgument(
                                fullTextSearch.columns().size() == 1,
                                "Tantivy full-text index reader expects a single-column query, got: %s",
                                fullTextSearch.columns());
                        org.apache.paimon.index.fulltext.FullTextQuery query =
                                org.apache.paimon.index.fulltext.FullTextQuery.json(
                                        toNativeQueryJson(fullTextSearch.query()));
                        FullTextSearchResult result =
                                includeRowIds == null
                                        ? reader.search(query, fullTextSearch.limit())
                                        : reader.search(
                                                query,
                                                fullTextSearch.limit(),
                                                includeRowIds.serialize());
                        return Optional.of(toScoredResult(result));
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to search Tantivy full-text index", e);
                    }
                },
                executor);
    }

    private ScoredGlobalIndexResult toScoredResult(FullTextSearchResult result) {
        RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
        HashMap<Long, Float> id2scores = new HashMap<>(result.size());
        for (int i = 0; i < result.size(); i++) {
            long rowId = result.rowIds()[i];
            bitmap.add(rowId);
            id2scores.put(rowId, result.scores()[i]);
        }
        return new TantivyScoredGlobalIndexResult(bitmap, id2scores);
    }

    static String toNativeQueryJson(FullTextQuery query) {
        return JsonSerdeUtil.toFlatJson(toNativeRootMap(query));
    }

    private static Map<String, Object> toNativeRootMap(FullTextQuery query) {
        if (query instanceof FullTextQuery.Match) {
            FullTextQuery.Match match = (FullTextQuery.Match) query;
            checkArgument(
                    match.fuzziness() == null || match.fuzziness() == 0,
                    "match query fuzziness is not supported by paimon-full-text yet");
            checkArgument(
                    match.maxExpansions() == 50,
                    "match query maxExpansions is not supported by paimon-full-text yet");
            checkArgument(
                    match.prefixLength() == 0,
                    "match query prefixLength is not supported by paimon-full-text yet");

            Map<String, Object> body = new LinkedHashMap<>();
            body.put("column", NATIVE_TEXT_FIELD);
            body.put("terms", match.terms());
            body.put("operator", match.operator().jsonValue());
            body.put("boost", match.boost());
            return root("match", body);
        }
        if (query instanceof FullTextQuery.Phrase) {
            FullTextQuery.Phrase phrase = (FullTextQuery.Phrase) query;
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("column", NATIVE_TEXT_FIELD);
            body.put("terms", phrase.terms());
            body.put("slop", phrase.slop());
            return root("match_phrase", body);
        }
        if (query instanceof FullTextQuery.Boost) {
            FullTextQuery.Boost boost = (FullTextQuery.Boost) query;
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("positive", toNativeRootMap(boost.positive()));
            body.put("negative", toNativeRootMap(boost.negative()));
            body.put("negative_boost", boost.negativeBoost());
            return root("boost", body);
        }
        if (query instanceof FullTextQuery.BooleanQuery) {
            FullTextQuery.BooleanQuery bool = (FullTextQuery.BooleanQuery) query;
            List<List<Object>> clauses = new ArrayList<>();
            for (FullTextQuery.Clause clause : bool.queries()) {
                List<Object> nativeClause = new ArrayList<>(2);
                nativeClause.add(toNativeOccur(clause.occur()));
                nativeClause.add(toNativeRootMap(clause.query()));
                clauses.add(nativeClause);
            }
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("queries", clauses);
            return root("boolean", body);
        }
        throw new IllegalArgumentException(
                "Unsupported single-column full-text query: " + query.getClass().getName());
    }

    private static String toNativeOccur(FullTextQuery.Occur occur) {
        switch (occur) {
            case SHOULD:
                return "Should";
            case MUST:
                return "Must";
            case MUST_NOT:
                return "MustNot";
            default:
                throw new IllegalArgumentException("Unsupported boolean occur: " + occur);
        }
    }

    private static Map<String, Object> root(String name, Map<String, Object> body) {
        Map<String, Object> root = new LinkedHashMap<>();
        root.put(name, body);
        return root;
    }

    private void ensureLoaded() throws IOException {
        if (reader == null) {
            synchronized (this) {
                if (reader == null) {
                    SeekableInputStream input = fileReader.getInputStream(ioMeta);
                    try {
                        inputStream = input;
                        reader = new FullTextIndexReader(new PaimonFullTextIndexInput(input));
                    } catch (RuntimeException e) {
                        input.close();
                        inputStream = null;
                        throw e;
                    }
                }
            }
        }
    }

    static TantivyFullTextIndexOptions deserializeIndexOptions(byte[] metadata) {
        try {
            return TantivyFullTextIndexOptions.deserialize(metadata);
        } catch (IOException e) {
            throw new IllegalArgumentException(
                    "Failed to deserialize Tantivy full-text index meta", e);
        }
    }

    private static void readFully(SeekableInputStream in, byte[] buf, int off, int len)
            throws IOException {
        int end = off + len;
        while (off < end) {
            int read = in.read(buf, off, end - off);
            if (read == -1) {
                throw new IOException("Unexpected end of stream");
            }
            off += read;
        }
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
            reader = null;
        }
        if (inputStream != null) {
            inputStream.close();
            inputStream = null;
        }
    }

    private static class PaimonFullTextIndexInput implements FullTextIndexInput {

        private final SeekableInputStream in;

        private PaimonFullTextIndexInput(SeekableInputStream in) {
            this.in = in;
        }

        @Override
        public synchronized void readAt(long position, byte[] buffer, int offset, int length)
                throws IOException {
            in.seek(position);
            readFully(in, buffer, offset, length);
        }

        @Override
        public synchronized void pread(long[] positions, byte[][] buffers) throws IOException {
            checkArgument(
                    positions.length == buffers.length,
                    "positions length %s does not match buffers length %s",
                    positions.length,
                    buffers.length);
            for (int i = 0; i < positions.length; i++) {
                readAt(positions[i], buffers[i], 0, buffers[i].length);
            }
        }
    }

    // =================== unsupported =====================

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitIsNotNull(FieldRef fieldRef) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitIsNull(FieldRef fieldRef) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitStartsWith(
            FieldRef fieldRef, Object literal) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitEndsWith(
            FieldRef fieldRef, Object literal) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitContains(
            FieldRef fieldRef, Object literal) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitLike(
            FieldRef fieldRef, Object literal) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitLessThan(
            FieldRef fieldRef, Object literal) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitGreaterOrEqual(
            FieldRef fieldRef, Object literal) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitNotEqual(
            FieldRef fieldRef, Object literal) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitLessOrEqual(
            FieldRef fieldRef, Object literal) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitEqual(
            FieldRef fieldRef, Object literal) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitGreaterThan(
            FieldRef fieldRef, Object literal) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitIn(
            FieldRef fieldRef, List<Object> literals) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitNotIn(
            FieldRef fieldRef, List<Object> literals) {
        return CompletableFuture.completedFuture(Optional.empty());
    }
}
