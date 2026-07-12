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

package org.apache.paimon.globalindex.testfulltext;

import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.FullTextSearch;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.RoaringNavigableMap64;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.concurrent.CompletableFuture;

/**
 * Test full-text index reader that performs brute-force text matching. Loads all documents into
 * memory and scores them against the query using simple term-frequency matching.
 *
 * <p>Scoring: for each query term found in the document (case-insensitive), score += 1.0 /
 * queryTermCount. A document that contains all query terms scores 1.0.
 */
public class TestFullTextGlobalIndexReader implements GlobalIndexReader {

    private final GlobalIndexFileReader fileReader;
    private final GlobalIndexIOMeta ioMeta;

    private String[] documents;
    private long[] rowIds;
    private int count;

    public TestFullTextGlobalIndexReader(
            GlobalIndexFileReader fileReader, GlobalIndexIOMeta ioMeta) {
        this.fileReader = fileReader;
        this.ioMeta = ioMeta;
    }

    @Override
    public CompletableFuture<Optional<ScoredGlobalIndexResult>> visitFullTextSearch(
            FullTextSearch fullTextSearch) {
        try {
            ensureLoaded();
        } catch (IOException e) {
            throw new RuntimeException("Failed to load test full-text index", e);
        }

        int limit = fullTextSearch.limit();
        int effectiveK = Math.min(limit, count);
        if (effectiveK <= 0) {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        // Min-heap: smallest score at head, so we evict the weakest candidate.
        PriorityQueue<ScoredRow> topK =
                new PriorityQueue<>(effectiveK + 1, Comparator.comparingDouble(s -> s.score));
        RoaringNavigableMap64 includeRowIds = fullTextSearch.includeRowIds();

        for (int i = 0; i < count; i++) {
            if (includeRowIds != null && !includeRowIds.contains(rowIds[i])) {
                continue;
            }
            float score = computeScore(documents[i], fullTextSearch.query());
            if (score <= 0) {
                continue;
            }
            if (topK.size() < effectiveK) {
                topK.offer(new ScoredRow(rowIds[i], score));
            } else if (score > topK.peek().score) {
                topK.poll();
                topK.offer(new ScoredRow(rowIds[i], score));
            }
        }

        if (topK.isEmpty()) {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        RoaringNavigableMap64 resultBitmap = new RoaringNavigableMap64();
        Map<Long, Float> scoreMap = new HashMap<>(topK.size());
        for (ScoredRow row : topK) {
            resultBitmap.add(row.rowId);
            scoreMap.put(row.rowId, row.score);
        }

        return CompletableFuture.completedFuture(
                Optional.of(ScoredGlobalIndexResult.create(resultBitmap, scoreMap::get)));
    }

    private static float computeScore(String document, String query) {
        try {
            return computeScore(document, JsonSerdeUtil.OBJECT_MAPPER_INSTANCE.readTree(query));
        } catch (IOException e) {
            throw new IllegalArgumentException("Invalid full-text query JSON: " + query, e);
        }
    }

    private static float computeScore(String document, JsonNode query) {
        if (query.has("match")) {
            JsonNode match = query.get("match");
            String terms = textValue(match, "query", "terms");
            boolean requireAllTerms =
                    "and".equalsIgnoreCase(textValueOrDefault(match, "operator", "Or"));
            return computeMatchScore(document, terms, requireAllTerms)
                    * floatValue(match, "boost", 1.0f);
        }
        if (query.has("match_phrase") || query.has("phrase")) {
            JsonNode phrase =
                    query.has("match_phrase") ? query.get("match_phrase") : query.get("phrase");
            String terms = textValue(phrase, "query", "terms");
            return document.toLowerCase(Locale.ROOT).contains(terms.toLowerCase(Locale.ROOT))
                    ? 1.0f
                    : 0.0f;
        }
        if (query.has("boost")) {
            JsonNode boost = query.get("boost");
            float score = computeScore(document, required(boost, "positive"));
            if (computeScore(document, required(boost, "negative")) > 0) {
                score *=
                        floatValue(
                                boost, "negative_boost", floatValue(boost, "negativeBoost", 0.5f));
            }
            return score;
        }
        if (query.has("boolean")) {
            return computeBooleanScore(document, query.get("boolean"));
        }
        throw new IllegalArgumentException("Unsupported full-text query JSON: " + query);
    }

    private static float computeMatchScore(
            String document, String queryText, boolean requireAllTerms) {
        String[] queryTerms = queryText.toLowerCase(Locale.ROOT).split("\\s+");
        String lowerDoc = document.toLowerCase(Locale.ROOT);
        float score = 0;
        for (String term : queryTerms) {
            if (lowerDoc.contains(term)) {
                score += 1.0f / queryTerms.length;
            } else if (requireAllTerms) {
                return 0;
            }
        }
        return score;
    }

    private static float computeBooleanScore(String document, JsonNode query) {
        float score = 0;
        float shouldScore = 0;
        int shouldCount = 0;
        int mustCount = 0;
        JsonNode clauses = required(query, "queries");
        for (JsonNode clause : clauses) {
            String occur;
            JsonNode child;
            if (clause.isArray()) {
                occur = clause.get(0).asText();
                child = clause.get(1);
            } else {
                occur = textValueOrDefault(clause, "occur", "Should");
                child = required(clause, "query");
            }

            float childScore = computeScore(document, child);
            if ("must".equalsIgnoreCase(occur)) {
                mustCount++;
                if (childScore <= 0) {
                    return 0;
                }
                score += childScore;
            } else if ("must_not".equalsIgnoreCase(occur) || "mustnot".equalsIgnoreCase(occur)) {
                if (childScore > 0) {
                    return 0;
                }
            } else {
                shouldCount++;
                shouldScore += childScore;
            }
        }
        if (mustCount == 0 && shouldCount > 0 && shouldScore <= 0) {
            return 0;
        }
        return score + shouldScore;
    }

    private static JsonNode required(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) {
            throw new IllegalArgumentException("Missing full-text query field: " + field);
        }
        return value;
    }

    private static String textValueOrDefault(JsonNode node, String field, String fallback) {
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? fallback : value.asText();
    }

    private static String textValue(JsonNode node, String firstField, String secondField) {
        JsonNode value = node.get(firstField);
        if (value == null || value.isNull()) {
            value = required(node, secondField);
        }
        return value.asText();
    }

    private static float floatValue(JsonNode node, String field, float fallback) {
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? fallback : (float) value.asDouble();
    }

    private void ensureLoaded() throws IOException {
        if (documents != null) {
            return;
        }

        try (SeekableInputStream in = fileReader.getInputStream(ioMeta)) {
            // Read header: count (4 bytes)
            byte[] headerBytes = new byte[4];
            readFully(in, headerBytes);
            ByteBuffer header = ByteBuffer.wrap(headerBytes);
            header.order(ByteOrder.LITTLE_ENDIAN);
            count = header.getInt();

            // Read documents
            documents = new String[count];
            rowIds = new long[count];
            for (int i = 0; i < count; i++) {
                byte[] entryHeaderBytes = new byte[Long.BYTES + Integer.BYTES];
                readFully(in, entryHeaderBytes);
                ByteBuffer entryHeader = ByteBuffer.wrap(entryHeaderBytes);
                entryHeader.order(ByteOrder.LITTLE_ENDIAN);
                rowIds[i] = entryHeader.getLong();
                int textLen = entryHeader.getInt();

                byte[] textBytes = new byte[textLen];
                readFully(in, textBytes);
                documents[i] = new String(textBytes, StandardCharsets.UTF_8);
            }
        }
    }

    private static void readFully(SeekableInputStream in, byte[] buf) throws IOException {
        int offset = 0;
        while (offset < buf.length) {
            int bytesRead = in.read(buf, offset, buf.length - offset);
            if (bytesRead < 0) {
                throw new IOException(
                        "Unexpected end of stream: read "
                                + offset
                                + " bytes but expected "
                                + buf.length);
            }
            offset += bytesRead;
        }
    }

    @Override
    public void close() throws IOException {
        documents = null;
        rowIds = null;
    }

    // =================== unsupported predicate operations =====================

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

    /** A row ID paired with its similarity score, used in the top-k min-heap. */
    private static class ScoredRow {
        final long rowId;
        final float score;

        ScoredRow(long rowId, float score) {
            this.rowId = rowId;
            this.score = score;
        }
    }
}
