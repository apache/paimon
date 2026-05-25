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
import org.apache.paimon.utils.RoaringNavigableMap64;

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
    private int count;

    public TestFullTextGlobalIndexReader(
            GlobalIndexFileReader fileReader, GlobalIndexIOMeta ioMeta) {
        this.fileReader = fileReader;
        this.ioMeta = ioMeta;
    }

    @Override
    public Optional<ScoredGlobalIndexResult> visitFullTextSearch(FullTextSearch fullTextSearch) {
        try {
            ensureLoaded();
        } catch (IOException e) {
            throw new RuntimeException("Failed to load test full-text index", e);
        }

        String queryText = fullTextSearch.queryText();
        int limit = fullTextSearch.limit();
        int effectiveK = Math.min(limit, count);
        if (effectiveK <= 0) {
            return Optional.empty();
        }

        String[] queryTerms = queryText.toLowerCase(Locale.ROOT).split("\\s+");

        // Min-heap: smallest score at head, so we evict the weakest candidate.
        PriorityQueue<ScoredRow> topK =
                new PriorityQueue<>(effectiveK + 1, Comparator.comparingDouble(s -> s.score));

        for (int i = 0; i < count; i++) {
            float score = computeScore(documents[i], queryTerms);
            if (score <= 0) {
                continue;
            }
            if (topK.size() < effectiveK) {
                topK.offer(new ScoredRow(i, score));
            } else if (score > topK.peek().score) {
                topK.poll();
                topK.offer(new ScoredRow(i, score));
            }
        }

        if (topK.isEmpty()) {
            return Optional.empty();
        }

        RoaringNavigableMap64 resultBitmap = new RoaringNavigableMap64();
        Map<Long, Float> scoreMap = new HashMap<>(topK.size());
        for (ScoredRow row : topK) {
            resultBitmap.add(row.rowId);
            scoreMap.put(row.rowId, row.score);
        }

        return Optional.of(ScoredGlobalIndexResult.create(() -> resultBitmap, scoreMap::get));
    }

    private static float computeScore(String document, String[] queryTerms) {
        String lowerDoc = document.toLowerCase(Locale.ROOT);
        float score = 0;
        for (String term : queryTerms) {
            if (lowerDoc.contains(term)) {
                score += 1.0f / queryTerms.length;
            }
        }
        return score;
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
            for (int i = 0; i < count; i++) {
                byte[] lenBytes = new byte[4];
                readFully(in, lenBytes);
                ByteBuffer lenBuf = ByteBuffer.wrap(lenBytes);
                lenBuf.order(ByteOrder.LITTLE_ENDIAN);
                int textLen = lenBuf.getInt();

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
    }

    // =================== unsupported predicate operations =====================

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
