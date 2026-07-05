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
import org.apache.paimon.index.fulltext.FullTextQuery;
import org.apache.paimon.index.fulltext.FullTextSearchResult;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.FullTextSearch;
import org.apache.paimon.utils.RoaringNavigableMap64;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Full-text global index reader using paimon-full-text.
 *
 * <p>The standalone native reader receives positional read callbacks backed by Paimon's
 * {@link SeekableInputStream}. No temp files are created.
 */
public class TantivyFullTextGlobalIndexReader implements GlobalIndexReader {

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
                        FullTextQuery query = FullTextQuery.json(fullTextSearch.queryJson());
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
