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

package org.apache.paimon.fulltext.index;

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
import org.apache.paimon.predicate.FullTextSearch;
import org.apache.paimon.utils.ExceptionUtils;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.RoaringNavigableMap64;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.TextNode;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Full-text global index reader using paimon-full-text-index.
 *
 * <p>The standalone native reader receives positional read callbacks backed by Paimon's {@link
 * SeekableInputStream}. No temp files are created.
 */
public class NativeFullTextGlobalIndexReader implements GlobalIndexReader {

    private final GlobalIndexIOMeta ioMeta;
    private final GlobalIndexFileReader fileReader;
    private final ExecutorService executor;

    private volatile FullTextIndexReader reader;
    private volatile SeekableInputStream inputStream;

    public NativeFullTextGlobalIndexReader(
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
                        byte[] filter = includeRowIds == null ? null : includeRowIds.serialize();
                        String query = nativeQuery(fullTextSearch);
                        FullTextSearchResult result =
                                filter == null
                                        ? reader.search(query, fullTextSearch.limit())
                                        : reader.search(query, fullTextSearch.limit(), filter);
                        return Optional.of(toScoredResult(result));
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to search native full-text index", e);
                    }
                },
                executor);
    }

    private String nativeQuery(FullTextSearch search) throws IOException {
        String nativeField = nativeTextField();
        if (nativeField.equals(search.column())) {
            return search.query();
        }
        // Archives use native field names, independently of the table column name.
        JsonNode query = JsonSerdeUtil.OBJECT_MAPPER_INSTANCE.readTree(search.query());
        rewriteColumn(query, search.column(), nativeField);
        return JsonSerdeUtil.OBJECT_MAPPER_INSTANCE.writeValueAsString(query);
    }

    private String nativeTextField() throws IOException {
        byte[] metadata = ioMeta.metadata();
        if (metadata == null || metadata.length == 0) {
            return "text";
        }
        JsonNode options = JsonSerdeUtil.OBJECT_MAPPER_INSTANCE.readTree(metadata);
        String field = "text";
        String fields = null;
        Iterator<Map.Entry<String, JsonNode>> entries = options.fields();
        while (entries.hasNext()) {
            Map.Entry<String, JsonNode> entry = entries.next();
            String key = entry.getKey();
            if (key.startsWith("fulltext.")) {
                key = key.substring("fulltext.".length());
            } else if (key.startsWith("tantivy.")) {
                key = key.substring("tantivy.".length());
            }
            key = key.trim().toLowerCase(Locale.ROOT).replace('_', '-');
            if ("text-field".equals(key)) {
                field = entry.getValue().asText().trim();
            } else if ("text-fields".equals(key) || "columns".equals(key)) {
                fields = entry.getValue().asText();
            }
        }
        // addDocument(rowId, text) writes to the first configured native text field.
        if (fields != null) {
            for (String name : fields.split("[,;]")) {
                if (!name.trim().isEmpty()) {
                    return name.trim();
                }
            }
        }
        return field;
    }

    private static void rewriteColumn(JsonNode node, String column, String nativeField) {
        if (node == null) {
            return;
        }
        if (node.isObject()) {
            JsonNode name = node.get("column");
            if (name != null && name.isTextual() && column.equals(name.asText())) {
                ((ObjectNode) node).put("column", nativeField);
            }
            JsonNode columns = node.get("columns");
            if (columns != null && columns.isArray()) {
                ArrayNode names = (ArrayNode) columns;
                for (int i = 0; i < names.size(); i++) {
                    if (names.get(i).isTextual() && column.equals(names.get(i).asText())) {
                        names.set(i, TextNode.valueOf(nativeField));
                    }
                }
            }
        }
        for (JsonNode child : node) {
            rewriteColumn(child, column, nativeField);
        }
    }

    private ScoredGlobalIndexResult toScoredResult(FullTextSearchResult result) {
        RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
        HashMap<Long, Float> id2scores = new HashMap<>(result.size());
        for (int i = 0; i < result.size(); i++) {
            long rowId = result.rowIds()[i];
            bitmap.add(rowId);
            id2scores.put(rowId, result.scores()[i]);
        }
        return new NativeFullTextScoredGlobalIndexResult(bitmap, id2scores);
    }

    private void ensureLoaded() throws IOException {
        if (reader == null) {
            synchronized (this) {
                if (reader == null) {
                    SeekableInputStream input = fileReader.getInputStream(ioMeta);
                    try {
                        inputStream = input;
                        reader = new FullTextIndexReader(new PaimonFullTextIndexInput(input));
                    } catch (RuntimeException | Error e) {
                        inputStream = null;
                        closeResource(input, e);
                        throw e;
                    }
                }
            }
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
        Throwable failure = null;
        try {
            failure = closeResource(reader, failure);
            failure = closeResource(inputStream, failure);
        } finally {
            reader = null;
            inputStream = null;
        }
        if (failure instanceof IOException) {
            throw (IOException) failure;
        } else if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        } else if (failure instanceof Error) {
            throw (Error) failure;
        } else if (failure != null) {
            throw new IOException("Failed to close native full-text index reader", failure);
        }
    }

    private static Throwable closeResource(AutoCloseable resource, Throwable failure) {
        try {
            if (resource != null) {
                resource.close();
            }
        } catch (Throwable closeFailure) {
            return ExceptionUtils.firstOrSuppressed(closeFailure, failure);
        }
        return failure;
    }

    private static class PaimonFullTextIndexInput implements FullTextIndexInput {

        private final SeekableInputStream in;

        private PaimonFullTextIndexInput(SeekableInputStream in) {
            this.in = in;
        }

        @Override
        public synchronized void pread(long position, byte[] buffer, int offset, int length)
                throws IOException {
            in.seek(position);
            readFully(in, buffer, offset, length);
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
