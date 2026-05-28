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

package org.apache.paimon.eslib.index;

import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.FullTextSearch;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.RoaringNavigableMap64;

import org.elasticsearch.eslib.api.ArchiveDataProvider;
import org.elasticsearch.eslib.api.ESIndexSearcher;
import org.elasticsearch.eslib.api.model.FieldIndexConfig;
import org.elasticsearch.eslib.api.model.IndexFilter;
import org.elasticsearch.eslib.api.model.ScalarPredicate;
import org.elasticsearch.eslib.api.model.SearchResult;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

/**
 * ES multi-index reader using ESLib. Supports vector search, full-text search, and scalar filtering
 * via Lucene-based indexes stored in archive format.
 */
public class ESIndexGlobalIndexReader implements GlobalIndexReader {

    private final GlobalIndexFileReader fileReader;
    private final List<GlobalIndexIOMeta> files;
    private final List<DataField> fields;
    private final ESIndexOptions indexOptions;
    private final ExecutorService searchExecutor;

    private final List<SeekableInputStream> allStreams = new ArrayList<>();
    private volatile ESIndexSearcher searcher;
    private volatile boolean closed;
    private volatile boolean loaded;

    public ESIndexGlobalIndexReader(
            GlobalIndexFileReader fileReader,
            List<GlobalIndexIOMeta> files,
            List<DataField> fields,
            ESIndexOptions indexOptions) {
        this(fileReader, files, fields, indexOptions, null);
    }

    public ESIndexGlobalIndexReader(
            GlobalIndexFileReader fileReader,
            List<GlobalIndexIOMeta> files,
            List<DataField> fields,
            ESIndexOptions indexOptions,
            ExecutorService searchExecutor) {
        this.fileReader = fileReader;
        this.files = files;
        this.fields = fields;
        this.indexOptions = indexOptions;
        this.searchExecutor = searchExecutor;
        this.loaded = false;
        this.closed = false;
    }

    @Override
    public Optional<ScoredGlobalIndexResult> visitVectorSearch(VectorSearch vectorSearch) {
        try {
            ensureLoaded();
            float[] queryVector = vectorSearch.vector();
            int topK = vectorSearch.limit();
            String fieldName = vectorSearch.fieldName();

            long[] candidateIds = null;
            RoaringNavigableMap64 includeRowIds = vectorSearch.includeRowIds();
            if (includeRowIds != null) {
                candidateIds = toArray(includeRowIds);
            }

            SearchResult result = searcher.vectorSearch(fieldName, queryVector, topK, candidateIds);
            return toScoredResult(result);
        } catch (IOException e) {
            throw new RuntimeException("Vector search failed", e);
        }
    }

    @Override
    public Optional<ScoredGlobalIndexResult> visitFullTextSearch(FullTextSearch fullTextSearch) {
        try {
            ensureLoaded();
            String fieldName = fullTextSearch.fieldName();
            String queryText = fullTextSearch.queryText();
            int topK = fullTextSearch.limit();

            SearchResult result = searcher.fullTextSearch(fieldName, queryText, topK);
            return toScoredResult(result);
        } catch (IOException e) {
            throw new RuntimeException("Full-text search failed", e);
        }
    }

    private Optional<ScoredGlobalIndexResult> toScoredResult(SearchResult result) {
        if (result == null || result.count == 0) {
            return Optional.empty();
        }

        RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
        Map<Long, Float> scoreMap = new HashMap<>(result.count);
        for (int i = 0; i < result.count; i++) {
            long id = result.ids[i];
            bitmap.add(id);
            scoreMap.put(id, result.scores[i]);
        }

        return Optional.of(ScoredGlobalIndexResult.create(() -> bitmap, scoreMap::get));
    }

    private void checkNotClosed() throws IOException {
        if (closed) {
            throw new IOException("Reader already closed");
        }
    }

    private void ensureLoaded() throws IOException {
        checkNotClosed();
        if (loaded) {
            return;
        }

        if (files.isEmpty()) {
            throw new IOException("No index files to load");
        }

        GlobalIndexIOMeta meta = files.get(0);
        byte[] metaBytes = meta.metadata();
        Map<String, long[]> fileOffsets = parseFileOffsets(metaBytes);

        ArchiveDataProvider dataProvider = createProvider(meta);

        searcher = ESIndexBuilderFactory.createSearcher();
        searcher.load(dataProvider, fileOffsets, indexOptions.getFieldConfigs(), searchExecutor);
        loaded = true;
    }

    private ArchiveDataProvider createProvider(GlobalIndexIOMeta meta) throws IOException {
        SeekableInputStream inputStream = fileReader.getInputStream(meta);
        synchronized (allStreams) {
            allStreams.add(inputStream);
        }
        return new ArchiveDataProvider() {
            @Override
            public byte[] readRange(long offset, int length) throws IOException {
                byte[] buf = new byte[length];
                inputStream.seek(offset);
                int read = 0;
                while (read < length) {
                    int n = inputStream.read(buf, read, length - read);
                    if (n < 0) {
                        throw new IOException("Unexpected EOF at offset " + (offset + read));
                    }
                    read += n;
                }
                return buf;
            }

            @Override
            public ArchiveDataProvider fork() throws IOException {
                return createProvider(meta);
            }

            @Override
            public void close() throws IOException {
                inputStream.close();
            }
        };
    }

    /**
     * Parse file offset metadata (big-endian). Format: [4-byte count] then per file: [4-byte name
     * length][name bytes][8-byte offset][8-byte length]
     */
    private Map<String, long[]> parseFileOffsets(byte[] metaBytes) throws IOException {
        Map<String, long[]> offsets = new LinkedHashMap<>();
        if (metaBytes == null || metaBytes.length == 0) {
            return offsets;
        }

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(metaBytes));
        int fileCount = dis.readInt();
        for (int i = 0; i < fileCount; i++) {
            int nameLen = dis.readInt();
            byte[] nameBytes = new byte[nameLen];
            dis.readFully(nameBytes);
            String fileName = new String(nameBytes, StandardCharsets.UTF_8);
            long offset = dis.readLong();
            long length = dis.readLong();
            offsets.put(fileName, new long[] {offset, length});
        }
        return offsets;
    }

    private static long[] toArray(RoaringNavigableMap64 bitmap) {
        long[] arr = new long[(int) bitmap.getIntCardinality()];
        int i = 0;
        for (long id : bitmap) {
            arr[i++] = id;
        }
        return arr;
    }

    @Override
    public void close() throws IOException {
        closed = true;
        if (searcher != null) {
            searcher.close();
            searcher = null;
        }
        synchronized (allStreams) {
            for (SeekableInputStream stream : allStreams) {
                try {
                    stream.close();
                } catch (IOException ignored) {
                }
            }
            allStreams.clear();
        }
        loaded = false;
    }

    // =================== unified filter dispatch =====================

    private Optional<GlobalIndexResult> executeFilter(String fieldName, IndexFilter filter) {
        try {
            ensureLoaded();
            long[] ids = searcher.filter(fieldName, filter);
            if (ids == null || ids.length == 0) {
                return Optional.empty();
            }
            RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
            for (long id : ids) {
                bitmap.add(id);
            }
            return Optional.of(GlobalIndexResult.create(() -> bitmap));
        } catch (IOException e) {
            throw new RuntimeException("Filter failed on field: " + fieldName, e);
        }
    }

    private Optional<GlobalIndexResult> dispatchFilter(FieldRef fieldRef, IndexFilter filter) {
        FieldIndexConfig config = indexOptions.getConfig(fieldRef.name());
        if (config == null) {
            return Optional.empty();
        }
        return executeFilter(fieldRef.name(), filter);
    }

    // =================== scalar / keyword comparison visitors =====================

    @Override
    public Optional<GlobalIndexResult> visitEqual(FieldRef fieldRef, Object literal) {
        FieldIndexConfig config = indexOptions.getConfig(fieldRef.name());
        if (config == null) {
            return Optional.empty();
        }
        FieldIndexConfig.IndexType type = config.indexType();
        if (type == FieldIndexConfig.IndexType.KEYWORD
                || type == FieldIndexConfig.IndexType.FULLTEXT) {
            return dispatchFilter(
                    fieldRef, IndexFilter.text(IndexFilter.TextFilter.TextOp.TERM, str(literal)));
        }
        return dispatchFilter(fieldRef, IndexFilter.scalar(ScalarPredicate.eq(literal)));
    }

    @Override
    public Optional<GlobalIndexResult> visitNotEqual(FieldRef fieldRef, Object literal) {
        return dispatchFilter(fieldRef, IndexFilter.scalar(ScalarPredicate.neq(literal)));
    }

    @Override
    public Optional<GlobalIndexResult> visitLessThan(FieldRef fieldRef, Object literal) {
        return dispatchFilter(fieldRef, IndexFilter.scalar(ScalarPredicate.lt(literal)));
    }

    @Override
    public Optional<GlobalIndexResult> visitLessOrEqual(FieldRef fieldRef, Object literal) {
        return dispatchFilter(fieldRef, IndexFilter.scalar(ScalarPredicate.lte(literal)));
    }

    @Override
    public Optional<GlobalIndexResult> visitGreaterThan(FieldRef fieldRef, Object literal) {
        return dispatchFilter(fieldRef, IndexFilter.scalar(ScalarPredicate.gt(literal)));
    }

    @Override
    public Optional<GlobalIndexResult> visitGreaterOrEqual(FieldRef fieldRef, Object literal) {
        return dispatchFilter(fieldRef, IndexFilter.scalar(ScalarPredicate.gte(literal)));
    }

    @Override
    public Optional<GlobalIndexResult> visitIn(FieldRef fieldRef, List<Object> literals) {
        return dispatchFilter(fieldRef, IndexFilter.scalar(ScalarPredicate.in(literals)));
    }

    @Override
    public Optional<GlobalIndexResult> visitNotIn(FieldRef fieldRef, List<Object> literals) {
        return dispatchFilter(fieldRef, IndexFilter.scalar(ScalarPredicate.notIn(literals)));
    }

    // =================== text pattern visitors (keyword / fulltext) =====================

    @Override
    public Optional<GlobalIndexResult> visitStartsWith(FieldRef fieldRef, Object literal) {
        return dispatchFilter(
                fieldRef, IndexFilter.text(IndexFilter.TextFilter.TextOp.PREFIX, str(literal)));
    }

    @Override
    public Optional<GlobalIndexResult> visitEndsWith(FieldRef fieldRef, Object literal) {
        return dispatchFilter(
                fieldRef,
                IndexFilter.text(IndexFilter.TextFilter.TextOp.WILDCARD, "*" + str(literal)));
    }

    @Override
    public Optional<GlobalIndexResult> visitContains(FieldRef fieldRef, Object literal) {
        return dispatchFilter(
                fieldRef,
                IndexFilter.text(IndexFilter.TextFilter.TextOp.WILDCARD, "*" + str(literal) + "*"));
    }

    @Override
    public Optional<GlobalIndexResult> visitLike(FieldRef fieldRef, Object literal) {
        String pattern = str(literal).replace('%', '*').replace('_', '?');
        return dispatchFilter(
                fieldRef, IndexFilter.text(IndexFilter.TextFilter.TextOp.WILDCARD, pattern));
    }

    // =================== null checks =====================

    @Override
    public Optional<GlobalIndexResult> visitIsNotNull(FieldRef fieldRef) {
        return dispatchFilter(fieldRef, IndexFilter.exists());
    }

    @Override
    public Optional<GlobalIndexResult> visitIsNull(FieldRef fieldRef) {
        return dispatchFilter(fieldRef, IndexFilter.notExists());
    }

    // =================== helpers =====================

    private static String str(Object literal) {
        return literal == null ? "" : literal.toString();
    }
}
