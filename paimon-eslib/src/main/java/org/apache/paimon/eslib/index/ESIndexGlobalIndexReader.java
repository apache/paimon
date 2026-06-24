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
import org.elasticsearch.eslib.api.model.FullTextParams;
import org.elasticsearch.eslib.api.model.IndexFilter;
import org.elasticsearch.eslib.api.model.ScalarPredicate;
import org.elasticsearch.eslib.api.model.SearchResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * ES multi-index reader using ESLib. Supports vector search, full-text search, and scalar filtering
 * via Lucene-based indexes stored in archive format.
 */
public class ESIndexGlobalIndexReader implements GlobalIndexReader {

    private static final Logger LOG = LoggerFactory.getLogger(ESIndexGlobalIndexReader.class);
    private static final String DEBUG_PROPERTY = "paimon.eslib.debug";
    private static final String HNSW_NUM_CANDIDATES_OPTION = "hnsw.num_candidates";
    private static final String HNSW_EF_SEARCH_OPTION = "hnsw.ef_search";
    private static final String NUM_CANDIDATES_OPTION = "num_candidates";
    private static final int DEFAULT_SAMPLE_LIMIT = 20;

    private final GlobalIndexFileReader fileReader;
    private final List<GlobalIndexIOMeta> files;
    private final List<DataField> fields;
    private final ESIndexOptions indexOptions;
    private final ExecutorService searchExecutor;

    private final List<SeekableInputStream> allStreams = new ArrayList<>();
    private final Object loadLock = new Object();
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
        checkArgument(files.size() == 1, "Expected exactly one ES index file per shard");
        this.fileReader = fileReader;
        this.files = files;
        this.fields = fields;
        this.indexOptions = indexOptions;
        this.searchExecutor = searchExecutor;
        this.loaded = false;
        this.closed = false;
    }

    /** The read/search executor in use (null = serial); visible for tests. */
    ExecutorService searchExecutor() {
        return searchExecutor;
    }

    @Override
    public CompletableFuture<Optional<ScoredGlobalIndexResult>> visitVectorSearch(
            VectorSearch vectorSearch) {
        return async(
                () -> {
                    try {
                        ensureLoaded();
                        float[] queryVector = vectorSearch.vector();
                        int topK = vectorSearch.limit();
                        int searchTopK = vectorSearchTopK(vectorSearch);
                        String fieldName = vectorSearch.fieldName();

                        long[] candidateIds = null;
                        RoaringNavigableMap64 includeRowIds = vectorSearch.includeRowIds();
                        if (includeRowIds != null) {
                            candidateIds = toArray(includeRowIds);
                        }

                        if (debugEnabled()) {
                            LOG.info(
                                    "PAIMON_ESLIB_VECTOR_SEARCH_BEGIN field={} topK={} searchTopK={} queryDim={} querySample={} includeCount={} includeSample={} loaded={} indexFiles={} fields={}",
                                    fieldName,
                                    topK,
                                    searchTopK,
                                    queryVector == null ? -1 : queryVector.length,
                                    sample(queryVector, DEFAULT_SAMPLE_LIMIT),
                                    candidateIds == null ? -1 : candidateIds.length,
                                    sample(candidateIds, DEFAULT_SAMPLE_LIMIT),
                                    loaded,
                                    files.size(),
                                    fieldsSample());
                        }

                        SearchResult result =
                                searcher.vectorSearch(
                                        fieldName, queryVector, searchTopK, candidateIds);
                        if (debugEnabled()) {
                            LOG.info(
                                    "PAIMON_ESLIB_VECTOR_SEARCH_RESULT field={} topK={} searchTopK={} resultCount={} ids={} scores={}",
                                    fieldName,
                                    topK,
                                    searchTopK,
                                    result == null ? 0 : result.count,
                                    result == null
                                            ? "[]"
                                            : sample(
                                                    result.ids, result.count, DEFAULT_SAMPLE_LIMIT),
                                    result == null
                                            ? "[]"
                                            : sample(
                                                    result.scores,
                                                    result.count,
                                                    DEFAULT_SAMPLE_LIMIT));
                        }
                        return toScoredResult(result);
                    } catch (IOException e) {
                        throw new RuntimeException("Vector search failed", e);
                    }
                });
    }

    @Override
    public CompletableFuture<Optional<ScoredGlobalIndexResult>> visitFullTextSearch(
            FullTextSearch fullTextSearch) {
        return async(
                () -> {
                    try {
                        ensureLoaded();
                        String fieldName = fullTextSearch.fieldName();
                        FieldIndexConfig config = indexOptions.getConfig(fieldName);
                        if (config == null
                                || config.indexType() != FieldIndexConfig.IndexType.FULLTEXT) {
                            // This ES index carries the column, but not as a FULLTEXT field (a
                            // string column defaults to KEYWORD unless an analyzer is configured).
                            // Full-text search is index-only, so a non-FULLTEXT field yields an
                            // empty result here rather than letting the eslib searcher throw.
                            return Optional.empty();
                        }
                        org.apache.paimon.predicate.FullTextQuery.Match match =
                                requireMatch(fullTextSearch);
                        int topK = fullTextSearch.limit();

                        SearchResult result =
                                searcher.fullTextSearch(
                                        fieldName, match.query(), topK, toFullTextParams(match));
                        return toScoredResult(result);
                    } catch (IOException e) {
                        throw new RuntimeException("Full-text search failed", e);
                    }
                });
    }

    /**
     * Returns the {@link org.apache.paimon.predicate.FullTextQuery.Match} of {@code
     * fullTextSearch}, rejecting structured queries. Only {@code Match} maps onto the eslib
     * single-field full-text API; Phrase/Boolean/Boost/MultiMatch are rejected rather than
     * serialized to JSON and fed to the text parser, which would silently search for the literal
     * JSON tokens.
     */
    private static org.apache.paimon.predicate.FullTextQuery.Match requireMatch(
            FullTextSearch fullTextSearch) {
        org.apache.paimon.predicate.FullTextQuery ftq = fullTextSearch.query();
        if (ftq instanceof org.apache.paimon.predicate.FullTextQuery.Match) {
            return (org.apache.paimon.predicate.FullTextQuery.Match) ftq;
        }
        throw new UnsupportedOperationException(
                "ES global index full-text search currently supports only Match queries; got "
                        + ftq.getClass().getSimpleName()
                        + ". Structured full-text queries (Phrase/Boolean/Boost/MultiMatch) are"
                        + " not yet implemented for the es-index backend.");
    }

    /**
     * Maps a paimon {@link org.apache.paimon.predicate.FullTextQuery.Match} onto eslib {@link
     * FullTextParams} so operator / boost / fuzziness / maxExpansions / prefixLength are honoured
     * by the underlying Lucene query (rather than silently dropped).
     */
    private static FullTextParams toFullTextParams(
            org.apache.paimon.predicate.FullTextQuery.Match match) {
        FullTextParams.Operator operator =
                match.operator() == org.apache.paimon.predicate.FullTextQuery.Operator.AND
                        ? FullTextParams.Operator.AND
                        : FullTextParams.Operator.OR;
        return new FullTextParams(
                operator,
                match.boost(),
                match.fuzziness(),
                match.maxExpansions(),
                match.prefixLength());
    }

    /**
     * Run {@code body} on the search executor when one is available, otherwise execute it inline
     * and return an already-completed future. Mirrors {@code VectorGlobalIndexReader}'s use of
     * {@link CompletableFuture#supplyAsync} while staying safe when no executor was provided.
     */
    private <T> CompletableFuture<T> async(Supplier<T> body) {
        if (searchExecutor != null) {
            return CompletableFuture.supplyAsync(body, searchExecutor);
        }
        return CompletableFuture.completedFuture(body.get());
    }

    static int vectorSearchTopK(VectorSearch vectorSearch) {
        int limit = vectorSearch.limit();
        Map<String, String> options = vectorSearch.options();
        int candidates = positiveIntOption(options, HNSW_NUM_CANDIDATES_OPTION);
        if (candidates <= 0) {
            candidates = positiveIntOption(options, NUM_CANDIDATES_OPTION);
        }
        if (candidates <= 0) {
            candidates = positiveIntOption(options, HNSW_EF_SEARCH_OPTION);
        }
        return candidates <= 0 ? limit : Math.max(limit, candidates);
    }

    private static int positiveIntOption(Map<String, String> options, String key) {
        String value = options.get(key);
        if (value == null || value.trim().isEmpty()) {
            return -1;
        }
        try {
            int parsed = Integer.parseInt(value.trim());
            return parsed > 0 ? parsed : -1;
        } catch (NumberFormatException e) {
            return -1;
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

        return Optional.of(ScoredGlobalIndexResult.create(bitmap, scoreMap::get));
    }

    private void checkNotClosed() throws IOException {
        if (closed) {
            throw new IOException("Reader already closed");
        }
    }

    private void ensureLoaded() throws IOException {
        checkNotClosed();
        // Fast path: once loaded, concurrent visit* calls (running on the search executor) skip the
        // lock entirely and run in parallel.
        if (loaded) {
            return;
        }
        // Slow path: serialize the one-time searcher build so concurrent first calls don't build it
        // twice (which would leak input streams / double-register them in allStreams).
        synchronized (loadLock) {
            if (loaded) {
                return;
            }

            if (files.isEmpty()) {
                throw new IOException("No index files to load");
            }

            long loadStartNanos = System.nanoTime();
            GlobalIndexIOMeta meta = files.get(0);
            byte[] metaBytes = meta.metadata();
            Map<String, long[]> fileOffsets = parseFileOffsets(metaBytes);

            if (debugEnabled()) {
                LOG.info(
                        "PAIMON_ESLIB_READER_LOAD files={} firstFile={} firstFileSize={} metadataBytes={} offsets={} offsetSample={} fieldConfigs={} fields={}",
                        files.size(),
                        meta.filePath(),
                        meta.fileSize(),
                        metaBytes == null ? 0 : metaBytes.length,
                        fileOffsets.size(),
                        offsetsSample(fileOffsets),
                        fieldConfigsSample(),
                        fieldsSample());
            }

            ArchiveDataProvider dataProvider = createProvider(meta);

            searcher = ESIndexBuilderFactory.createSearcher();
            searcher.load(
                    dataProvider, fileOffsets, indexOptions.getFieldConfigs(), searchExecutor);
            loaded = true;
            if (debugEnabled()) {
                LOG.info(
                        "PAIMON_ESLIB_READER_LOADED files={} firstFile={} elapsedMs={} searcher={}",
                        files.size(),
                        meta.filePath(),
                        (System.nanoTime() - loadStartNanos) / 1_000_000L,
                        searcher == null ? "null" : searcher.getClass().getName());
            }
        }
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

    private static boolean debugEnabled() {
        return Boolean.parseBoolean(System.getProperties().getProperty(DEBUG_PROPERTY, "false"));
    }

    static String sample(long[] values, int limit) {
        return values == null ? "[]" : sample(values, values.length, limit);
    }

    static String sample(long[] values, int count, int limit) {
        if (values == null || count <= 0 || limit <= 0) {
            return "[]";
        }
        int actual = Math.min(Math.min(values.length, count), limit);
        StringJoiner joiner = new StringJoiner(", ", "[", "]");
        for (int i = 0; i < actual; i++) {
            joiner.add(String.valueOf(values[i]));
        }
        if (count > actual) {
            joiner.add("...(+" + (count - actual) + ")");
        }
        return joiner.toString();
    }

    static String sample(float[] values, int limit) {
        return values == null ? "[]" : sample(values, values.length, limit);
    }

    static String sample(float[] values, int count, int limit) {
        if (values == null || count <= 0 || limit <= 0) {
            return "[]";
        }
        int actual = Math.min(Math.min(values.length, count), limit);
        StringJoiner joiner = new StringJoiner(", ", "[", "]");
        for (int i = 0; i < actual; i++) {
            joiner.add(Float.toString(values[i]));
        }
        if (count > actual) {
            joiner.add("...(+" + (count - actual) + ")");
        }
        return joiner.toString();
    }

    private String fieldsSample() {
        StringJoiner joiner = new StringJoiner(", ", "[", "]");
        for (DataField field : fields) {
            joiner.add(field.name() + "#" + field.id());
        }
        return joiner.toString();
    }

    private String fieldConfigsSample() {
        Map<String, FieldIndexConfig> configs = indexOptions.getFieldConfigs();
        if (configs == null || configs.isEmpty()) {
            return "[]";
        }
        StringJoiner joiner = new StringJoiner(", ", "[", "]");
        for (Map.Entry<String, FieldIndexConfig> entry : configs.entrySet()) {
            FieldIndexConfig config = entry.getValue();
            joiner.add(
                    entry.getKey()
                            + "="
                            + config.indexType()
                            + "/"
                            + config.algorithm()
                            + "/"
                            + config.algorithmParams());
        }
        return joiner.toString();
    }

    private static String offsetsSample(Map<String, long[]> fileOffsets) {
        if (fileOffsets == null || fileOffsets.isEmpty()) {
            return "[]";
        }
        StringJoiner joiner = new StringJoiner(", ", "[", "]");
        int seen = 0;
        for (Map.Entry<String, long[]> entry : fileOffsets.entrySet()) {
            if (seen++ >= DEFAULT_SAMPLE_LIMIT) {
                joiner.add("...(+" + (fileOffsets.size() - DEFAULT_SAMPLE_LIMIT) + ")");
                break;
            }
            long[] range = entry.getValue();
            joiner.add(entry.getKey() + "=" + sample(range, range == null ? 0 : range.length, 2));
        }
        return joiner.toString();
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
            if (debugEnabled()) {
                FieldIndexConfig config = indexOptions.getConfig(fieldName);
                LOG.info(
                        "PAIMON_ESLIB_FILTER_BEGIN field={} filter={} config={} fields={}",
                        fieldName,
                        filterSummary(filter),
                        config == null ? "null" : config.indexType() + "/" + config.scalarType(),
                        fieldsSample());
            }
            long[] ids = searcher.filter(fieldName, filter);
            if (debugEnabled()) {
                LOG.info(
                        "PAIMON_ESLIB_FILTER_RESULT field={} filter={} resultCount={} ids={}",
                        fieldName,
                        filterSummary(filter),
                        ids == null ? 0 : ids.length,
                        sample(ids, DEFAULT_SAMPLE_LIMIT));
            }
            RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
            if (ids != null) {
                for (long id : ids) {
                    bitmap.add(id);
                }
            }
            // The field IS indexed (dispatchFilter returned empty() already when it is not), so a
            // zero-hit result is a genuine "0 rows match" — return an empty bitmap so the index
            // prunes
            // everything. Returning Optional.empty() here would mean "index can't evaluate" and
            // force a
            // raw-scan fallback (wrong semantics + loses the index's selectivity).
            return Optional.of(GlobalIndexResult.create(bitmap));
        } catch (IOException e) {
            throw new RuntimeException("Filter failed on field: " + fieldName, e);
        }
    }

    private Optional<GlobalIndexResult> dispatchFilter(FieldRef fieldRef, IndexFilter filter) {
        FieldIndexConfig config = indexOptions.getConfig(fieldRef.name());
        if (config == null) {
            return Optional.empty();
        }
        if (config.indexType() == FieldIndexConfig.IndexType.FULLTEXT) {
            // A FULLTEXT field is indexed as analyzer-produced tokens, which are not equivalent to
            // the raw column value. Ordinary SQL predicates (=, <>, <, >, IN, LIKE, IS NULL, ...)
            // evaluated against those tokens would return an incorrect bitmap and wrongly prune
            // rows, so we disable them and fall back to raw scan (Optional.empty). Only true
            // full-text search (visitFullTextSearch) is served for FULLTEXT fields.
            return Optional.empty();
        }
        return executeFilter(fieldRef.name(), filter);
    }

    private static String filterSummary(IndexFilter filter) {
        if (filter == null) {
            return "null";
        }
        switch (filter.filterType()) {
            case TEXT:
                IndexFilter.TextFilter text = (IndexFilter.TextFilter) filter;
                return "TEXT/" + text.op() + "/" + text.value();
            case SCALAR:
                return "SCALAR/" + ((IndexFilter.ScalarFilter) filter).predicate().op();
            case EXISTS:
                return "EXISTS/" + ((IndexFilter.ExistsFilter) filter).mustExist();
            case GEO:
                return "GEO";
            default:
                return filter.filterType().name();
        }
    }

    // =================== scalar / keyword comparison visitors =====================

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitEqual(
            FieldRef fieldRef, Object literal) {
        return async(
                () -> {
                    FieldIndexConfig config = indexOptions.getConfig(fieldRef.name());
                    if (config == null) {
                        return Optional.empty();
                    }
                    FieldIndexConfig.IndexType type = config.indexType();
                    if (type == FieldIndexConfig.IndexType.KEYWORD
                            || type == FieldIndexConfig.IndexType.FULLTEXT) {
                        return dispatchFilter(
                                fieldRef,
                                IndexFilter.text(IndexFilter.TextFilter.TextOp.TERM, str(literal)));
                    }
                    return dispatchFilter(
                            fieldRef, IndexFilter.scalar(ScalarPredicate.eq(literal)));
                });
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitNotEqual(
            FieldRef fieldRef, Object literal) {
        return async(
                () -> dispatchFilter(fieldRef, IndexFilter.scalar(ScalarPredicate.neq(literal))));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitLessThan(
            FieldRef fieldRef, Object literal) {
        return async(
                () -> dispatchFilter(fieldRef, IndexFilter.scalar(ScalarPredicate.lt(literal))));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitLessOrEqual(
            FieldRef fieldRef, Object literal) {
        return async(
                () -> dispatchFilter(fieldRef, IndexFilter.scalar(ScalarPredicate.lte(literal))));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitGreaterThan(
            FieldRef fieldRef, Object literal) {
        return async(
                () -> dispatchFilter(fieldRef, IndexFilter.scalar(ScalarPredicate.gt(literal))));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitGreaterOrEqual(
            FieldRef fieldRef, Object literal) {
        return async(
                () -> dispatchFilter(fieldRef, IndexFilter.scalar(ScalarPredicate.gte(literal))));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitIn(
            FieldRef fieldRef, List<Object> literals) {
        return async(
                () -> dispatchFilter(fieldRef, IndexFilter.scalar(ScalarPredicate.in(literals))));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitNotIn(
            FieldRef fieldRef, List<Object> literals) {
        return async(
                () ->
                        dispatchFilter(
                                fieldRef, IndexFilter.scalar(ScalarPredicate.notIn(literals))));
    }

    // =================== text pattern visitors (keyword / fulltext) =====================

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitStartsWith(
            FieldRef fieldRef, Object literal) {
        return async(
                () ->
                        dispatchFilter(
                                fieldRef,
                                IndexFilter.text(
                                        IndexFilter.TextFilter.TextOp.PREFIX, str(literal))));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitEndsWith(
            FieldRef fieldRef, Object literal) {
        return async(
                () ->
                        dispatchFilter(
                                fieldRef,
                                IndexFilter.text(
                                        IndexFilter.TextFilter.TextOp.WILDCARD,
                                        "*" + escapeWildcardLiteral(str(literal)))));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitContains(
            FieldRef fieldRef, Object literal) {
        return async(
                () ->
                        dispatchFilter(
                                fieldRef,
                                IndexFilter.text(
                                        IndexFilter.TextFilter.TextOp.WILDCARD,
                                        "*" + escapeWildcardLiteral(str(literal)) + "*")));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitLike(
            FieldRef fieldRef, Object literal) {
        return async(
                () ->
                        dispatchFilter(
                                fieldRef,
                                IndexFilter.text(
                                        IndexFilter.TextFilter.TextOp.WILDCARD,
                                        sqlLikeToWildcard(str(literal)))));
    }

    /**
     * Escape the Lucene {@link org.apache.lucene.search.WildcardQuery} metacharacters ({@code *},
     * {@code ?} and the escape char {@code \}) in a literal so it is matched verbatim. StartsWith /
     * EndsWith / Contains literals carry no wildcards, so any such character in them is data and
     * must not be reinterpreted as a wildcard.
     */
    private static String escapeWildcardLiteral(String s) {
        StringBuilder sb = new StringBuilder(s.length() + 4);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '*' || c == '?' || c == '\\') {
                sb.append('\\');
            }
            sb.append(c);
        }
        return sb.toString();
    }

    /**
     * Translate a SQL LIKE pattern into a Lucene wildcard pattern. paimon evaluates LIKE with no
     * escape character ({@code Like -> sqlToRegexLike(pattern, null)}), so {@code %} and {@code _}
     * are the only wildcards; {@code *}, {@code ?} and {@code \} are ordinary characters in SQL and
     * are therefore escaped for Lucene rather than passed through as wildcards.
     */
    private static String sqlLikeToWildcard(String sql) {
        StringBuilder sb = new StringBuilder(sql.length() + 4);
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            switch (c) {
                case '%':
                    sb.append('*');
                    break;
                case '_':
                    sb.append('?');
                    break;
                case '*':
                case '?':
                case '\\':
                    sb.append('\\').append(c);
                    break;
                default:
                    sb.append(c);
            }
        }
        return sb.toString();
    }

    // =================== null checks =====================

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitIsNotNull(FieldRef fieldRef) {
        return async(() -> dispatchFilter(fieldRef, IndexFilter.exists()));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitIsNull(FieldRef fieldRef) {
        // The writer registers an empty doc for every null row (addNullDoc / flushPendingDocs
        // padding), with the field absent. So notExists() = MUST_NOT FieldExistsQuery matches
        // exactly
        // those rows and IS NULL is index-evaluable (no raw-scan fallback needed).
        return async(() -> dispatchFilter(fieldRef, IndexFilter.notExists()));
    }

    // =================== helpers =====================

    private static String str(Object literal) {
        return literal == null ? "" : literal.toString();
    }
}
