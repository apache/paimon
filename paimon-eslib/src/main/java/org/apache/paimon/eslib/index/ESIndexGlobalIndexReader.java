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

import org.apache.paimon.data.Timestamp;
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
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.RoaringNavigableMap64;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import org.elasticsearch.eslib.api.ArchiveDataProvider;
import org.elasticsearch.eslib.api.ESIndexSearcher;
import org.elasticsearch.eslib.api.model.FieldIndexConfig;
import org.elasticsearch.eslib.api.model.FullTextParams;
import org.elasticsearch.eslib.api.model.FullTextQuerySpec;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import static org.apache.paimon.predicate.CompareUtils.compareLiteral;
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
    // Paimon's caller-owned executor is used only for the outer CompletableFuture. DiskBBQ must
    // never submit its cluster tasks to an executor already occupied by that outer task.
    private final ExecutorService queryExecutor;
    // Factory-owned pool used only by ESLib / DiskBBQ's internal parallel cluster search.
    private final ExecutorService searchExecutor;

    // Identity semantics are intentional: distinct streams may wrap the same path and must each be
    // tracked until their own provider closes.
    private final Set<TrackedArchiveDataProvider> openProviders =
            Collections.newSetFromMap(new IdentityHashMap<>());
    // Queries hold the read lock for their complete load/search operation. close() takes the write
    // lock, so it cannot close or replace resources that an in-flight query is still using.
    private final ReentrantReadWriteLock lifecycleLock = new ReentrantReadWriteLock();
    // Serializes the one-time lazy load while still allowing already-loaded queries to run in
    // parallel under lifecycleLock's read lock.
    private final Object loadLock = new Object();
    private volatile ESIndexSearcher searcher;
    private volatile boolean closed;
    private volatile boolean loaded;

    public ESIndexGlobalIndexReader(
            GlobalIndexFileReader fileReader,
            List<GlobalIndexIOMeta> files,
            List<DataField> fields,
            ESIndexOptions indexOptions) {
        this(fileReader, files, fields, indexOptions, null, null);
    }

    /**
     * Creates a reader that executes the outer operation inline and reserves {@code searchExecutor}
     * exclusively for ESLib's internal search work.
     */
    public ESIndexGlobalIndexReader(
            GlobalIndexFileReader fileReader,
            List<GlobalIndexIOMeta> files,
            List<DataField> fields,
            ESIndexOptions indexOptions,
            ExecutorService searchExecutor) {
        this(fileReader, files, fields, indexOptions, null, searchExecutor);
    }

    public ESIndexGlobalIndexReader(
            GlobalIndexFileReader fileReader,
            List<GlobalIndexIOMeta> files,
            List<DataField> fields,
            ESIndexOptions indexOptions,
            ExecutorService queryExecutor,
            ExecutorService searchExecutor) {
        checkArgument(files.size() == 1, "Expected exactly one ES index file per shard");
        this.fileReader = fileReader;
        this.files = files;
        this.fields = fields;
        this.indexOptions = indexOptions;
        // A caller can accidentally pass the same executor for both roles. Keep its outer async
        // scheduling but disable nested DiskBBQ parallelism in that case; merely running the outer
        // operation inline is insufficient when the caller itself is already a worker of the pool.
        boolean sameExecutor = queryExecutor != null && queryExecutor == searchExecutor;
        this.queryExecutor = queryExecutor;
        this.searchExecutor = sameExecutor ? null : searchExecutor;
        this.loaded = false;
        this.closed = false;
    }

    ExecutorService queryExecutor() {
        return queryExecutor;
    }

    ExecutorService searchExecutor() {
        return searchExecutor;
    }

    int openStreamCount() {
        synchronized (openProviders) {
            return openProviders.size();
        }
    }

    @Override
    public CompletableFuture<Optional<ScoredGlobalIndexResult>> visitVectorSearch(
            VectorSearch vectorSearch) {
        return async(
                () -> {
                    try {
                        ensureLoaded();
                        // Extract params before vector-search: query vector, field, result count
                        // (topK), candidate
                        float[] queryVector = vectorSearch.vector();
                        int topK = vectorSearch.limit();
                        int searchTopK = vectorSearchTopK(vectorSearch);
                        String fieldName = vectorSearch.fieldName();

                        RoaringNavigableMap64 includeRowIds = vectorSearch.includeRowIds();
                        long[] candidateIds = includeRowIds == null ? null : toArray(includeRowIds);

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
                        String searchField =
                                indexOptions.fullTextSearchField(fullTextSearch.fieldName());
                        if (searchField == null) {
                            return Optional.empty();
                        }
                        // Parse the JSON DSL query into an eslib FullTextQuerySpec and run it. A
                        // FULLTEXT primary field is searched directly; a KEYWORD primary field is
                        // searched through its analyzed .fulltext multi-field.
                        FullTextQuerySpec spec = parseSpec(searchField, fullTextSearch.query());
                        SearchResult result =
                                fullTextSearch(
                                        spec,
                                        fullTextSearch.limit(),
                                        fullTextSearch.includeRowIds());
                        return toScoredResult(result);
                    } catch (IOException e) {
                        throw new RuntimeException("Full-text search failed", e);
                    }
                });
    }

    /**
     * Applies {@link FullTextSearch#includeRowIds()} without changing top-K semantics. ESLib does
     * not expose a full-text candidate-filter argument, so search progressively over-fetches until
     * it has the requested number of included hits or has exhausted all matching documents. The
     * table path normally supplies the whole shard row count as {@code limit}, making this a single
     * search; the loop also preserves the direct reader API for smaller limits.
     */
    private SearchResult fullTextSearch(
            FullTextQuerySpec spec, int limit, RoaringNavigableMap64 includeRowIds)
            throws IOException {
        if (includeRowIds == null) {
            return searcher.fullTextSearch(spec, limit);
        }
        if (includeRowIds.isEmpty()) {
            return emptySearchResult();
        }

        int searchLimit = limit;
        while (true) {
            SearchResult unfiltered = searcher.fullTextSearch(spec, searchLimit);
            SearchResult filtered = filterSearchResult(unfiltered, includeRowIds, limit);
            if (filtered.count >= limit
                    || filtered.count >= includeRowIds.getLongCardinality()
                    || unfiltered == null
                    || unfiltered.count < searchLimit
                    || searchLimit == Integer.MAX_VALUE) {
                return filtered;
            }
            searchLimit =
                    searchLimit >= Integer.MAX_VALUE / 2 ? Integer.MAX_VALUE : searchLimit * 2;
        }
    }

    private static SearchResult filterSearchResult(
            SearchResult result, RoaringNavigableMap64 includeRowIds, int limit) {
        if (result == null || result.count == 0) {
            return emptySearchResult();
        }
        int capacity = Math.min(limit, result.count);
        long[] ids = new long[capacity];
        float[] scores = new float[capacity];
        int count = 0;
        for (int i = 0; i < result.count && count < limit; i++) {
            if (includeRowIds.contains(result.ids[i])) {
                ids[count] = result.ids[i];
                scores[count] = result.scores[i];
                count++;
            }
        }
        if (count == 0) {
            return emptySearchResult();
        }
        return new SearchResult(Arrays.copyOf(ids, count), Arrays.copyOf(scores, count), count);
    }

    private static SearchResult emptySearchResult() {
        return new SearchResult(new long[0], new float[0], 0);
    }

    /**
     * Parses the {@link FullTextSearch#query()} JSON DSL into an eslib {@link FullTextQuerySpec}.
     */
    private static FullTextQuerySpec parseSpec(String field, String json) {
        try {
            return parseSpec(field, JsonSerdeUtil.OBJECT_MAPPER_INSTANCE.readTree(json));
        } catch (IOException e) {
            throw new IllegalArgumentException("Invalid full-text query JSON: " + json, e);
        }
    }

    /**
     * Maps the JSON query DSL ({@code {"match":...}}, {@code {"match_phrase":...}}, {@code
     * {"boolean":...}}, {@code {"boost":...}}) onto an eslib spec. The field comes from {@link
     * FullTextSearch#fieldName()} (the flat model carries a single field); the query semantics come
     * from the JSON. The caller resolves the logical Paimon field to its physical FULLTEXT field.
     */
    private static FullTextQuerySpec parseSpec(String field, JsonNode q) {
        if (q.has("match")) {
            JsonNode m = q.get("match");
            return new FullTextQuerySpec.Match(field, queryText(m), parseParams(m));
        } else if (q.has("match_phrase") || q.has("phrase")) {
            JsonNode p = q.has("match_phrase") ? q.get("match_phrase") : q.get("phrase");
            int slop = p.has("slop") ? p.get("slop").asInt() : 0;
            return new FullTextQuerySpec.Phrase(field, queryText(p), slop);
        } else if (q.has("boost")) {
            JsonNode b = q.get("boost");
            float negativeBoost = 0.5f;
            if (b.has("negative_boost")) {
                negativeBoost = (float) b.get("negative_boost").asDouble();
            } else if (b.has("negativeBoost")) {
                negativeBoost = (float) b.get("negativeBoost").asDouble();
            }
            return new FullTextQuerySpec.Boost(
                    parseSpec(field, b.get("positive")),
                    parseSpec(field, b.get("negative")),
                    negativeBoost);
        } else if (q.has("boolean")) {
            List<FullTextQuerySpec> must = new ArrayList<>();
            List<FullTextQuerySpec> should = new ArrayList<>();
            List<FullTextQuerySpec> mustNot = new ArrayList<>();
            JsonNode clauses = q.get("boolean").get("queries");
            if (clauses != null) {
                for (JsonNode clause : clauses) {
                    String occur;
                    JsonNode child;
                    if (clause.isArray()) {
                        occur = clause.get(0).asText();
                        child = clause.get(1);
                    } else {
                        occur = clause.has("occur") ? clause.get("occur").asText() : "Should";
                        child = clause.get("query");
                    }
                    FullTextQuerySpec spec = parseSpec(field, child);
                    if ("Must".equalsIgnoreCase(occur)) {
                        must.add(spec);
                    } else if ("MustNot".equalsIgnoreCase(occur)) {
                        mustNot.add(spec);
                    } else {
                        should.add(spec);
                    }
                }
            }
            return new FullTextQuerySpec.Bool(must, should, mustNot);
        }
        throw new UnsupportedOperationException("Unsupported full-text query JSON: " + q);
    }

    private static String queryText(JsonNode node) {
        if (node.has("query")) {
            return node.get("query").asText();
        } else if (node.has("terms")) {
            return node.get("terms").asText();
        }
        throw new IllegalArgumentException("Full-text query missing 'query'/'terms': " + node);
    }

    /**
     * Reads the ES-style match params ({@code operator} / {@code boost} / {@code fuzziness} /
     * {@code max_expansions} / {@code prefix_length}) from a match node into {@link
     * FullTextParams}; the eslib searcher applies them to the Lucene query. Absent params keep
     * their defaults.
     */
    private static FullTextParams parseParams(JsonNode match) {
        FullTextParams.Operator operator =
                match.has("operator") && "and".equalsIgnoreCase(match.get("operator").asText())
                        ? FullTextParams.Operator.AND
                        : FullTextParams.Operator.OR;
        float boost = match.has("boost") ? (float) match.get("boost").asDouble() : 1.0f;
        int fuzziness = match.has("fuzziness") ? match.get("fuzziness").asInt() : 0;
        int maxExpansions = match.has("max_expansions") ? match.get("max_expansions").asInt() : 50;
        int prefixLength = match.has("prefix_length") ? match.get("prefix_length").asInt() : 0;
        return new FullTextParams(operator, boost, fuzziness, maxExpansions, prefixLength);
    }

    /**
     * Run {@code body} on Paimon's caller-owned query executor when one is available, otherwise
     * execute it inline. The separate {@link #searchExecutor} is reserved for DiskBBQ's internal
     * cluster tasks; using one pool for both layers can deadlock when the outer tasks occupy every
     * worker and wait for nested tasks submitted to the same pool.
     */
    private <T> CompletableFuture<T> async(Supplier<T> body) {
        Supplier<T> guardedBody =
                () -> {
                    Lock readLock = lifecycleLock.readLock();
                    readLock.lock();
                    try {
                        return body.get();
                    } finally {
                        readLock.unlock();
                    }
                };
        if (queryExecutor != null) {
            return CompletableFuture.supplyAsync(guardedBody, queryExecutor);
        }
        return CompletableFuture.completedFuture(guardedBody.get());
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
        if (loaded) {
            return;
        }
        synchronized (loadLock) {
            // close() may have won the lifecycle write lock while this operation was waiting to
            // start. Recheck under the load lock before opening any stream or creating a searcher.
            checkNotClosed();
            if (loaded) {
                return;
            }

            if (files.isEmpty()) {
                throw new IOException("No index files to load");
            }

            long loadStartNanos = System.nanoTime();
            GlobalIndexIOMeta meta = files.get(0);
            byte[] metaBytes = meta.metadata();
            // Map each lucene file to its [start, end] byte offsets
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

            // Offset/length + fork() reader over the Paimon stream of the global-index archive
            // (*.index)
            // dataProvider bridge Lucene's per-file reads into offset/length reads over the packed
            // archive
            // ESLib's ArchiveIndexInput pools provider forks across clones so concurrent reads
            // remain independent without opening one stream for every query-scoped clone.
            ArchiveDataProvider dataProvider = null;
            ESIndexSearcher candidateSearcher = null;
            try {
                // Mount the packed archive as a Lucene index, then open the reader/searcher. Keep
                // it local until load succeeds so a retry cannot overwrite a partially loaded
                // searcher and leak its directory/providers.
                dataProvider = createProvider(meta);
                candidateSearcher = ESIndexBuilderFactory.createSearcher();
                candidateSearcher.load(
                        dataProvider, fileOffsets, indexOptions.getFieldConfigs(), searchExecutor);
            } catch (IOException | RuntimeException | Error failure) {
                IOException cleanupFailure = cleanupFailedLoad(candidateSearcher, dataProvider);
                if (cleanupFailure != null) {
                    failure.addSuppressed(cleanupFailure);
                }
                throw failure;
            }
            searcher = candidateSearcher;
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
        TrackedArchiveDataProvider provider = new TrackedArchiveDataProvider(meta, inputStream);
        synchronized (openProviders) {
            openProviders.add(provider);
        }
        return provider;
    }

    private final class TrackedArchiveDataProvider implements ArchiveDataProvider {
        private final GlobalIndexIOMeta meta;
        private final SeekableInputStream inputStream;
        private final AtomicBoolean providerClosed = new AtomicBoolean();

        private TrackedArchiveDataProvider(
                GlobalIndexIOMeta meta, SeekableInputStream inputStream) {
            this.meta = meta;
            this.inputStream = inputStream;
        }

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
            if (!providerClosed.compareAndSet(false, true)) {
                return;
            }
            boolean closeSucceeded = false;
            try {
                inputStream.close();
                closeSucceeded = true;
            } finally {
                // Retain a provider whose close failed so reader close / failed-load cleanup can
                // make one final best-effort attempt.
                if (closeSucceeded) {
                    synchronized (openProviders) {
                        openProviders.remove(this);
                    }
                } else {
                    // Allow failed-load cleanup or the reader's final close to retry.
                    providerClosed.set(false);
                }
            }
        }
    }

    private IOException cleanupFailedLoad(
            ESIndexSearcher candidateSearcher, ArchiveDataProvider dataProvider) {
        IOException failure = null;
        if (candidateSearcher != null) {
            try {
                candidateSearcher.close();
            } catch (IOException e) {
                failure = e;
            }
        }
        if (dataProvider != null) {
            try {
                dataProvider.close();
            } catch (IOException e) {
                failure = mergeFailure(failure, e);
            }
        }
        return mergeFailure(failure, closeTrackedProviders());
    }

    private IOException closeTrackedProviders() {
        List<TrackedArchiveDataProvider> providers;
        synchronized (openProviders) {
            providers = new ArrayList<>(openProviders);
        }

        IOException failure = null;
        for (TrackedArchiveDataProvider provider : providers) {
            try {
                provider.close();
            } catch (IOException e) {
                failure = mergeFailure(failure, e);
            }
        }
        return failure;
    }

    private static IOException mergeFailure(IOException failure, IOException next) {
        if (next == null) {
            return failure;
        }
        if (failure == null) {
            return next;
        }
        failure.addSuppressed(next);
        return failure;
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
        Lock writeLock = lifecycleLock.writeLock();
        writeLock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;

            IOException failure = null;
            ESIndexSearcher currentSearcher = searcher;
            searcher = null;
            if (currentSearcher != null) {
                try {
                    currentSearcher.close();
                } catch (IOException e) {
                    failure = e;
                }
            }

            failure = mergeFailure(failure, closeTrackedProviders());
            loaded = false;
            if (failure != null) {
                throw failure;
            }
        } finally {
            writeLock.unlock();
        }
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
            // the raw column value, so ordinary predicates (=, <>, <, >, IN, LIKE, IS NULL, ...)
            // cannot run on it directly. Route them to the keyword multi-field sub-field
            // (content.keyword) when present — it holds the exact value and serves these correctly.
            // Without the sub-field, fall back to raw scan (Optional.empty). Full-text search
            // (visitFullTextSearch) still targets the analyzed primary field.
            String subField = indexOptions.keywordSubField(fieldRef.name());
            if (subField != null) {
                if (isKeywordRangeFilter(filter)) {
                    return Optional.empty();
                }
                try {
                    return executeFilter(subField, filter);
                } catch (UnsupportedOperationException | IllegalArgumentException e) {
                    // The keyword sub-field serves term/IN/prefix/wildcard/exists but not e.g. a
                    // numeric range on a string; fall back to raw scan rather than failing.
                    return Optional.empty();
                }
            }
            return Optional.empty();
        }
        if (config.indexType() == FieldIndexConfig.IndexType.KEYWORD
                && isKeywordRangeFilter(filter)) {
            return Optional.empty();
        }
        return executeFilter(fieldRef.name(), filter);
    }

    private static boolean isKeywordRangeFilter(IndexFilter filter) {
        if (filter.filterType() != IndexFilter.FilterType.SCALAR) {
            return false;
        }
        ScalarPredicate.Op op = ((IndexFilter.ScalarFilter) filter).predicate().op();
        return op == ScalarPredicate.Op.LESS_THAN
                || op == ScalarPredicate.Op.LESS_OR_EQUAL
                || op == ScalarPredicate.Op.GREATER_THAN
                || op == ScalarPredicate.Op.GREATER_OR_EQUAL
                || op == ScalarPredicate.Op.RANGE;
    }

    /**
     * ESLib does not implement NOT_EQUAL / NOT_IN scalar predicates and falls through to
     * MatchAllDocsQuery. Evaluate the negation here as exists(field) AND NOT matchingFilter, so
     * null rows are excluded and vector/hybrid includeRowIds cannot become an accidental full
     * match.
     */
    private Optional<GlobalIndexResult> dispatchExistingRowsNotMatching(
            FieldRef fieldRef, IndexFilter matchingFilter) {
        Optional<GlobalIndexResult> existing = dispatchFilter(fieldRef, IndexFilter.exists());
        if (existing.isEmpty()) {
            return Optional.empty();
        }
        Optional<GlobalIndexResult> matching = dispatchFilter(fieldRef, matchingFilter);
        if (matching.isEmpty()) {
            return Optional.empty();
        }

        RoaringNavigableMap64 result = new RoaringNavigableMap64();
        result.or(existing.get().results());
        result.andNot(matching.get().results());
        return Optional.of(GlobalIndexResult.create(result));
    }

    private IndexFilter exactValueFilter(FieldRef fieldRef, Object literal) {
        FieldIndexConfig config = indexOptions.getConfig(fieldRef.name());
        if (config != null
                && (config.indexType() == FieldIndexConfig.IndexType.KEYWORD
                        || config.indexType() == FieldIndexConfig.IndexType.FULLTEXT)) {
            return IndexFilter.text(IndexFilter.TextFilter.TextOp.TERM, str(literal));
        }
        return IndexFilter.scalar(ScalarPredicate.eq(indexedScalarLiteral(fieldRef, literal)));
    }

    private static List<Object> indexedScalarLiterals(FieldRef fieldRef, List<Object> literals) {
        if (literals == null) {
            return null;
        }
        List<Object> indexed = new ArrayList<>(literals.size());
        for (Object literal : literals) {
            indexed.add(indexedScalarLiteral(fieldRef, literal));
        }
        return indexed;
    }

    private static Object indexedScalarLiteral(FieldRef fieldRef, Object literal) {
        if (literal == null || fieldRef.type() == null) {
            return literal;
        }
        switch (fieldRef.type().getTypeRoot()) {
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return literal instanceof Number ? ((Number) literal).longValue() : literal;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if (literal instanceof Timestamp) {
                    return ((Timestamp) literal).getMillisecond();
                }
                if (literal instanceof java.sql.Timestamp) {
                    return ((java.sql.Timestamp) literal).getTime();
                }
                return literal instanceof Number ? ((Number) literal).longValue() : literal;
            default:
                return literal;
        }
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
        if (literal == null) {
            return emptyFilterFuture();
        }
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
                            fieldRef,
                            IndexFilter.scalar(
                                    ScalarPredicate.eq(indexedScalarLiteral(fieldRef, literal))));
                });
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitNotEqual(
            FieldRef fieldRef, Object literal) {
        if (literal == null) {
            return emptyFilterFuture();
        }
        return async(
                () ->
                        dispatchExistingRowsNotMatching(
                                fieldRef, exactValueFilter(fieldRef, literal)));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitLessThan(
            FieldRef fieldRef, Object literal) {
        if (literal == null) {
            return emptyFilterFuture();
        }
        return async(
                () ->
                        dispatchFilter(
                                fieldRef,
                                IndexFilter.scalar(
                                        ScalarPredicate.lt(
                                                indexedScalarLiteral(fieldRef, literal)))));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitLessOrEqual(
            FieldRef fieldRef, Object literal) {
        if (literal == null) {
            return emptyFilterFuture();
        }
        return async(
                () ->
                        dispatchFilter(
                                fieldRef,
                                IndexFilter.scalar(
                                        ScalarPredicate.lte(
                                                indexedScalarLiteral(fieldRef, literal)))));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitGreaterThan(
            FieldRef fieldRef, Object literal) {
        if (literal == null) {
            return emptyFilterFuture();
        }
        return async(
                () ->
                        dispatchFilter(
                                fieldRef,
                                IndexFilter.scalar(
                                        ScalarPredicate.gt(
                                                indexedScalarLiteral(fieldRef, literal)))));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitGreaterOrEqual(
            FieldRef fieldRef, Object literal) {
        if (literal == null) {
            return emptyFilterFuture();
        }
        return async(
                () ->
                        dispatchFilter(
                                fieldRef,
                                IndexFilter.scalar(
                                        ScalarPredicate.gte(
                                                indexedScalarLiteral(fieldRef, literal)))));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitBetween(
            FieldRef fieldRef, Object from, Object to) {
        if (from == null || to == null || isReversedRange(fieldRef, from, to)) {
            return emptyFilterFuture();
        }
        return async(
                () ->
                        dispatchFilter(
                                fieldRef,
                                IndexFilter.scalar(
                                        ScalarPredicate.range(
                                                indexedScalarLiteral(fieldRef, from),
                                                indexedScalarLiteral(fieldRef, to)))));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitNotBetween(
            FieldRef fieldRef, Object from, Object to) {
        if (from == null || to == null) {
            return emptyFilterFuture();
        }
        return async(
                () -> {
                    if (isReversedRange(fieldRef, from, to)) {
                        return dispatchFilter(fieldRef, IndexFilter.exists());
                    }
                    return dispatchExistingRowsNotMatching(
                            fieldRef,
                            IndexFilter.scalar(
                                    ScalarPredicate.range(
                                            indexedScalarLiteral(fieldRef, from),
                                            indexedScalarLiteral(fieldRef, to))));
                });
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitIn(
            FieldRef fieldRef, List<Object> literals) {
        List<Object> nonNullLiterals = withoutNullLiterals(literals);
        if (nonNullLiterals.isEmpty()) {
            return emptyFilterFuture();
        }
        return async(
                () ->
                        dispatchFilter(
                                fieldRef,
                                IndexFilter.scalar(
                                        ScalarPredicate.in(
                                                indexedScalarLiterals(
                                                        fieldRef, nonNullLiterals)))));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitNotIn(
            FieldRef fieldRef, List<Object> literals) {
        if (literals == null || literals.contains(null)) {
            return emptyFilterFuture();
        }
        return async(
                () ->
                        dispatchExistingRowsNotMatching(
                                fieldRef,
                                IndexFilter.scalar(
                                        ScalarPredicate.in(
                                                indexedScalarLiterals(fieldRef, literals)))));
    }

    // =================== text pattern visitors (keyword / fulltext) =====================

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitStartsWith(
            FieldRef fieldRef, Object literal) {
        if (literal == null) {
            return emptyFilterFuture();
        }
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
        if (literal == null) {
            return emptyFilterFuture();
        }
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
        if (literal == null) {
            return emptyFilterFuture();
        }
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
        if (literal == null) {
            return emptyFilterFuture();
        }
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
     * Translate a SQL LIKE pattern into a Lucene wildcard pattern. Paimon's default SQL LIKE escape
     * is {@code \}: escaped {@code _}, {@code %}, and {@code \} are literals, while unescaped
     * {@code %} and {@code _} are the only SQL wildcards. Lucene's own wildcard metacharacters are
     * escaped whenever they represent literal input.
     */
    private static String sqlLikeToWildcard(String sql) {
        StringBuilder sb = new StringBuilder(sql.length() + 4);
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            switch (c) {
                case '\\':
                    if (i == sql.length() - 1) {
                        throw invalidLikeEscapeSequence(sql, i);
                    }
                    char escaped = sql.charAt(++i);
                    if (escaped != '_' && escaped != '%' && escaped != '\\') {
                        throw invalidLikeEscapeSequence(sql, i - 1);
                    }
                    if (escaped == '\\') {
                        sb.append('\\');
                    }
                    sb.append(escaped);
                    break;
                case '%':
                    sb.append('*');
                    break;
                case '_':
                    sb.append('?');
                    break;
                case '*':
                case '?':
                    sb.append('\\').append(c);
                    break;
                default:
                    sb.append(c);
            }
        }
        return sb.toString();
    }

    private static RuntimeException invalidLikeEscapeSequence(String pattern, int position) {
        return new RuntimeException("Invalid escape sequence '" + pattern + "', " + position);
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

    private static CompletableFuture<Optional<GlobalIndexResult>> emptyFilterFuture() {
        return CompletableFuture.completedFuture(
                Optional.of(GlobalIndexResult.create(new RoaringNavigableMap64())));
    }

    private static boolean isReversedRange(FieldRef fieldRef, Object from, Object to) {
        return fieldRef.type() != null && compareLiteral(fieldRef.type(), from, to) > 0;
    }

    private static List<Object> withoutNullLiterals(List<Object> literals) {
        if (literals == null || literals.isEmpty()) {
            return Collections.emptyList();
        }
        List<Object> result = new ArrayList<>(literals.size());
        for (Object literal : literals) {
            if (literal != null) {
                result.add(literal);
            }
        }
        return result;
    }

    private static String str(Object literal) {
        return literal.toString();
    }
}
