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
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.VectorType;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.RoaringNavigableMap64;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import org.elasticsearch.eslib.api.ArchiveDataProvider;
import org.elasticsearch.eslib.api.ESIndexSearcher;
import org.elasticsearch.eslib.api.model.FieldIndexConfig;
import org.elasticsearch.eslib.api.model.FullTextParams;
import org.elasticsearch.eslib.api.model.FullTextQuerySpec;
import org.elasticsearch.eslib.api.model.IndexFilter;
import org.elasticsearch.eslib.api.model.ScalarFieldType;
import org.elasticsearch.eslib.api.model.ScalarPredicate;
import org.elasticsearch.eslib.api.model.SearchResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
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
    private static final int MAX_FULL_TEXT_QUERY_DEPTH = 64;

    private final GlobalIndexFileReader fileReader;
    private final List<GlobalIndexIOMeta> files;
    private final List<DataField> fields;
    private final Map<String, String> logicalToPhysicalFields;
    private final Set<String> incompatibleLogicalFields;
    private final ESIndexOptions indexOptions;
    private final Map<String, long[]> fileOffsets;
    private final boolean hasPersistedFieldConfigs;
    // Paimon's caller-owned executor schedules the outer CompletableFuture. The standalone ES940
    // reader currently scores clusters serially and does not accept a per-reader scoring pool.
    private final ExecutorService queryExecutor;

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
        this(fileReader, files, fields, indexOptions, null);
    }

    public ESIndexGlobalIndexReader(
            GlobalIndexFileReader fileReader,
            List<GlobalIndexIOMeta> files,
            List<DataField> fields,
            ESIndexOptions indexOptions,
            ExecutorService queryExecutor) {
        Objects.requireNonNull(files, "files");
        checkArgument(files.size() == 1, "Expected exactly one ES index file per shard");
        this.fileReader = Objects.requireNonNull(fileReader, "fileReader");
        this.files = Collections.unmodifiableList(new ArrayList<>(files));
        this.fields =
                Collections.unmodifiableList(
                        new ArrayList<>(Objects.requireNonNull(fields, "fields")));
        ESIndexOptions fallbackIndexOptions = Objects.requireNonNull(indexOptions, "indexOptions");
        try {
            GlobalIndexIOMeta archiveMeta =
                    Objects.requireNonNull(this.files.get(0), "index file metadata");
            ESIndexFileMeta.Parsed parsed = ESIndexFileMeta.read(archiveMeta.metadata());
            this.fileOffsets = parsed.fileOffsets();
            validateArchiveOffsets(archiveMeta, fileOffsets);
            this.hasPersistedFieldConfigs = parsed.hasFieldConfigs();
            this.indexOptions =
                    parsed.hasFieldConfigs()
                            ? ESIndexOptions.fromFieldConfigs(parsed.fieldConfigs())
                            : fallbackIndexOptions;
            List<String> physicalFields =
                    parsed.hasFieldConfigs()
                            ? parsed.indexedFieldNames()
                            : currentFieldNames(this.fields);
            this.logicalToPhysicalFields =
                    createFieldMapping(this.fields, physicalFields, this.indexOptions);
            this.incompatibleLogicalFields =
                    findIncompatibleFields(
                            this.fields,
                            physicalFields,
                            parsed.indexedFieldTypes(),
                            this.indexOptions,
                            parsed.hasFieldConfigs());
        } catch (IOException e) {
            throw new IllegalArgumentException("Invalid es-index metadata", e);
        }
        this.queryExecutor = queryExecutor;
        this.loaded = false;
        this.closed = false;
    }

    private static List<String> currentFieldNames(List<DataField> fields) throws IOException {
        List<String> names = new ArrayList<>(fields.size());
        for (DataField field : fields) {
            if (field == null) {
                throw new IOException("Null current field in es-index reader");
            }
            names.add(field.name());
        }
        return names;
    }

    private static Map<String, String> createFieldMapping(
            List<DataField> currentFields, List<String> physicalFields, ESIndexOptions indexOptions)
            throws IOException {
        if (physicalFields.size() != currentFields.size()) {
            throw new IOException(
                    "ES index field count does not match current schema: indexed="
                            + physicalFields.size()
                            + ", current="
                            + currentFields.size());
        }
        Map<String, String> mapping = new HashMap<>();
        Set<String> physicalNames = new HashSet<>();
        for (int i = 0; i < currentFields.size(); i++) {
            DataField current = currentFields.get(i);
            if (current == null) {
                throw new IOException("Null current field in es-index reader at position " + i);
            }
            String physical = physicalFields.get(i);
            if (physical == null || physical.isEmpty()) {
                throw new IOException(
                        "Empty indexed field name in es-index metadata at position " + i);
            }
            if (indexOptions.getConfig(physical) == null) {
                throw new IOException(
                        "Missing primary field config in es-index metadata: " + physical);
            }
            if (mapping.put(current.name(), physical) != null) {
                throw new IOException(
                        "Duplicate current field in es-index reader: " + current.name());
            }
            if (!physicalNames.add(physical)) {
                throw new IOException("Duplicate indexed field in es-index metadata: " + physical);
            }
        }
        return Collections.unmodifiableMap(mapping);
    }

    private String physicalField(String logicalField) {
        return logicalToPhysicalFields.get(logicalField);
    }

    private FieldIndexConfig logicalFieldConfig(String logicalField) {
        if (incompatibleLogicalFields.contains(logicalField)) {
            return null;
        }
        String physicalField = physicalField(logicalField);
        return physicalField == null ? null : indexOptions.getConfig(physicalField);
    }

    private static Set<String> findIncompatibleFields(
            List<DataField> currentFields,
            List<String> physicalFields,
            List<String> indexedFieldTypes,
            ESIndexOptions indexOptions,
            boolean hasPersistedFieldConfigs)
            throws IOException {
        if (!indexedFieldTypes.isEmpty() && indexedFieldTypes.size() != currentFields.size()) {
            throw new IOException(
                    "ES index field type count does not match current schema: indexed="
                            + indexedFieldTypes.size()
                            + ", current="
                            + currentFields.size());
        }
        if (!hasPersistedFieldConfigs) {
            return Collections.emptySet();
        }

        Set<String> incompatible = new HashSet<>();
        for (int i = 0; i < currentFields.size(); i++) {
            DataField current = currentFields.get(i);
            boolean compatible;
            if (!indexedFieldTypes.isEmpty()) {
                compatible =
                        current.type().copy(true).asSQLString().equals(indexedFieldTypes.get(i));
            } else {
                compatible =
                        legacyTypeCompatible(
                                current.type(), indexOptions.getConfig(physicalFields.get(i)));
            }
            if (!compatible) {
                incompatible.add(current.name());
            }
        }
        return incompatible.isEmpty()
                ? Collections.emptySet()
                : Collections.unmodifiableSet(incompatible);
    }

    private static boolean legacyTypeCompatible(DataType type, FieldIndexConfig config) {
        if (config == null) {
            return false;
        }
        switch (config.indexType()) {
            case VECTOR:
                if (type instanceof VectorType) {
                    VectorType vectorType = (VectorType) type;
                    return vectorType.getElementType().getTypeRoot() == DataTypeRoot.FLOAT
                            && vectorType.getLength() == config.dimension();
                }
                return type instanceof ArrayType
                        && ((ArrayType) type).getElementType().getTypeRoot() == DataTypeRoot.FLOAT;
            case FULLTEXT:
            case KEYWORD:
                return type.getTypeRoot() == DataTypeRoot.CHAR
                        || type.getTypeRoot() == DataTypeRoot.VARCHAR;
            case SCALAR:
            case DATE:
                return expectedScalarType(type) == config.scalarType();
            default:
                return false;
        }
    }

    private static ScalarFieldType expectedScalarType(DataType type) {
        DataType valueType = type instanceof ArrayType ? ((ArrayType) type).getElementType() : type;
        switch (valueType.getTypeRoot()) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
                return ScalarFieldType.INT;
            case BIGINT:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return ScalarFieldType.LONG;
            case FLOAT:
                return ScalarFieldType.FLOAT;
            case DOUBLE:
                return ScalarFieldType.DOUBLE;
            case CHAR:
            case VARCHAR:
                return ScalarFieldType.KEYWORD;
            default:
                return null;
        }
    }

    private static void validateArchiveOffsets(
            GlobalIndexIOMeta archiveMeta, Map<String, long[]> fileOffsets) throws IOException {
        if (archiveMeta.fileSize() < 0) {
            throw new IOException("Negative es-index archive size: " + archiveMeta.fileSize());
        }
        if (fileOffsets.isEmpty()) {
            throw new IOException("ES index metadata contains no Lucene files");
        }
        for (Map.Entry<String, long[]> entry : fileOffsets.entrySet()) {
            long[] range = entry.getValue();
            if (range == null
                    || range.length != 2
                    || range[0] < 0
                    || range[1] < 0
                    || range[0] > archiveMeta.fileSize() - range[1]) {
                throw new IOException(
                        "File range exceeds es-index archive size for "
                                + entry.getKey()
                                + ": archiveSize="
                                + archiveMeta.fileSize()
                                + ", range="
                                + sample(range, range == null ? 0 : range.length, 2));
            }
        }
    }

    ExecutorService queryExecutor() {
        return queryExecutor;
    }

    /** Returns the primary vector field's effective metric in Paimon's metric vocabulary. */
    String primaryVectorMetric() {
        if (fields.isEmpty()) {
            return null;
        }
        String physicalField = physicalField(fields.get(0).name());
        FieldIndexConfig config =
                physicalField == null ? null : indexOptions.getConfig(physicalField);
        return config != null && config.indexType() == FieldIndexConfig.IndexType.VECTOR
                ? ESIndexOptions.toPaimonVectorMetric(config.metric())
                : null;
    }

    int openStreamCount() {
        synchronized (openProviders) {
            return openProviders.size();
        }
    }

    @Override
    public CompletableFuture<Optional<ScoredGlobalIndexResult>> visitVectorSearch(
            VectorSearch vectorSearch) {
        Objects.requireNonNull(vectorSearch, "vectorSearch");
        return async(
                () -> {
                    try {
                        // Extract params before vector-search: query vector, field, result count
                        // (topK), candidate
                        float[] queryVector = vectorSearch.vector();
                        int topK = vectorSearch.limit();
                        if (topK <= 0) {
                            throw new IllegalArgumentException(
                                    "Vector search limit must be positive; got: " + topK);
                        }
                        int searchTopK = vectorSearchTopK(vectorSearch);
                        String fieldName = vectorSearch.fieldName();
                        String physicalField = physicalField(fieldName);
                        if (physicalField == null) {
                            return Optional.empty();
                        }
                        FieldIndexConfig config = indexOptions.getConfig(physicalField);
                        if (config == null
                                || config.indexType() != FieldIndexConfig.IndexType.VECTOR) {
                            if (incompatibleLogicalFields.contains(fieldName)) {
                                throw incompatibleSearchSchema(fieldName, "vector");
                            }
                            return Optional.empty();
                        }
                        if (incompatibleLogicalFields.contains(fieldName)
                                && !vectorTypeCompatible(fieldName, config)) {
                            throw incompatibleSearchSchema(fieldName, "vector");
                        }
                        if (queryVector == null || queryVector.length != config.dimension()) {
                            throw new IllegalArgumentException(
                                    "Vector query for field '"
                                            + fieldName
                                            + "' expects dimension "
                                            + config.dimension()
                                            + " but received "
                                            + (queryVector == null ? "null" : queryVector.length)
                                            + ".");
                        }
                        ensureLoaded();

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
                                        physicalField, queryVector, searchTopK, candidateIds);
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
                        return toVectorScoredResult(result, topK, config.metric());
                    } catch (IOException e) {
                        throw new RuntimeException("Vector search failed", e);
                    }
                });
    }

    @Override
    public CompletableFuture<Optional<ScoredGlobalIndexResult>> visitFullTextSearch(
            FullTextSearch fullTextSearch) {
        Objects.requireNonNull(fullTextSearch, "fullTextSearch");
        return async(
                () -> {
                    try {
                        if (fullTextSearch.limit() <= 0) {
                            throw new IllegalArgumentException(
                                    "Full-text search limit must be positive; got: "
                                            + fullTextSearch.limit());
                        }
                        String physicalField = physicalField(fullTextSearch.fieldName());
                        String searchField =
                                physicalField == null
                                        ? null
                                        : indexOptions.fullTextSearchField(physicalField);
                        if (physicalField != null
                                && incompatibleLogicalFields.contains(fullTextSearch.fieldName())
                                && (searchField == null
                                        || !isCurrentTextField(fullTextSearch.fieldName()))) {
                            throw incompatibleSearchSchema(fullTextSearch.fieldName(), "full-text");
                        }
                        if (searchField == null) {
                            return Optional.empty();
                        }
                        // Parse the JSON DSL query into an eslib FullTextQuerySpec and run it. A
                        // FULLTEXT primary field is searched directly; a KEYWORD primary field is
                        // searched through its analyzed .fulltext multi-field.
                        FullTextQuerySpec spec = parseSpec(searchField, fullTextSearch.query());
                        ensureLoaded();
                        SearchResult result =
                                fullTextSearch(
                                        spec,
                                        fullTextSearch.limit(),
                                        fullTextSearch.includeRowIds());
                        return toScoredResult(result, fullTextSearch.limit());
                    } catch (IOException e) {
                        throw new RuntimeException("Full-text search failed", e);
                    }
                });
    }

    private boolean vectorTypeCompatible(String logicalField, FieldIndexConfig config) {
        DataType currentType = currentFieldType(logicalField);
        if (currentType instanceof VectorType) {
            VectorType vectorType = (VectorType) currentType;
            return vectorType.getElementType().getTypeRoot() == DataTypeRoot.FLOAT
                    && vectorType.getLength() == config.dimension();
        }
        return currentType instanceof ArrayType
                && ((ArrayType) currentType).getElementType().getTypeRoot() == DataTypeRoot.FLOAT;
    }

    private boolean isCurrentTextField(String logicalField) {
        DataType currentType = currentFieldType(logicalField);
        return currentType != null
                && (currentType.getTypeRoot() == DataTypeRoot.CHAR
                        || currentType.getTypeRoot() == DataTypeRoot.VARCHAR);
    }

    private DataType currentFieldType(String logicalField) {
        for (DataField field : fields) {
            if (field.name().equals(logicalField)) {
                return field.type();
            }
        }
        return null;
    }

    private static IllegalStateException incompatibleSearchSchema(
            String logicalField, String searchType) {
        return new IllegalStateException(
                "The persisted es-index for field '"
                        + logicalField
                        + "' is incompatible with the current schema and cannot safely answer "
                        + searchType
                        + " search. Rebuild the es-index after the schema change.");
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
    static FullTextQuerySpec parseSpec(String field, String json) {
        try {
            return parseSpec(field, JsonSerdeUtil.OBJECT_MAPPER_INSTANCE.readTree(json), 0);
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
    private static FullTextQuerySpec parseSpec(String field, JsonNode q, int depth) {
        if (depth >= MAX_FULL_TEXT_QUERY_DEPTH) {
            throw new IllegalArgumentException(
                    "Full-text query exceeds the maximum nesting depth of "
                            + MAX_FULL_TEXT_QUERY_DEPTH);
        }
        requireObject(q, "full-text query");
        requireOnlyFields(
                q,
                "full-text query",
                "match",
                "match_phrase",
                "phrase",
                "boost",
                "boolean",
                "bool");
        int queryTypeCount =
                present(q, "match")
                        + present(q, "match_phrase")
                        + present(q, "phrase")
                        + present(q, "boost")
                        + present(q, "boolean")
                        + present(q, "bool");
        if (queryTypeCount != 1) {
            throw new IllegalArgumentException(
                    "Full-text query must contain exactly one supported query type: " + q);
        }
        if (q.has("match")) {
            JsonNode m = requireObject(q.get("match"), "match query");
            // Older SQL serialization includes a logical "column" routing hint. Accept it for
            // compatibility, but keep the already resolved physical field authoritative so column
            // renames continue to work.
            requireOnlyFields(
                    m,
                    "match query",
                    "column",
                    "query",
                    "terms",
                    "operator",
                    "boost",
                    "fuzziness",
                    "max_expansions",
                    "maxExpansions",
                    "prefix_length",
                    "prefixLength");
            return new FullTextQuerySpec.Match(field, queryText(m), parseParams(m));
        } else if (q.has("match_phrase") || q.has("phrase")) {
            JsonNode p =
                    requireObject(
                            q.has("match_phrase") ? q.get("match_phrase") : q.get("phrase"),
                            "phrase query");
            requireOnlyFields(p, "phrase query", "column", "query", "terms", "slop");
            int slop = p.has("slop") ? intValue(p.get("slop"), "slop") : 0;
            return new FullTextQuerySpec.Phrase(field, queryText(p), slop);
        } else if (q.has("boost")) {
            JsonNode b = requireObject(q.get("boost"), "boost query");
            requireOnlyFields(
                    b, "boost query", "positive", "negative", "negative_boost", "negativeBoost");
            rejectAliasPair(b, "negative_boost", "negativeBoost");
            float negativeBoost = 0.5f;
            if (b.has("negative_boost")) {
                negativeBoost = floatValue(b.get("negative_boost"), "negative_boost");
            } else if (b.has("negativeBoost")) {
                negativeBoost = floatValue(b.get("negativeBoost"), "negativeBoost");
            }
            return new FullTextQuerySpec.Boost(
                    parseSpec(field, required(b, "positive"), depth + 1),
                    parseSpec(field, required(b, "negative"), depth + 1),
                    negativeBoost);
        } else if (q.has("boolean") || q.has("bool")) {
            List<FullTextQuerySpec> must = new ArrayList<>();
            List<FullTextQuerySpec> should = new ArrayList<>();
            List<FullTextQuerySpec> mustNot = new ArrayList<>();
            JsonNode booleanNode =
                    requireObject(
                            q.has("boolean") ? q.get("boolean") : q.get("bool"), "boolean query");
            requireOnlyFields(
                    booleanNode,
                    "boolean query",
                    "must",
                    "should",
                    "must_not",
                    "mustNot",
                    "queries");
            rejectAliasPair(booleanNode, "must_not", "mustNot");
            addBooleanClauses(field, booleanNode.get("must"), must, depth + 1);
            addBooleanClauses(field, booleanNode.get("should"), should, depth + 1);
            JsonNode namedMustNot =
                    booleanNode.has("must_not")
                            ? booleanNode.get("must_not")
                            : booleanNode.get("mustNot");
            addBooleanClauses(field, namedMustNot, mustNot, depth + 1);

            // Keep the original tuple/object representation for compatibility.
            JsonNode clauses = booleanNode.get("queries");
            if (clauses != null && !clauses.isNull()) {
                if (!clauses.isArray()) {
                    throw new IllegalArgumentException(
                            "Boolean 'queries' must be an array: " + clauses);
                }
                for (JsonNode clause : clauses) {
                    String occur;
                    JsonNode child;
                    if (clause.isArray()) {
                        if (clause.size() != 2 || !clause.get(0).isTextual()) {
                            throw new IllegalArgumentException(
                                    "Boolean tuple clause must be [occur, query]: " + clause);
                        }
                        occur = clause.get(0).textValue();
                        child = clause.get(1);
                    } else {
                        JsonNode clauseObject = requireObject(clause, "boolean clause");
                        requireOnlyFields(clauseObject, "boolean clause", "occur", "query");
                        JsonNode occurNode = clauseObject.get("occur");
                        if (occurNode != null && !occurNode.isTextual()) {
                            throw new IllegalArgumentException(
                                    "Boolean clause 'occur' must be a string: " + occurNode);
                        }
                        occur = occurNode == null ? "Should" : occurNode.textValue();
                        child = required(clauseObject, "query");
                    }
                    FullTextQuerySpec spec = parseSpec(field, child, depth + 1);
                    String normalizedOccur =
                            occur.replace("_", "").replace("-", "").toLowerCase(Locale.ROOT);
                    if ("must".equals(normalizedOccur)) {
                        must.add(spec);
                    } else if ("mustnot".equals(normalizedOccur)) {
                        mustNot.add(spec);
                    } else if ("should".equals(normalizedOccur)) {
                        should.add(spec);
                    } else {
                        throw new IllegalArgumentException(
                                "Boolean occur must be MUST, SHOULD, or MUST_NOT; got: " + occur);
                    }
                }
            }
            return new FullTextQuerySpec.Bool(must, should, mustNot);
        }
        throw new UnsupportedOperationException("Unsupported full-text query JSON: " + q);
    }

    private static void addBooleanClauses(
            String field, JsonNode node, List<FullTextQuerySpec> destination, int depth) {
        if (node == null || node.isNull()) {
            return;
        }
        if (node.isArray()) {
            for (JsonNode child : node) {
                destination.add(parseSpec(field, child, depth));
            }
        } else {
            destination.add(parseSpec(field, node, depth));
        }
    }

    private static String queryText(JsonNode node) {
        if (node.has("query") && node.has("terms")) {
            throw new IllegalArgumentException(
                    "Full-text query cannot contain both 'query' and 'terms': " + node);
        }
        JsonNode text = node.has("query") ? node.get("query") : node.get("terms");
        if (text == null || !text.isTextual()) {
            throw new IllegalArgumentException(
                    "Full-text query requires a string 'query' or 'terms': " + node);
        }
        return text.textValue();
    }

    /**
     * Reads the ES-style match params ({@code operator} / {@code boost} / {@code fuzziness} /
     * {@code max_expansions} / {@code prefix_length}) from a match node into {@link
     * FullTextParams}; the eslib searcher applies them to the Lucene query. Absent params keep
     * their defaults.
     */
    private static FullTextParams parseParams(JsonNode match) {
        FullTextParams.Operator operator = FullTextParams.Operator.OR;
        if (match.has("operator")) {
            String value = match.get("operator").asText();
            if ("and".equalsIgnoreCase(value)) {
                operator = FullTextParams.Operator.AND;
            } else if (!"or".equalsIgnoreCase(value)) {
                throw new IllegalArgumentException(
                        "Full-text match operator must be AND or OR; got: " + value);
            }
        }
        float boost = match.has("boost") ? floatValue(match.get("boost"), "boost") : 1.0f;
        int fuzziness = 0;
        if (match.has("fuzziness")) {
            JsonNode value = match.get("fuzziness");
            if (value.isTextual() && "auto".equalsIgnoreCase(value.asText())) {
                fuzziness = FullTextParams.AUTO_FUZZINESS;
            } else if (value.isTextual()) {
                try {
                    fuzziness = Integer.parseInt(value.asText());
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(
                            "fuzziness must be 0, 1, 2, or AUTO; got: " + value.asText(), e);
                }
            } else {
                fuzziness = intValue(value, "fuzziness");
            }
        }
        int maxExpansions = intParam(match, "max_expansions", "maxExpansions", 50);
        int prefixLength = intParam(match, "prefix_length", "prefixLength", 0);
        return new FullTextParams(operator, boost, fuzziness, maxExpansions, prefixLength);
    }

    private static int intParam(
            JsonNode node, String snakeCaseName, String camelCaseName, int defaultValue) {
        rejectAliasPair(node, snakeCaseName, camelCaseName);
        if (node.has(snakeCaseName)) {
            return intValue(node.get(snakeCaseName), snakeCaseName);
        }
        return node.has(camelCaseName)
                ? intValue(node.get(camelCaseName), camelCaseName)
                : defaultValue;
    }

    private static int present(JsonNode node, String field) {
        return node.has(field) ? 1 : 0;
    }

    private static JsonNode requireObject(JsonNode node, String description) {
        if (node == null || !node.isObject()) {
            throw new IllegalArgumentException(description + " must be a JSON object: " + node);
        }
        return node;
    }

    private static void requireOnlyFields(
            JsonNode node, String description, String... allowedFields) {
        Set<String> allowed = new HashSet<>(Arrays.asList(allowedFields));
        Iterator<String> fields = node.fieldNames();
        while (fields.hasNext()) {
            String field = fields.next();
            if (!allowed.contains(field)) {
                throw new IllegalArgumentException(
                        "Unknown field '" + field + "' in " + description + ": " + node);
            }
        }
    }

    private static void rejectAliasPair(JsonNode node, String first, String second) {
        if (node.has(first) && node.has(second)) {
            throw new IllegalArgumentException(
                    "Full-text query cannot contain both '"
                            + first
                            + "' and '"
                            + second
                            + "': "
                            + node);
        }
    }

    private static JsonNode required(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) {
            throw new IllegalArgumentException("Missing full-text query field: " + field);
        }
        return value;
    }

    private static int intValue(JsonNode value, String name) {
        if (value != null && value.isIntegralNumber() && value.canConvertToInt()) {
            return value.intValue();
        }
        if (value != null && value.isTextual()) {
            try {
                return Integer.parseInt(value.textValue());
            } catch (NumberFormatException ignored) {
                // Report the same parameter-specific error below.
            }
        }
        throw new IllegalArgumentException(name + " must be an integer; got: " + value);
    }

    private static float floatValue(JsonNode value, String name) {
        if (value == null || (!value.isNumber() && !value.isTextual())) {
            throw new IllegalArgumentException(name + " must be a number; got: " + value);
        }
        try {
            return Float.parseFloat(value.asText());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(name + " must be a number; got: " + value, e);
        }
    }

    /**
     * Run {@code body} on Paimon's caller-owned query executor when one is available, otherwise
     * execute it inline.
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
            try {
                return CompletableFuture.supplyAsync(guardedBody, queryExecutor);
            } catch (Throwable failure) {
                CompletableFuture<T> future = new CompletableFuture<>();
                future.completeExceptionally(failure);
                return future;
            }
        }
        CompletableFuture<T> future = new CompletableFuture<>();
        try {
            future.complete(guardedBody.get());
        } catch (Throwable failure) {
            future.completeExceptionally(failure);
        }
        return future;
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
            if (parsed <= 0) {
                throw new IllegalArgumentException(
                        "Vector search option '" + key + "' must be positive; got: " + value);
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Vector search option '" + key + "' must be an integer; got: " + value, e);
        }
    }

    private Optional<ScoredGlobalIndexResult> toScoredResult(SearchResult result, int limit) {
        if (result == null || result.count == 0) {
            return Optional.empty();
        }

        int count = Math.min(result.count, limit);
        RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
        Map<Long, Float> scoreMap = new HashMap<>(count);
        for (int i = 0; i < count; i++) {
            long id = result.ids[i];
            bitmap.add(id);
            scoreMap.put(id, result.scores[i]);
        }

        return Optional.of(ScoredGlobalIndexResult.create(bitmap, scoreMap::get));
    }

    private Optional<ScoredGlobalIndexResult> toVectorScoredResult(
            SearchResult result, int limit, String metric) {
        if (result == null || result.count == 0) {
            return Optional.empty();
        }

        int count = Math.min(result.count, limit);
        RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
        Map<Long, Float> scoreMap = new HashMap<>(count);
        for (int i = 0; i < count; i++) {
            long id = result.ids[i];
            bitmap.add(id);
            scoreMap.put(id, toPaimonVectorScore(result.scores[i], metric));
        }

        return Optional.of(ScoredGlobalIndexResult.create(bitmap, scoreMap::get));
    }

    /** Converts Lucene's metric-specific score into Paimon's exact vector-search score. */
    static float toPaimonVectorScore(float luceneScore, String metric) {
        if (!Float.isFinite(luceneScore)) {
            throw new IllegalArgumentException(
                    "Non-finite Lucene vector score for metric '" + metric + "': " + luceneScore);
        }
        String normalized = metric == null ? "l2" : metric.toLowerCase(Locale.ROOT);
        switch (normalized) {
            case "l2":
            case "euclidean":
                return luceneScore;
            case "cosine":
            case "dot_product":
            case "dp":
                return 2.0f * luceneScore - 1.0f;
            case "inner_product":
            case "mip":
            case "maximum_inner_product":
                if (luceneScore <= 0.0f) {
                    throw new IllegalArgumentException(
                            "Lucene maximum-inner-product score must be positive; got: "
                                    + luceneScore);
                }
                return luceneScore < 1.0f ? 1.0f - 1.0f / luceneScore : luceneScore - 1.0f;
            default:
                throw new IllegalArgumentException("Unknown ESLib vector metric: " + metric);
        }
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
                candidateSearcher.load(dataProvider, fileOffsets, indexOptions.getFieldConfigs());
            } catch (IOException | RuntimeException | Error failure) {
                Throwable cleanupFailure = cleanupFailedLoad(candidateSearcher, dataProvider);
                if (cleanupFailure != null) {
                    addSuppressed(failure, cleanupFailure);
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
        if (inputStream == null) {
            throw new IOException(
                    "Global index file reader returned a null stream for " + meta.filePath());
        }
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
                if (n == 0) {
                    int single = inputStream.read();
                    if (single < 0) {
                        throw new IOException("Unexpected EOF at offset " + (offset + read));
                    }
                    buf[read++] = (byte) single;
                } else {
                    read += n;
                }
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

    private Throwable cleanupFailedLoad(
            ESIndexSearcher candidateSearcher, ArchiveDataProvider dataProvider) {
        Throwable failure = null;
        if (candidateSearcher != null) {
            try {
                candidateSearcher.close();
            } catch (Throwable e) {
                failure = e;
            }
        }
        if (dataProvider != null) {
            try {
                dataProvider.close();
            } catch (Throwable e) {
                failure = mergeFailure(failure, e);
            }
        }
        return mergeFailure(failure, closeTrackedProviders());
    }

    private Throwable closeTrackedProviders() {
        List<TrackedArchiveDataProvider> providers;
        synchronized (openProviders) {
            providers = new ArrayList<>(openProviders);
        }

        Throwable failure = null;
        for (TrackedArchiveDataProvider provider : providers) {
            try {
                provider.close();
            } catch (Throwable e) {
                failure = mergeFailure(failure, e);
            }
        }
        return failure;
    }

    private static Throwable mergeFailure(Throwable failure, Throwable next) {
        if (next == null) {
            return failure;
        }
        if (failure == null) {
            return next;
        }
        addSuppressed(failure, next);
        return failure;
    }

    private static void addSuppressed(Throwable failure, Throwable suppressed) {
        if (failure != suppressed) {
            failure.addSuppressed(suppressed);
        }
    }

    private static long[] toArray(RoaringNavigableMap64 bitmap) {
        long cardinality = bitmap.getLongCardinality();
        if (cardinality > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "Candidate row-id bitmap is too large to materialize: " + cardinality);
        }
        long[] arr = new long[(int) cardinality];
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
            // Prevent new queries on the first attempt, but keep cleanup retryable. A seekable
            // stream may fail to close transiently; TrackedArchiveDataProvider then remains in
            // openProviders with its close guard reset so a later close() can retry it.
            closed = true;

            Throwable failure = null;
            ESIndexSearcher currentSearcher = searcher;
            if (currentSearcher != null) {
                try {
                    currentSearcher.close();
                    searcher = null;
                } catch (Throwable e) {
                    failure = e;
                }
            }

            failure = mergeFailure(failure, closeTrackedProviders());
            loaded = false;
            if (failure != null) {
                rethrowCloseFailure(failure);
            }
        } finally {
            writeLock.unlock();
        }
    }

    private static void rethrowCloseFailure(Throwable failure) throws IOException {
        if (failure instanceof IOException) {
            throw (IOException) failure;
        }
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        throw new IOException("Failed to close ES index reader", failure);
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

    private Optional<GlobalIndexResult> conservativeAllRows(String logicalField) {
        if (physicalField(logicalField) == null) {
            return Optional.empty();
        }
        try {
            ensureLoaded();
            RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
            for (long id : searcher.allRowIds()) {
                bitmap.add(id);
            }
            return Optional.of(GlobalIndexResult.create(bitmap, false));
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to read conservative ES index rows for field: " + logicalField, e);
        }
    }

    private boolean canEvaluateFilter(FieldRef fieldRef, IndexFilter filter) {
        String physicalField = physicalField(fieldRef.name());
        FieldIndexConfig config = logicalFieldConfig(fieldRef.name());
        if (physicalField == null || config == null) {
            return false;
        }
        if (filter.filterType() == IndexFilter.FilterType.EXISTS
                && fieldRef.type() instanceof ArrayType) {
            return hasPersistedFieldConfigs
                    && indexOptions.arrayPresenceField(physicalField) != null;
        }
        if (filter.filterType() != IndexFilter.FilterType.EXISTS
                && losesTimestampPrecision(fieldRef.type())) {
            return false;
        }
        switch (config.indexType()) {
            case FULLTEXT:
                return indexOptions.keywordSubField(physicalField) != null
                        && !isKeywordRangeFilter(filter);
            case KEYWORD:
                return !isKeywordRangeFilter(filter);
            case SCALAR:
            case DATE:
                return filter.filterType() == IndexFilter.FilterType.SCALAR
                        || filter.filterType() == IndexFilter.FilterType.EXISTS;
            case VECTOR:
                return filter.filterType() == IndexFilter.FilterType.EXISTS;
            default:
                return false;
        }
    }

    private Optional<GlobalIndexResult> dispatchFilter(FieldRef fieldRef, IndexFilter filter) {
        String physicalField = physicalField(fieldRef.name());
        FieldIndexConfig config = logicalFieldConfig(fieldRef.name());
        if (!canEvaluateFilter(fieldRef, filter)) {
            return conservativeAllRows(fieldRef.name());
        }
        if (filter.filterType() == IndexFilter.FilterType.EXISTS
                && fieldRef.type() instanceof ArrayType) {
            String presenceField = indexOptions.arrayPresenceField(physicalField);
            try {
                return executeFilter(presenceField, filter);
            } catch (UnsupportedOperationException | IllegalArgumentException e) {
                return conservativeAllRows(fieldRef.name());
            }
        }
        if (config.indexType() == FieldIndexConfig.IndexType.FULLTEXT) {
            // A FULLTEXT field is indexed as analyzer-produced tokens, which are not equivalent to
            // the raw column value, so ordinary predicates (=, <>, <, >, IN, LIKE, IS NULL, ...)
            // cannot run on it directly. Route them to the keyword multi-field sub-field
            // (content.keyword) when present — it holds the exact value and serves these correctly.
            // Without the sub-field, conservatively retain every row so the caller can evaluate
            // the predicate. Full-text search (visitFullTextSearch) still targets the analyzed
            // primary field.
            String subField = indexOptions.keywordSubField(physicalField);
            try {
                return executeFilter(subField, filter);
            } catch (UnsupportedOperationException | IllegalArgumentException e) {
                return conservativeAllRows(fieldRef.name());
            }
        }
        try {
            return executeFilter(physicalField, filter);
        } catch (UnsupportedOperationException | IllegalArgumentException e) {
            return conservativeAllRows(fieldRef.name());
        }
    }

    private static boolean losesTimestampPrecision(DataType type) {
        if (type instanceof TimestampType) {
            return ((TimestampType) type).getPrecision() > 3;
        }
        return type instanceof LocalZonedTimestampType
                && ((LocalZonedTimestampType) type).getPrecision() > 3;
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
     * Evaluates a SQL negation as exists(field) AND NOT matchingFilter. Keeping the complement at
     * the Paimon layer works for both scalar and keyword fields and preserves SQL null semantics.
     */
    private Optional<GlobalIndexResult> dispatchExistingRowsNotMatching(
            FieldRef fieldRef, IndexFilter matchingFilter) {
        if (!canEvaluateFilter(fieldRef, IndexFilter.exists())
                || !canEvaluateFilter(fieldRef, matchingFilter)) {
            return conservativeAllRows(fieldRef.name());
        }
        Optional<GlobalIndexResult> existing = dispatchFilter(fieldRef, IndexFilter.exists());
        if (existing.isEmpty()) {
            return Optional.empty();
        }
        Optional<GlobalIndexResult> matching = dispatchFilter(fieldRef, matchingFilter);
        if (matching.isEmpty()) {
            return Optional.empty();
        }
        if (!existing.get().isExact() || !matching.get().isExact()) {
            return conservativeAllRows(fieldRef.name());
        }

        RoaringNavigableMap64 result = new RoaringNavigableMap64();
        result.or(existing.get().results());
        result.andNot(matching.get().results());
        return Optional.of(GlobalIndexResult.create(result));
    }

    private IndexFilter exactValueFilter(FieldRef fieldRef, Object literal) {
        FieldIndexConfig config = logicalFieldConfig(fieldRef.name());
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
                    FieldIndexConfig config = logicalFieldConfig(fieldRef.name());
                    if (config == null) {
                        return conservativeAllRows(fieldRef.name());
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
