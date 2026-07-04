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

package org.apache.paimon.table.source;

import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalVector;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.GlobalIndexScanner;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.GlobalIndexerFactoryUtils;
import org.apache.paimon.globalindex.OffsetGlobalIndexReader;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.globalindex.VectorGlobalIndexer;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.BatchVectorSearch;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Base implementation for vector reads. */
public abstract class AbstractVectorRead implements Serializable {

    private static final long serialVersionUID = 1L;

    protected final FileStoreTable table;
    @Nullable private final PartitionPredicate partitionFilter;
    @Nullable private final Predicate filter;
    protected final int limit;
    protected final DataField vectorColumn;
    protected final Map<String, String> options;

    protected AbstractVectorRead(
            FileStoreTable table,
            @Nullable PartitionPredicate partitionFilter,
            @Nullable Predicate filter,
            int limit,
            DataField vectorColumn,
            @Nullable Map<String, String> options) {
        this.table = table;
        this.partitionFilter = partitionFilter;
        this.filter = filter;
        this.limit = limit;
        this.vectorColumn = vectorColumn;
        this.options =
                options == null
                        ? Collections.emptyMap()
                        : Collections.unmodifiableMap(new HashMap<>(options));
    }

    protected GlobalIndexer createGlobalIndexer(List<IndexVectorSearchSplit> splits) {
        IndexFileMeta firstFile = firstVectorIndexFile(splits);
        GlobalIndexMeta firstMeta = checkNotNull(firstFile.globalIndexMeta());
        return createGlobalIndexer(firstFile.indexType(), firstMeta);
    }

    protected GlobalIndexer createGlobalIndexer(String indexType) {
        return GlobalIndexerFactoryUtils.load(indexType)
                .create(vectorColumn, table.coreOptions().toConfiguration());
    }

    private GlobalIndexer createGlobalIndexer(String indexType, GlobalIndexMeta meta) {
        if (meta.extraFieldIds() == null) {
            return createGlobalIndexer(indexType);
        }
        return GlobalIndexerFactoryUtils.load(indexType)
                .create(
                        meta.getIndexField(table.rowType()),
                        meta.getExtraFields(table.rowType()),
                        table.coreOptions().toConfiguration());
    }

    protected List<RoaringNavigableMap64> preFilters(List<IndexVectorSearchSplit> splits) {
        RoaringNavigableMap64 liveRows = GlobalIndexLiveRowFilter.liveRows(table, partitionFilter);
        RoaringNavigableMap64 matchedRows = scalarMatchedRows(splits);

        List<RoaringNavigableMap64> includeRowIds = new ArrayList<>(splits.size());
        boolean hasFilter = false;
        for (IndexVectorSearchSplit split : splits) {
            Range splitRange = new Range(split.rowRangeStart(), split.rowRangeEnd());
            RoaringNavigableMap64 splitRows = bitmapOf(splitRange);

            RoaringNavigableMap64 include = new RoaringNavigableMap64();
            include.or(splitRows);
            if (liveRows != null) {
                include.and(liveRows);
            }
            if (matchedRows != null) {
                include.and(matchedRows);
            }

            if (include.getLongCardinality() == splitRange.count()) {
                includeRowIds.add(null);
            } else {
                includeRowIds.add(include);
                hasFilter = true;
            }
        }
        return hasFilter ? includeRowIds : Collections.emptyList();
    }

    @Nullable
    private RoaringNavigableMap64 scalarMatchedRows(List<IndexVectorSearchSplit> splits) {
        if (filter == null) {
            return null;
        }

        Set<IndexFileMeta> scalarIndexFiles =
                new TreeSet<>(Comparator.comparing(IndexFileMeta::fileName));
        for (IndexVectorSearchSplit split : splits) {
            scalarIndexFiles.addAll(split.scalarIndexFiles());
        }

        Optional<GlobalIndexScanner> optionalScanner =
                GlobalIndexScanner.create(table, partitionFilter, scalarIndexFiles);
        if (!optionalScanner.isPresent()) {
            return new RoaringNavigableMap64();
        }

        try (GlobalIndexScanner scanner = optionalScanner.get()) {
            Optional<GlobalIndexResult> result = scanner.scan(filter);
            if (!result.isPresent()) {
                return new RoaringNavigableMap64();
            }
            return result.get().results();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Nullable
    protected RoaringNavigableMap64 rawPreFilter(List<RawVectorSearchSplit> splits) {
        if (filter == null) {
            return null;
        }

        RoaringNavigableMap64 rawRows = bitmapOf(rawRowRanges(splits));
        if (rawRows.isEmpty()) {
            return null;
        }

        Set<IndexFileMeta> scalarIndexFiles =
                new TreeSet<>(Comparator.comparing(IndexFileMeta::fileName));
        for (RawVectorSearchSplit split : splits) {
            scalarIndexFiles.addAll(split.scalarIndexFiles());
        }
        Optional<GlobalIndexScanner> optionalScanner =
                GlobalIndexScanner.create(table, partitionFilter, scalarIndexFiles);
        if (!optionalScanner.isPresent()) {
            return null;
        }

        RoaringNavigableMap64 include = new RoaringNavigableMap64();
        try (GlobalIndexScanner scanner = optionalScanner.get()) {
            Optional<GlobalIndexResult> result = scanner.scan(filter);
            if (!result.isPresent()) {
                return null;
            }
            include.or(result.get().results());
            include.or(scanner.unindexedRows(filter).results());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        include.and(rawRows);
        return include;
    }

    protected CompletableFuture<Optional<ScoredGlobalIndexResult>> eval(
            GlobalIndexer globalIndexer,
            IndexPathFactory indexPathFactory,
            long rowRangeStart,
            long rowRangeEnd,
            List<IndexFileMeta> vectorIndexFiles,
            float[] vector,
            int searchLimit,
            @Nullable RoaringNavigableMap64 includeRowIds,
            ExecutorService executor) {
        if (vectorIndexFiles.isEmpty()) {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        List<GlobalIndexIOMeta> indexIOMetaList =
                buildIOMetaList(indexPathFactory, vectorIndexFiles);
        @SuppressWarnings("resource")
        FileIO fileIO = table.fileIO();
        GlobalIndexFileReader indexFileReader = m -> fileIO.newInputStream(m.filePath());
        GlobalIndexReader reader =
                globalIndexer.createReader(indexFileReader, indexIOMetaList, executor);
        VectorSearch vectorSearch =
                new VectorSearch(vector, searchLimit, vectorColumn.name(), options)
                        .withIncludeRowIds(includeRowIds);
        return new OffsetGlobalIndexReader(reader, rowRangeStart, rowRangeEnd)
                .visitVectorSearch(vectorSearch)
                .whenComplete((r, t) -> IOUtils.closeQuietly(reader));
    }

    protected CompletableFuture<List<Optional<ScoredGlobalIndexResult>>> evalBatch(
            GlobalIndexer globalIndexer,
            IndexPathFactory indexPathFactory,
            long rowRangeStart,
            long rowRangeEnd,
            List<IndexFileMeta> vectorIndexFiles,
            float[][] vectors,
            int searchLimit,
            @Nullable RoaringNavigableMap64 includeRowIds,
            ExecutorService executor) {
        if (vectorIndexFiles.isEmpty()) {
            return CompletableFuture.completedFuture(emptyOptionalResults(vectors.length));
        }

        List<GlobalIndexIOMeta> indexIOMetaList =
                buildIOMetaList(indexPathFactory, vectorIndexFiles);
        @SuppressWarnings("resource")
        FileIO fileIO = table.fileIO();
        GlobalIndexFileReader indexFileReader = m -> fileIO.newInputStream(m.filePath());
        GlobalIndexReader reader =
                globalIndexer.createReader(indexFileReader, indexIOMetaList, executor);
        BatchVectorSearch batchVectorSearch =
                new BatchVectorSearch(vectors, searchLimit, vectorColumn.name(), options)
                        .withIncludeRowIds(includeRowIds);
        return new OffsetGlobalIndexReader(reader, rowRangeStart, rowRangeEnd)
                .visitBatchVectorSearch(batchVectorSearch)
                .whenComplete((r, t) -> IOUtils.closeQuietly(reader));
    }

    protected ScoredGlobalIndexResult withRawSearch(
            ScoredGlobalIndexResult result,
            List<RawVectorSearchSplit> rawSplits,
            @Nullable GlobalIndexer globalIndexer,
            @Nullable RoaringNavigableMap64 preFilter,
            float[] queryVector) {
        List<Range> rawRowRanges = rawRowRanges(rawSplits);
        if (rawRowRanges.isEmpty()) {
            return result.topK(limit);
        }

        ScoredGlobalIndexResult rawResult =
                readRawSearch(
                        rawRowRanges,
                        preFilter,
                        rawSearchIndexer(rawSplits, globalIndexer),
                        queryVector);
        return result.or(rawResult).topK(limit);
    }

    protected int indexedSearchLimit(String indexType) {
        int refineFactor = configuredRefineFactor(indexType);
        if (refineFactor == 0) {
            return limit;
        }
        if (limit > Integer.MAX_VALUE / refineFactor) {
            throw new IllegalArgumentException(
                    String.format(
                            "Vector search limit overflow: limit=%d, refine factor=%d",
                            limit, refineFactor));
        }
        return limit * refineFactor;
    }

    protected ScoredGlobalIndexResult maybeRerankIndexedResult(
            ScoredGlobalIndexResult result,
            String indexType,
            @Nullable GlobalIndexer globalIndexer,
            float[] queryVector) {
        if (configuredRefineFactor(indexType) == 0 || result.results().isEmpty()) {
            return result;
        }
        ScoredGlobalIndexResult candidates = result.topK(indexedSearchLimit(indexType));
        return readRawSearch(
                candidates.results().toRangeList(),
                candidates.results(),
                globalIndexer,
                queryVector);
    }

    protected String vectorIndexType(List<IndexVectorSearchSplit> splits) {
        return firstVectorIndexFile(splits).indexType();
    }

    protected ScoredGlobalIndexResult[] emptyScoredResults(int n) {
        ScoredGlobalIndexResult[] results = new ScoredGlobalIndexResult[n];
        for (int i = 0; i < n; i++) {
            results[i] = ScoredGlobalIndexResult.createEmpty();
        }
        return results;
    }

    private List<GlobalIndexIOMeta> buildIOMetaList(
            IndexPathFactory indexPathFactory, List<IndexFileMeta> vectorIndexFiles) {
        List<GlobalIndexIOMeta> indexIOMetaList = new ArrayList<>();
        for (IndexFileMeta indexFile : vectorIndexFiles) {
            GlobalIndexMeta meta = checkNotNull(indexFile.globalIndexMeta());
            indexIOMetaList.add(
                    new GlobalIndexIOMeta(
                            indexPathFactory.toPath(indexFile),
                            indexFile.fileSize(),
                            meta.indexMeta()));
        }
        return indexIOMetaList;
    }

    private static RoaringNavigableMap64 bitmapOf(Range range) {
        RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
        bitmap.addRange(range);
        return bitmap;
    }

    private static RoaringNavigableMap64 bitmapOf(List<Range> ranges) {
        RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
        for (Range range : ranges) {
            bitmap.addRange(range);
        }
        return bitmap;
    }

    protected ScoredGlobalIndexResult readRawSearch(
            List<Range> rawRowRanges,
            @Nullable RoaringNavigableMap64 preFilter,
            @Nullable GlobalIndexer globalIndexer,
            float[] queryVector) {
        return readRawSearch(rawRowRanges, preFilter, rawSearchMetric(globalIndexer), queryVector);
    }

    protected ScoredGlobalIndexResult readRawSearch(
            List<Range> rawRowRanges,
            @Nullable RoaringNavigableMap64 preFilter,
            String metric,
            float[] queryVector) {
        RowType readType = SpecialFields.rowTypeWithRowId(table.rowType());
        if (preFilter != null) {
            rawRowRanges =
                    Range.and(
                            Range.sortAndMergeOverlap(rawRowRanges, true),
                            Range.sortAndMergeOverlap(preFilter.toRangeList(), true));
        }
        if (rawRowRanges.isEmpty()) {
            return ScoredGlobalIndexResult.createEmpty();
        }

        TableScan.Plan plan =
                newRawReadBuilder(readType, false).withRowRanges(rawRowRanges).newScan().plan();
        ReadBuilder readBuilder = newRawReadBuilder(readType, true);
        RoaringNavigableMap64 resultBitmap = new RoaringNavigableMap64();
        Map<Long, Float> scoreMap = new HashMap<>();
        int vectorIndex = readType.getFieldIndex(vectorColumn.name());
        int rowIdIndex = readType.getFieldIndex(SpecialFields.ROW_ID.name());

        try (RecordReader<InternalRow> reader =
                readBuilder.newRead().executeFilter().createReader(plan)) {
            reader.forEachRemaining(
                    row -> {
                        if (row.isNullAt(vectorIndex)) {
                            return;
                        }
                        float[] stored = getVector(row, vectorIndex);
                        if (stored.length != queryVector.length) {
                            throw new IllegalArgumentException(
                                    String.format(
                                            "Query vector dimension mismatch: expected %d, got %d",
                                            stored.length, queryVector.length));
                        }
                        long rowId = row.getLong(rowIdIndex);
                        resultBitmap.add(rowId);
                        scoreMap.put(rowId, computeScore(queryVector, stored, metric));
                    });
        } catch (IOException e) {
            throw new RuntimeException("Failed to read raw vectors for vector search.", e);
        }

        return ScoredGlobalIndexResult.create(resultBitmap, scoreMap::get).topK(limit);
    }

    private ReadBuilder newRawReadBuilder(RowType readType, boolean includeFilter) {
        ReadBuilder readBuilder = table.newReadBuilder().withReadType(readType);
        if (partitionFilter != null) {
            readBuilder.withPartitionFilter(partitionFilter);
        }
        if (includeFilter && filter != null) {
            readBuilder.withFilter(filter);
        }
        return readBuilder;
    }

    protected static void splitSearchSplits(
            List<? extends VectorSearchSplit> splits,
            List<IndexVectorSearchSplit> indexSplits,
            List<RawVectorSearchSplit> rawSplits) {
        for (VectorSearchSplit split : splits) {
            if (split instanceof IndexVectorSearchSplit) {
                indexSplits.add((IndexVectorSearchSplit) split);
            } else if (split instanceof RawVectorSearchSplit) {
                rawSplits.add((RawVectorSearchSplit) split);
            }
        }
    }

    protected static List<Range> rawRowRanges(List<RawVectorSearchSplit> rawSplits) {
        List<Range> rawRowRanges = new ArrayList<>();
        for (RawVectorSearchSplit split : rawSplits) {
            rawRowRanges.addAll(split.rowRanges());
        }
        return Range.sortAndMergeOverlap(rawRowRanges, true);
    }

    @Nullable
    protected GlobalIndexer rawSearchIndexer(
            List<RawVectorSearchSplit> rawSplits, @Nullable GlobalIndexer globalIndexer) {
        if (globalIndexer != null) {
            return globalIndexer;
        }
        for (RawVectorSearchSplit split : rawSplits) {
            String indexType = split.indexType();
            if (indexType != null) {
                return createGlobalIndexer(indexType);
            }
        }
        return null;
    }

    private float[] getVector(InternalRow row, int vectorIndex) {
        if (vectorColumn.type().getTypeRoot() == DataTypeRoot.VECTOR) {
            InternalVector vector = row.getVector(vectorIndex);
            return vector.toFloatArray();
        } else if (vectorColumn.type().getTypeRoot() == DataTypeRoot.ARRAY) {
            InternalArray array = row.getArray(vectorIndex);
            return array.toFloatArray();
        }
        throw new IllegalArgumentException(
                "Unsupported vector column type: " + vectorColumn.type());
    }

    protected String rawSearchMetric(@Nullable GlobalIndexer globalIndexer) {
        String metric = null;
        if (globalIndexer != null) {
            if (!(globalIndexer instanceof VectorGlobalIndexer)) {
                throw new IllegalArgumentException(
                        "Index type '"
                                + globalIndexer.getClass().getName()
                                + "' does not provide vector metric for raw search.");
            }
            metric = ((VectorGlobalIndexer) globalIndexer).metric();
        }
        if (metric == null) {
            metric = configuredRawSearchMetric();
        }
        return metric == null ? "l2" : normalizeMetric(metric);
    }

    @Nullable
    private String configuredRawSearchMetric() {
        String metric = configuredRawSearchMetric(options);
        return metric == null ? configuredRawSearchMetric(table.options()) : metric;
    }

    @Nullable
    private String configuredRawSearchMetric(Map<String, String> options) {
        String fieldPrefix = "fields." + vectorColumn.name() + ".";
        String metric = option(options, fieldPrefix + "distance.metric");
        if (metric == null) {
            metric = option(options, fieldPrefix + "metric");
        }
        if (metric == null) {
            metric = option(options, "test.vector.metric");
        }
        if (metric == null) {
            metric = option(options, "lumina.distance.metric");
        }
        if (metric == null) {
            metric = option(options, "distance.metric");
        }
        if (metric == null) {
            metric = option(options, "metric");
        }
        if (metric != null) {
            return metric;
        }

        for (Map.Entry<String, String> entry : options.entrySet()) {
            String key = entry.getKey();
            if (key.endsWith(".distance.metric") || key.endsWith(".metric")) {
                String value = normalizeMetric(entry.getValue());
                if (isRawSearchMetric(value)) {
                    if (metric != null && !metric.equals(value)) {
                        return null;
                    }
                    metric = value;
                }
            }
        }
        return metric;
    }

    @Nullable
    private static String option(Map<String, String> options, String key) {
        String value = options.get(key);
        return value == null ? null : normalizeMetric(value);
    }

    private static boolean isRawSearchMetric(String metric) {
        return "l2".equals(metric) || "cosine".equals(metric) || "inner_product".equals(metric);
    }

    private static String normalizeMetric(String metric) {
        return metric.toLowerCase().replace('-', '_');
    }

    private int configuredRefineFactor(String indexType) {
        String value = configuredRefineFactor(options, indexType);
        if (value == null) {
            value = configuredRefineFactor(table.options(), indexType);
        }
        if (value == null) {
            return 0;
        }
        try {
            int factor = Integer.parseInt(value);
            if (factor <= 0) {
                throw new IllegalArgumentException(
                        "Vector refine factor must be positive, got: " + value);
            }
            return factor;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Invalid vector refine factor: " + value + ". Must be an integer.", e);
        }
    }

    @Nullable
    private String configuredRefineFactor(Map<String, String> options, String indexType) {
        List<String> prefixes = new ArrayList<>();
        String fieldPrefix = "fields." + vectorColumn.name() + ".";
        addRefinePrefixes(prefixes, fieldPrefix, indexType);
        addRefinePrefixes(prefixes, "", indexType);

        for (String prefix : prefixes) {
            String value = refineFactorOption(options, prefix + "refine_factor");
            if (value == null) {
                value = refineFactorOption(options, prefix + "refine-factor");
            }
            if (value == null) {
                value = refineFactorOption(options, prefix + "rerank_factor");
            }
            if (value == null) {
                value = refineFactorOption(options, prefix + "rerank-factor");
            }
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    private static void addRefinePrefixes(List<String> prefixes, String base, String indexType) {
        if (indexType != null && !indexType.isEmpty()) {
            prefixes.add(base + indexType + ".");
            String normalizedIndexType = normalizeIndexType(indexType);
            if (!normalizedIndexType.equals(indexType)) {
                prefixes.add(base + normalizedIndexType + ".");
            }
            if (normalizedIndexType.startsWith("ivf")) {
                prefixes.add(base + "ivf.");
            }
        }
        prefixes.add(base);
    }

    @Nullable
    private static String refineFactorOption(Map<String, String> options, String key) {
        String value = options.get(key);
        return value == null ? null : value.trim();
    }

    private static String normalizeIndexType(String indexType) {
        return indexType.toLowerCase().replace('-', '_');
    }

    private static IndexFileMeta firstVectorIndexFile(List<IndexVectorSearchSplit> splits) {
        for (IndexVectorSearchSplit split : splits) {
            if (!split.vectorIndexFiles().isEmpty()) {
                return split.vectorIndexFiles().get(0);
            }
        }
        throw new IllegalArgumentException("No vector index files found.");
    }

    private static float computeScore(float[] query, float[] stored, String metric) {
        if ("l2".equals(metric)) {
            float sumSq = 0;
            for (int i = 0; i < query.length; i++) {
                float diff = query[i] - stored[i];
                sumSq += diff * diff;
            }
            return 1.0f / (1.0f + sumSq);
        } else if ("cosine".equals(metric)) {
            float dot = 0;
            float normA = 0;
            float normB = 0;
            for (int i = 0; i < query.length; i++) {
                dot += query[i] * stored[i];
                normA += query[i] * query[i];
                normB += stored[i] * stored[i];
            }
            float denominator = (float) (Math.sqrt(normA) * Math.sqrt(normB));
            return denominator == 0 ? 0 : dot / denominator;
        } else if ("inner_product".equals(metric)) {
            float dot = 0;
            for (int i = 0; i < query.length; i++) {
                dot += query[i] * stored[i];
            }
            return dot;
        }
        throw new IllegalArgumentException("Unknown vector search metric: " + metric);
    }

    private static List<Optional<ScoredGlobalIndexResult>> emptyOptionalResults(int n) {
        List<Optional<ScoredGlobalIndexResult>> results = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            results.add(Optional.empty());
        }
        return results;
    }
}
