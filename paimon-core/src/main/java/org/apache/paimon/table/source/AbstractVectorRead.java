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
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.globalindex.OffsetGlobalIndexReader;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.globalindex.VectorGlobalIndexer;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.io.DataFileMeta;
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
    private final PartitionPredicate partitionFilter;
    private final Predicate filter;
    protected final int limit;
    protected final DataField vectorColumn;
    protected final Map<String, String> options;

    protected AbstractVectorRead(
            FileStoreTable table,
            Predicate filter,
            int limit,
            DataField vectorColumn,
            Map<String, String> options) {
        this(table, null, filter, limit, vectorColumn, options);
    }

    protected AbstractVectorRead(
            FileStoreTable table,
            PartitionPredicate partitionFilter,
            Predicate filter,
            int limit,
            DataField vectorColumn,
            Map<String, String> options) {
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

    protected GlobalIndexer createGlobalIndexer(List<VectorSearchSplit> splits) {
        IndexFileMeta firstFile = firstVectorIndexFile(splits);
        String indexType = firstFile.indexType();
        GlobalIndexMeta firstMeta = checkNotNull(firstFile.globalIndexMeta());
        if (firstMeta.extraFieldIds() != null) {
            return GlobalIndexerFactoryUtils.load(indexType)
                    .create(
                            firstMeta.getIndexField(table.rowType()),
                            firstMeta.getExtraFields(table.rowType()),
                            table.coreOptions().toConfiguration());
        }
        return GlobalIndexerFactoryUtils.load(indexType)
                .create(vectorColumn, table.coreOptions().toConfiguration());
    }

    protected Optional<RoaringNavigableMap64> preFilter(List<VectorSearchSplit> splits) {
        Set<IndexFileMeta> scalarIndexFiles =
                new TreeSet<>(Comparator.comparing(IndexFileMeta::fileName));
        for (VectorSearchSplit split : splits) {
            scalarIndexFiles.addAll(split.scalarIndexFiles());
        }

        Optional<GlobalIndexScanner> optionalScanner =
                GlobalIndexScanner.create(table, scalarIndexFiles);
        if (!optionalScanner.isPresent()) {
            return Optional.empty();
        }
        try (GlobalIndexScanner scanner = optionalScanner.get()) {
            return scanner.scan(filter).map(GlobalIndexResult::results);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected CompletableFuture<Optional<ScoredGlobalIndexResult>> eval(
            GlobalIndexer globalIndexer,
            IndexPathFactory indexPathFactory,
            long rowRangeStart,
            long rowRangeEnd,
            List<IndexFileMeta> vectorIndexFiles,
            float[] vector,
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
                new VectorSearch(vector, limit, vectorColumn.name(), options)
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
                new BatchVectorSearch(vectors, limit, vectorColumn.name(), options)
                        .withIncludeRowIds(includeRowIds);
        return new OffsetGlobalIndexReader(reader, rowRangeStart, rowRangeEnd)
                .visitBatchVectorSearch(batchVectorSearch)
                .whenComplete((r, t) -> IOUtils.closeQuietly(reader));
    }

    protected boolean slowSearchEnabled() {
        return !fastSearch();
    }

    protected boolean fastSearch() {
        return table.coreOptions().globalIndexFastSearch();
    }

    protected ScoredGlobalIndexResult withSlowSearch(
            ScoredGlobalIndexResult result,
            List<VectorSearchSplit> splits,
            @Nullable GlobalIndexer globalIndexer,
            float[] queryVector) {
        if (!slowSearchEnabled()) {
            return result.topK(limit);
        }

        ScoredGlobalIndexResult rawResult =
                readSlowSearch(splits, slowSearchMetric(globalIndexer), queryVector);
        return result.or(rawResult).topK(limit);
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

    private ScoredGlobalIndexResult readSlowSearch(
            List<VectorSearchSplit> splits, String metric, float[] queryVector) {
        RowType readType = SpecialFields.rowTypeWithRowId(table.rowType());
        ReadBuilder rangeDiscoveryBuilder = newRawReadBuilder(readType, false);
        TableScan.Plan allDataPlan = rangeDiscoveryBuilder.newScan().plan();
        List<Range> nonIndexedRanges = nonIndexedRanges(allDataPlan, splits);
        if (nonIndexedRanges.isEmpty()) {
            return ScoredGlobalIndexResult.createEmpty();
        }

        TableScan.Plan plan =
                newRawReadBuilder(readType, false).withRowRanges(nonIndexedRanges).newScan().plan();
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
            throw new RuntimeException("Failed to read raw vectors for vector slow search.", e);
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

    private List<Range> nonIndexedRanges(
            TableScan.Plan allDataPlan, List<VectorSearchSplit> splits) {
        List<Range> dataRanges = new ArrayList<>();
        for (Split split : allDataPlan.splits()) {
            if (split instanceof IndexedSplit) {
                dataRanges.addAll(((IndexedSplit) split).rowRanges());
                continue;
            }
            if (!(split instanceof DataSplit)) {
                continue;
            }
            for (DataFileMeta file : ((DataSplit) split).dataFiles()) {
                if (file.firstRowId() != null) {
                    dataRanges.add(file.nonNullRowIdRange());
                }
            }
        }

        List<Range> indexedRanges = new ArrayList<>();
        for (VectorSearchSplit split : splits) {
            indexedRanges.add(new Range(split.rowRangeStart(), split.rowRangeEnd()));
        }
        indexedRanges = Range.sortAndMergeOverlap(indexedRanges, true);

        List<Range> ranges = new ArrayList<>();
        for (Range dataRange : Range.sortAndMergeOverlap(dataRanges, true)) {
            ranges.addAll(dataRange.exclude(indexedRanges));
        }
        return Range.sortAndMergeOverlap(ranges, true);
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

    private String slowSearchMetric(@Nullable GlobalIndexer globalIndexer) {
        String metric = null;
        if (globalIndexer != null) {
            if (!(globalIndexer instanceof VectorGlobalIndexer)) {
                throw new IllegalArgumentException(
                        "Index type '"
                                + globalIndexer.getClass().getName()
                                + "' does not provide vector metric for slow search.");
            }
            metric = ((VectorGlobalIndexer) globalIndexer).metric();
        }
        if (metric == null) {
            return "l2";
        }
        return metric.toLowerCase().replace('-', '_');
    }

    private static IndexFileMeta firstVectorIndexFile(List<VectorSearchSplit> splits) {
        for (VectorSearchSplit split : splits) {
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
