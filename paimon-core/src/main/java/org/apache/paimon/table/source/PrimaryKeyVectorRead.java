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

import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.globalindex.GlobalIndexReadThreadPool;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.globalindex.VectorSearchMetric;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourcePolicy;
import org.apache.paimon.index.pkvector.PkVectorAnnSegmentSearcher;
import org.apache.paimon.index.pkvector.PkVectorBucketIndexState;
import org.apache.paimon.index.pkvector.PkVectorDataFileReader;
import org.apache.paimon.index.pkvector.PkVectorSearchResult;
import org.apache.paimon.index.pkvector.PrimaryKeyVectorBucketSearch;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateVisitor;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.ScoreRecordIterator;
import org.apache.paimon.reader.ScoreRecordReader;
import org.apache.paimon.table.DelegatedFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VectorType;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.GLOBAL_INDEX_THREAD_NUM;
import static org.apache.paimon.globalindex.VectorSearchMetric.normalize;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Executes bucket-local primary-key vector search and merges physical candidates globally. */
public class PrimaryKeyVectorRead implements VectorRead, Serializable {

    private static final long serialVersionUID = 1L;

    private static final Comparator<Candidate> BEST_FIRST =
            (left, right) -> {
                int distance = Float.compare(left.distance, right.distance);
                if (distance != 0) {
                    return distance;
                }
                int partition = compareBytes(left.partition.toBytes(), right.partition.toBytes());
                if (partition != 0) {
                    return partition;
                }
                int bucket = Integer.compare(left.bucket, right.bucket);
                if (bucket != 0) {
                    return bucket;
                }
                int fileName = left.dataFileName.compareTo(right.dataFileName);
                return fileName != 0 ? fileName : Long.compare(left.rowPosition, right.rowPosition);
            };

    protected final FileStoreTable table;
    protected final DataField vectorField;
    private final String indexType;
    private final Options indexOptions;
    private final Map<String, String> searchOptions;
    private final float[] query;
    protected final int limit;
    private final String metric;
    private final int refineFactor;
    private final int indexedLimit;
    @Nullable private final Predicate filter;

    public PrimaryKeyVectorRead(
            FileStoreTable table,
            DataField vectorField,
            float[] query,
            int limit,
            Map<String, String> searchOptions) {
        this(table, vectorField, query, limit, searchOptions, null);
    }

    public PrimaryKeyVectorRead(
            FileStoreTable table,
            DataField vectorField,
            float[] query,
            int limit,
            Map<String, String> searchOptions,
            @Nullable Predicate filter) {
        checkArgument(
                vectorField.type() instanceof VectorType, "Vector field must use VECTOR type.");
        checkArgument(
                query.length == ((VectorType) vectorField.type()).getLength(),
                "Query dimension %s does not match vector field dimension %s.",
                query.length,
                ((VectorType) vectorField.type()).getLength());
        checkArgument(limit > 0, "Vector search limit must be positive: %s.", limit);
        this.table = table;
        this.vectorField = vectorField;
        this.indexType = table.coreOptions().primaryKeyVectorIndexType(vectorField.name());
        this.indexOptions = table.coreOptions().primaryKeyVectorIndexOptions(vectorField.name());
        this.searchOptions = Collections.unmodifiableMap(new HashMap<>(searchOptions));
        this.query = query.clone();
        this.limit = limit;
        this.metric =
                normalize(table.coreOptions().primaryKeyVectorDistanceMetric(vectorField.name()));
        this.refineFactor =
                VectorSearchRefineOptions.resolve(
                        this.searchOptions, table.options(), vectorField.name(), indexType);
        this.indexedLimit = VectorSearchRefineOptions.searchLimit(limit, refineFactor);
        this.filter = filter;
    }

    private static KeyValueFileStore keyValueStore(FileStoreTable table) {
        FileStoreTable unwrapped = table;
        while (unwrapped instanceof DelegatedFileStoreTable) {
            unwrapped = ((DelegatedFileStoreTable) unwrapped).wrapped();
        }
        checkArgument(
                unwrapped.store() instanceof KeyValueFileStore,
                "Primary-key vector search requires a key-value file store.");
        return (KeyValueFileStore) unwrapped.store();
    }

    @Override
    public GlobalIndexResult read(VectorScan.Plan plan) {
        PrimaryKeyVectorScan.Plan primaryKeyPlan = primaryKeyPlan(plan);
        return createResult(primaryKeyPlan, searchBuckets(bucketSplits(primaryKeyPlan)));
    }

    protected PrimaryKeyVectorScan.Plan primaryKeyPlan(VectorScan.Plan plan) {
        checkArgument(
                plan instanceof PrimaryKeyVectorScan.Plan,
                "Primary-key vector read requires a PrimaryKeyVectorScan plan.");
        PrimaryKeyVectorScan.Plan primaryKeyPlan = (PrimaryKeyVectorScan.Plan) plan;
        for (VectorSearchSplit searchSplit : primaryKeyPlan.splits()) {
            BucketVectorSearchSplit split = (BucketVectorSearchSplit) searchSplit;
            checkArgument(
                    split.dataSplit().snapshotId() == primaryKeyPlan.snapshotId(),
                    "Vector bucket split snapshot does not match its plan.");
        }
        return primaryKeyPlan;
    }

    protected List<BucketVectorSearchSplit> bucketSplits(PrimaryKeyVectorScan.Plan plan) {
        List<BucketVectorSearchSplit> splits = new ArrayList<>(plan.splits().size());
        for (VectorSearchSplit split : plan.splits()) {
            splits.add((BucketVectorSearchSplit) split);
        }
        return splits;
    }

    protected SearchResult searchBuckets(List<BucketVectorSearchSplit> splits) {
        try {
            SearchContext context = createSearchContext();
            List<CompletableFuture<SearchResult>> futures = new ArrayList<>(splits.size());
            // Start from the caller because each bucket submits leaf searches to the same executor.
            // Wrapping the whole bucket in that executor and waiting inside it can starve leaf
            // work.
            for (BucketVectorSearchSplit split : splits) {
                futures.add(searchAsync(split, context));
            }
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            List<SearchResult> results = new ArrayList<>(futures.size());
            for (CompletableFuture<SearchResult> future : futures) {
                results.add(future.join());
            }
            return mergeSearchResults(results);
        } catch (IOException e) {
            throw new RuntimeException("Failed to search primary-key vector index.", e);
        } catch (CompletionException e) {
            throw new RuntimeException("Failed to search primary-key vector index.", e.getCause());
        }
    }

    protected List<SearchResult> searchBuckets(
            List<BucketVectorSearchSplit> splits, float[][] queries) {
        try {
            SearchContext context = createSearchContext();
            List<CompletableFuture<List<SearchResult>>> futures = new ArrayList<>(splits.size());
            for (BucketVectorSearchSplit split : splits) {
                futures.add(searchBatchAsync(split, context, queries));
            }
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            List<List<SearchResult>> resultsByQuery = new ArrayList<>(queries.length);
            for (int i = 0; i < queries.length; i++) {
                resultsByQuery.add(new ArrayList<>());
            }
            for (CompletableFuture<List<SearchResult>> future : futures) {
                List<SearchResult> splitResults = future.join();
                checkArgument(
                        splitResults.size() == queries.length,
                        "Primary-key vector batch result count does not match query count.");
                for (int i = 0; i < queries.length; i++) {
                    resultsByQuery.get(i).add(splitResults.get(i));
                }
            }
            List<SearchResult> results = new ArrayList<>(queries.length);
            for (List<SearchResult> queryResults : resultsByQuery) {
                results.add(mergeSearchResults(queryResults));
            }
            return Collections.unmodifiableList(results);
        } catch (IOException e) {
            throw new RuntimeException("Failed to search primary-key vector index.", e);
        } catch (CompletionException e) {
            throw new RuntimeException("Failed to search primary-key vector index.", e.getCause());
        }
    }

    SearchContext createSearchContext() {
        return new SearchContext(table);
    }

    protected GlobalIndexResult createResult(
            PrimaryKeyVectorScan.Plan plan, SearchResult searchResult) {
        List<Candidate> indexedCandidates = topK(searchResult.indexedCandidates(), indexedLimit);
        if (refineFactor > 0 && !indexedCandidates.isEmpty()) {
            indexedCandidates = rerank(plan, indexedCandidates);
        } else {
            indexedCandidates = topK(indexedCandidates, limit);
        }
        List<Candidate> candidates = new ArrayList<>(indexedCandidates);
        candidates.addAll(topK(searchResult.exactCandidates(), limit));
        return new PrimaryKeyVectorResult(plan, topK(candidates, limit), metric);
    }

    protected SearchResult mergeSearchResults(List<SearchResult> results) {
        List<Candidate> indexedCandidates = new ArrayList<>();
        List<Candidate> exactCandidates = new ArrayList<>();
        for (SearchResult result : results) {
            indexedCandidates.addAll(result.indexedCandidates());
            exactCandidates.addAll(result.exactCandidates());
        }
        return new SearchResult(
                topK(indexedCandidates, indexedLimit), topK(exactCandidates, limit));
    }

    CompletableFuture<SearchResult> searchAsync(
            BucketVectorSearchSplit split, SearchContext context) throws IOException {
        return searchBatchAsync(split, context, new float[][] {query})
                .thenApply(results -> results.get(0));
    }

    CompletableFuture<List<SearchResult>> searchBatchAsync(
            BucketVectorSearchSplit split, SearchContext context, float[][] queries)
            throws IOException {
        DataSplit dataSplit = split.dataSplit();
        List<DataFileMeta> activeFiles =
                dataSplit.dataFiles().stream()
                        .filter(PrimaryKeyIndexSourcePolicy::shouldRead)
                        .collect(Collectors.toList());
        PkVectorBucketIndexState state =
                PkVectorBucketIndexState.fromActivePayloads(
                        vectorField.id(), indexType, split.payloadFiles());
        Map<String, DeletionVector> deletionVectors = deletionVectors(dataSplit, context.fileIO);
        PkVectorDataFileReader.Factory readerFactory =
                new PkVectorDataFileReader.Factory(
                        context.readerFactoryBuilder,
                        dataSplit.partition(),
                        dataSplit.bucket(),
                        vectorField,
                        ((VectorType) vectorField.type()).getLength());
        PkVectorAnnSegmentSearcher annSearcher =
                new PkVectorAnnSegmentSearcher(
                        context.fileIO,
                        context.indexFileHandler.pkVectorAnnSegment(
                                dataSplit.partition(), dataSplit.bucket()),
                        vectorField,
                        indexOptions,
                        metric,
                        context.executor);
        PrimaryKeyVectorBucketSearch bucketSearch =
                new PrimaryKeyVectorBucketSearch(
                        readerFactory,
                        annSearcher,
                        searchOptions,
                        metric,
                        table.coreOptions().globalIndexSearchMode());
        return bucketSearch
                .searchBatchAsync(
                        state,
                        activeFiles,
                        deletionVectors,
                        rowRangesByFile(split),
                        queries,
                        indexedLimit,
                        limit,
                        context.executor)
                .thenApply(
                        bucketResults -> {
                            List<SearchResult> results = new ArrayList<>(bucketResults.size());
                            for (PrimaryKeyVectorBucketSearch.Result result : bucketResults) {
                                results.add(
                                        new SearchResult(
                                                candidates(dataSplit, result.indexedCandidates()),
                                                candidates(dataSplit, result.exactCandidates())));
                            }
                            return Collections.unmodifiableList(results);
                        });
    }

    private Map<String, List<Range>> rowRangesByFile(BucketVectorSearchSplit split)
            throws IOException {
        Map<String, List<Range>> result = new LinkedHashMap<>(split.rowRangesByFile());
        if (filter == null) {
            return result;
        }

        DataSplit dataSplit = split.dataSplit();
        for (int i = 0; i < dataSplit.dataFiles().size(); i++) {
            DataFileMeta dataFile = dataSplit.dataFiles().get(i);
            result.put(
                    dataFile.fileName(),
                    residualRowRanges(dataSplit, i, result.get(dataFile.fileName())));
        }
        return result;
    }

    private List<Range> residualRowRanges(
            DataSplit source, int fileIndex, @Nullable List<Range> candidateRanges)
            throws IOException {
        DataFileMeta dataFile = source.dataFiles().get(fileIndex);
        if (dataFile.rowCount() == 0) {
            return Collections.emptyList();
        }
        if (candidateRanges != null && candidateRanges.isEmpty()) {
            return Collections.emptyList();
        }

        DataSplit.Builder builder =
                DataSplit.builder()
                        .withSnapshot(source.snapshotId())
                        .withPartition(source.partition())
                        .withBucket(source.bucket())
                        .withBucketPath(source.bucketPath())
                        .withTotalBuckets(source.totalBuckets())
                        .withDataFiles(Collections.singletonList(dataFile))
                        .isStreaming(false)
                        .rawConvertible(false);
        if (source.deletionFiles().isPresent()) {
            builder.withDataDeletionFiles(
                    Collections.singletonList(source.deletionFiles().get().get(fileIndex)));
        }
        IndexedSplit allRows =
                new IndexedSplit(
                        builder.build(),
                        candidateRanges == null
                                ? Collections.singletonList(new Range(0, dataFile.rowCount() - 1))
                                : candidateRanges,
                        null);
        ReadBuilder readBuilder =
                table.newReadBuilder().withReadType(filterReadType()).withFilter(filter);
        RoaringNavigableMap64 positions = new RoaringNavigableMap64();
        try (RecordReader<InternalRow> reader =
                readBuilder.newRead().executeFilter().createReader(allRows)) {
            RecordReader.RecordIterator<InternalRow> batch;
            while ((batch = reader.readBatch()) != null) {
                checkArgument(
                        batch instanceof ScoreRecordIterator,
                        "Residual primary-key vector filter requires physical row positions.");
                ScoreRecordIterator<InternalRow> positionsBatch =
                        (ScoreRecordIterator<InternalRow>) batch;
                try {
                    while (positionsBatch.next() != null) {
                        positions.add(positionsBatch.returnedRowId());
                    }
                } finally {
                    positionsBatch.releaseBatch();
                }
            }
        }
        return positions.toRangeList();
    }

    private RowType filterReadType() {
        Set<String> filterFields = PredicateVisitor.collectFieldNames(filter);
        List<String> readFields = new ArrayList<>();
        for (String field : table.rowType().getFieldNames()) {
            if (filterFields.contains(field)) {
                readFields.add(field);
            }
        }
        return table.rowType().project(readFields);
    }

    private static List<Candidate> candidates(
            DataSplit split, List<PkVectorSearchResult> searchResults) {
        List<Candidate> candidates = new ArrayList<>(searchResults.size());
        for (PkVectorSearchResult result : searchResults) {
            candidates.add(
                    new Candidate(
                            split.partition(),
                            split.bucket(),
                            result.dataFileName(),
                            result.rowPosition(),
                            result.distance()));
        }
        return candidates;
    }

    private List<Candidate> rerank(
            PrimaryKeyVectorScan.Plan plan, List<Candidate> indexedCandidates) {
        Map<PhysicalPosition, Candidate> candidatesByPosition = new HashMap<>();
        for (Candidate candidate : indexedCandidates) {
            PhysicalPosition position = new PhysicalPosition(candidate);
            checkArgument(
                    candidatesByPosition.put(position, candidate) == null,
                    "Duplicate primary-key vector candidate %s.",
                    position);
        }

        List<IndexedSplit> splits =
                new PrimaryKeyVectorResult(plan, indexedCandidates, metric).splits();
        TableRead read = table.newReadBuilder().withReadType(RowType.of(vectorField)).newRead();
        List<Candidate> reranked = new ArrayList<>(indexedCandidates.size());
        try {
            for (IndexedSplit split : splits) {
                rerankSplit(read, split, candidatesByPosition, reranked);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to rerank primary-key vector candidates.", e);
        }
        checkArgument(
                candidatesByPosition.isEmpty(),
                "Failed to read %s primary-key vector candidates for reranking.",
                candidatesByPosition.size());
        return topK(reranked, limit);
    }

    @SuppressWarnings("unchecked")
    private void rerankSplit(
            TableRead read,
            IndexedSplit split,
            Map<PhysicalPosition, Candidate> candidatesByPosition,
            List<Candidate> reranked)
            throws IOException {
        DataSplit dataSplit = split.dataSplit();
        checkArgument(
                dataSplit.dataFiles().size() == 1,
                "Primary-key vector rerank split must contain exactly one data file.");
        String dataFileName = dataSplit.dataFiles().get(0).fileName();
        try (RecordReader<InternalRow> reader = read.createReader(split)) {
            checkArgument(
                    reader instanceof ScoreRecordReader,
                    "Primary-key vector rerank requires a score record reader.");
            ScoreRecordReader<InternalRow> scoreReader = (ScoreRecordReader<InternalRow>) reader;
            ScoreRecordIterator<InternalRow> batch;
            while ((batch = scoreReader.readBatch()) != null) {
                try {
                    InternalRow row;
                    while ((row = batch.next()) != null) {
                        PhysicalPosition position =
                                new PhysicalPosition(
                                        dataSplit.partition(),
                                        dataSplit.bucket(),
                                        dataFileName,
                                        batch.returnedRowId());
                        Candidate candidate = candidatesByPosition.remove(position);
                        checkArgument(
                                candidate != null,
                                "Primary-key vector rerank read unexpected position %s.",
                                position);
                        checkArgument(
                                !row.isNullAt(0),
                                "Primary-key vector candidate %s contains a null vector.",
                                position);
                        float[] vector = row.getVector(0).toFloatArray();
                        checkArgument(
                                vector.length == query.length,
                                "Primary-key vector candidate %s has dimension %s instead of %s.",
                                position,
                                vector.length,
                                query.length);
                        reranked.add(
                                new Candidate(
                                        candidate.partition(),
                                        candidate.bucket(),
                                        candidate.dataFileName(),
                                        candidate.rowPosition(),
                                        VectorSearchMetric.computeDistance(query, vector, metric)));
                    }
                } finally {
                    batch.releaseBatch();
                }
            }
        }
    }

    private Map<String, DeletionVector> deletionVectors(DataSplit split, FileIO fileIO)
            throws IOException {
        DeletionVector.Factory factory =
                DeletionVector.factory(
                        fileIO, split.dataFiles(), split.deletionFiles().orElse(null));
        Map<String, DeletionVector> result = new HashMap<>();
        for (DataFileMeta file : split.dataFiles()) {
            Optional<DeletionVector> deletionVector = factory.create(file.fileName());
            if (deletionVector.isPresent()) {
                result.put(file.fileName(), deletionVector.get());
            }
        }
        return result;
    }

    protected static List<Candidate> topK(List<Candidate> candidates, int limit) {
        checkArgument(limit > 0, "Vector search limit must be positive: %s.", limit);
        PriorityQueue<Candidate> nearest = new PriorityQueue<>(limit, BEST_FIRST.reversed());
        for (Candidate candidate : candidates) {
            if (nearest.size() < limit) {
                nearest.add(candidate);
            } else if (BEST_FIRST.compare(candidate, nearest.peek()) < 0) {
                nearest.poll();
                nearest.add(candidate);
            }
        }
        List<Candidate> result = new ArrayList<>(nearest);
        Collections.sort(result, BEST_FIRST);
        return Collections.unmodifiableList(result);
    }

    private static int compareBytes(byte[] left, byte[] right) {
        int count = Math.min(left.length, right.length);
        for (int i = 0; i < count; i++) {
            int comparison = Integer.compare(left[i] & 0xFF, right[i] & 0xFF);
            if (comparison != 0) {
                return comparison;
            }
        }
        return Integer.compare(left.length, right.length);
    }

    /** Snapshot-scoped physical row candidate. */
    public static class Candidate implements Serializable {

        private static final long serialVersionUID = 1L;

        private final BinaryRow partition;
        private final int bucket;
        private final String dataFileName;
        private final long rowPosition;
        private final float distance;

        Candidate(
                BinaryRow partition,
                int bucket,
                String dataFileName,
                long rowPosition,
                float distance) {
            this.partition = partition.copy();
            this.bucket = bucket;
            this.dataFileName = dataFileName;
            this.rowPosition = rowPosition;
            this.distance = distance;
        }

        public BinaryRow partition() {
            return partition;
        }

        public int bucket() {
            return bucket;
        }

        public String dataFileName() {
            return dataFileName;
        }

        public long rowPosition() {
            return rowPosition;
        }

        public float distance() {
            return distance;
        }
    }

    /** Separately bounded approximate-index and exact-fallback candidates. */
    public static class SearchResult implements Serializable {

        private static final long serialVersionUID = 1L;

        private final List<Candidate> indexedCandidates;
        private final List<Candidate> exactCandidates;

        public SearchResult(List<Candidate> indexedCandidates, List<Candidate> exactCandidates) {
            this.indexedCandidates =
                    Collections.unmodifiableList(new ArrayList<>(indexedCandidates));
            this.exactCandidates = Collections.unmodifiableList(new ArrayList<>(exactCandidates));
        }

        public List<Candidate> indexedCandidates() {
            return indexedCandidates;
        }

        public List<Candidate> exactCandidates() {
            return exactCandidates;
        }
    }

    private static class PhysicalPosition {

        private final BinaryRow partition;
        private final int bucket;
        private final String dataFileName;
        private final long rowPosition;

        private PhysicalPosition(Candidate candidate) {
            this(
                    candidate.partition(),
                    candidate.bucket(),
                    candidate.dataFileName(),
                    candidate.rowPosition());
        }

        private PhysicalPosition(
                BinaryRow partition, int bucket, String dataFileName, long rowPosition) {
            this.partition = partition.copy();
            this.bucket = bucket;
            this.dataFileName = dataFileName;
            this.rowPosition = rowPosition;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PhysicalPosition that = (PhysicalPosition) o;
            return bucket == that.bucket
                    && rowPosition == that.rowPosition
                    && partition.equals(that.partition)
                    && dataFileName.equals(that.dataFileName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(partition, bucket, dataFileName, rowPosition);
        }

        @Override
        public String toString() {
            return dataFileName + '@' + rowPosition + " in bucket " + bucket;
        }
    }

    static class SearchContext {

        private final FileIO fileIO;
        private final IndexFileHandler indexFileHandler;
        private final KeyValueFileReaderFactory.Builder readerFactoryBuilder;
        private final ExecutorService executor;

        private SearchContext(FileStoreTable table) {
            this.fileIO = table.fileIO();
            this.indexFileHandler = table.store().newIndexFileHandler();
            this.readerFactoryBuilder = keyValueStore(table).newReaderFactoryBuilder();
            this.executor =
                    GlobalIndexReadThreadPool.getExecutorService(
                            table.coreOptions().toConfiguration().get(GLOBAL_INDEX_THREAD_NUM));
        }
    }
}
