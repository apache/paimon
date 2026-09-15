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

import org.apache.paimon.CoreOptions.GlobalIndexSearchMode;
import org.apache.paimon.Snapshot;
import org.apache.paimon.globalindex.DataEvolutionGlobalIndexScanner;
import org.apache.paimon.globalindex.GlobalIndexEvaluator;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReadThreadPool;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.GlobalIndexerFactoryUtils;
import org.apache.paimon.globalindex.OffsetGlobalIndexReader;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.FullTextSearch;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static org.apache.paimon.CoreOptions.GLOBAL_INDEX_THREAD_NUM;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Implementation for {@link FullTextRead}. */
public class DataEvolutionFullTextRead implements FullTextRead {

    private static final Logger LOG = LoggerFactory.getLogger(DataEvolutionFullTextRead.class);

    private final FileStoreTable table;
    @Nullable private final PartitionPredicate partitionFilter;
    @Nullable private final Predicate filter;
    private final int limit;
    private final DataField textColumn;
    private final String query;

    public DataEvolutionFullTextRead(
            FileStoreTable table,
            @Nullable PartitionPredicate partitionFilter,
            int limit,
            List<DataField> textColumns,
            String query) {
        this(table, partitionFilter, null, limit, textColumns, query);
    }

    public DataEvolutionFullTextRead(
            FileStoreTable table,
            @Nullable PartitionPredicate partitionFilter,
            @Nullable Predicate filter,
            int limit,
            List<DataField> textColumns,
            String query) {
        this.table = table;
        this.partitionFilter = partitionFilter;
        this.filter = filter;
        this.limit = limit;
        if (textColumns.size() != 1) {
            throw new IllegalArgumentException(
                    "Full-text search expects exactly one text column, got: " + textColumns);
        }
        this.textColumn = textColumns.get(0);
        this.query = query;
    }

    @Override
    public GlobalIndexResult read(List<FullTextSearchSplit> splits) {
        return read(splits, null);
    }

    @Override
    public GlobalIndexResult read(FullTextScan.Plan plan) {
        return read(plan.splits(), plan.snapshot());
    }

    private GlobalIndexResult read(
            List<FullTextSearchSplit> splits, @Nullable Snapshot planSnapshot) {
        if (splits.isEmpty()) {
            return GlobalIndexResult.createEmpty();
        }

        IndexPathFactory indexPathFactory = table.store().pathFactory().globalIndexFileFactory();

        int parallelism = table.coreOptions().toConfiguration().get(GLOBAL_INDEX_THREAD_NUM);
        ExecutorService executor = GlobalIndexReadThreadPool.getExecutorService(parallelism);

        Map<String, List<IndexFullTextSearchSplit>> splitsByColumn = new HashMap<>();
        List<IndexFullTextSearchSplit> indexSplits = new ArrayList<>();
        List<Range> rawRowRanges = new ArrayList<>();
        List<RawFullTextSearchSplit> rawSplits = new ArrayList<>();
        for (FullTextSearchSplit split : splits) {
            if (split instanceof IndexFullTextSearchSplit) {
                IndexFullTextSearchSplit indexSplit = (IndexFullTextSearchSplit) split;
                indexSplits.add(indexSplit);
                splitsByColumn
                        .computeIfAbsent(indexSplit.columnName(), k -> new ArrayList<>())
                        .add(indexSplit);
            } else if (split instanceof RawFullTextSearchSplit) {
                RawFullTextSearchSplit rawSplit = (RawFullTextSearchSplit) split;
                rawSplits.add(rawSplit);
                rawRowRanges.addAll(rawSplit.rowRanges());
            }
        }

        GlobalIndexFileReader indexFileReader = m -> table.fileIO().newInputStream(m.filePath());
        RoaringNavigableMap64 liveRows =
                GlobalIndexLiveRowFilter.liveRows(table, planSnapshot, partitionFilter, null);
        RoaringNavigableMap64 matchedRows = scalarMatchedRows(indexSplits, planSnapshot);
        ScoredGlobalIndexResult result =
                evalQuery(
                        splitsByColumn,
                        indexPathFactory,
                        indexFileReader,
                        executor,
                        liveRows,
                        matchedRows);
        if (!rawRowRanges.isEmpty()) {
            result =
                    new RawFullTextReadImpl(
                                    table,
                                    planSnapshot,
                                    partitionFilter,
                                    filter,
                                    limit,
                                    textColumn,
                                    this::evalQuery)
                            .withRawSearch(
                                    result,
                                    rawRowRanges,
                                    rawPreFilter(rawSplits, planSnapshot),
                                    splitsByColumn,
                                    executor);
        }
        return result.topK(limit);
    }

    /**
     * Rows of the indexed splits that satisfy {@link #filter} according to the scalar global
     * indexes attached to them, or {@code null} when there is no filter. Rows whose filter columns
     * are not indexed are absent from the result: in {@code fast} scalar search mode they are
     * excluded, in other modes the scan already routed them to a raw split where the predicate is
     * evaluated on the data.
     */
    @Nullable
    private RoaringNavigableMap64 scalarMatchedRows(
            List<IndexFullTextSearchSplit> indexSplits, @Nullable Snapshot planSnapshot) {
        if (filter == null || indexSplits.isEmpty()) {
            return null;
        }

        Set<IndexFileMeta> scalarIndexFiles =
                new TreeSet<>(Comparator.comparing(IndexFileMeta::fileName));
        for (IndexFullTextSearchSplit split : indexSplits) {
            scalarIndexFiles.addAll(split.scalarIndexFiles());
        }

        Optional<DataEvolutionGlobalIndexScanner> optionalScanner =
                DataEvolutionGlobalIndexScanner.create(
                        table, planSnapshot, partitionFilter, scalarIndexFiles);
        if (!optionalScanner.isPresent()) {
            warnUnindexedFilter();
            return new RoaringNavigableMap64();
        }

        try (DataEvolutionGlobalIndexScanner scanner = optionalScanner.get()) {
            Optional<GlobalIndexResult> result = scanner.scan(filter);
            if (!result.isPresent()) {
                warnUnindexedFilter();
                return new RoaringNavigableMap64();
            }
            return result.get().results();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Rows of the raw splits that may satisfy {@link #filter}: rows matched by the scalar indexes
     * plus rows those indexes do not cover, or {@code null} when nothing can be pre-filtered. The
     * raw read still evaluates the predicate row by row, so this only bounds the rows to scan.
     */
    @Nullable
    private RoaringNavigableMap64 rawPreFilter(
            List<RawFullTextSearchSplit> rawSplits, @Nullable Snapshot planSnapshot) {
        if (filter == null || rawSplits.isEmpty()) {
            return null;
        }

        Set<IndexFileMeta> scalarIndexFiles =
                new TreeSet<>(Comparator.comparing(IndexFileMeta::fileName));
        for (RawFullTextSearchSplit split : rawSplits) {
            scalarIndexFiles.addAll(split.scalarIndexFiles());
        }
        Optional<DataEvolutionGlobalIndexScanner> optionalScanner =
                DataEvolutionGlobalIndexScanner.create(
                        table, planSnapshot, partitionFilter, scalarIndexFiles);
        if (!optionalScanner.isPresent()) {
            return null;
        }

        RoaringNavigableMap64 include = new RoaringNavigableMap64();
        try (DataEvolutionGlobalIndexScanner scanner = optionalScanner.get()) {
            Optional<GlobalIndexEvaluator.Evaluation> result = scanner.scanWithCoverage(filter);
            if (!result.isPresent()) {
                return null;
            }
            include.or(result.get().result().results());
            include.or(
                    scanner.unindexedRowsForContributingFields(result.get().contributingFieldIds())
                            .results());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return include;
    }

    private void warnUnindexedFilter() {
        if (table.coreOptions().scalarIndexSearchMode() == GlobalIndexSearchMode.FAST) {
            LOG.warn(
                    "Full-text search on table {} has a row filter {} that no scalar global index "
                            + "can evaluate; indexed rows are excluded from the result because "
                            + "scalar-index.search-mode is fast. Build a scalar index on the "
                            + "filtered columns or use search mode full.",
                    table.name(),
                    filter);
        }
    }

    ScoredGlobalIndexResult evalQuery(
            Map<String, List<IndexFullTextSearchSplit>> splitsByColumn,
            IndexPathFactory indexPathFactory,
            GlobalIndexFileReader indexFileReader,
            ExecutorService executor) {
        return evalQuery(splitsByColumn, indexPathFactory, indexFileReader, executor, null, null);
    }

    private ScoredGlobalIndexResult evalQuery(
            Map<String, List<IndexFullTextSearchSplit>> splitsByColumn,
            IndexPathFactory indexPathFactory,
            GlobalIndexFileReader indexFileReader,
            ExecutorService executor,
            @Nullable RoaringNavigableMap64 liveRows,
            @Nullable RoaringNavigableMap64 matchedRows) {
        return evalColumnQuery(
                textColumn.name(),
                splitsByColumn,
                indexPathFactory,
                indexFileReader,
                executor,
                liveRows,
                matchedRows);
    }

    private ScoredGlobalIndexResult evalColumnQuery(
            String column,
            Map<String, List<IndexFullTextSearchSplit>> splitsByColumn,
            IndexPathFactory indexPathFactory,
            GlobalIndexFileReader indexFileReader,
            ExecutorService executor,
            @Nullable RoaringNavigableMap64 liveRows,
            @Nullable RoaringNavigableMap64 matchedRows) {
        List<IndexFullTextSearchSplit> columnSplits = splitsByColumn.get(column);
        if (columnSplits == null || columnSplits.isEmpty()) {
            return ScoredGlobalIndexResult.createEmpty();
        }

        // A column can carry splits from more than one index identity (per-range selection in the
        // scan when different indexes cover different ranges of the same column), so build the
        // reader from each split's own file meta rather than reusing the first split's identity.
        List<CompletableFuture<Optional<ScoredGlobalIndexResult>>> futures =
                new ArrayList<>(columnSplits.size());
        for (IndexFullTextSearchSplit split : columnSplits) {
            GlobalIndexer globalIndexer =
                    createIndexer(split.fullTextIndexFiles().get(0), textColumn);
            futures.add(
                    eval(
                            globalIndexer,
                            indexPathFactory,
                            split.rowRangeStart(),
                            split.rowRangeEnd(),
                            split.fullTextIndexFiles(),
                            indexFileReader,
                            executor,
                            includeRowIds(split, liveRows, matchedRows)));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        List<ScoredGlobalIndexResult> results = new ArrayList<>(futures.size());
        for (CompletableFuture<Optional<ScoredGlobalIndexResult>> f : futures) {
            Optional<ScoredGlobalIndexResult> next = f.join();
            if (next.isPresent()) {
                results.add(next.get());
            }
        }

        return ScoredGlobalIndexResult.merge(results);
    }

    @Nullable
    private static RoaringNavigableMap64 includeRowIds(
            IndexFullTextSearchSplit split,
            @Nullable RoaringNavigableMap64 liveRows,
            @Nullable RoaringNavigableMap64 matchedRows) {
        RoaringNavigableMap64 include = new RoaringNavigableMap64();
        for (Range range : split.searchRowRanges()) {
            include.addRange(range);
        }
        if (liveRows != null) {
            include.and(liveRows);
        }
        if (matchedRows != null) {
            include.and(matchedRows);
        }
        long physicalRowCount = split.rowRangeEnd() - split.rowRangeStart() + 1;
        return include.getLongCardinality() == physicalRowCount ? null : include;
    }

    /**
     * Builds the {@link GlobalIndexer} for a single split from its own index file meta, so a column
     * served by several index identities (over different row ranges) reads each split with the
     * matching field configuration instead of the first split's.
     */
    private GlobalIndexer createIndexer(IndexFileMeta file, DataField textColumn) {
        String indexType = file.indexType();
        GlobalIndexMeta meta = checkNotNull(file.globalIndexMeta());
        if (meta.extraFieldIds() != null) {
            return GlobalIndexerFactoryUtils.load(indexType)
                    .create(
                            meta.getIndexField(table.rowType()),
                            meta.getExtraFields(table.rowType()),
                            table.coreOptions().toConfiguration());
        }
        return GlobalIndexerFactoryUtils.load(indexType)
                .create(textColumn, table.coreOptions().toConfiguration());
    }

    private CompletableFuture<Optional<ScoredGlobalIndexResult>> eval(
            GlobalIndexer globalIndexer,
            IndexPathFactory indexPathFactory,
            long rowRangeStart,
            long rowRangeEnd,
            List<IndexFileMeta> fullTextIndexFiles,
            GlobalIndexFileReader indexFileReader,
            ExecutorService executor,
            @Nullable RoaringNavigableMap64 includeRowIds) {
        if (includeRowIds != null && includeRowIds.isEmpty()) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        List<GlobalIndexIOMeta> indexIOMetaList = new ArrayList<>();
        for (IndexFileMeta indexFile : fullTextIndexFiles) {
            GlobalIndexMeta meta = checkNotNull(indexFile.globalIndexMeta());
            indexIOMetaList.add(
                    new GlobalIndexIOMeta(
                            indexPathFactory.toPath(indexFile),
                            indexFile.fileSize(),
                            meta.indexMeta()));
        }
        GlobalIndexReader reader =
                globalIndexer.createReader(
                        indexFileReader,
                        indexIOMetaList,
                        rowRangeEnd - rowRangeStart + 1,
                        executor);
        FullTextSearch fullTextSearch =
                new FullTextSearch(
                                textColumn.name(),
                                query,
                                candidateLimit(rowRangeStart, rowRangeEnd))
                        .withIncludeRowIds(includeRowIds);
        return new OffsetGlobalIndexReader(reader, rowRangeStart, rowRangeEnd)
                .visitFullTextSearch(fullTextSearch)
                .whenComplete((r, t) -> IOUtils.closeQuietly(reader));
    }

    private static int candidateLimit(long rowRangeStart, long rowRangeEnd) {
        long size = rowRangeEnd - rowRangeStart + 1;
        return size > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) size;
    }
}
