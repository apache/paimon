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

import org.apache.paimon.Snapshot;
import org.apache.paimon.globalindex.DeletionVectorRowIdFilter;
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
import org.apache.paimon.predicate.FullTextQuery;
import org.apache.paimon.predicate.FullTextSearch;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static org.apache.paimon.CoreOptions.GLOBAL_INDEX_THREAD_NUM;
import static org.apache.paimon.table.source.snapshot.TimeTravelUtil.tryTravelOrLatest;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Implementation for {@link FullTextRead}. */
public class FullTextReadImpl implements FullTextRead {

    private final FileStoreTable table;
    @Nullable private final PartitionPredicate partitionFilter;
    private final int limit;
    private final List<DataField> textColumns;
    private final FullTextQuery query;

    public FullTextReadImpl(
            FileStoreTable table, int limit, DataField textColumn, FullTextQuery query) {
        this(table, null, limit, Collections.singletonList(textColumn), query);
    }

    public FullTextReadImpl(
            FileStoreTable table, int limit, List<DataField> textColumns, FullTextQuery query) {
        this(table, null, limit, textColumns, query);
    }

    public FullTextReadImpl(
            FileStoreTable table,
            @Nullable PartitionPredicate partitionFilter,
            int limit,
            List<DataField> textColumns,
            FullTextQuery query) {
        this.table = table;
        this.partitionFilter = partitionFilter;
        this.limit = limit;
        this.textColumns = Collections.unmodifiableList(new ArrayList<>(textColumns));
        this.query = query;
    }

    @Override
    public GlobalIndexResult read(List<FullTextSearchSplit> splits) {
        if (splits.isEmpty()) {
            return GlobalIndexResult.createEmpty();
        }

        IndexPathFactory indexPathFactory = table.store().pathFactory().globalIndexFileFactory();

        int parallelism = table.coreOptions().toConfiguration().get(GLOBAL_INDEX_THREAD_NUM);
        ExecutorService executor = GlobalIndexReadThreadPool.getExecutorService(parallelism);

        Map<String, DataField> fieldsByName = new HashMap<>();
        for (DataField textColumn : textColumns) {
            fieldsByName.put(textColumn.name(), textColumn);
        }

        Map<String, List<IndexFullTextSearchSplit>> splitsByColumn = new HashMap<>();
        List<Range> rawRowRanges = new ArrayList<>();
        for (FullTextSearchSplit split : splits) {
            if (split instanceof IndexFullTextSearchSplit) {
                IndexFullTextSearchSplit indexSplit = (IndexFullTextSearchSplit) split;
                splitsByColumn
                        .computeIfAbsent(indexSplit.columnName(), k -> new ArrayList<>())
                        .add(indexSplit);
            } else if (split instanceof RawFullTextSearchSplit) {
                rawRowRanges.addAll(((RawFullTextSearchSplit) split).rowRanges());
            }
        }

        GlobalIndexFileReader indexFileReader = m -> table.fileIO().newInputStream(m.filePath());
        ScoredGlobalIndexResult result =
                evalQuery(
                        query,
                        fieldsByName,
                        splitsByColumn,
                        indexPathFactory,
                        indexFileReader,
                        executor);
        result = filterDeletedRows(result);
        if (!rawRowRanges.isEmpty()) {
            result =
                    new RawFullTextReadImpl(table, partitionFilter, limit, query, this::evalQuery)
                            .withRawSearch(
                                    result, rawRowRanges, fieldsByName, splitsByColumn, executor);
        }
        return filterDeletedRows(result).topK(limit);
    }

    private ScoredGlobalIndexResult filterDeletedRows(ScoredGlobalIndexResult result) {
        if (!table.coreOptions().deletionVectorsEnabled() || result.results().isEmpty()) {
            return result;
        }

        Snapshot snapshot = tryTravelOrLatest(table);
        if (snapshot == null) {
            return result;
        }
        return new DeletionVectorRowIdFilter(table, snapshot, partitionFilter).filter(result);
    }

    ScoredGlobalIndexResult evalQuery(
            FullTextQuery query,
            Map<String, DataField> fieldsByName,
            Map<String, List<IndexFullTextSearchSplit>> splitsByColumn,
            IndexPathFactory indexPathFactory,
            GlobalIndexFileReader indexFileReader,
            ExecutorService executor) {
        if (query instanceof FullTextQuery.Match) {
            return evalColumnQuery(
                    query,
                    ((FullTextQuery.Match) query).column(),
                    fieldsByName,
                    splitsByColumn,
                    indexPathFactory,
                    indexFileReader,
                    executor);
        }
        if (query instanceof FullTextQuery.Phrase) {
            return evalColumnQuery(
                    query,
                    ((FullTextQuery.Phrase) query).column(),
                    fieldsByName,
                    splitsByColumn,
                    indexPathFactory,
                    indexFileReader,
                    executor);
        }
        if (query instanceof FullTextQuery.MultiMatch) {
            return evalMultiMatch(
                    (FullTextQuery.MultiMatch) query,
                    fieldsByName,
                    splitsByColumn,
                    indexPathFactory,
                    indexFileReader,
                    executor);
        }
        if (query instanceof FullTextQuery.Boost) {
            FullTextQuery.Boost boost = (FullTextQuery.Boost) query;
            return boost(
                    evalQuery(
                            boost.positive(),
                            fieldsByName,
                            splitsByColumn,
                            indexPathFactory,
                            indexFileReader,
                            executor),
                    evalQuery(
                            boost.negative(),
                            fieldsByName,
                            splitsByColumn,
                            indexPathFactory,
                            indexFileReader,
                            executor),
                    boost.negativeBoost());
        }
        if (query instanceof FullTextQuery.BooleanQuery) {
            return evalBoolean(
                    (FullTextQuery.BooleanQuery) query,
                    fieldsByName,
                    splitsByColumn,
                    indexPathFactory,
                    indexFileReader,
                    executor);
        }
        throw new IllegalArgumentException("Unsupported full-text query: " + query);
    }

    private ScoredGlobalIndexResult evalMultiMatch(
            FullTextQuery.MultiMatch query,
            Map<String, DataField> fieldsByName,
            Map<String, List<IndexFullTextSearchSplit>> splitsByColumn,
            IndexPathFactory indexPathFactory,
            GlobalIndexFileReader indexFileReader,
            ExecutorService executor) {
        List<String> columns = query.columns();
        List<Float> boosts = query.boosts();
        List<ScoredGlobalIndexResult> results = new ArrayList<>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            FullTextQuery.Match match =
                    new FullTextQuery.Match(
                            query.query(),
                            columns.get(i),
                            boosts.get(i),
                            0,
                            50,
                            query.operator(),
                            0);
            results.add(
                    evalColumnQuery(
                            match,
                            columns.get(i),
                            fieldsByName,
                            splitsByColumn,
                            indexPathFactory,
                            indexFileReader,
                            executor));
        }
        return filterDeletedRows(or(results)).topK(limit);
    }

    private ScoredGlobalIndexResult evalBoolean(
            FullTextQuery.BooleanQuery query,
            Map<String, DataField> fieldsByName,
            Map<String, List<IndexFullTextSearchSplit>> splitsByColumn,
            IndexPathFactory indexPathFactory,
            GlobalIndexFileReader indexFileReader,
            ExecutorService executor) {
        ScoredGlobalIndexResult result = null;
        for (FullTextQuery child : query.must()) {
            ScoredGlobalIndexResult childResult =
                    evalQuery(
                            child,
                            fieldsByName,
                            splitsByColumn,
                            indexPathFactory,
                            indexFileReader,
                            executor);
            result = result == null ? childResult : and(result, childResult);
        }

        List<ScoredGlobalIndexResult> shouldResults = new ArrayList<>(query.should().size());
        for (FullTextQuery child : query.should()) {
            shouldResults.add(
                    evalQuery(
                            child,
                            fieldsByName,
                            splitsByColumn,
                            indexPathFactory,
                            indexFileReader,
                            executor));
        }
        if (!shouldResults.isEmpty()) {
            ScoredGlobalIndexResult shouldResult = or(shouldResults);
            result = result == null ? shouldResult : andWithBonus(result, shouldResult);
        }

        if (result == null) {
            return ScoredGlobalIndexResult.createEmpty();
        }
        for (FullTextQuery child : query.mustNot()) {
            ScoredGlobalIndexResult childResult =
                    evalQuery(
                            child,
                            fieldsByName,
                            splitsByColumn,
                            indexPathFactory,
                            indexFileReader,
                            executor);
            result = andNot(result, childResult);
        }
        return filterDeletedRows(result).topK(limit);
    }

    private ScoredGlobalIndexResult evalColumnQuery(
            FullTextQuery query,
            String column,
            Map<String, DataField> fieldsByName,
            Map<String, List<IndexFullTextSearchSplit>> splitsByColumn,
            IndexPathFactory indexPathFactory,
            GlobalIndexFileReader indexFileReader,
            ExecutorService executor) {
        List<IndexFullTextSearchSplit> columnSplits = splitsByColumn.get(column);
        if (columnSplits == null || columnSplits.isEmpty()) {
            return ScoredGlobalIndexResult.createEmpty();
        }
        DataField textColumn = checkNotNull(fieldsByName.get(column));

        IndexFileMeta firstFile = columnSplits.get(0).fullTextIndexFiles().get(0);
        String indexType = firstFile.indexType();
        GlobalIndexMeta firstMeta = checkNotNull(firstFile.globalIndexMeta());
        GlobalIndexer globalIndexer;
        if (firstMeta.extraFieldIds() != null) {
            globalIndexer =
                    GlobalIndexerFactoryUtils.load(indexType)
                            .create(
                                    firstMeta.getIndexField(table.rowType()),
                                    firstMeta.getExtraFields(table.rowType()),
                                    table.coreOptions().toConfiguration());
        } else {
            globalIndexer =
                    GlobalIndexerFactoryUtils.load(indexType)
                            .create(textColumn, table.coreOptions().toConfiguration());
        }

        List<CompletableFuture<Optional<ScoredGlobalIndexResult>>> futures =
                new ArrayList<>(columnSplits.size());
        for (IndexFullTextSearchSplit split : columnSplits) {
            futures.add(
                    eval(
                            globalIndexer,
                            indexPathFactory,
                            split.rowRangeStart(),
                            split.rowRangeEnd(),
                            split.fullTextIndexFiles(),
                            query,
                            indexFileReader,
                            executor));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        ScoredGlobalIndexResult result = ScoredGlobalIndexResult.createEmpty();
        for (CompletableFuture<Optional<ScoredGlobalIndexResult>> f : futures) {
            Optional<ScoredGlobalIndexResult> next = f.join();
            if (next.isPresent()) {
                result = result.or(next.get());
            }
        }

        return result;
    }

    private CompletableFuture<Optional<ScoredGlobalIndexResult>> eval(
            GlobalIndexer globalIndexer,
            IndexPathFactory indexPathFactory,
            long rowRangeStart,
            long rowRangeEnd,
            List<IndexFileMeta> fullTextIndexFiles,
            FullTextQuery query,
            GlobalIndexFileReader indexFileReader,
            ExecutorService executor) {
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
                globalIndexer.createReader(indexFileReader, indexIOMetaList, executor);
        FullTextSearch fullTextSearch =
                new FullTextSearch(query, candidateLimit(rowRangeStart, rowRangeEnd));
        return new OffsetGlobalIndexReader(reader, rowRangeStart, rowRangeEnd)
                .visitFullTextSearch(fullTextSearch)
                .whenComplete((r, t) -> IOUtils.closeQuietly(reader));
    }

    private static int candidateLimit(long rowRangeStart, long rowRangeEnd) {
        long size = rowRangeEnd - rowRangeStart + 1;
        return size > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) size;
    }

    private static ScoredGlobalIndexResult and(
            ScoredGlobalIndexResult left, ScoredGlobalIndexResult right) {
        RoaringNavigableMap64 rowIds = RoaringNavigableMap64.and(left.results(), right.results());
        return ScoredGlobalIndexResult.create(
                rowIds,
                rowId -> left.scoreGetter().score(rowId) + right.scoreGetter().score(rowId));
    }

    private static ScoredGlobalIndexResult or(List<ScoredGlobalIndexResult> results) {
        if (results.isEmpty()) {
            return ScoredGlobalIndexResult.createEmpty();
        }
        RoaringNavigableMap64 rowIds = new RoaringNavigableMap64();
        for (ScoredGlobalIndexResult result : results) {
            rowIds = RoaringNavigableMap64.or(rowIds, result.results());
        }
        final RoaringNavigableMap64 mergedRowIds = rowIds;
        return ScoredGlobalIndexResult.create(
                mergedRowIds,
                rowId -> {
                    float score = 0.0f;
                    for (ScoredGlobalIndexResult result : results) {
                        if (result.results().contains(rowId)) {
                            score += result.scoreGetter().score(rowId);
                        }
                    }
                    return score;
                });
    }

    private static ScoredGlobalIndexResult andWithBonus(
            ScoredGlobalIndexResult base, ScoredGlobalIndexResult bonus) {
        RoaringNavigableMap64 rowIds = base.results();
        return ScoredGlobalIndexResult.create(
                rowIds,
                rowId ->
                        base.scoreGetter().score(rowId)
                                + (bonus.results().contains(rowId)
                                        ? bonus.scoreGetter().score(rowId)
                                        : 0.0f));
    }

    private static ScoredGlobalIndexResult andNot(
            ScoredGlobalIndexResult left, ScoredGlobalIndexResult right) {
        RoaringNavigableMap64 rowIds =
                RoaringNavigableMap64.or(new RoaringNavigableMap64(), left.results());
        rowIds.andNot(right.results());
        return ScoredGlobalIndexResult.create(rowIds, left.scoreGetter());
    }

    private static ScoredGlobalIndexResult boost(
            ScoredGlobalIndexResult positive,
            ScoredGlobalIndexResult negative,
            float negativeBoost) {
        RoaringNavigableMap64 rowIds = positive.results();
        return ScoredGlobalIndexResult.create(
                rowIds,
                rowId ->
                        positive.scoreGetter().score(rowId)
                                * (negative.results().contains(rowId) ? negativeBoost : 1.0f));
    }
}
