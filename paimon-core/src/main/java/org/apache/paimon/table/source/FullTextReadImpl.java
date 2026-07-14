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
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Implementation for {@link FullTextRead}. */
public class FullTextReadImpl implements FullTextRead {

    private final FileStoreTable table;
    @Nullable private final PartitionPredicate partitionFilter;
    private final int limit;
    private final DataField textColumn;
    private final String query;

    public FullTextReadImpl(FileStoreTable table, int limit, DataField textColumn, String query) {
        this(table, null, limit, Collections.singletonList(textColumn), query);
    }

    public FullTextReadImpl(
            FileStoreTable table, int limit, List<DataField> textColumns, String query) {
        this(table, null, limit, textColumns, query);
    }

    public FullTextReadImpl(
            FileStoreTable table,
            @Nullable PartitionPredicate partitionFilter,
            int limit,
            List<DataField> textColumns,
            String query) {
        this.table = table;
        this.partitionFilter = partitionFilter;
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
        if (splits.isEmpty()) {
            return GlobalIndexResult.createEmpty();
        }

        IndexPathFactory indexPathFactory = table.store().pathFactory().globalIndexFileFactory();

        int parallelism = table.coreOptions().toConfiguration().get(GLOBAL_INDEX_THREAD_NUM);
        ExecutorService executor = GlobalIndexReadThreadPool.getExecutorService(parallelism);

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
        RoaringNavigableMap64 liveRows = GlobalIndexLiveRowFilter.liveRows(table, partitionFilter);
        ScoredGlobalIndexResult result =
                evalQuery(splitsByColumn, indexPathFactory, indexFileReader, executor, liveRows);
        if (!rawRowRanges.isEmpty()) {
            result =
                    new RawFullTextReadImpl(
                                    table, partitionFilter, limit, textColumn, this::evalQuery)
                            .withRawSearch(result, rawRowRanges, splitsByColumn, executor);
        }
        return result.topK(limit);
    }

    ScoredGlobalIndexResult evalQuery(
            Map<String, List<IndexFullTextSearchSplit>> splitsByColumn,
            IndexPathFactory indexPathFactory,
            GlobalIndexFileReader indexFileReader,
            ExecutorService executor) {
        return evalQuery(splitsByColumn, indexPathFactory, indexFileReader, executor, null);
    }

    private ScoredGlobalIndexResult evalQuery(
            Map<String, List<IndexFullTextSearchSplit>> splitsByColumn,
            IndexPathFactory indexPathFactory,
            GlobalIndexFileReader indexFileReader,
            ExecutorService executor,
            @Nullable RoaringNavigableMap64 liveRows) {
        return evalColumnQuery(
                textColumn.name(),
                splitsByColumn,
                indexPathFactory,
                indexFileReader,
                executor,
                liveRows);
    }

    private ScoredGlobalIndexResult evalColumnQuery(
            String column,
            Map<String, List<IndexFullTextSearchSplit>> splitsByColumn,
            IndexPathFactory indexPathFactory,
            GlobalIndexFileReader indexFileReader,
            ExecutorService executor,
            @Nullable RoaringNavigableMap64 liveRows) {
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
                            includeRowIds(split, liveRows)));
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

    @Nullable
    private static RoaringNavigableMap64 includeRowIds(
            IndexFullTextSearchSplit split, @Nullable RoaringNavigableMap64 liveRows) {
        RoaringNavigableMap64 include = new RoaringNavigableMap64();
        for (Range range : split.searchRowRanges()) {
            include.addRange(range);
        }
        if (liveRows != null) {
            include.and(liveRows);
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
                globalIndexer.createReader(indexFileReader, indexIOMetaList, executor);
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
