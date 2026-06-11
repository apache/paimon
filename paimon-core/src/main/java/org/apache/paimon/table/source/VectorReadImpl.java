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

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReadThreadPool;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.GlobalIndexScanner;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.GlobalIndexerFactoryUtils;
import org.apache.paimon.globalindex.OffsetGlobalIndexReader;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.predicate.BatchVectorSearch;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.IOUtils;
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

import static org.apache.paimon.CoreOptions.GLOBAL_INDEX_THREAD_NUM;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Implementation for {@link VectorRead}. */
public class VectorReadImpl implements VectorRead, Serializable {

    private static final long serialVersionUID = 1L;

    protected final FileStoreTable table;
    private final Predicate filter;
    protected final int limit;
    protected final DataField vectorColumn;
    protected final float[][] vectors;
    protected final Map<String, String> options;

    public VectorReadImpl(
            FileStoreTable table,
            Predicate filter,
            int limit,
            DataField vectorColumn,
            float[][] vectors) {
        this(table, filter, limit, vectorColumn, vectors, Collections.emptyMap());
    }

    public VectorReadImpl(
            FileStoreTable table,
            Predicate filter,
            int limit,
            DataField vectorColumn,
            float[][] vectors,
            Map<String, String> options) {
        this.table = table;
        this.filter = filter;
        this.limit = limit;
        this.vectorColumn = vectorColumn;
        this.vectors = vectors;
        this.options =
                options == null
                        ? Collections.emptyMap()
                        : Collections.unmodifiableMap(new HashMap<>(options));
    }

    @Override
    public GlobalIndexResult read(List<VectorSearchSplit> splits) {
        if (vectors.length > 1) {
            throw new IllegalStateException(
                    "read() supports single vector only; use readBatch() for multiple vectors");
        }
        return readBatch(splits).get(0);
    }

    @Override
    public List<GlobalIndexResult> readBatch(List<VectorSearchSplit> splits) {
        int n = vectors.length;
        if (splits.isEmpty()) {
            List<GlobalIndexResult> empty = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                empty.add(GlobalIndexResult.createEmpty());
            }
            return empty;
        }

        RoaringNavigableMap64 preFilter = preFilter(splits).orElse(null);

        String indexType = splits.get(0).vectorIndexFiles().get(0).indexType();
        GlobalIndexer globalIndexer =
                GlobalIndexerFactoryUtils.load(indexType)
                        .create(vectorColumn, table.coreOptions().toConfiguration());
        IndexPathFactory indexPathFactory = table.store().pathFactory().globalIndexFileFactory();

        int parallelism = table.coreOptions().toConfiguration().get(GLOBAL_INDEX_THREAD_NUM);
        ExecutorService executor = GlobalIndexReadThreadPool.getExecutorService(parallelism);

        List<CompletableFuture<List<Optional<ScoredGlobalIndexResult>>>> futures =
                new ArrayList<>(splits.size());
        for (VectorSearchSplit split : splits) {
            futures.add(
                    evalBatch(
                            globalIndexer,
                            indexPathFactory,
                            split.rowRangeStart(),
                            split.rowRangeEnd(),
                            split.vectorIndexFiles(),
                            preFilter,
                            executor));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        ScoredGlobalIndexResult[] merged = new ScoredGlobalIndexResult[n];
        for (int i = 0; i < n; i++) {
            merged[i] = ScoredGlobalIndexResult.createEmpty();
        }

        for (CompletableFuture<List<Optional<ScoredGlobalIndexResult>>> future : futures) {
            List<Optional<ScoredGlobalIndexResult>> splitResults = future.join();
            for (int i = 0; i < n; i++) {
                if (splitResults.get(i).isPresent()) {
                    merged[i] = merged[i].or(splitResults.get(i).get());
                }
            }
        }

        List<GlobalIndexResult> results = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            results.add(merged[i].topK(limit));
        }
        return results;
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

    protected CompletableFuture<List<Optional<ScoredGlobalIndexResult>>> evalBatch(
            GlobalIndexer globalIndexer,
            IndexPathFactory indexPathFactory,
            long rowRangeStart,
            long rowRangeEnd,
            List<IndexFileMeta> vectorIndexFiles,
            @Nullable RoaringNavigableMap64 includeRowIds,
            ExecutorService executor) {
        List<GlobalIndexIOMeta> indexIOMetaList =
                buildIOMetaList(indexPathFactory, vectorIndexFiles);
        @SuppressWarnings("resource")
        FileIO fileIO = table.fileIO();
        GlobalIndexFileReader indexFileReader = m -> fileIO.newInputStream(m.filePath());
        GlobalIndexReader reader =
                globalIndexer.createReader(indexFileReader, indexIOMetaList, executor);
        BatchVectorSearch batchVectorSearch =
                new BatchVectorSearch(vectors, limit, vectorColumn.name())
                        .withIncludeRowIds(includeRowIds);
        return new OffsetGlobalIndexReader(reader, rowRangeStart, rowRangeEnd)
                .visitBatchVectorSearch(batchVectorSearch)
                .whenComplete((r, t) -> IOUtils.closeQuietly(reader));
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
}
