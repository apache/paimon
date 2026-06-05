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
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.RoaringNavigableMap64;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
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
    protected final float[] vector;

    public VectorReadImpl(
            FileStoreTable table,
            Predicate filter,
            int limit,
            DataField vectorColumn,
            float[] vector) {
        this.table = table;
        this.filter = filter;
        this.limit = limit;
        this.vectorColumn = vectorColumn;
        this.vector = vector;
    }

    @Override
    public GlobalIndexResult read(List<VectorSearchSplit> splits) {
        if (splits.isEmpty()) {
            return GlobalIndexResult.createEmpty();
        }

        RoaringNavigableMap64 preFilter = preFilter(splits).orElse(null);

        String indexType = splits.get(0).vectorIndexFiles().get(0).indexType();
        GlobalIndexer globalIndexer =
                GlobalIndexerFactoryUtils.load(indexType)
                        .create(vectorColumn, table.coreOptions().toConfiguration());
        IndexPathFactory indexPathFactory = table.store().pathFactory().globalIndexFileFactory();

        int parallelism = table.coreOptions().toConfiguration().get(GLOBAL_INDEX_THREAD_NUM);
        ExecutorService executor = GlobalIndexReadThreadPool.getExecutorService(parallelism);

        List<CompletableFuture<Optional<ScoredGlobalIndexResult>>> futures =
                new ArrayList<>(splits.size());
        for (VectorSearchSplit split : splits) {
            futures.add(
                    eval(
                            globalIndexer,
                            indexPathFactory,
                            split.rowRangeStart(),
                            split.rowRangeEnd(),
                            split.vectorIndexFiles(),
                            preFilter,
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

        return result.topK(limit);
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
            @Nullable RoaringNavigableMap64 includeRowIds,
            ExecutorService executor) {
        List<GlobalIndexIOMeta> indexIOMetaList = new ArrayList<>();
        for (IndexFileMeta indexFile : vectorIndexFiles) {
            GlobalIndexMeta meta = checkNotNull(indexFile.globalIndexMeta());
            indexIOMetaList.add(
                    new GlobalIndexIOMeta(
                            indexPathFactory.toPath(indexFile),
                            indexFile.fileSize(),
                            meta.indexMeta()));
        }
        @SuppressWarnings("resource")
        FileIO fileIO = table.fileIO();
        GlobalIndexFileReader indexFileReader = m -> fileIO.newInputStream(m.filePath());
        GlobalIndexReader reader =
                globalIndexer.createReader(indexFileReader, indexIOMetaList, executor);
        VectorSearch vectorSearch =
                new VectorSearch(vector, limit, vectorColumn.name())
                        .withIncludeRowIds(includeRowIds);
        return new OffsetGlobalIndexReader(reader, rowRangeStart, rowRangeEnd)
                .visitVectorSearch(vectorSearch)
                .whenComplete((r, t) -> IOUtils.closeQuietly(reader));
    }
}
