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
import org.apache.paimon.utils.RoaringNavigableMap64;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.apache.paimon.utils.ManifestReadThreadPool.randomlyExecuteSequentialReturn;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Implementation for {@link VectorRead}. */
public class VectorReadImpl implements VectorRead {

    private final FileStoreTable table;
    private final Predicate filter;
    private final int limit;
    private final DataField vectorColumn;
    private final float[] vector;

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
        Integer threadNum = table.coreOptions().globalIndexThreadNum();

        String indexType = splits.get(0).vectorIndexFiles().get(0).indexType();
        GlobalIndexer globalIndexer =
                GlobalIndexerFactoryUtils.load(indexType)
                        .create(vectorColumn, table.coreOptions().toConfiguration());
        IndexPathFactory indexPathFactory = table.store().pathFactory().globalIndexFileFactory();
        Iterator<Optional<ScoredGlobalIndexResult>> resultIterators =
                randomlyExecuteSequentialReturn(
                        split ->
                                singletonList(
                                        eval(
                                                globalIndexer,
                                                indexPathFactory,
                                                split.rowRangeStart(),
                                                split.rowRangeEnd(),
                                                split.vectorIndexFiles(),
                                                preFilter)),
                        splits,
                        threadNum);

        ScoredGlobalIndexResult result = ScoredGlobalIndexResult.createEmpty();
        while (resultIterators.hasNext()) {
            Optional<ScoredGlobalIndexResult> next = resultIterators.next();
            if (next.isPresent()) {
                result = result.or(next.get());
            }
        }

        return result.topK(limit);
    }

    private Optional<RoaringNavigableMap64> preFilter(List<VectorSearchSplit> splits) {
        Set<IndexFileMeta> scalarIndexFiles =
                splits.stream()
                        .flatMap(split -> split.scalarIndexFiles().stream())
                        .collect(Collectors.toSet());
        if (scalarIndexFiles.isEmpty()) {
            return Optional.empty();
        }

        try (GlobalIndexScanner scanner = GlobalIndexScanner.create(table, scalarIndexFiles)) {
            return scanner.scan(filter).map(GlobalIndexResult::results);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Optional<ScoredGlobalIndexResult> eval(
            GlobalIndexer globalIndexer,
            IndexPathFactory indexPathFactory,
            long rowRangeStart,
            long rowRangeEnd,
            List<IndexFileMeta> vectorIndexFiles,
            @Nullable RoaringNavigableMap64 includeRowIds) {
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
        try (GlobalIndexReader reader =
                globalIndexer.createReader(indexFileReader, indexIOMetaList)) {
            VectorSearch vectorSearch =
                    new VectorSearch(vector, limit, vectorColumn.name())
                            .withIncludeRowIds(includeRowIds);
            return new OffsetGlobalIndexReader(reader, rowRangeStart, rowRangeEnd)
                    .visitVectorSearch(vectorSearch);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
