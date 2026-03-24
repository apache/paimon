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
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.GlobalIndexerFactory;
import org.apache.paimon.globalindex.GlobalIndexerFactoryUtils;
import org.apache.paimon.globalindex.OffsetGlobalIndexReader;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.InnerTable;
import org.apache.paimon.table.source.snapshot.TimeTravelUtil;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.RoaringNavigableMap64;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.apache.paimon.partition.PartitionPredicate.splitPartitionPredicate;
import static org.apache.paimon.utils.ManifestReadThreadPool.randomlyExecuteSequentialReturn;

/** Implementation for {@link VectorSearchBuilder}. */
public class VectorSearchBuilderImpl implements VectorSearchBuilder {

    private static final long serialVersionUID = 1L;

    private final FileStoreTable table;

    private PartitionPredicate partitionFilter;
    private Predicate filter;
    private int limit;
    private DataField vectorColumn;
    private float[] vector;

    public VectorSearchBuilderImpl(InnerTable table) {
        this.table = (FileStoreTable) table;
    }

    @Override
    public VectorSearchBuilder withPartitionFilter(PartitionPredicate partitionFilter) {
        this.partitionFilter = partitionFilter;
        return this;
    }

    @Override
    public VectorSearchBuilder withFilter(Predicate predicate) {
        if (this.filter == null) {
            this.filter = predicate;
        } else {
            this.filter = PredicateBuilder.and(this.filter, predicate);
        }
        splitPartitionPredicate(predicate, table.rowType(), table.partitionKeys())
                .ifPresent(value -> this.partitionFilter = value);
        return this;
    }

    @Override
    public VectorSearchBuilder withLimit(int limit) {
        this.limit = limit;
        return this;
    }

    @Override
    public VectorSearchBuilder withVectorColumn(String name) {
        this.vectorColumn = table.rowType().getField(name);
        return this;
    }

    @Override
    public VectorSearchBuilder withVector(float[] vector) {
        this.vector = vector;
        return this;
    }

    @Override
    public VectorScan newVectorScan() {
        return new VectorScanImpl();
    }

    @Override
    public VectorRead newVectorRead() {
        return new VectorReadImpl();
    }

    private class VectorScanImpl implements VectorScan {

        @Override
        public Plan scan() {
            Objects.requireNonNull(vector, "Vector must be set");
            Objects.requireNonNull(vectorColumn, "Vector column must be set");
            if (limit <= 0) {
                throw new IllegalArgumentException("Limit must be positive");
            }

            Snapshot snapshot = TimeTravelUtil.tryTravelOrLatest(table);
            IndexFileHandler indexFileHandler = table.store().newIndexFileHandler();
            Filter<IndexManifestEntry> indexFileFilter =
                    partitionFilter == null
                            ? Filter.alwaysTrue()
                            : entry -> partitionFilter.test(entry.partition());
            List<IndexManifestEntry> indexManifestEntries =
                    indexFileHandler.scan(snapshot, indexFileFilter);

            List<IndexFileMeta> vectorIndexFiles =
                    indexManifestEntries.stream()
                            .map(IndexManifestEntry::indexFile)
                            .filter(
                                    indexFile -> {
                                        GlobalIndexMeta indexMeta = indexFile.globalIndexMeta();
                                        if (indexMeta == null) {
                                            return false;
                                        }
                                        return indexMeta.indexFieldId() == vectorColumn.id();
                                    })
                            .collect(Collectors.toList());

            return new Plan() {
                @Override
                public List<IndexFileMeta> indexFiles() {
                    return vectorIndexFiles;
                }

                @Override
                public @Nullable RoaringNavigableMap64 includeRowIds() {
                    // TODO pre filter by btree index
                    return null;
                }
            };
        }
    }

    private class VectorReadImpl implements VectorRead {

        @Override
        public GlobalIndexResult read(VectorScan.Plan plan) {
            List<IndexFileMeta> indexFiles = plan.indexFiles();
            if (indexFiles.isEmpty()) {
                return GlobalIndexResult.createEmpty();
            }

            Integer threadNum = table.coreOptions().globalIndexThreadNum();
            Iterator<Optional<ScoredGlobalIndexResult>> resultIterators =
                    randomlyExecuteSequentialReturn(
                            vectorIndex -> singletonList(eval(vectorIndex, plan.includeRowIds())),
                            indexFiles,
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

        private Optional<ScoredGlobalIndexResult> eval(
                IndexFileMeta indexFile, @Nullable RoaringNavigableMap64 includeRowIds) {
            GlobalIndexerFactory globalIndexerFactory =
                    GlobalIndexerFactoryUtils.load(indexFile.indexType());
            GlobalIndexer globalIndexer =
                    globalIndexerFactory.create(
                            vectorColumn, table.coreOptions().toConfiguration());
            GlobalIndexMeta meta = indexFile.globalIndexMeta();
            Preconditions.checkNotNull(meta);
            Path filePath = table.store().pathFactory().globalIndexFileFactory().toPath(indexFile);
            GlobalIndexIOMeta globalIndexIOMeta =
                    new GlobalIndexIOMeta(filePath, indexFile.fileSize(), meta.indexMeta());
            @SuppressWarnings("resource")
            FileIO fileIO = table.fileIO();
            GlobalIndexFileReader indexFileReader = m -> fileIO.newInputStream(m.filePath());
            try (GlobalIndexReader reader =
                    globalIndexer.createReader(indexFileReader, singletonList(globalIndexIOMeta))) {
                VectorSearch vectorSearch =
                        new VectorSearch(vector, limit, vectorColumn.name())
                                .withIncludeRowIds(includeRowIds);
                return new OffsetGlobalIndexReader(reader, meta.rowRangeStart(), meta.rowRangeEnd())
                        .visitVectorSearch(vectorSearch);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
