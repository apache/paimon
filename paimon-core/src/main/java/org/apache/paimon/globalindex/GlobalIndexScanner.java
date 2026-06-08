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

package org.apache.paimon.globalindex;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Range;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.GLOBAL_INDEX_THREAD_NUM;
import static org.apache.paimon.predicate.PredicateVisitor.collectFieldNames;
import static org.apache.paimon.table.source.snapshot.TimeTravelUtil.tryTravelOrLatest;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Scanner for shard-based global indexes. */
public class GlobalIndexScanner implements Closeable {

    private final Options options;
    private final ExecutorService executor;
    private final GlobalIndexEvaluator globalIndexEvaluator;
    private final IndexPathFactory indexPathFactory;

    public GlobalIndexScanner(
            Options options,
            RowType rowType,
            FileIO fileIO,
            IndexPathFactory indexPathFactory,
            Collection<IndexFileMeta> indexFiles) {
        this.options = options;
        this.executor =
                GlobalIndexReadThreadPool.getExecutorService(options.get(GLOBAL_INDEX_THREAD_NUM));
        this.indexPathFactory = indexPathFactory;
        GlobalIndexFileReader indexFileReader = meta -> fileIO.newInputStream(meta.filePath());
        Map<Integer, IndexMetaFileGroup> indexMetas = new HashMap<>();
        Map<Integer, List<IndexMetaFileGroup>> extraIndexMetas = new HashMap<>();
        for (IndexFileMeta indexFile : indexFiles) {
            GlobalIndexMeta meta = checkNotNull(indexFile.globalIndexMeta());
            String indexType = indexFile.indexType();
            Range range = new Range(meta.rowRangeStart(), meta.rowRangeEnd());
            int indexFieldId = meta.indexFieldId();
            List<Integer> fieldIds = meta.getIndexedFieldIds();
            IndexMetaFileGroup group = indexMetas.get(indexFieldId);
            if (group == null) {
                group = new IndexMetaFileGroup(indexFieldId, fieldIds);
                indexMetas.put(indexFieldId, group);
                if (meta.extraFieldIds() != null) {
                    for (int extra : meta.extraFieldIds()) {
                        extraIndexMetas.computeIfAbsent(extra, k -> new ArrayList<>()).add(group);
                    }
                }
            } else {
                checkArgument(
                        group.fieldIds.equals(fieldIds),
                        "Primary field %s owns multiple indexes with different columns %s and %s; "
                                + "a primary column can own at most one index.",
                        indexFieldId,
                        group.fieldIds,
                        fieldIds);
            }
            group.addFile(indexType, range, indexFile);
        }

        IntFunction<Collection<GlobalIndexReader>> readersFunction =
                fId -> {
                    IndexMetaFileGroup group = indexMetas.get(fId);
                    if (group != null) {
                        List<DataField> fields =
                                group.fieldIds.stream()
                                        .map(rowType::getField)
                                        .collect(Collectors.toList());
                        return createReaders(indexFileReader, group.metas, fields);
                    }
                    List<IndexMetaFileGroup> extraGroups = extraIndexMetas.get(fId);
                    if (extraGroups == null || extraGroups.isEmpty()) {
                        return Collections.emptyList();
                    }
                    // Union readers from all groups that share this extra column
                    List<GlobalIndexReader> allReaders = new ArrayList<>();
                    for (IndexMetaFileGroup g : extraGroups) {
                        List<DataField> fields =
                                g.fieldIds.stream()
                                        .map(rowType::getField)
                                        .collect(Collectors.toList());
                        allReaders.addAll(createReaders(indexFileReader, g.metas, fields));
                    }
                    return allReaders;
                };
        this.globalIndexEvaluator = new GlobalIndexEvaluator(rowType, readersFunction);
    }

    /** All index files of one global index (single- or multi-column), grouped for reading. */
    private static class IndexMetaFileGroup {

        private final int indexFieldId;
        private final List<Integer> fieldIds;
        private final Map<String, Map<Range, List<IndexFileMeta>>> metas = new HashMap<>();

        IndexMetaFileGroup(int indexFieldId, List<Integer> fieldIds) {
            this.indexFieldId = indexFieldId;
            this.fieldIds = fieldIds;
        }

        void addFile(String indexType, Range range, IndexFileMeta indexFile) {
            metas.computeIfAbsent(indexType, k -> new HashMap<>())
                    .computeIfAbsent(range, k -> new ArrayList<>())
                    .add(indexFile);
        }
    }

    public static Optional<GlobalIndexScanner> create(
            FileStoreTable table, Collection<IndexFileMeta> indexFiles) {
        if (indexFiles.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(
                new GlobalIndexScanner(
                        table.coreOptions().toConfiguration(),
                        table.rowType(),
                        table.fileIO(),
                        table.store().pathFactory().globalIndexFileFactory(),
                        indexFiles));
    }

    public static Optional<GlobalIndexScanner> create(
            FileStoreTable table, PartitionPredicate partitionFilter, Predicate filter) {
        Set<Integer> filterFieldIds =
                collectFieldNames(filter).stream()
                        .filter(name -> table.rowType().containsField(name))
                        .map(name -> table.rowType().getField(name).id())
                        .collect(Collectors.toSet());
        Filter<IndexManifestEntry> indexFileFilter =
                entry -> {
                    if (partitionFilter != null && !partitionFilter.test(entry.partition())) {
                        return false;
                    }
                    GlobalIndexMeta globalIndex = entry.indexFile().globalIndexMeta();
                    if (globalIndex == null) {
                        return false;
                    }
                    // Collect indexes whose primary column is filtered, and also multi-column
                    // indexes that have a filtered column as an extra (used as a fallback).
                    if (filterFieldIds.contains(globalIndex.indexFieldId())) {
                        return true;
                    }
                    if (globalIndex.extraFieldIds() != null) {
                        for (int id : globalIndex.extraFieldIds()) {
                            if (filterFieldIds.contains(id)) {
                                return true;
                            }
                        }
                    }
                    return false;
                };

        List<IndexFileMeta> indexFiles =
                table.store().newIndexFileHandler().scan(tryTravelOrLatest(table), indexFileFilter)
                        .stream()
                        .map(IndexManifestEntry::indexFile)
                        .collect(Collectors.toList());
        return create(table, indexFiles);
    }

    public Optional<GlobalIndexResult> scan(Predicate predicate) {
        return globalIndexEvaluator.evaluate(predicate);
    }

    private Collection<GlobalIndexReader> createReaders(
            GlobalIndexFileReader indexFileReadWrite,
            Map<String, Map<Range, List<IndexFileMeta>>> indexMetas,
            List<DataField> fields) {
        if (indexMetas == null) {
            return Collections.emptyList();
        }

        Set<GlobalIndexReader> readers = new HashSet<>();
        for (Map.Entry<String, Map<Range, List<IndexFileMeta>>> entry : indexMetas.entrySet()) {
            String indexType = entry.getKey();
            Map<Range, List<IndexFileMeta>> metas = entry.getValue();
            GlobalIndexerFactory globalIndexerFactory = GlobalIndexerFactoryUtils.load(indexType);
            GlobalIndexer globalIndexer =
                    globalIndexerFactory.create(
                            fields.get(0), fields.subList(1, fields.size()), options);

            List<CompletableFuture<GlobalIndexReader>> futures = new ArrayList<>(metas.size());
            for (Map.Entry<Range, List<IndexFileMeta>> rangeMetas : metas.entrySet()) {
                Range range = rangeMetas.getKey();
                List<IndexFileMeta> indexFileMetas = rangeMetas.getValue();
                List<GlobalIndexIOMeta> globalMetas =
                        indexFileMetas.stream()
                                .map(this::toGlobalMeta)
                                .collect(Collectors.toList());
                futures.add(
                        CompletableFuture.supplyAsync(
                                () ->
                                        new OffsetGlobalIndexReader(
                                                globalIndexer.createReader(
                                                        indexFileReadWrite, globalMetas, executor),
                                                range.from,
                                                range.to),
                                executor));
            }

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            List<GlobalIndexReader> unionReader = new ArrayList<>(futures.size());
            for (CompletableFuture<GlobalIndexReader> future : futures) {
                unionReader.add(future.join());
            }
            readers.add(new UnionGlobalIndexReader(unionReader));
        }

        return readers;
    }

    private GlobalIndexIOMeta toGlobalMeta(IndexFileMeta meta) {
        GlobalIndexMeta globalIndex = meta.globalIndexMeta();
        checkNotNull(globalIndex);
        Path filePath = indexPathFactory.toPath(meta);
        return new GlobalIndexIOMeta(filePath, meta.fileSize(), globalIndex.indexMeta());
    }

    @Override
    public void close() throws IOException {
        globalIndexEvaluator.close();
    }
}
