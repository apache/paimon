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
import org.apache.paimon.globalindex.btree.BTreeGlobalIndexerFactory;
import org.apache.paimon.globalindex.btree.BTreeWithFileMetaBuilder;
import org.apache.paimon.globalindex.btree.BTreeWithFileMetaReader;
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
import org.apache.paimon.utils.ManifestReadThreadPool;
import org.apache.paimon.utils.Range;

import javax.annotation.Nullable;

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
import java.util.concurrent.ExecutorService;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.GLOBAL_INDEX_THREAD_NUM;
import static org.apache.paimon.predicate.PredicateVisitor.collectFieldNames;
import static org.apache.paimon.table.source.snapshot.TimeTravelUtil.tryTravelOrLatest;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Scanner for shard-based global indexes. */
public class GlobalIndexScanner implements Closeable {

    private final Options options;
    private final ExecutorService executor;
    private final GlobalIndexEvaluator globalIndexEvaluator;
    private final IndexPathFactory indexPathFactory;

    // Per-field map of BTreeWithFileMetaReader instances, keyed by field ID.
    // Non-null only when btree (with companion btree_file_meta) index files are present.
    @Nullable private final Map<Integer, BTreeWithFileMetaReader> filePathIndexReaders;

    public GlobalIndexScanner(
            Options options,
            RowType rowType,
            FileIO fileIO,
            IndexPathFactory indexPathFactory,
            Collection<IndexFileMeta> indexFiles) {
        this.options = options;
        this.executor =
                ManifestReadThreadPool.getExecutorService(options.get(GLOBAL_INDEX_THREAD_NUM));
        this.indexPathFactory = indexPathFactory;
        GlobalIndexFileReader indexFileReader = meta -> fileIO.newInputStream(meta.filePath());

        // Step 1: collect (fieldId, range) pairs that have a btree_file_meta companion
        Map<Integer, Set<Long>> binaryFileMetaKeyMap = new HashMap<>();
        for (IndexFileMeta indexFile : indexFiles) {
            if (BTreeWithFileMetaBuilder.INDEX_TYPE_FILE_META.equals(indexFile.indexType())) {
                GlobalIndexMeta meta = checkNotNull(indexFile.globalIndexMeta());
                binaryFileMetaKeyMap
                        .computeIfAbsent(meta.indexFieldId(), k -> new HashSet<>())
                        .add(meta.rowRangeStart());
            }
        }

        // Step 2: classify index files
        // fieldId -> indexType -> range -> List<IndexFileMeta>
        Map<Integer, Map<String, Map<Range, List<IndexFileMeta>>>> indexMetas = new HashMap<>();
        // fieldId -> (keyIndexByRange, binaryFileMetaByRange)
        Map<Integer, Map<Range, List<GlobalIndexIOMeta>>> keyIndexByField = new HashMap<>();
        Map<Integer, Map<Range, List<GlobalIndexIOMeta>>> binaryFileMetaByField = new HashMap<>();
        Map<Integer, DataField> fieldById = new HashMap<>();

        for (IndexFileMeta indexFile : indexFiles) {
            GlobalIndexMeta meta = checkNotNull(indexFile.globalIndexMeta());
            int fieldId = meta.indexFieldId();
            String indexType = indexFile.indexType();
            Range range = new Range(meta.rowRangeStart(), meta.rowRangeEnd());

            if (BTreeWithFileMetaBuilder.INDEX_TYPE_FILE_META.equals(indexType)) {
                binaryFileMetaByField
                        .computeIfAbsent(fieldId, k -> new HashMap<>())
                        .computeIfAbsent(range, k -> new ArrayList<>())
                        .add(toGlobalMeta(indexFile));
            } else if (BTreeGlobalIndexerFactory.IDENTIFIER.equals(indexType)
                    && binaryFileMetaKeyMap
                            .getOrDefault(fieldId, Collections.emptySet())
                            .contains(meta.rowRangeStart())) {
                keyIndexByField
                        .computeIfAbsent(fieldId, k -> new HashMap<>())
                        .computeIfAbsent(range, k -> new ArrayList<>())
                        .add(toGlobalMeta(indexFile));
                fieldById.put(fieldId, rowType.getField(fieldId));
            } else {
                indexMetas
                        .computeIfAbsent(fieldId, k -> new HashMap<>())
                        .computeIfAbsent(indexType, k -> new HashMap<>())
                        .computeIfAbsent(range, k -> new ArrayList<>())
                        .add(indexFile);
            }
        }

        // Build BTreeWithFileMetaReader for fields that have btree (with btree_file_meta) files
        if (!keyIndexByField.isEmpty()) {
            this.filePathIndexReaders = new HashMap<>();
            for (Map.Entry<Integer, Map<Range, List<GlobalIndexIOMeta>>> entry :
                    keyIndexByField.entrySet()) {
                int fieldId = entry.getKey();
                DataField dataField = fieldById.get(fieldId);
                Map<Range, List<GlobalIndexIOMeta>> binaryFileMetaForField =
                        binaryFileMetaByField.getOrDefault(fieldId, Collections.emptyMap());
                filePathIndexReaders.put(
                        fieldId,
                        new BTreeWithFileMetaReader(
                                entry.getValue(),
                                binaryFileMetaForField,
                                dataField,
                                options,
                                indexFileReader,
                                executor));
            }
        } else {
            this.filePathIndexReaders = null;
        }

        IntFunction<Collection<GlobalIndexReader>> readersFunction =
                fieldId ->
                        createReaders(
                                indexFileReader,
                                indexMetas.get(fieldId),
                                rowType.getField(fieldId));
        this.globalIndexEvaluator = new GlobalIndexEvaluator(rowType, readersFunction);
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
                    return filterFieldIds.contains(globalIndex.indexFieldId());
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

    /**
     * Evaluates the predicate using btree/btree_file_meta index files and returns a {@link
     * FilePathGlobalIndexResult} when available. Returns {@link Optional#empty()} if no FilePath
     * Global Index files are present for the predicate fields.
     */
    public Optional<FilePathGlobalIndexResult> scanWithFilePath(Predicate predicate)
            throws IOException {
        if (filePathIndexReaders == null || filePathIndexReaders.isEmpty()) {
            return Optional.empty();
        }
        // For simplicity, we support one field with file-path index at a time.
        // In the future this could be extended to multi-field AND/OR combinations.
        for (BTreeWithFileMetaReader reader : filePathIndexReaders.values()) {
            Optional<FilePathGlobalIndexResult> result = reader.scan(predicate);
            if (result.isPresent()) {
                return result;
            }
        }
        return Optional.empty();
    }

    /**
     * Returns true if this scanner has any btree/btree_file_meta index files, i.e., whether {@link
     * #scanWithFilePath(Predicate)} may return a non-empty result.
     */
    public boolean hasFilePathIndex() {
        return filePathIndexReaders != null && !filePathIndexReaders.isEmpty();
    }

    private Collection<GlobalIndexReader> createReaders(
            GlobalIndexFileReader indexFileReadWrite,
            Map<String, Map<Range, List<IndexFileMeta>>> indexMetas,
            DataField dataField) {
        if (indexMetas == null) {
            return Collections.emptyList();
        }

        Set<GlobalIndexReader> readers = new HashSet<>();
        try {
            for (Map.Entry<String, Map<Range, List<IndexFileMeta>>> entry : indexMetas.entrySet()) {
                String indexType = entry.getKey();
                Map<Range, List<IndexFileMeta>> metas = entry.getValue();
                GlobalIndexerFactory globalIndexerFactory =
                        GlobalIndexerFactoryUtils.load(indexType);
                GlobalIndexer globalIndexer = globalIndexerFactory.create(dataField, options);

                List<GlobalIndexReader> unionReader = new ArrayList<>();
                for (Map.Entry<Range, List<IndexFileMeta>> rangeMetas : metas.entrySet()) {
                    Range range = rangeMetas.getKey();
                    List<IndexFileMeta> indexFileMetas = rangeMetas.getValue();

                    List<GlobalIndexIOMeta> globalMetas =
                            indexFileMetas.stream()
                                    .map(this::toGlobalMeta)
                                    .collect(Collectors.toList());
                    GlobalIndexReader innerReader =
                            new OffsetGlobalIndexReader(
                                    globalIndexer.createReader(indexFileReadWrite, globalMetas),
                                    range.from,
                                    range.to);
                    unionReader.add(innerReader);
                }

                readers.add(new UnionGlobalIndexReader(unionReader, executor));
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to create global index reader", e);
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
